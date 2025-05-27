import { Readable, Transform } from 'stream';
import { parse } from 'csv-parse';
import { pipeline } from 'stream/promises';
import { PrismaClient, Prisma } from '@prisma/client';
import logger from './logger';
import config from '@/config';
import { parseProductionCompanies, parseCastNames } from './parser';

// 为了类型安全，定义Prisma事件类型
type PrismaEventTypes = 'query' | 'error' | 'info' | 'warn';

/**
 * CSV流处理器 - 用于处理大型CSV文件上传并实时写入数据库
 * 实现了流式处理，避免将整个文件加载到内存中
 */
export class StreamProcessor {
    private prisma: PrismaClient;
    private batchSize: number;
    private processedCount: number = 0;
    private errorRows: { row: number, error: string }[] = [];
    private startTime: number;
    private currentBatch: any[] = [];
    private isProcessing: boolean = false;
    private columnMappings: Record<string, string> = {};
    private keepAliveInterval: NodeJS.Timeout | null = null;
    private processingActive: boolean = false;

    constructor() {
        this.startTime = Date.now();
        this.batchSize = config.csv.batchSize || 1000;
        this.prisma = new PrismaClient({
            log: [
                { level: 'query', emit: 'event' },
                { level: 'error', emit: 'event' }
            ]
        });

        // 修复Prisma事件类型
        (this.prisma.$on as any)('query', (e: Prisma.QueryEvent) => {
            logger.debug(`Query: ${e.query} | Params: ${e.params}`);
        });

        (this.prisma.$on as any)('error', (e: Prisma.LogEvent) => {
            logger.error(`Prisma Error: ${e.message}`);
        });
    }

    /**
     * 处理CSV文件流
     * @param fileStream 文件流
     * @returns 处理结果
     */
    public async processStream(fileStream: Readable): Promise<{
        success: boolean;
        count: number;
        errorCount: number;
        errors: { row: number, error: string }[];
        processingTime: number;
    }> {
        this.processingActive = true;

        try {
            logger.info('开始处理CSV流...');

            // 设置数据库连接保活
            this.startKeepAlive();

            // 添加上传进度监控
            let totalBytes = 0;
            const fileWithProgress = new Transform({
                objectMode: false,
                transform: (chunk, encoding, callback) => {
                    totalBytes += chunk.length;
                    if (totalBytes % (10 * 1024 * 1024) === 0) { // 每10MB记录一次
                        logger.info(`已接收 ${Math.round(totalBytes / 1024 / 1024)}MB 数据`);
                    }
                    callback(null, chunk);
                }
            });

            // 处理流错误
            fileStream.on('error', (err) => {
                logger.error(`文件流错误: ${err.message}`);
            });

            // 创建CSV解析流水线
            try {
                await pipeline(
                    fileStream,
                    fileWithProgress,
                    this.createCSVParser(),
                    this.createTransformer()
                );
            } catch (pipelineError: any) {
                // 如果是流提前关闭的错误，但我们已经处理了一些数据，尝试优雅地继续处理
                if (pipelineError.code === 'ERR_STREAM_PREMATURE_CLOSE' && this.processedCount > 0) {
                    logger.warn(`流处理中断，但已处理 ${this.processedCount} 条记录，尝试完成剩余批次处理`);
                } else {
                    throw pipelineError;
                }
            }

            // 处理最后一批数据
            if (this.currentBatch.length > 0) {
                await this.processBatch();
            }

            // 确保所有数据都被处理
            await this.waitForProcessing();

            logger.info(`CSV流处理完成，共处理 ${this.processedCount} 条记录，耗时 ${Date.now() - this.startTime}ms`);

            const result = {
                success: true,
                count: this.processedCount,
                errorCount: this.errorRows.length,
                errors: this.errorRows.slice(0, 10), // 只返回前10个错误
                processingTime: Date.now() - this.startTime
            };

            this.stopKeepAlive();
            this.processingActive = false;

            // 关闭数据库连接
            await this.prisma.$disconnect();
            logger.info('数据库连接已关闭');

            return result;
        } catch (error: unknown) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            logger.error(`CSV流处理失败: ${errorMsg}`, error);

            this.stopKeepAlive();
            this.processingActive = false;

            try {
                await this.prisma.$disconnect();
            } catch (disconnectError) {
                logger.error('断开数据库连接时出错', disconnectError);
            }

            throw error;
        }
    }

    /**
     * 启动数据库连接保活机制
     */
    private startKeepAlive() {
        // 每30秒执行一次简单查询以保持连接活跃
        this.keepAliveInterval = setInterval(async () => {
            if (!this.processingActive) {
                this.stopKeepAlive();
                return;
            }

            try {
                await this.prisma.$queryRaw`SELECT 1`;
                logger.debug('数据库连接保活查询成功');
            } catch (error) {
                logger.warn('数据库连接保活查询失败，尝试重新连接', error);
                // 尝试重新连接
                try {
                    await this.prisma.$disconnect();
                    // 重新创建Prisma客户端
                    this.prisma = new PrismaClient();
                } catch (reconnectError) {
                    logger.error('数据库重连失败', reconnectError);
                }
            }
        }, 30000); // 30秒一次
    }

    /**
     * 停止数据库连接保活
     */
    private stopKeepAlive() {
        if (this.keepAliveInterval) {
            clearInterval(this.keepAliveInterval);
            this.keepAliveInterval = null;
        }
    }

    /**
     * 创建CSV解析器
     */
    private createCSVParser() {
        return parse({
            columns: (headers: string[]) => {
                logger.info(`CSV标题: ${headers.join(', ')}`);

                // 存储列映射关系，以便后续处理
                this.columnMappings = {};
                headers.forEach((header: string) => {
                    // 将列名标准化（去除引号、空格等）
                    const normalizedHeader = header.replace(/^["']|["']$/g, '').trim();
                    this.columnMappings[normalizedHeader] = normalizedHeader;
                });

                return headers;
            },
            trim: true,
            skip_empty_lines: true,
            relax_quotes: true, // 放宽引号要求，提高对格式不规范CSV的兼容性
            escape: '\\', // 设置转义字符
            cast: (value, context) => {
                if (context.header) return value;

                // 尝试解析日期
                if (context.column === 'release_date') {
                    if (!value) return null;
                    try {
                        return new Date(value);
                    } catch (e) {
                        logger.warn(`无法解析日期: ${value}`);
                        return null;
                    }
                }

                return value;
            },
            skip_records_with_error: true // 跳过有错误的记录而不是中断整个处理
        });
    }

    /**
     * 创建数据转换器
     */
    private createTransformer() {
        let rowNumber = 0;
        let lastLogTime = Date.now();
        const LOG_INTERVAL = 5000; // 每5秒记录一次处理进度

        return new Transform({
            objectMode: true,
            transform: async (row, _, callback) => {
                rowNumber++;
                try {
                    // 定期记录处理进度
                    const now = Date.now();
                    if (now - lastLogTime > LOG_INTERVAL) {
                        logger.info(`正在处理行 ${rowNumber}，已完成 ${this.processedCount} 条记录`);
                        lastLogTime = now;
                    }

                    // 记录原始数据用于调试
                    if (rowNumber === 1) {
                        logger.debug(`第一行数据样本: ${JSON.stringify(row)}`);
                    }

                    if (!row.id || !row.title) {
                        this.errorRows.push({
                            row: rowNumber,
                            error: '缺少必填字段 ID 或 title'
                        });
                        return callback();
                    }

                    // 转换行数据
                    const transformed = this.transformRow(row);
                    this.currentBatch.push(transformed);

                    // 达到批处理大小，处理当前批次
                    if (this.currentBatch.length >= this.batchSize) {
                        // 不等待批处理完成，允许继续处理下一批数据
                        this.processBatch().catch(error => {
                            const errorMsg = error instanceof Error ? error.message : String(error);
                            logger.error(`批处理异步处理失败: ${errorMsg}`);
                        });
                    }

                    callback();
                } catch (error: unknown) {
                    const errorMsg = error instanceof Error ? error.message : String(error);
                    this.errorRows.push({
                        row: rowNumber,
                        error: errorMsg
                    });
                    logger.error(`数据转换失败 [行 ${rowNumber}]: ${errorMsg}`);
                    callback();
                }
            }
        });
    }

    /**
     * 处理一批数据
     */
    private async processBatch(): Promise<void> {
        // 如果已经在处理中，等待
        while (this.isProcessing) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        this.isProcessing = true;
        const batchToProcess = [...this.currentBatch];
        this.currentBatch = []; // 清空当前批次，准备接收新数据

        if (batchToProcess.length === 0) {
            this.isProcessing = false;
            return;
        }

        try {
            const start = Date.now();

            // 在提交前，将BigInt转换为数字
            const processedBatch = batchToProcess.map(item => {
                const processedItem = { ...item };

                // 处理BigInt序列化问题
                if (typeof processedItem.revenue === 'bigint') {
                    processedItem.revenue = Number(processedItem.revenue);
                }

                if (typeof processedItem.budget === 'bigint') {
                    processedItem.budget = Number(processedItem.budget);
                }

                return processedItem;
            });

            let insertedCount = 0;
            const MAX_RETRIES = 3;

            // 逐个处理记录，使用upsert而不是事务
            for (const item of processedBatch) {
                let retries = 0;
                let success = false;

                while (!success && retries < MAX_RETRIES) {
                    try {
                        // 使用upsert操作 - 如果存在则更新，不存在则创建
                        await this.prisma.movie.upsert({
                            where: { id: item.id },
                            update: item,
                            create: item
                        });

                        insertedCount++;
                        success = true;

                        if (insertedCount % 10 === 0) {
                            logger.debug(`已处理 ${insertedCount}/${processedBatch.length} 条记录`);
                        }
                    } catch (error: any) {
                        retries++;
                        const errorMsg = error instanceof Error ? error.message : String(error);

                        // 如果是最后一次重试，记录错误
                        if (retries >= MAX_RETRIES) {
                            logger.error(`处理记录ID ${item.id} 失败(已重试${retries}次): ${errorMsg}`);
                            // 记录错误但继续处理下一条
                            this.errorRows.push({
                                row: -1,
                                error: `处理记录ID ${item.id} 失败: ${errorMsg}`
                            });
                        } else {
                            logger.warn(`处理记录ID ${item.id} 失败，准备重试(${retries}/${MAX_RETRIES}): ${errorMsg}`);
                            // 短暂等待后重试
                            await new Promise(resolve => setTimeout(resolve, 500 * retries));
                        }
                    }
                }
            }

            // 更新处理计数
            this.processedCount += insertedCount;

            logger.info(`批处理完成: 处理了 ${batchToProcess.length} 条记录，成功插入/更新 ${insertedCount} 条，累计 ${this.processedCount} 条，耗时 ${Date.now() - start}ms`);

            if (insertedCount === 0 && batchToProcess.length > 0) {
                logger.warn('数据库未插入任何记录，可能是因为数据格式不匹配');
            }
        } catch (error: unknown) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            logger.error(`批量处理失败: ${errorMsg}`);
            // 记录错误但不中断处理
            this.errorRows.push({
                row: -1, // 不知道具体是哪一行出错
                error: `批量处理错误: ${errorMsg}`
            });
        } finally {
            this.isProcessing = false;
        }
    }

    /**
     * 等待所有处理完成
     */
    private async waitForProcessing(): Promise<void> {
        while (this.isProcessing || this.currentBatch.length > 0) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    /**
     * 转换行数据为数据库格式
     */
    private transformRow(row: any) {
        try {
            // 安全获取值的辅助函数
            const getValue = (key: string, defaultValue: any = null) => {
                return row[key] !== undefined ? row[key] : defaultValue;
            };

            // 转换字符串数组
            const parseStringArray = (value: string | null | undefined) => {
                if (!value) return [];
                try {
                    // 如果已经是JSON字符串，尝试解析
                    if (value.startsWith('[') && value.endsWith(']')) {
                        return JSON.parse(value);
                    }
                    // 否则按逗号分隔
                    return value.split(/,\s*/).filter(Boolean);
                } catch (e) {
                    logger.warn(`解析字符串数组失败: ${value}`);
                    return value.split(/,\s*/).filter(Boolean);
                }
            };

            // 转换为数字
            const parseNumber = (value: string | null | undefined, defaultValue: number | undefined = 0) => {
                if (value === null || value === undefined || value === '') return defaultValue;
                const num = parseFloat(value);
                return isNaN(num) ? defaultValue : num;
            };

            // 转换为整数
            const parseInt2 = (value: string | null | undefined, defaultValue: number | undefined = 0) => {
                if (value === null || value === undefined || value === '') return defaultValue;
                const num = parseInt(value, 10);
                return isNaN(num) ? defaultValue : num;
            };

            // 转换为数字而不是BigInt，避免序列化问题
            const parseMoneyValue = (value: string | null | undefined) => {
                if (value === null || value === undefined || value === '') return 0;
                try {
                    // 直接返回数字而不是BigInt
                    return parseFloat(value) || 0;
                } catch (e) {
                    return 0;
                }
            };

            // 尝试解析更复杂的JSON结构
            const parseComplexJson = (value: string | null | undefined, defaultValue: any = []) => {
                if (!value) return defaultValue;
                try {
                    // 如果已是JSON字符串，尝试解析
                    if (value.startsWith('[') && value.endsWith(']')) {
                        return JSON.parse(value);
                    }
                    // 首先检查是否有具体的解析函数
                    if (value.includes('{') && value.includes('}')) {
                        // 尝试使用通用结构解析
                        const cleanValue = value.replace(/\s+/g, ' ').trim();
                        return JSON.parse(cleanValue);
                    }
                    // 如果无法解析，使用通用分隔符
                    return defaultValue;
                } catch (e) {
                    logger.warn(`解析复杂JSON失败，使用专用解析函数: ${value}`);
                    return defaultValue;
                }
            };

            // 确保ID字段存在且处理正确
            const originalId = getValue('id');
            if (!originalId) {
                logger.warn('CSV行缺少ID字段，这可能导致数据导入问题');
            }

            // 记录ID处理
            logger.debug(`处理ID: ${originalId}, 类型: ${typeof originalId}`);

            return {
                id: String(originalId), // 确保ID被转换为字符串
                title: getValue('title'),
                vote_average: parseNumber(getValue('vote_average')),
                vote_count: parseInt2(getValue('vote_count')),
                status: getValue('status'),
                release_date: getValue('release_date') ? new Date(getValue('release_date')) : null,
                revenue: parseMoneyValue(getValue('revenue')),
                runtime: parseInt2(getValue('runtime'), undefined),
                budget: parseMoneyValue(getValue('budget')),
                imdb_id: getValue('imdb_id'),
                original_language: getValue('original_language'),
                original_title: getValue('original_title', getValue('title')),
                overview: getValue('overview'),
                popularity: parseNumber(getValue('popularity'), undefined),
                tagline: getValue('tagline'),
                genres: parseStringArray(getValue('genres')),
                production_companies: parseProductionCompanies(getValue('production_companies')),
                production_countries: parseStringArray(getValue('production_countries')),
                spoken_languages: parseStringArray(getValue('spoken_languages')),
                cast: parseCastNames(getValue('cast')),
                director: parseStringArray(getValue('director')),
                director_of_photography: parseStringArray(getValue('director_of_photography')),
                writers: parseStringArray(getValue('writers')),
                producers: parseStringArray(getValue('producers')),
                music_composer: parseStringArray(getValue('music_composer')),
                imdb_rating: parseNumber(getValue('imdb_rating'), undefined),
                imdb_votes: parseInt2(getValue('imdb_votes'), undefined),
                poster_path: getValue('poster_path')
            };
        } catch (error: unknown) {
            const errorMsg = error instanceof Error ? error.message : String(error);
            logger.error('行数据转换失败:', {
                error: errorMsg,
                row: JSON.stringify(row)
            });
            throw error;
        }
    }
} 