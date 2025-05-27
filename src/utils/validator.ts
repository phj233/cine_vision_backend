import { Readable } from 'stream';
import { parse } from 'csv-parse';
import logger from '@/utils/logger';
import { ValidationError } from './error-handler';

// 完整的必需CSV头列表（根据用户提供的列名）
const REQUIRED_HEADERS = [
    'id',
    'title',
    'vote_average',
    'vote_count',
    'status',
    'release_date',
    'revenue',
    'runtime',
    'budget',
    'imdb_id',
    'original_language',
    'original_title',
    'overview',
    'popularity',
    'tagline',
    'genres',
    'production_companies',
    'production_countries',
    'spoken_languages',
    'cast',
    'director',
    'director_of_photography',
    'writers',
    'producers',
    'music_composer',
    'imdb_rating',
    'imdb_votes',
    'poster_path'
];

// 验证CSV头完整性
export async function validateCsvHeaders(stream: Readable): Promise<string | null> {
    // 克隆流以避免消费原始流
    const chunks: Buffer[] = [];

    // 收集流中的数据，不会消费原始流
    stream.on('data', (chunk) => {
        chunks.push(Buffer.from(chunk));
    });

    // 等待流结束
    await new Promise<void>((resolve) => {
        stream.on('end', () => {
            resolve();
        });
    });

    // 如果没有数据，返回错误
    if (chunks.length === 0) {
        return 'CSV文件为空';
    }

    // 从收集的数据创建新流
    const bufferStream = new Readable();
    chunks.forEach(chunk => {
        bufferStream.push(chunk);
    });
    bufferStream.push(null); // 表示流结束

    return new Promise((resolve, reject) => {
        let headers: string[] = [];
        let isFirstLine = true;
        let dataRead = false;

        const parser = parse({
            trim: true,
            skip_empty_lines: true
        });

        parser.on('readable', () => {
            let record;
            while ((record = parser.read()) !== null) {
                if (isFirstLine) {
                    headers = record;
                    isFirstLine = false;
                    dataRead = true;
                    break;
                }
            }
        });

        parser.on('error', (error) => {
            logger.error('CSV解析错误:', error);
            reject(new ValidationError('CSV文件解析失败', error));
        });

        parser.on('end', () => {
            if (!dataRead || headers.length === 0) {
                resolve('CSV文件为空或格式不正确');
                return;
            }

            const missingHeaders = REQUIRED_HEADERS.filter(
                header => !headers.includes(header)
            );

            if (missingHeaders.length > 0) {
                resolve(`缺少必需的列: ${missingHeaders.join(', ')}`);
                return;
            }

            resolve(null);
        });

        bufferStream.pipe(parser);

        // 确保流正确结束
        bufferStream.on('error', (err) => {
            logger.error('缓冲流错误:', err);
            reject(new ValidationError('文件流处理失败', err));
        });
    });
}

// 增强型查询参数验证
export function validateMovieQueryParams(params: any): string[] {
    const errors = [];
    const validSortFields = [
        'release_date', 'vote_average', 'popularity',
        'runtime', 'revenue', 'budget', 'imdb_rating'
    ];

    // 分页验证
    if (params.page && (isNaN(params.page) || params.page < 1)) {
        errors.push('页码必须为大于0的整数');
    }

    if (params.pageSize && (isNaN(params.pageSize) || params.pageSize < 1 || params.pageSize > 100)) {
        errors.push('每页数量必须在1-100之间');
    }

    // 评分验证
    if (params.minRating && (isNaN(params.minRating) || params.minRating < 0 || params.minRating > 10)) {
        errors.push('最低评分必须在0-10之间');
    }

    // 年份验证
    if (params.year && (isNaN(params.year) || params.year < 1900 || params.year > new Date().getFullYear() + 5)) {
        errors.push(`年份必须在1900-${new Date().getFullYear() + 5}之间`);
    }

    // 排序字段验证
    if (params.sortBy && !validSortFields.includes(params.sortBy)) {
        errors.push(`无效的排序字段，可用字段: ${validSortFields.join(', ')}`);
    }

    // 类型验证（允许多选）
    if (params.genres) {
        const maxGenres = 5;
        const genres = params.genres.split(',');
        if (genres.length > maxGenres) {
            errors.push(`最多支持同时选择${maxGenres}种类型`);
        }
    }

    return errors;
}

// 辅助方法：验证CSV行数据
export function validateCsvRow(row: any): string[] {
    const errors = [];

    // ID验证
    if (!row.id || row.id.trim() === '') {
        errors.push('ID不能为空');
    }

    // 评分验证
    if (isNaN(row.vote_average)) {
        errors.push('投票平均分必须为数字');
    } else {
        const rating = parseFloat(row.vote_average);
        if (rating < 0 || rating > 10) {
            errors.push('投票平均分必须在0-10之间');
        }
    }

    // 日期格式验证
    if (row.release_date && !/^\d{4}-\d{2}-\d{2}$/.test(row.release_date)) {
        errors.push('发布日期格式应为YYYY-MM-DD');
    }

    // 类型数组验证
    if (row.genres) {
        try {
            const genres = JSON.parse(row.genres);
            if (!Array.isArray(genres)) {
                errors.push('类型字段应为JSON数组');
            }
        } catch {
            errors.push('类型字段格式无效');
        }
    }

    return errors;
}

// 快速验证并抛出错误
export function validateAndThrow(condition: boolean, message: string, details: any = null): void {
    if (!condition) {
        throw new ValidationError(message, details);
    }
}
