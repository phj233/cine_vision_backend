import { FastifyRequest, FastifyReply } from 'fastify';
import logger from '@/utils/logger';
import { asyncHandler, ValidationError } from '@/utils/error-handler';
import { StreamProcessor } from '@/utils/stream-processor';
import fs from 'fs-extra';
import path from 'path';
import { pipeline } from 'stream/promises';

/**
 * 文件数据接口定义
 * @interface FilePart
 * @property {string} type - 部分类型
 * @property {string} filename - 文件名
 * @property {string} encoding - 编码方式
 * @property {string} mimetype - MIME类型
 * @property {any} file - 文件流
 */
interface FilePart {
    type: string;
    filename: string;
    encoding: string;
    mimetype: string;
    file: any; // Readable流
}

/**
 * 数据导入控制器类
 * 提供电影数据的导入功能
 */
export class ImportController {
    /**
     * 处理文件上传和数据导入
     * @param req 请求对象，包含上传的CSV文件
     * @param reply 响应对象
     * @returns 返回处理结果，包括导入的数据统计信息
     */
    static handleImport = asyncHandler(async (req: FastifyRequest, reply: FastifyReply) => {
        let tempFilePath = '';

        try {
            logger.info('收到文件上传请求');

            // 使用 parts() 方法获取上传的内容
            const parts = req.parts();

            let filePart: FilePart | null = null;

            // 遍历所有 parts 找到文件
            for await (const part of parts) {
                if (part.type === 'file') {
                    filePart = part as unknown as FilePart;
                    break;
                }
            }

            if (!filePart || !filePart.file) {
                logger.error('未找到文件部分');
                throw new ValidationError('未收到上传文件');
            }

            const { filename, mimetype, file } = filePart;

            logger.info(`收到文件: ${filename} (${mimetype})`);

            // 确保文件类型是CSV
            if (mimetype !== 'text/csv' && !filename.toLowerCase().endsWith('.csv')) {
                throw new ValidationError('文件必须是CSV格式');
            }

            // 对于大文件，先将文件保存到临时目录
            try {
                // 确保临时目录存在
                await fs.ensureDir('tmp');

                // 生成临时文件路径
                tempFilePath = path.join('tmp', `import_${Date.now()}_${filename}`);

                // 将上传的文件流写入临时文件
                const writeStream = fs.createWriteStream(tempFilePath);

                // 设置错误处理
                writeStream.on('error', (err) => {
                    logger.error(`写入临时文件失败: ${err.message}`);
                });

                // 使用管道将上传流写入文件
                await pipeline(file, writeStream);

                logger.info(`文件已保存到临时位置: ${tempFilePath}`);

                // 创建文件流用于处理
                const fileStream = fs.createReadStream(tempFilePath, {
                    encoding: 'utf8',
                    highWaterMark: 64 * 1024 // 64KB chunks
                });

                // 使用StreamProcessor处理文件流，实现边接收边处理
                const streamProcessor = new StreamProcessor();
                const result = await streamProcessor.processStream(fileStream);

                // 处理完成后清理临时文件
                try {
                    await fs.remove(tempFilePath);
                    logger.info(`临时文件已清理: ${tempFilePath}`);
                } catch (cleanupError: unknown) {
                    const errorMessage = cleanupError instanceof Error ? cleanupError.message : String(cleanupError);
                    logger.warn(`临时文件清理失败: ${errorMessage}`);
                    // 不因清理失败而中断操作
                }

                return result;
            } catch (fileError: unknown) {
                const errorMessage = fileError instanceof Error ? fileError.message : String(fileError);
                logger.error(`文件处理失败: ${errorMessage}`);
                // 尝试直接处理流，作为备选方案
                logger.warn('尝试直接处理文件流作为备选方案');

                const streamProcessor = new StreamProcessor();
                return await streamProcessor.processStream(file);
            }
        } catch (error: any) {
            // 清理可能存在的临时文件
            if (tempFilePath) {
                try {
                    await fs.remove(tempFilePath);
                    logger.info(`错误发生后清理临时文件: ${tempFilePath}`);
                } catch (cleanupError: unknown) {
                    const errorMessage = cleanupError instanceof Error ? cleanupError.message : String(cleanupError);
                    logger.warn(`临时文件清理失败: ${errorMessage}`);
                }
            }

            // 这里不需要额外处理，因为asyncHandler会捕获并处理所有异常
            logger.error(`导入失败: ${error.stack || error.message}`);
            throw error;
        }
    });
}
