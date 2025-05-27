import { FastifyReply } from 'fastify';
import logger from './logger';

// 自定义错误类
export class AppError extends Error {
    statusCode: number;
    isOperational: boolean;
    details: any;

    constructor(message: string, statusCode = 500, details: any = null) {
        super(message);
        this.statusCode = statusCode;
        this.isOperational = true; // 是否为已知操作错误（可以友好提示用户的错误）
        this.details = details;
        Error.captureStackTrace(this, this.constructor);
    }
}

// 特定类型的错误
export class ValidationError extends AppError {
    constructor(message: string, details: any = null) {
        super(message, 400, details);
    }
}

export class NotFoundError extends AppError {
    constructor(message: string, details: any = null) {
        super(message, 404, details);
    }
}

export class AuthorizationError extends AppError {
    constructor(message: string, details: any = null) {
        super(message, 403, details);
    }
}

export class DatabaseError extends AppError {
    constructor(message: string, details: any = null) {
        super(message, 500, details);
    }
}

// HTTP错误处理函数
export const handleHttpError = (error: any, reply: FastifyReply) => {
    // 判断是否为我们的自定义错误
    if (error instanceof AppError) {
        logger.warn(`操作错误: ${error.message}`, {
            statusCode: error.statusCode,
            stackTrace: error.stack
        });

        return reply.code(error.statusCode).send({
            error: error.message,
            status: 'error',
            statusCode: error.statusCode,
            details: error.details,
            // 提供用户友好的消息
            userMessage: getFriendlyErrorMessage(error)
        });
    }

    // 处理Prisma错误
    if (error.code && error.code.startsWith('P')) {
        let statusCode = 500;
        let message = '数据库操作失败';

        // 处理常见的Prisma错误
        switch (error.code) {
            case 'P2002': // 唯一约束冲突
                statusCode = 409;
                message = '记录已存在';
                break;
            case 'P2025': // 记录未找到
                statusCode = 404;
                message = '记录不存在';
                break;
            default:
                break;
        }

        logger.error(`数据库错误 [${error.code}]: ${error.message}`, {
            stackTrace: error.stack
        });

        return reply.code(statusCode).send({
            error: message,
            status: 'error',
            statusCode,
            details: process.env.NODE_ENV === 'development' ? error.message : null,
            userMessage: `数据操作失败: ${message}`
        });
    }

    // 处理文件上传错误
    if (error.code === 'FST_REQ_FILE_TOO_LARGE') {
        logger.warn(`文件过大: ${error.message}`);
        return reply.code(413).send({
            error: '上传文件过大',
            status: 'error',
            statusCode: 413,
            userMessage: '您上传的文件超过了允许的大小限制，请分割文件或减小文件大小后重试'
        });
    }

    if (error.code === 'FST_FILES_LIMIT') {
        logger.warn(`文件数量超限: ${error.message}`);
        return reply.code(413).send({
            error: '上传文件数量超限',
            status: 'error',
            statusCode: 413,
            userMessage: '您一次只能上传一个文件'
        });
    }

    // 处理其他文件上传错误
    if (error.code && error.code.startsWith('FST_')) {
        logger.warn(`文件上传错误: ${error.message}`);
        return reply.code(400).send({
            error: '文件上传失败',
            status: 'error',
            statusCode: 400,
            details: process.env.NODE_ENV === 'development' ? error.message : null,
            userMessage: '文件上传处理失败，请检查文件格式和大小'
        });
    }

    // 处理未知错误
    logger.error(`未预期的错误: ${error.message}`, {
        stackTrace: error.stack
    });

    return reply.code(500).send({
        error: '服务器内部错误',
        status: 'error',
        statusCode: 500,
        details: process.env.NODE_ENV === 'development' ? error.message : null,
        userMessage: '抱歉，服务器暂时无法处理您的请求，请稍后重试'
    });
};

// 获取用户友好的错误消息
function getFriendlyErrorMessage(error: AppError): string {
    if (error instanceof ValidationError) {
        return `输入验证失败: ${error.message}`;
    }

    if (error instanceof NotFoundError) {
        return `找不到您请求的资源: ${error.message}`;
    }

    if (error instanceof AuthorizationError) {
        return `权限不足: ${error.message}`;
    }

    if (error instanceof DatabaseError) {
        return `数据操作失败: ${error.message}`;
    }

    return error.message;
}

// 异步处理包装器，用于路由处理函数
export const asyncHandler = (fn: Function) => {
    return async (request: any, reply: FastifyReply) => {
        try {
            return await fn(request, reply);
        } catch (error) {
            return handleHttpError(error, reply);
        }
    };
};

// 全局未捕获异常处理
export const setupGlobalErrorHandlers = () => {
    // 处理未捕获的Promise错误
    process.on('unhandledRejection', (reason: any) => {
        logger.error('未捕获的Promise错误:', reason);
        // 在开发环境可以抛出以便立即发现问题
        if (process.env.NODE_ENV === 'development') {
            throw reason;
        }
    });

    // 处理未捕获的异常
    process.on('uncaughtException', (error: Error) => {
        logger.error('未捕获的异常:', {
            message: error.message,
            stack: error.stack
        });

        // 记录错误并优雅地退出
        // 注意：在生产环境中，应该使用进程管理器(如PM2)自动重启服务
        if (process.env.NODE_ENV === 'production') {
            process.exit(1);
        }
    });
}; 