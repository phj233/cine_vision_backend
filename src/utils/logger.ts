import fs from 'fs-extra';
import path from 'path';

// 确保日志目录存在
const logDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

// 简单的日志记录器，使用console.log
const logger = {
    info: (message: string, ...args: any[]) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] INFO: ${message}`;
        console.log(logMessage, ...args);

        // 写入文件
        fs.appendFileSync(
            path.join(logDir, 'info.log'),
            `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`,
            { encoding: 'utf8' }
        );
    },

    error: (message: string, ...args: any[]) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ERROR: ${message}`;
        console.error(logMessage, ...args);

        // 写入文件
        fs.appendFileSync(
            path.join(logDir, 'error.log'),
            `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`,
            { encoding: 'utf8' }
        );
    },

    warn: (message: string, ...args: any[]) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] WARN: ${message}`;
        console.warn(logMessage, ...args);

        // 写入文件
        fs.appendFileSync(
            path.join(logDir, 'info.log'),
            `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`,
            { encoding: 'utf8' }
        );
    },

    debug: (message: string, ...args: any[]) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] DEBUG: ${message}`;
        console.debug(logMessage, ...args);

        if (process.env.NODE_ENV === 'development') {
            // 在开发环境下也写入文件
            fs.appendFileSync(
                path.join(logDir, 'debug.log'),
                `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`,
                { encoding: 'utf8' }
            );
        }
    }
};

export default logger;
