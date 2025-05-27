"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_extra_1 = __importDefault(require("fs-extra"));
const path_1 = __importDefault(require("path"));
// 确保日志目录存在
const logDir = path_1.default.join(process.cwd(), 'logs');
if (!fs_extra_1.default.existsSync(logDir)) {
    fs_extra_1.default.mkdirSync(logDir, { recursive: true });
}
// 简单的日志记录器，使用console.log
const logger = {
    info: (message, ...args) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] INFO: ${message}`;
        console.log(logMessage, ...args);
        // 写入文件
        fs_extra_1.default.appendFileSync(path_1.default.join(logDir, 'info.log'), `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`, { encoding: 'utf8' });
    },
    error: (message, ...args) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ERROR: ${message}`;
        console.error(logMessage, ...args);
        // 写入文件
        fs_extra_1.default.appendFileSync(path_1.default.join(logDir, 'error.log'), `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`, { encoding: 'utf8' });
    },
    warn: (message, ...args) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] WARN: ${message}`;
        console.warn(logMessage, ...args);
        // 写入文件
        fs_extra_1.default.appendFileSync(path_1.default.join(logDir, 'info.log'), `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`, { encoding: 'utf8' });
    },
    debug: (message, ...args) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] DEBUG: ${message}`;
        console.debug(logMessage, ...args);
        if (process.env.NODE_ENV === 'development') {
            // 在开发环境下也写入文件
            fs_extra_1.default.appendFileSync(path_1.default.join(logDir, 'debug.log'), `${logMessage} ${args.length ? JSON.stringify(args) : ''}\n`, { encoding: 'utf8' });
        }
    }
};
exports.default = logger;
