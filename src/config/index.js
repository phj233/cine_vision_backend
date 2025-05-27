"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
exports.default = {
    port: process.env.PORT || 3000,
    database: {
        url: process.env.DATABASE_URL || 'postgresql://postgre:postgre@localhost:5432/movies'
    },
    csv: {
        batchSize: parseInt(process.env.CSV_BATCH_SIZE || '1000'),
        maxFileSize: parseInt(process.env.CSV_MAX_FILE_SIZE || '1073741824'), // 1GB
        timeout: parseInt(process.env.CSV_TIMEOUT || '3600000'), // 1小时
        retryCount: parseInt(process.env.CSV_RETRY_COUNT || '3'),
        retryDelay: parseInt(process.env.CSV_RETRY_DELAY || '1000') // 1秒
    },
    logging: {
        level: process.env.LOG_LEVEL || 'debug',
        file: {
            enabled: process.env.LOG_FILE_ENABLED === 'true',
            path: process.env.LOG_FILE_PATH || 'logs/app.log'
        }
    }
};
