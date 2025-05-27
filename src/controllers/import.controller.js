"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.ImportController = void 0;
const logger_1 = __importDefault(require("@/utils/logger"));
const error_handler_1 = require("@/utils/error-handler");
const stream_processor_1 = require("@/utils/stream-processor");
const fs_extra_1 = __importDefault(require("fs-extra"));
const path_1 = __importDefault(require("path"));
const promises_1 = require("stream/promises");
/**
 * 数据导入控制器类
 * 提供电影数据的导入功能
 */
class ImportController {
}
exports.ImportController = ImportController;
_a = ImportController;
/**
 * 处理文件上传和数据导入
 * @param req 请求对象，包含上传的CSV文件
 * @param reply 响应对象
 * @returns 返回处理结果，包括导入的数据统计信息
 */
ImportController.handleImport = (0, error_handler_1.asyncHandler)((req, reply) => __awaiter(void 0, void 0, void 0, function* () {
    var _b, e_1, _c, _d;
    let tempFilePath = '';
    try {
        logger_1.default.info('收到文件上传请求');
        // 使用 parts() 方法获取上传的内容
        const parts = yield req.parts();
        let filePart = null;
        try {
            // 遍历所有 parts 找到文件
            for (var _e = true, parts_1 = __asyncValues(parts), parts_1_1; parts_1_1 = yield parts_1.next(), _b = parts_1_1.done, !_b; _e = true) {
                _d = parts_1_1.value;
                _e = false;
                const part = _d;
                if (part.type === 'file') {
                    filePart = part;
                    break;
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (!_e && !_b && (_c = parts_1.return)) yield _c.call(parts_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        if (!filePart || !filePart.file) {
            logger_1.default.error('未找到文件部分');
            throw new error_handler_1.ValidationError('未收到上传文件');
        }
        const { filename, mimetype, file } = filePart;
        logger_1.default.info(`收到文件: ${filename} (${mimetype})`);
        // 确保文件类型是CSV
        if (mimetype !== 'text/csv' && !filename.toLowerCase().endsWith('.csv')) {
            throw new error_handler_1.ValidationError('文件必须是CSV格式');
        }
        // 对于大文件，先将文件保存到临时目录
        try {
            // 确保临时目录存在
            yield fs_extra_1.default.ensureDir('tmp');
            // 生成临时文件路径
            tempFilePath = path_1.default.join('tmp', `import_${Date.now()}_${filename}`);
            // 将上传的文件流写入临时文件
            const writeStream = fs_extra_1.default.createWriteStream(tempFilePath);
            // 设置错误处理
            writeStream.on('error', (err) => {
                logger_1.default.error(`写入临时文件失败: ${err.message}`);
            });
            // 使用管道将上传流写入文件
            yield (0, promises_1.pipeline)(file, writeStream);
            logger_1.default.info(`文件已保存到临时位置: ${tempFilePath}`);
            // 创建文件流用于处理
            const fileStream = fs_extra_1.default.createReadStream(tempFilePath, {
                encoding: 'utf8',
                highWaterMark: 64 * 1024 // 64KB chunks
            });
            // 使用StreamProcessor处理文件流，实现边接收边处理
            const streamProcessor = new stream_processor_1.StreamProcessor();
            const result = yield streamProcessor.processStream(fileStream);
            // 处理完成后清理临时文件
            try {
                yield fs_extra_1.default.remove(tempFilePath);
                logger_1.default.info(`临时文件已清理: ${tempFilePath}`);
            }
            catch (cleanupError) {
                const errorMessage = cleanupError instanceof Error ? cleanupError.message : String(cleanupError);
                logger_1.default.warn(`临时文件清理失败: ${errorMessage}`);
                // 不因清理失败而中断操作
            }
            return result;
        }
        catch (fileError) {
            const errorMessage = fileError instanceof Error ? fileError.message : String(fileError);
            logger_1.default.error(`文件处理失败: ${errorMessage}`);
            // 尝试直接处理流，作为备选方案
            logger_1.default.warn('尝试直接处理文件流作为备选方案');
            const streamProcessor = new stream_processor_1.StreamProcessor();
            return yield streamProcessor.processStream(file);
        }
    }
    catch (error) {
        // 清理可能存在的临时文件
        if (tempFilePath) {
            try {
                yield fs_extra_1.default.remove(tempFilePath);
                logger_1.default.info(`错误发生后清理临时文件: ${tempFilePath}`);
            }
            catch (cleanupError) {
                const errorMessage = cleanupError instanceof Error ? cleanupError.message : String(cleanupError);
                logger_1.default.warn(`临时文件清理失败: ${errorMessage}`);
            }
        }
        // 这里不需要额外处理，因为asyncHandler会捕获并处理所有异常
        logger_1.default.error(`导入失败: ${error.stack || error.message}`);
        throw error;
    }
}));
