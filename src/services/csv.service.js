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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CsvService = void 0;
const fs_1 = require("fs");
const csv_parse_1 = require("csv-parse");
const promises_1 = require("stream/promises");
const stream_1 = require("stream");
const client_1 = require("@prisma/client");
const parser_1 = require("@/utils/parser");
const logger_1 = __importDefault(require("@/utils/logger"));
const config_1 = __importDefault(require("@/config"));
const error_handler_1 = require("@/utils/error-handler");
const prisma = new client_1.PrismaClient({
    log: [
        { level: 'query', emit: 'event' },
        { level: 'error', emit: 'event' }
    ]
});
prisma.$on('query', (e) => {
    logger_1.default.debug(`Query: ${e.query} | Params: ${e.params}`);
});
prisma.$on('error', (e) => {
    logger_1.default.error(`Prisma Error: ${e.message}`);
});
class CsvService {
    static importMovies(filePath) {
        return __awaiter(this, void 0, void 0, function* () {
            let processedCount = 0;
            let batch = [];
            const startTime = Date.now();
            let errorRows = [];
            try {
                logger_1.default.info(`开始处理文件: ${filePath}`);
                yield (0, promises_1.pipeline)((0, fs_1.createReadStream)(filePath), this.createCSVParser(), this.createTransformer(batch, errorRows), this.createBatchProcessor(batch, processedCount));
                // 如果有错误行，记录但继续处理
                if (errorRows.length > 0) {
                    logger_1.default.warn(`文件处理完成，但有 ${errorRows.length} 行数据无法处理`, {
                        errors: errorRows.slice(0, 10) // 只记录前10个错误，避免日志过大
                    });
                }
                logger_1.default.info(`文件处理完成，共处理 ${processedCount} 条记录，耗时 ${Date.now() - startTime}ms`);
                return {
                    success: true,
                    count: processedCount,
                    errorCount: errorRows.length,
                    errors: errorRows.slice(0, 10)
                };
            }
            catch (error) {
                logger_1.default.error(`文件处理失败: ${error.message}`);
                throw new error_handler_1.DatabaseError(`CSV导入失败: ${error.message}`, { cause: error });
            }
            finally {
                yield prisma.$disconnect();
                logger_1.default.info('数据库连接已关闭');
            }
        });
    }
    static createCSVParser() {
        return (0, csv_parse_1.parse)({
            columns: true,
            trim: true,
            skip_empty_lines: true,
            cast: (value, context) => {
                if (context.header)
                    return value;
                if (context.column === 'release_date')
                    return new Date(value);
                return value;
            }
        });
    }
    static createTransformer(batch, errorRows) {
        let rowNumber = 0;
        return new stream_1.Transform({
            objectMode: true,
            transform: (row, _, callback) => {
                rowNumber++;
                try {
                    if (!row.id || !row.title) {
                        errorRows.push({
                            row: rowNumber,
                            error: '缺少必填字段 ID 或 title'
                        });
                        return callback();
                    }
                    const transformed = this.transformRow(row);
                    batch.push(transformed);
                    callback();
                }
                catch (error) {
                    errorRows.push({
                        row: rowNumber,
                        error: error.message
                    });
                    logger_1.default.error(`数据转换失败 [行 ${rowNumber}]: ${error.message}`);
                    callback();
                }
            }
        });
    }
    static createBatchProcessor(batch, processedCount) {
        return new stream_1.Transform({
            objectMode: true,
            transform(_chunk, _encoding, callback) {
                return __awaiter(this, void 0, void 0, function* () {
                    if (batch.length >= config_1.default.csv.batchSize) {
                        try {
                            yield CsvService.processBatch(batch);
                            processedCount += batch.length;
                            batch.length = 0; // 清空数组但保持引用
                        }
                        catch (error) {
                            logger_1.default.error(`批量处理失败:`, error);
                            // 我们在这里只记录错误，但不阻止处理继续进行
                        }
                    }
                    callback();
                });
            },
            flush(callback) {
                return __awaiter(this, void 0, void 0, function* () {
                    if (batch.length > 0) {
                        try {
                            yield CsvService.processBatch(batch);
                            processedCount += batch.length;
                        }
                        catch (error) {
                            logger_1.default.error(`最后批次处理失败:`, error);
                        }
                    }
                    callback();
                });
            }
        });
    }
    static processBatch(batch) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const start = Date.now();
                yield prisma.$transaction([
                    prisma.movie.createMany({
                        data: batch,
                        skipDuplicates: true
                    })
                ]);
                logger_1.default.debug(`已插入 ${batch.length} 条记录，耗时 ${Date.now() - start}ms`);
            }
            catch (error) {
                logger_1.default.error(`批量插入失败: ${error.message}`);
                throw new error_handler_1.DatabaseError(`数据库插入失败: ${error.message}`);
            }
        });
    }
    static transformRow(row) {
        var _a, _b, _c, _d, _e, _f, _g, _h;
        try {
            return {
                id: row.id,
                title: row.title,
                vote_average: parseFloat(row.vote_average) || 0,
                vote_count: parseInt(row.vote_count) || 0,
                status: row.status,
                release_date: row.release_date ? new Date(row.release_date) : null,
                revenue: BigInt(Math.round(parseFloat(row.revenue) || 0)),
                runtime: row.runtime ? parseInt(row.runtime) : null,
                budget: BigInt(Math.round(parseFloat(row.budget) || 0)),
                imdb_id: row.imdb_id || null,
                original_language: row.original_language || null,
                original_title: row.original_title || row.title,
                overview: row.overview || null,
                popularity: row.popularity ? parseFloat(row.popularity) : null,
                tagline: row.tagline || null,
                genres: ((_a = row.genres) === null || _a === void 0 ? void 0 : _a.split(/, */).filter(Boolean)) || [],
                production_companies: (0, parser_1.parseProductionCompanies)(row.production_companies),
                production_countries: ((_b = row.production_countries) === null || _b === void 0 ? void 0 : _b.split(/, */)) || [],
                spoken_languages: ((_c = row.spoken_languages) === null || _c === void 0 ? void 0 : _c.split(/, */)) || [],
                cast: (0, parser_1.parseCast)(row.cast),
                director: ((_d = row.director) === null || _d === void 0 ? void 0 : _d.split(/, */)) || [],
                director_of_photography: ((_e = row.director_of_photography) === null || _e === void 0 ? void 0 : _e.split(/, */)) || [],
                writers: ((_f = row.writers) === null || _f === void 0 ? void 0 : _f.split(/, */)) || [],
                producers: ((_g = row.producers) === null || _g === void 0 ? void 0 : _g.split(/, */)) || [],
                music_composer: ((_h = row.music_composer) === null || _h === void 0 ? void 0 : _h.split(/, */)) || [],
                imdb_rating: row.imdb_rating ? parseFloat(row.imdb_rating) : null,
                imdb_votes: row.imdb_votes ? parseInt(row.imdb_votes) : null,
                poster_path: row.poster_path || null
            };
        }
        catch (error) {
            logger_1.default.error('行数据转换失败:', {
                error,
                row: JSON.stringify(row)
            });
            throw new error_handler_1.ValidationError(`数据格式无效: ${error.message}`);
        }
    }
}
exports.CsvService = CsvService;
