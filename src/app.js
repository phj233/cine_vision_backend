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
exports.startServer = void 0;
// 设置控制台输出编码
process.stdout.setDefaultEncoding('utf8');
process.stderr.setDefaultEncoding('utf8');
// 设置Windows系统的控制台代码页
if (process.platform === 'win32') {
    try {
        const { execSync } = require('child_process');
        execSync('chcp 65001', { stdio: 'ignore' });
    }
    catch (e) {
        // 忽略错误
    }
}
// 然后再导入其他模块
require("./alias");
const fastify_1 = __importDefault(require("fastify"));
const cors_1 = __importDefault(require("@fastify/cors"));
const multipart_1 = __importDefault(require("@fastify/multipart"));
const config_1 = __importDefault(require("./config"));
const movie_controller_1 = require("@/controllers/movie.controller");
const import_controller_1 = require("@/controllers/import.controller");
const visualization_controller_1 = require("@/controllers/visualization.controller");
const fs_extra_1 = __importDefault(require("fs-extra"));
const logger_1 = __importDefault(require("@/utils/logger"));
const error_handler_1 = require("@/utils/error-handler");
const error_handler_2 = require("@/utils/error-handler");
const app = (0, fastify_1.default)({
    logger: true,
    bodyLimit: 1024 * 1024 * 1024, // 1GB
    connectionTimeout: 300000, // 5分钟超时
    keepAliveTimeout: 300000 // 5分钟超时
});
// 解决BigInt序列化问题，在JSON.stringify时将BigInt转换为字符串
app.addHook('preSerialization', (request, reply, payload, done) => {
    const replaceBigInt = (key, value) => {
        if (typeof value === 'bigint') {
            return value.toString();
        }
        return value;
    };
    if (payload) {
        try {
            // 先序列化成字符串，再解析回JSON，确保BigInt被处理
            const stringified = JSON.stringify(payload, replaceBigInt);
            const parsed = JSON.parse(stringified);
            done(null, parsed);
        }
        catch (error) {
            logger_1.default.error('序列化JSON失败:', error);
            done(error);
        }
    }
    else {
        done(null, payload);
    }
});
// 注册插件
app.register(cors_1.default, {
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Content-Disposition'],
    exposedHeaders: ['Content-Disposition']
});
// 添加全局错误处理
app.setErrorHandler((error, request, reply) => {
    return (0, error_handler_2.handleHttpError)(error, reply);
});
// 注册 multipart 插件，优化大文件上传配置
app.register(multipart_1.default, {
    attachFieldsToBody: false, // 不把字段附加到body上
    limits: {
        fieldNameSize: 100, // 字段名大小限制
        fieldSize: 100, // 字段值大小限制
        fields: 10, // 字段数量限制
        fileSize: 1024 * 1024 * 1024, // 文件大小限制 (1GB)
        files: 1 // 文件数量限制
    },
    throwFileSizeLimit: true // 文件大小超限时抛出错误
});
// 注册路由
app.register((instance) => __awaiter(void 0, void 0, void 0, function* () {
    // 特定路径必须在通配路径之前
    // 电影搜索接口
    instance.get('/movies/search', movie_controller_1.MovieController.searchMovies);
    // 随机推荐电影
    instance.get('/recommendations/random', movie_controller_1.MovieController.getRandomMovies);
    // 电影分类和统计接口
    instance.get('/genres', movie_controller_1.MovieController.getGenres);
    instance.get('/genres/stats', visualization_controller_1.VisualizationController.getGenresWithStats);
    instance.get('/stats/years', movie_controller_1.MovieController.getMoviesByYear);
    instance.get('/stats/summary', movie_controller_1.MovieController.getMovieStats);
    // 数据可视化接口
    instance.get('/visualization/rating-distribution', visualization_controller_1.VisualizationController.getRatingDistribution);
    instance.get('/visualization/yearly-trends', visualization_controller_1.VisualizationController.getYearlyTrends);
    instance.get('/visualization/genre-comparison', visualization_controller_1.VisualizationController.getGenreComparison);
    instance.get('/visualization/runtime-distribution', visualization_controller_1.VisualizationController.getRuntimeDistribution);
    instance.get('/visualization/top-production-companies', visualization_controller_1.VisualizationController.getTopProductionCompanies);
    instance.get('/visualization/actor-collaborations', visualization_controller_1.VisualizationController.getActorCollaborations);
    instance.get('/visualization/budget-revenue', visualization_controller_1.VisualizationController.getBudgetRevenueAnalysis);
    // 新增数据可视化接口
    instance.get('/visualization/language-distribution', visualization_controller_1.VisualizationController.getLanguageDistribution);
    instance.get('/visualization/top-directors', visualization_controller_1.VisualizationController.getTopDirectors);
    instance.get('/visualization/top-cinematographers', visualization_controller_1.VisualizationController.getTopCinematographers);
    instance.get('/visualization/top-composers', visualization_controller_1.VisualizationController.getTopComposers);
    instance.get('/visualization/cross-role-talents', visualization_controller_1.VisualizationController.getCrossRoleTalents);
    // 带参数的路由需要放在特定路由之后
    // 相似电影推荐 (必须在 movies/:id 前面)
    instance.get('/movies/:id/similar', movie_controller_1.MovieController.getSimilarMovies);
    // 基础电影接口
    instance.get('/movies', movie_controller_1.MovieController.getMovies);
    instance.get('/movies/all', movie_controller_1.MovieController.getAllMovies);
    instance.get('/movies/:id', movie_controller_1.MovieController.getMovieDetails);
}), { prefix: '/api/v1' });
app.register((instance) => __awaiter(void 0, void 0, void 0, function* () {
    instance.post('/import', {
        config: {
            timeout: 3600000 // 1小时超时
        },
        handler: import_controller_1.ImportController.handleImport
    });
}), { prefix: '/api/v1' });
function initialize() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            // 确保日志目录存在
            yield fs_extra_1.default.ensureDir('logs');
            // 确保临时目录存在
            yield fs_extra_1.default.ensureDir('tmp');
            // 其他初始化逻辑...
        }
        catch (error) {
            console.error('初始化失败:', error);
            process.exit(1);
        }
    });
}
const startServer = () => __awaiter(void 0, void 0, void 0, function* () {
    yield initialize();
    // 设置全局错误处理
    (0, error_handler_1.setupGlobalErrorHandlers)();
    try {
        yield app.listen({
            port: Number(config_1.default.port),
            host: '0.0.0.0'
        });
        console.log(`服务器运行于 http://localhost:${config_1.default.port}`);
    }
    catch (err) {
        app.log.error(err);
        process.exit(1);
    }
});
exports.startServer = startServer;
(0, exports.startServer)().then(r => console.log('服务器已启动'));
