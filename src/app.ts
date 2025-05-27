// 设置控制台输出编码
process.stdout.setDefaultEncoding('utf8');
process.stderr.setDefaultEncoding('utf8');

// 设置Windows系统的控制台代码页
if (process.platform === 'win32') {
    try {
        const { execSync } = require('child_process');
        execSync('chcp 65001', { stdio: 'ignore' });
    } catch (e) {
        // 忽略错误
    }
}

// 然后再导入其他模块
import './alias';

import fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import fastifyMultipart from '@fastify/multipart';
import config from './config';
import { MovieController } from "@/controllers/movie.controller";
import { ImportController } from "@/controllers/import.controller";
import { VisualizationController } from "@/controllers/visualization.controller";
import fs from "fs-extra";
import logger from '@/utils/logger';
import { setupGlobalErrorHandlers } from '@/utils/error-handler';
import { handleHttpError } from '@/utils/error-handler';

const app = fastify({
    logger: true,
    bodyLimit: 1024 * 1024 * 1024, // 1GB
    connectionTimeout: 300000, // 5分钟超时
    keepAliveTimeout: 300000  // 5分钟超时
});

// 解决BigInt序列化问题，在JSON.stringify时将BigInt转换为字符串
app.addHook('preSerialization', (request, reply, payload, done) => {
    const replaceBigInt = (key: string, value: any) => {
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
        } catch (error) {
            logger.error('序列化JSON失败:', error);
            done(error as Error);
        }
    } else {
        done(null, payload);
    }
});

// 注册插件
app.register(fastifyCors, {
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Content-Disposition'],
    exposedHeaders: ['Content-Disposition']
});

// 添加全局错误处理
app.setErrorHandler((error, request, reply) => {
    return handleHttpError(error, reply);
});

// 注册 multipart 插件，优化大文件上传配置
app.register(fastifyMultipart, {
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
app.register(async (instance) => {
    // 特定路径必须在通配路径之前
    // 电影搜索接口
    instance.get('/movies/search', MovieController.searchMovies);

    // 随机推荐电影
    instance.get('/recommendations/random', MovieController.getRandomMovies);

    // 电影分类和统计接口
    instance.get('/genres', MovieController.getGenres);
    instance.get('/genres/stats', VisualizationController.getGenresWithStats);
    instance.get('/stats/years', MovieController.getMoviesByYear);
    instance.get('/stats/summary', MovieController.getMovieStats);

    // 数据可视化接口
    instance.get('/visualization/rating-distribution', VisualizationController.getRatingDistribution);
    instance.get('/visualization/yearly-trends', VisualizationController.getYearlyTrends);
    instance.get('/visualization/genre-comparison', VisualizationController.getGenreComparison);
    instance.get('/visualization/runtime-distribution', VisualizationController.getRuntimeDistribution);
    instance.get('/visualization/top-production-companies', VisualizationController.getTopProductionCompanies);
    instance.get('/visualization/actor-collaborations', VisualizationController.getActorCollaborations);
    instance.get('/visualization/budget-revenue', VisualizationController.getBudgetRevenueAnalysis);

    // 新增数据可视化接口
    instance.get('/visualization/language-distribution', VisualizationController.getLanguageDistribution);
    instance.get('/visualization/top-directors', VisualizationController.getTopDirectors);
    instance.get('/visualization/top-cinematographers', VisualizationController.getTopCinematographers);
    instance.get('/visualization/top-composers', VisualizationController.getTopComposers);
    instance.get('/visualization/cross-role-talents', VisualizationController.getCrossRoleTalents);

    // 带参数的路由需要放在特定路由之后
    // 相似电影推荐 (必须在 movies/:id 前面)
    instance.get('/movies/:id/similar', MovieController.getSimilarMovies);

    // 基础电影接口
    instance.get('/movies', MovieController.getMovies);
    instance.get('/movies/all', MovieController.getAllMovies);
    instance.get('/movies/:id', MovieController.getMovieDetails);
}, { prefix: '/api/v1' });

app.register(async (instance) => {
    instance.post('/import', {
        config: {
            timeout: 3600000 // 1小时超时
        },
        handler: ImportController.handleImport
    });
}, { prefix: '/api/v1' });

async function initialize() {
    try {
        // 确保日志目录存在
        await fs.ensureDir('logs');
        // 确保临时目录存在
        await fs.ensureDir('tmp');
        // 其他初始化逻辑...
    } catch (error) {
        console.error('初始化失败:', error);
        process.exit(1);
    }
}

export const startServer = async () => {
    await initialize()

    // 设置全局错误处理
    setupGlobalErrorHandlers();

    try {
        await app.listen({
            port: Number(config.port),
            host: '0.0.0.0'
        });
        console.log(`服务器运行于 http://localhost:${config.port}`);
    } catch (err) {
        app.log.error(err);
        process.exit(1);
    }
};

startServer().then(r =>
    console.log('服务器已启动')
);
