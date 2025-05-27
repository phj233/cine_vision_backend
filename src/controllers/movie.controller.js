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
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.MovieController = void 0;
const client_1 = require("@prisma/client");
const error_handler_1 = require("@/utils/error-handler");
const logger_1 = __importDefault(require("@/utils/logger"));
const prisma = new client_1.PrismaClient();
/**
 * 电影控制器类
 * 提供电影相关的所有API接口实现
 */
class MovieController {
}
exports.MovieController = MovieController;
_a = MovieController;
/**
 * 获取电影列表（分页）
 * @param req 请求对象，包含分页参数、筛选条件等
 * @param res 响应对象
 * @returns 返回分页后的电影列表和分页元数据
 */
MovieController.getMovies = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { page = 1, pageSize = 20, genre, minRating, year, sortBy = "release_date", } = req.query;
    const where = {};
    if (genre) {
        where.genres = { has: genre };
    }
    if (minRating) {
        where.vote_average = { gte: minRating };
    }
    if (year) {
        where.release_date = {
            gte: new Date(`${year}-01-01`),
            lte: new Date(`${year}-12-31`),
        };
    }
    try {
        const [movies, total] = yield Promise.all([
            prisma.movie.findMany({
                where,
                skip: (page - 1) * pageSize,
                take: pageSize,
                orderBy: { [sortBy]: "desc" },
                select: {
                    id: true,
                    title: true,
                    release_date: true,
                    vote_average: true,
                    genres: true,
                    poster_path: true,
                    runtime: true,
                    director: true,
                    imdb_id: true,
                },
            }),
            prisma.movie.count({ where }),
        ]);
        return {
            data: movies.map((m) => {
                var _b, _c, _d;
                return (Object.assign(Object.assign({}, m), { vote_average: ((_b = m.vote_average) === null || _b === void 0 ? void 0 : _b.toString()) || "0", runtime: ((_c = m.runtime) === null || _c === void 0 ? void 0 : _c.toString()) || "0", release_date: (_d = m.release_date) === null || _d === void 0 ? void 0 : _d.toISOString().split("T")[0] }));
            }),
            meta: {
                total,
                page,
                pageSize,
                totalPages: Math.ceil(total / pageSize),
            },
        };
    }
    catch (error) {
        logger_1.default.error("获取电影列表失败:", error);
        throw error;
    }
}));
/**
 * 获取电影详情
 * @param req 请求对象，包含电影ID参数
 * @param res 响应对象
 * @returns 返回电影的详细信息
 */
MovieController.getMovieDetails = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _b, _c, _d, _e, _f, _g;
    try {
        const movie = yield prisma.movie.findUnique({
            where: { id: req.params.id },
            select: {
                id: true,
                title: true,
                overview: true,
                tagline: true,
                genres: true,
                release_date: true,
                runtime: true,
                vote_average: true,
                vote_count: true,
                budget: true,
                revenue: true,
                poster_path: true,
                imdb_id: true,
                imdb_rating: true,
                imdb_votes: true,
                production_companies: true,
                cast: true,
                director: true,
                writers: true,
            },
        });
        if (!movie) {
            throw new error_handler_1.NotFoundError(`找不到ID为 ${req.params.id} 的电影`);
        }
        return Object.assign(Object.assign({}, movie), { vote_average: ((_b = movie.vote_average) === null || _b === void 0 ? void 0 : _b.toString()) || "0", vote_count: ((_c = movie.vote_count) === null || _c === void 0 ? void 0 : _c.toString()) || "0", runtime: ((_d = movie.runtime) === null || _d === void 0 ? void 0 : _d.toString()) || "0", imdb_rating: ((_e = movie.imdb_rating) === null || _e === void 0 ? void 0 : _e.toString()) || "0", imdb_votes: ((_f = movie.imdb_votes) === null || _f === void 0 ? void 0 : _f.toString()) || "0", release_date: (_g = movie.release_date) === null || _g === void 0 ? void 0 : _g.toISOString().split("T")[0], budget: movie.budget.toString(), revenue: movie.revenue.toString(), production_companies: movie.production_companies, cast: movie.cast });
    }
    catch (error) {
        if (error instanceof error_handler_1.NotFoundError) {
            throw error;
        }
        logger_1.default.error(`获取电影详情失败 [ID:${req.params.id}]:`, error);
        throw error;
    }
}));
/**
 * 搜索电影
 * @param req 请求对象，包含搜索关键词和分页参数
 * @param res 响应对象
 * @returns 返回匹配搜索条件的电影列表
 */
MovieController.searchMovies = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { query, page = 1, pageSize = 20 } = req.query;
    if (!query || query.trim().length < 2) {
        throw new error_handler_1.ValidationError("搜索关键词至少需要2个字符");
    }
    try {
        // 构建搜索条件，支持标题、标语和简介
        const searchCondition = {
            OR: [
                {
                    title: {
                        contains: query,
                        mode: "insensitive",
                    },
                },
                {
                    tagline: {
                        contains: query,
                        mode: "insensitive",
                    },
                },
                {
                    overview: {
                        contains: query,
                        mode: "insensitive",
                    },
                },
            ],
        };
        const [movies, total] = yield Promise.all([
            prisma.movie.findMany({
                where: searchCondition,
                skip: (page - 1) * pageSize,
                take: pageSize,
                orderBy: { vote_average: "desc" },
                select: {
                    id: true,
                    title: true,
                    release_date: true,
                    vote_average: true,
                    genres: true,
                    poster_path: true,
                    overview: true,
                    imdb_id: true,
                },
            }),
            prisma.movie.count({ where: searchCondition }),
        ]);
        return {
            data: movies.map((m) => {
                var _b, _c;
                return (Object.assign(Object.assign({}, m), { vote_average: ((_b = m.vote_average) === null || _b === void 0 ? void 0 : _b.toString()) || "0", release_date: (_c = m.release_date) === null || _c === void 0 ? void 0 : _c.toISOString().split("T")[0] }));
            }),
            meta: {
                total,
                page,
                pageSize,
                totalPages: Math.ceil(total / pageSize),
                query,
            },
        };
    }
    catch (error) {
        logger_1.default.error(`搜索电影失败 [关键词:${query}]:`, error);
        throw error;
    }
}));
/**
 * 获取所有电影类型及其电影数量
 * @param req 请求对象
 * @param res 响应对象
 * @returns 返回所有电影类型及其对应的电影数量
 */
MovieController.getGenres = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // 直接使用原生SQL查询，聚合所有类型
        const genresResult = yield prisma.$queryRaw `
                SELECT unnest(genres) as genre, COUNT(*) as movie_count
                FROM "Movie"
                GROUP BY genre
                ORDER BY movie_count DESC
            `;
        return {
            data: Array.isArray(genresResult) ? genresResult.map((item) => {
                var _b;
                return (Object.assign(Object.assign({}, item), { movie_count: ((_b = item.movie_count) === null || _b === void 0 ? void 0 : _b.toString()) || "0" }));
            }) : [],
            meta: {
                total: Array.isArray(genresResult) ? genresResult.length : 0,
            },
        };
    }
    catch (error) {
        logger_1.default.error("获取电影类型统计失败:", error);
        throw error;
    }
}));
/**
 * 获取按年份分组的电影数量
 * @param req 请求对象
 * @param res 响应对象
 * @returns 返回按年份分组的电影数量统计
 */
MovieController.getMoviesByYear = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // 使用原生SQL按年份分组
        const yearlyMovies = yield prisma.$queryRaw `
                SELECT EXTRACT(YEAR FROM release_date) as year, COUNT(*) as movie_count
                FROM "Movie"
                WHERE release_date IS NOT NULL
                GROUP BY year
                ORDER BY year DESC
            `;
        return {
            data: Array.isArray(yearlyMovies) ? yearlyMovies.map((item) => {
                var _b, _c;
                return (Object.assign(Object.assign({}, item), { year: ((_b = item.year) === null || _b === void 0 ? void 0 : _b.toString()) || "0", movie_count: ((_c = item.movie_count) === null || _c === void 0 ? void 0 : _c.toString()) || "0" }));
            }) : [],
            meta: {
                total: Array.isArray(yearlyMovies) ? yearlyMovies.length : 0,
            },
        };
    }
    catch (error) {
        logger_1.default.error("获取电影年份统计失败:", error);
        throw error;
    }
}));
/**
 * 获取随机推荐电影
 * @param req 请求对象，包含推荐数量和最低评分参数
 * @param res 响应对象
 * @returns 返回随机推荐的电影列表
 */
MovieController.getRandomMovies = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { count = 5, minRating = 7.0 } = req.query;
    try {
        // 使用原生SQL随机获取电影
        const randomMovies = yield prisma.$queryRaw `
                SELECT id, title, release_date, vote_average, poster_path
                FROM "Movie"
                WHERE vote_average >= ${minRating}
                ORDER BY RANDOM()
                LIMIT ${count}
            `;
        return {
            data: Array.isArray(randomMovies)
                ? randomMovies.map((m) => {
                    var _b;
                    return (Object.assign(Object.assign({}, m), { vote_average: ((_b = m.vote_average) === null || _b === void 0 ? void 0 : _b.toString()) || "0", release_date: m.release_date
                            ? new Date(m.release_date).toISOString().split("T")[0]
                            : null }));
                })
                : [],
            meta: {
                count: Array.isArray(randomMovies) ? randomMovies.length : 0,
            },
        };
    }
    catch (error) {
        logger_1.default.error("获取随机推荐电影失败:", error);
        throw error;
    }
}));
/**
 * 获取电影数据统计信息
 * @param req 请求对象
 * @param res 响应对象
 * @returns 返回电影数据的各项统计信息
 */
MovieController.getMovieStats = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _b, _c, _d, _e, _f, _g, _h;
    try {
        const [totalMovies, avgRating, highestRated, mostPopular, longestMovie,] = yield Promise.all([
            // 总电影数
            prisma.movie.count(),
            // 平均评分
            prisma.$queryRaw `
                    SELECT AVG(vote_average) as avg_rating
                    FROM "Movie"
                    WHERE vote_count > 10
                `,
            // 评分最高的电影
            prisma.movie.findMany({
                where: {
                    vote_count: { gt: 100 }, // 至少100票才有代表性
                },
                orderBy: { vote_average: "desc" },
                take: 1,
                select: {
                    id: true,
                    title: true,
                    vote_average: true,
                    release_date: true,
                },
            }),
            // 最受欢迎的电影(票数最多)
            prisma.movie.findMany({
                orderBy: { vote_count: "desc" },
                take: 1,
                select: {
                    id: true,
                    title: true,
                    vote_count: true,
                    release_date: true,
                },
            }),
            // 最长的电影
            prisma.movie.findMany({
                where: {
                    runtime: { not: null },
                },
                orderBy: { runtime: "desc" },
                take: 1,
                select: {
                    id: true,
                    title: true,
                    runtime: true,
                    release_date: true,
                },
            }),
        ]);
        return {
            totalMovies,
            avgRating: Array.isArray(avgRating) && avgRating.length > 0
                ? ((_b = avgRating[0].avg_rating) === null || _b === void 0 ? void 0 : _b.toString()) || "0"
                : "0",
            highestRated: highestRated.length > 0
                ? Object.assign(Object.assign({}, highestRated[0]), { vote_average: ((_c = highestRated[0].vote_average) === null || _c === void 0 ? void 0 : _c.toString()) || "0", release_date: (_d = highestRated[0].release_date) === null || _d === void 0 ? void 0 : _d.toISOString().split("T")[0] }) : null,
            mostPopular: mostPopular.length > 0
                ? Object.assign(Object.assign({}, mostPopular[0]), { vote_count: ((_e = mostPopular[0].vote_count) === null || _e === void 0 ? void 0 : _e.toString()) || "0", release_date: (_f = mostPopular[0].release_date) === null || _f === void 0 ? void 0 : _f.toISOString().split("T")[0] }) : null,
            longestMovie: longestMovie.length > 0
                ? Object.assign(Object.assign({}, longestMovie[0]), { runtime: ((_g = longestMovie[0].runtime) === null || _g === void 0 ? void 0 : _g.toString()) || "0", release_date: (_h = longestMovie[0].release_date) === null || _h === void 0 ? void 0 : _h.toISOString().split("T")[0] }) : null,
        };
    }
    catch (error) {
        logger_1.default.error("获取电影统计信息失败:", error);
        throw error;
    }
}));
/**
 * 获取相似电影推荐
 * @param req 请求对象，包含电影ID和推荐数量参数
 * @param res 响应对象
 * @returns 返回与指定电影相似的电影列表
 */
MovieController.getSimilarMovies = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { id } = req.params;
    const { limit = 5 } = req.query;
    try {
        // 先获取目标电影的信息
        const movie = yield prisma.movie.findUnique({
            where: { id },
            select: {
                genres: true,
                director: true,
            },
        });
        if (!movie) {
            throw new error_handler_1.NotFoundError(`找不到ID为 ${id} 的电影`);
        }
        // 基于类型和导演查找相似电影
        const similarMovies = yield prisma.movie.findMany({
            where: {
                id: { not: id },
                OR: [
                    // 相同类型
                    ...(movie.genres && movie.genres.length > 0
                        ? [{ genres: { hasSome: movie.genres } }]
                        : []),
                    // 相同导演
                    ...(movie.director && movie.director.length > 0
                        ? [{ director: { hasSome: movie.director } }]
                        : []),
                ],
            },
            orderBy: { vote_average: "desc" },
            take: limit,
            select: {
                id: true,
                title: true,
                release_date: true,
                vote_average: true,
                genres: true,
                poster_path: true,
                imdb_id: true,
            },
        });
        return {
            data: similarMovies.map((m) => {
                var _b, _c;
                return (Object.assign(Object.assign({}, m), { vote_average: ((_b = m.vote_average) === null || _b === void 0 ? void 0 : _b.toString()) || "0", release_date: (_c = m.release_date) === null || _c === void 0 ? void 0 : _c.toISOString().split("T")[0] }));
            }),
            meta: {
                count: similarMovies.length,
                sourceId: id,
            },
        };
    }
    catch (error) {
        if (error instanceof error_handler_1.NotFoundError) {
            throw error;
        }
        logger_1.default.error(`获取相似电影失败 [ID:${id}]:`, error);
        throw error;
    }
}));
/**
 * 获取所有电影（不分页）
 * @param req 请求对象，包含筛选条件参数
 * @param res 响应对象
 * @returns 返回所有符合条件的电影列表
 */
MovieController.getAllMovies = (0, error_handler_1.asyncHandler)((req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { genre, minRating, year, sortBy = "release_date", } = req.query;
    const where = {};
    if (genre) {
        where.genres = { has: genre };
    }
    if (minRating) {
        where.vote_average = { gte: minRating };
    }
    if (year) {
        where.release_date = {
            gte: new Date(`${year}-01-01`),
            lte: new Date(`${year}-12-31`),
        };
    }
    try {
        const movies = yield prisma.movie.findMany({
            where,
            orderBy: { [sortBy]: "desc" },
            select: {
                id: true,
                title: true,
                release_date: true,
                vote_average: true,
                genres: true,
                poster_path: true,
                runtime: true,
                director: true,
                budget: true,
                revenue: true,
                imdb_id: true,
                production_countries: true,
            },
        });
        return {
            data: movies.map((m) => {
                var _b, _c, _d;
                return (Object.assign(Object.assign({}, m), { release_date: (_b = m.release_date) === null || _b === void 0 ? void 0 : _b.toISOString().split("T")[0], budget: ((_c = m.budget) === null || _c === void 0 ? void 0 : _c.toString()) || "0", revenue: ((_d = m.revenue) === null || _d === void 0 ? void 0 : _d.toString()) || "0" }));
            }),
            meta: {
                total: movies.length,
            },
        };
    }
    catch (error) {
        logger_1.default.error("获取所有电影列表失败:", error);
        throw error;
    }
}));
