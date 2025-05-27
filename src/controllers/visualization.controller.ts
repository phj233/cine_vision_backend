import { PrismaClient } from "@prisma/client";
import { FastifyRequest, FastifyReply } from "fastify";
import {
    asyncHandler,
    ValidationError,
} from "@/utils/error-handler";
import logger from "@/utils/logger";

const prisma = new PrismaClient();

/**
 * 数据可视化控制器类
 * 提供电影数据的各类统计和可视化API接口实现
 */
export class VisualizationController {
    /**
     * 获取电影评分分布
     * @param req 请求对象，包含评分区间大小参数
     * @param res 响应对象
     * @returns 返回不同评分区间的电影数量分布
     */
    static getRatingDistribution = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: { bucketSize?: number };
            }>,
            res: FastifyReply,
        ) => {
            // 评分区间大小，默认为0.5
            const { bucketSize = 0.5 } = req.query;
            // 确保bucketSize是数字类型
            const bucketSizeNum = Number(bucketSize);

            try {
                // 使用SQL生成评分分布
                const ratingDistribution = await prisma.$queryRaw`
                WITH rating_buckets AS (
                    SELECT 
                        FLOOR(vote_average / ${bucketSizeNum}) * ${bucketSizeNum} as bucket_start,
                        COUNT(*) as movie_count
                    FROM "Movie"
                    WHERE vote_average > 0 AND vote_count > 10
                    GROUP BY bucket_start
                    ORDER BY bucket_start
                )
                SELECT 
                    bucket_start,
                    bucket_start + ${bucketSizeNum} as bucket_end,
                    movie_count
                FROM rating_buckets
            `;

                return {
                    data: Array.isArray(ratingDistribution) ? ratingDistribution.map((bucket: any) => ({
                        ...bucket,
                        bucket_start: bucket.bucket_start?.toString() || "0",
                        bucket_end: bucket.bucket_end?.toString() || "0"
                    })) : [],
                    meta: {
                        bucketSize,
                        count: Array.isArray(ratingDistribution)
                            ? ratingDistribution.length
                            : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取电影评分分布失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取每年发行电影数量和平均评分趋势
     * @param req 请求对象，包含开始年份、结束年份和最小投票数参数
     * @param res 响应对象
     * @returns 返回年度电影数据趋势，包括发行数量、平均评分和预算
     */
    static getYearlyTrends = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    startYear?: number;
                    endYear?: number;
                    minVotes?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const currentYear = new Date().getFullYear();
            const {
                startYear = 1990,
                endYear = currentYear,
                minVotes = 10,
            } = req.query;

            // 确保所有参数都是数字类型
            const startYearNum = Number(startYear);
            const endYearNum = Number(endYear);
            const minVotesNum = Number(minVotes);

            if (startYearNum > endYearNum) {
                throw new ValidationError("起始年份不能大于结束年份");
            }

            try {
                // 使用SQL获取年度趋势数据
                const yearlyTrends = await prisma.$queryRaw`
                SELECT 
                    EXTRACT(YEAR FROM release_date) as year,
                    COUNT(*) as movie_count,
                    ROUND(AVG(vote_average)::numeric, 2) as avg_rating,
                    ROUND(AVG(budget)::numeric, 2) as avg_budget,
                    ROUND(AVG(revenue)::numeric, 2) as avg_revenue
                FROM "Movie"
                WHERE 
                    release_date IS NOT NULL 
                    AND EXTRACT(YEAR FROM release_date) BETWEEN ${startYearNum} AND ${endYearNum}
                    AND vote_count >= ${minVotesNum}
                GROUP BY year
                ORDER BY year ASC
            `;

                return {
                    data: Array.isArray(yearlyTrends) ? yearlyTrends.map((trend: any) => ({
                        ...trend,
                        year: trend.year?.toString() || "0",
                        avg_budget: trend.avg_budget?.toString() || "0",
                        avg_revenue: trend.avg_revenue?.toString() || "0"
                    })) : [],
                    meta: {
                        startYear,
                        endYear,
                        minVotes,
                        count: Array.isArray(yearlyTrends) ? yearlyTrends.length : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取年度趋势数据失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取不同电影类型的对比数据
     * @param req 请求对象，包含电影类型列表和比较指标参数
     * @param res 响应对象
     * @returns 返回不同电影类型在指定指标上的对比数据
     */
    static getGenreComparison = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    genres?: string;
                    metric?: string;
                };
            }>,
            res: FastifyReply,
        ) => {
            let { genres, metric = "avg_rating" } = req.query;

            // 默认分析前10个最受欢迎的类型
            let topGenres: any[] = [];
            if (!genres) {
                // 获取排名前10的类型
                topGenres = await prisma.$queryRaw`
                SELECT unnest(genres) as genre, COUNT(*) as count
                FROM "Movie"
                GROUP BY genre
                ORDER BY count DESC
                LIMIT 10
            `;
                genres = topGenres.map((g: any) => g.genre).join(",");
            }

            // 验证指标
            const validMetrics = [
                "avg_rating",
                "avg_budget",
                "avg_revenue",
                "count",
                "avg_runtime",
            ];
            if (!validMetrics.includes(metric)) {
                throw new ValidationError(
                    "无效的指标，有效值为: " + validMetrics.join(", "),
                );
            }

            const genreList = genres.split(",").map((g) => g.trim());

            try {
                // 基于指标构建SQL查询
                let queryMetric = "";
                let queryTitle = "";

                switch (metric) {
                    case "avg_rating":
                        queryMetric = "ROUND(AVG(vote_average)::numeric, 2)";
                        queryTitle = "平均评分";
                        break;
                    case "avg_budget":
                        queryMetric = "ROUND(AVG(budget)::numeric, 2)";
                        queryTitle = "平均预算";
                        break;
                    case "avg_revenue":
                        queryMetric = "ROUND(AVG(revenue)::numeric, 2)";
                        queryTitle = "平均票房";
                        break;
                    case "count":
                        queryMetric = "COUNT(*)";
                        queryTitle = "电影数量";
                        break;
                    case "avg_runtime":
                        queryMetric = "ROUND(AVG(runtime)::numeric, 2)";
                        queryTitle = "平均时长";
                        break;
                }

                // 为每个类型查询数据
                const genreResults = [];
                for (const genre of genreList) {
                    // 使用动态构建的查询，但需要使用模板字符串正确处理
                    let result;
                    if (metric === "avg_rating") {
                        result = await prisma.$queryRaw`
                            SELECT ROUND(AVG(vote_average)::numeric, 2) as value
                            FROM "Movie"
                            WHERE ${genre} = ANY(genres)
                            AND vote_count > 10
                        `;
                    } else if (metric === "avg_budget") {
                        result = await prisma.$queryRaw`
                            SELECT ROUND(AVG(budget)::numeric, 2) as value
                            FROM "Movie"
                            WHERE ${genre} = ANY(genres)
                            AND vote_count > 10
                        `;
                    } else if (metric === "avg_revenue") {
                        result = await prisma.$queryRaw`
                            SELECT ROUND(AVG(revenue)::numeric, 2) as value
                            FROM "Movie"
                            WHERE ${genre} = ANY(genres)
                            AND vote_count > 10
                        `;
                    } else if (metric === "count") {
                        result = await prisma.$queryRaw`
                            SELECT COUNT(*) as value
                            FROM "Movie"
                            WHERE ${genre} = ANY(genres)
                            AND vote_count > 10
                        `;
                    } else if (metric === "avg_runtime") {
                        result = await prisma.$queryRaw`
                            SELECT ROUND(AVG(runtime)::numeric, 2) as value
                            FROM "Movie"
                            WHERE ${genre} = ANY(genres)
                            AND vote_count > 10
                        `;
                    }

                    genreResults.push({
                        genre,
                        value:
                            Array.isArray(result) && result.length > 0
                                ? metric === "avg_budget" || metric === "avg_revenue"
                                    ? result[0].value?.toString() || "0"
                                    : result[0].value
                                : null,
                        metric: queryTitle,
                    });
                }

                return {
                    data: genreResults,
                    meta: {
                        genres: genreList,
                        metric,
                        metricTitle: queryTitle,
                        count: genreResults.length,
                    },
                };
            } catch (error: any) {
                logger.error("获取类型对比数据失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取电影时长分布
     * @param req 请求对象，包含时长区间大小和最小投票数参数
     * @param res 响应对象
     * @returns 返回不同时长区间的电影数量分布
     */
    static getRuntimeDistribution = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    bucketSize?: number;
                    minVotes?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { bucketSize = 15, minVotes = 10 } = req.query;

            // 确保参数都是数字类型
            const bucketSizeNum = Number(bucketSize);
            const minVotesNum = Number(minVotes);

            try {
                // 使用SQL获取时长分布
                const runtimeDistribution = await prisma.$queryRaw`
                WITH runtime_buckets AS (
                    SELECT 
                        FLOOR(runtime / ${bucketSizeNum}) * ${bucketSizeNum} as bucket_start,
                        COUNT(*) as movie_count
                    FROM "Movie"
                    WHERE runtime IS NOT NULL AND runtime > 0 AND vote_count >= ${minVotesNum}
                    GROUP BY bucket_start
                    ORDER BY bucket_start
                )
                SELECT 
                    bucket_start,
                    bucket_start + ${bucketSizeNum} as bucket_end,
                    movie_count
                FROM runtime_buckets
            `;

                return {
                    data: Array.isArray(runtimeDistribution) ? runtimeDistribution.map((bucket: any) => ({
                        ...bucket,
                        bucket_start: bucket.bucket_start?.toString() || "0",
                        bucket_end: bucket.bucket_end?.toString() || "0"
                    })) : [],
                    meta: {
                        bucketSize,
                        minVotes,
                        count: Array.isArray(runtimeDistribution)
                            ? runtimeDistribution.length
                            : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取电影时长分布失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取顶级制作公司排名分析
     * @param req 请求对象，包含返回数量、最小电影数和排序指标参数
     * @param res 响应对象
     * @returns 返回按指定指标排序的顶级制作公司数据
     */
    static getTopProductionCompanies = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    limit?: number;
                    minMovies?: number;
                    metric?: string;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { limit = 10, minMovies = 5, metric = "movie_count" } = req.query;

            // 确保数值参数都是数字类型
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            // 验证指标
            const validMetrics = [
                "movie_count",
                "avg_rating",
                "total_revenue",
                "avg_revenue",
            ];
            if (!validMetrics.includes(metric)) {
                throw new ValidationError(
                    "无效的指标，有效值为: " + validMetrics.join(", "),
                );
            }

            try {
                // 首先提取所有制作公司
                const productionCompanies = await prisma.$queryRaw`
                WITH companies AS (
                    SELECT 
                        c->>'name' as company_name,
                        m.id,
                        m.vote_average,
                        m.revenue
                    FROM 
                        "Movie" m,
                        jsonb_array_elements(m.production_companies::jsonb) as c
                    WHERE 
                        c->>'name' IS NOT NULL AND 
                        c->>'name' != '' AND
                        vote_count > 10
                )
                SELECT 
                    company_name,
                    COUNT(*) as movie_count,
                    ROUND(AVG(vote_average)::numeric, 2) as avg_rating,
                    SUM(revenue) as total_revenue,
                    ROUND(AVG(revenue)::numeric, 2) as avg_revenue
                FROM companies
                GROUP BY company_name
                HAVING COUNT(*) >= ${minMoviesNum}
                ORDER BY ${metric} DESC
                LIMIT ${limitNum}
            `;

                return {
                    data: Array.isArray(productionCompanies) ? productionCompanies.map((company: any) => ({
                        ...company,
                        total_revenue: company.total_revenue?.toString() || "0",
                        avg_revenue: company.avg_revenue?.toString() || "0"
                    })) : [],
                    meta: {
                        limit,
                        minMovies,
                        metric,
                        count: Array.isArray(productionCompanies)
                            ? productionCompanies.length
                            : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取制作公司排名失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取演员合作网络数据
     * @param req 请求对象，包含演员名称、最小合作次数和返回数量参数
     * @param res 响应对象
     * @returns 返回与指定演员合作的其他演员及合作电影数据
     */
    static getActorCollaborations = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    actor?: string;
                    minCollaborations?: number;
                    limit?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            let { actor, minCollaborations = 2, limit = 20 } = req.query;

            // 确保数值参数都是数字类型
            const minCollaborationsNum = Number(minCollaborations);
            const limitNum = Number(limit);

            if (!actor) {
                throw new ValidationError("必须指定演员名称");
            }

            try {
                // 查找与指定演员合作的其他演员
                const collaborations = await prisma.$queryRaw`
                WITH actor_movies AS (
                    SELECT id
                    FROM "Movie"
                    WHERE ${actor} = ANY(cast)
                ),
                co_actors AS (
                    SELECT 
                        unnest(cast) as co_actor,
                        COUNT(*) as collaboration_count
                    FROM "Movie"
                    WHERE 
                        id IN (SELECT id FROM actor_movies) AND
                        ${actor} = ANY(cast)
                    GROUP BY co_actor
                    HAVING unnest(cast) != ${actor}
                    ORDER BY collaboration_count DESC
                )
                SELECT 
                    co_actor, 
                    collaboration_count
                FROM co_actors
                WHERE collaboration_count >= ${minCollaborationsNum}
                LIMIT ${limitNum}
            `;

                // 获取合作电影列表 - 使用原生SQL
                const collaborationMovies = await prisma.$queryRaw`
                    SELECT id, title, release_date, cast
                    FROM "Movie"
                    WHERE ${actor} = ANY(cast)
                    ORDER BY release_date DESC
                `;

                return {
                    data: {
                        actor,
                        collaborations,
                        movies: Array.isArray(collaborationMovies)
                            ? collaborationMovies.map((m: any) => ({
                                ...m,
                                release_date: m.release_date ? new Date(m.release_date).toISOString().split("T")[0] : null,
                            }))
                            : [],
                    },
                    meta: {
                        collaborationCount: Array.isArray(collaborations)
                            ? collaborations.length
                            : 0,
                        movieCount: Array.isArray(collaborationMovies) ? collaborationMovies.length : 0,
                    },
                };
            } catch (error: any) {
                logger.error(`获取演员合作网络数据失败 [演员:${actor}]:`, error);
                throw error;
            }
        },
    );

    /**
     * 获取预算与票房关系分析
     * @param req 请求对象，包含年份范围、最小投票数和预算区间大小参数
     * @param res 响应对象
     * @returns 返回不同预算区间的平均票房和投资回报率分析
     */
    static getBudgetRevenueAnalysis = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    startYear?: number;
                    endYear?: number;
                    minVotes?: number;
                    bucketSize?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const currentYear = new Date().getFullYear();
            let { startYear = 1900, endYear = currentYear, minVotes = 10, bucketSize = 1000000 } = req.query;
            // 确保所有参数都是数字类型
            const startYearNum = Number(startYear);
            const endYearNum = Number(endYear);
            const minVotesNum = Number(minVotes);
            const bucketSizeNum = Number(bucketSize);

            try {
                // 分析预算与票房关系
                const budgetVsRevenue = await prisma.$queryRaw`
                WITH budget_buckets AS (
                    SELECT 
                        FLOOR(budget / ${bucketSizeNum}) * ${bucketSizeNum} as budget_bucket,
                        AVG(revenue) as avg_revenue,
                        AVG(revenue / NULLIF(budget, 0)) as avg_roi,
                        COUNT(*) as movie_count
                    FROM "Movie"
                    WHERE 
                        budget > 0 AND 
                        revenue > 0 AND
                        vote_count >= ${minVotesNum} AND
                        release_date IS NOT NULL AND
                        EXTRACT(YEAR FROM release_date) BETWEEN ${startYearNum} AND ${endYearNum}
                    GROUP BY budget_bucket
                    ORDER BY budget_bucket
                )
                SELECT 
                    budget_bucket,
                    budget_bucket + ${bucketSizeNum} as budget_bucket_end,
                    ROUND(avg_revenue::numeric, 2) as avg_revenue,
                    ROUND(avg_roi::numeric, 2) as avg_roi,
                    movie_count
                FROM budget_buckets
            `;

                // 计算整体统计数据
                const overallStats = await prisma.$queryRaw`
                SELECT 
                    ROUND(AVG(revenue / NULLIF(budget, 0))::numeric, 2) as avg_roi,
                    ROUND(AVG(revenue)::numeric, 2) as avg_revenue,
                    ROUND(AVG(budget)::numeric, 2) as avg_budget,
                    COUNT(*) as total_movies
                FROM "Movie"
                WHERE 
                    budget > 0 AND 
                    revenue > 0 AND
                    vote_count >= ${minVotesNum} AND
                    release_date IS NOT NULL AND
                    EXTRACT(YEAR FROM release_date) BETWEEN ${startYearNum} AND ${endYearNum}
            `;

                return {
                    data: {
                        buckets: Array.isArray(budgetVsRevenue) ? budgetVsRevenue.map((bucket: any) => ({
                            ...bucket,
                            budget_bucket: bucket.budget_bucket?.toString() || "0",
                            budget_bucket_end: bucket.budget_bucket_end?.toString() || "0",
                            avg_revenue: bucket.avg_revenue?.toString() || "0"
                        })) : [],
                        overall:
                            Array.isArray(overallStats) && overallStats.length > 0
                                ? {
                                    ...overallStats[0],
                                    avg_revenue: overallStats[0].avg_revenue?.toString() || "0",
                                    avg_budget: overallStats[0].avg_budget?.toString() || "0"
                                }
                                : null,
                    },
                    meta: {
                        startYear,
                        endYear,
                        minVotes,
                        bucketSize,
                        bucketCount: Array.isArray(budgetVsRevenue)
                            ? budgetVsRevenue.length
                            : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取预算与票房关系分析失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取电影类型及其统计信息
     * @param req 请求对象
     * @param res 响应对象
     * @returns 返回所有电影类型的详细统计数据，包括电影数量、平均评分、总票房和平均预算
     */
    static getGenresWithStats = asyncHandler(
        async (req: FastifyRequest, res: FastifyReply) => {
            try {
                // 查询所有类型及其详细统计信息
                const genresWithStats = await prisma.$queryRaw`
                    WITH genre_stats AS (
                        SELECT 
                            unnest(genres) as genre,
                            COUNT(*) as total_movies,
                            ROUND(AVG(vote_average)::numeric, 2) as avg_rating,
                            SUM(revenue) as total_revenue,
                            ROUND(AVG(budget)::numeric, 2) as avg_budget
                        FROM "Movie"
                        GROUP BY genre
                    )
                    SELECT 
                        genre,
                        total_movies,
                        avg_rating,
                        total_revenue,
                        avg_budget
                    FROM genre_stats
                    ORDER BY total_movies DESC
                `;

                return {
                    data: Array.isArray(genresWithStats) ? genresWithStats.map((genre: any) => ({
                        ...genre,
                        total_revenue: genre.total_revenue?.toString() || "0"
                    })) : [],
                    meta: {
                        total: Array.isArray(genresWithStats) ? genresWithStats.length : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取电影类型统计信息失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取电影语言分布分析
     * @param req 请求对象，包含最小电影数量参数
     * @param res 响应对象
     * @returns 返回不同语言的电影数量分布及其平均评分
     */
    static getLanguageDistribution = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    minMovies?: number;
                    limit?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { minMovies = 5, limit = 20 } = req.query;
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            try {
                // 确定要使用的LIMIT值
                let actualLimit = 20; // 默认值
                if (limitNum <= 10) {
                    actualLimit = 10;
                } else if (limitNum <= 20) {
                    actualLimit = 20;
                } else {
                    actualLimit = 50;
                }

                // 构建基本查询 - 确保使用::text避免BigInt问题
                const query = `
                    SELECT 
                        original_language,
                        COUNT(*)::text as movie_count,
                        ROUND(AVG(vote_average)::numeric, 2) as avg_rating,
                        MIN(release_date) as earliest_film,
                        MAX(release_date) as latest_film
                    FROM "Movie"
                    WHERE original_language IS NOT NULL AND original_language != ''
                    GROUP BY original_language
                    HAVING COUNT(*) >= $1
                    ORDER BY COUNT(*) DESC
                `;

                // 根据actualLimit选择不同的查询
                let languageDistribution;
                if (actualLimit === 10) {
                    languageDistribution = await prisma.$queryRawUnsafe(query + ' LIMIT 10', minMoviesNum);
                } else if (actualLimit === 20) {
                    languageDistribution = await prisma.$queryRawUnsafe(query + ' LIMIT 20', minMoviesNum);
                } else {
                    languageDistribution = await prisma.$queryRawUnsafe(query + ' LIMIT 50', minMoviesNum);
                }

                // 获取语言总体分布比例
                const totalMoviesCount = await prisma.movie.count({
                    where: {
                        original_language: {
                            not: null,
                        },
                    },
                });

                return {
                    data: Array.isArray(languageDistribution)
                        ? languageDistribution.map((lang: any) => ({
                            ...lang,
                            movie_count: lang.movie_count || "0",
                            avg_rating: lang.avg_rating?.toString() || "0",
                            earliest_film: lang.earliest_film
                                ? new Date(lang.earliest_film).toISOString().split("T")[0]
                                : null,
                            latest_film: lang.latest_film
                                ? new Date(lang.latest_film).toISOString().split("T")[0]
                                : null,
                            percentage: totalMoviesCount > 0
                                ? ((Number(lang.movie_count) / totalMoviesCount) * 100).toFixed(2)
                                : "0",
                        }))
                        : [],
                    meta: {
                        total: totalMoviesCount,
                        count: Array.isArray(languageDistribution)
                            ? languageDistribution.length
                            : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取电影语言分布失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取顶级导演作品分析
     * @param req 请求对象，包含返回数量和最小电影数量参数
     * @param res 响应对象
     * @returns 返回顶级导演的电影数量、平均评分和代表作品
     */
    static getTopDirectors = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    limit?: number;
                    minMovies?: number;
                    sortBy?: string;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { limit = 20, minMovies = 3, sortBy = "movie_count" } = req.query;
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            // 验证排序字段
            const validSortFields = ["movie_count", "avg_rating", "total_revenue"];
            if (!validSortFields.includes(sortBy)) {
                throw new ValidationError(
                    "无效的排序字段，有效值为: " + validSortFields.join(", ")
                );
            }

            try {
                // 使用简单的SQL查询而不使用嵌套子查询和LIMIT
                const query = `
                    SELECT 
                        unnest(director) as director_name,
                        COUNT(*) as movie_count,
                        ROUND(AVG(vote_average)::numeric, 2) as avg_rating,
                        SUM(revenue) as total_revenue
                    FROM "Movie"
                    WHERE 
                        director IS NOT NULL AND 
                        array_length(director, 1) > 0
                    GROUP BY director_name
                    HAVING COUNT(*) >= $1
                `;

                // 执行基本查询
                const directorsData = await prisma.$queryRawUnsafe(query, minMoviesNum);

                // 在JavaScript中处理排序和截断
                let sortedData = [];
                if (Array.isArray(directorsData)) {
                    if (sortBy === "movie_count") {
                        sortedData = directorsData.sort((a, b) => Number(b.movie_count) - Number(a.movie_count));
                    } else if (sortBy === "avg_rating") {
                        sortedData = directorsData.sort((a, b) => Number(b.avg_rating) - Number(a.avg_rating));
                    } else if (sortBy === "total_revenue") {
                        sortedData = directorsData.sort((a, b) => Number(b.total_revenue) - Number(a.total_revenue));
                    }

                    // 限制结果数量
                    sortedData = sortedData.slice(0, limitNum);

                    // 为每个导演获取他们的热门电影
                    for (const director of sortedData) {
                        const topMovies = await prisma.movie.findMany({
                            where: {
                                director: {
                                    has: director.director_name
                                }
                            },
                            select: {
                                id: true,
                                title: true,
                                vote_average: true,
                                release_date: true
                            },
                            orderBy: {
                                vote_average: 'desc'
                            },
                            take: 3
                        });

                        director.top_movies = topMovies;
                    }
                }

                return {
                    data: sortedData.map((director: any) => ({
                        ...director,
                        movie_count: director.movie_count?.toString() || "0",
                        avg_rating: director.avg_rating?.toString() || "0",
                        total_revenue: director.total_revenue?.toString() || "0",
                        top_movies: Array.isArray(director.top_movies)
                            ? director.top_movies.map((movie: any) => ({
                                ...movie,
                                vote_average: movie.vote_average?.toString() || "0",
                                release_date: movie.release_date
                                    ? new Date(movie.release_date).toISOString().split("T")[0]
                                    : null,
                            }))
                            : [],
                    })),
                    meta: {
                        limit: limitNum,
                        minMovies: minMoviesNum,
                        sortBy,
                        count: sortedData.length,
                    },
                };
            } catch (error: any) {
                logger.error("获取顶级导演分析失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取摄影指导作品分析
     * @param req 请求对象，包含返回数量和最小电影数量参数
     * @param res 响应对象
     * @returns 返回顶级摄影指导的电影数量、平均评分和代表作品
     */
    static getTopCinematographers = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    limit?: number;
                    minMovies?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { limit = 15, minMovies = 2 } = req.query;
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            try {
                // 使用简单的SQL查询而不使用嵌套子查询和LIMIT
                const query = `
                    SELECT 
                        unnest(director_of_photography) as cinematographer_name,
                        COUNT(*) as movie_count,
                        ROUND(AVG(vote_average)::numeric, 2) as avg_rating
                    FROM "Movie"
                    WHERE 
                        director_of_photography IS NOT NULL AND 
                        array_length(director_of_photography, 1) > 0
                    GROUP BY cinematographer_name
                    HAVING COUNT(*) >= $1
                    ORDER BY movie_count DESC, avg_rating DESC
                `;

                // 执行基本查询
                const cinematographersData = await prisma.$queryRawUnsafe(query, minMoviesNum);

                // 在JavaScript中处理限制数量
                let resultData = [];
                if (Array.isArray(cinematographersData)) {
                    resultData = cinematographersData.slice(0, limitNum);

                    // 为每个摄影指导获取更多信息
                    for (const cinematographer of resultData) {
                        // 获取热门电影
                        const topMovies = await prisma.movie.findMany({
                            where: {
                                director_of_photography: {
                                    has: cinematographer.cinematographer_name
                                }
                            },
                            select: {
                                id: true,
                                title: true,
                                vote_average: true,
                                release_date: true,
                                director: true
                            },
                            orderBy: {
                                vote_average: 'desc'
                            },
                            take: 3
                        });

                        // 获取合作导演
                        const movies = await prisma.movie.findMany({
                            where: {
                                director_of_photography: {
                                    has: cinematographer.cinematographer_name
                                }
                            },
                            select: {
                                director: true
                            }
                        });

                        // 提取所有合作过的导演
                        const allDirectors = new Set<string>();
                        movies.forEach(movie => {
                            if (Array.isArray(movie.director)) {
                                movie.director.forEach(dir => allDirectors.add(dir));
                            }
                        });

                        cinematographer.top_movies = topMovies;
                        cinematographer.collaborated_directors = Array.from(allDirectors);
                    }
                }

                return {
                    data: resultData.map((cinematographer: any) => ({
                        ...cinematographer,
                        movie_count: cinematographer.movie_count?.toString() || "0",
                        avg_rating: cinematographer.avg_rating?.toString() || "0",
                        top_movies: Array.isArray(cinematographer.top_movies)
                            ? cinematographer.top_movies.map((movie: any) => ({
                                ...movie,
                                vote_average: movie.vote_average?.toString() || "0",
                                release_date: movie.release_date
                                    ? new Date(movie.release_date).toISOString().split("T")[0]
                                    : null,
                            }))
                            : [],
                    })),
                    meta: {
                        limit: limitNum,
                        minMovies: minMoviesNum,
                        count: resultData.length,
                    },
                };
            } catch (error: any) {
                logger.error("获取顶级摄影指导分析失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取音乐作曲家作品分析
     * @param req 请求对象，包含返回数量和最小电影数量参数
     * @param res 响应对象
     * @returns 返回顶级音乐作曲家的电影数量、平均评分和代表作品
     */
    static getTopComposers = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    limit?: number;
                    minMovies?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { limit = 15, minMovies = 2 } = req.query;
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            try {
                // 使用简单的SQL查询而不使用嵌套子查询和LIMIT
                const query = `
                    SELECT 
                        unnest(music_composer) as composer_name,
                        COUNT(*) as movie_count,
                        ROUND(AVG(vote_average)::numeric, 2) as avg_rating
                    FROM "Movie"
                    WHERE 
                        music_composer IS NOT NULL AND 
                        array_length(music_composer, 1) > 0
                    GROUP BY composer_name
                    HAVING COUNT(*) >= $1
                    ORDER BY movie_count DESC, avg_rating DESC
                `;

                // 执行基本查询
                const composersData = await prisma.$queryRawUnsafe(query, minMoviesNum);

                // 在JavaScript中处理限制数量
                let resultData = [];
                if (Array.isArray(composersData)) {
                    resultData = composersData.slice(0, limitNum);

                    // 为每个作曲家获取更多信息
                    for (const composer of resultData) {
                        // 获取热门电影
                        const topMovies = await prisma.movie.findMany({
                            where: {
                                music_composer: {
                                    has: composer.composer_name
                                }
                            },
                            select: {
                                id: true,
                                title: true,
                                vote_average: true,
                                release_date: true
                            },
                            orderBy: {
                                vote_average: 'desc'
                            },
                            take: 3
                        });

                        // 获取类型
                        const movies = await prisma.movie.findMany({
                            where: {
                                music_composer: {
                                    has: composer.composer_name
                                }
                            },
                            select: {
                                genres: true
                            }
                        });

                        // 提取所有类型
                        const allGenres = new Set<string>();
                        movies.forEach(movie => {
                            if (Array.isArray(movie.genres)) {
                                movie.genres.forEach(genre => allGenres.add(genre));
                            }
                        });

                        composer.top_movies = topMovies;
                        composer.genres = Array.from(allGenres);
                    }
                }

                return {
                    data: resultData.map((composer: any) => ({
                        ...composer,
                        movie_count: composer.movie_count?.toString() || "0",
                        avg_rating: composer.avg_rating?.toString() || "0",
                        top_movies: Array.isArray(composer.top_movies)
                            ? composer.top_movies.map((movie: any) => ({
                                ...movie,
                                vote_average: movie.vote_average?.toString() || "0",
                                release_date: movie.release_date
                                    ? new Date(movie.release_date).toISOString().split("T")[0]
                                    : null,
                            }))
                            : [],
                    })),
                    meta: {
                        limit: limitNum,
                        minMovies: minMoviesNum,
                        count: resultData.length,
                    },
                };
            } catch (error: any) {
                logger.error("获取顶级音乐作曲家分析失败:", error);
                throw error;
            }
        },
    );

    /**
     * 获取跨领域电影工作者分析（同时担任多种角色的人员）
     * @param req 请求对象，包含返回数量和角色类型参数
     * @param res 响应对象
     * @returns 返回同时担任多个职位的电影工作者及其作品分析
     */
    static getCrossRoleTalents = asyncHandler(
        async (
            req: FastifyRequest<{
                Querystring: {
                    limit?: number;
                    minMovies?: number;
                };
            }>,
            res: FastifyReply,
        ) => {
            const { limit = 15, minMovies = 2 } = req.query;
            const limitNum = Number(limit);
            const minMoviesNum = Number(minMovies);

            try {
                // 确定要使用的LIMIT值并直接构建完整SQL查询
                let query;
                if (limitNum <= 10) {
                    query = `
                        WITH all_talents AS (
                            SELECT id, title, release_date, vote_average, 
                                unnest(director) as person_name, 'director' as role
                            FROM "Movie"
                            WHERE array_length(director, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(writers) as person_name, 'writer' as role
                            FROM "Movie"
                            WHERE array_length(writers, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(producers) as person_name, 'producer' as role
                            FROM "Movie"
                            WHERE array_length(producers, 1) > 0
                        ),
                        role_counts AS (
                            SELECT 
                                person_name,
                                COUNT(DISTINCT id)::integer as movie_count,
                                array_agg(DISTINCT role) as roles,
                                COUNT(DISTINCT role)::integer as role_count,
                                ROUND(AVG(vote_average)::numeric, 2) as avg_rating
                            FROM all_talents
                            GROUP BY person_name
                            HAVING 
                                COUNT(DISTINCT id) >= $1 AND
                                COUNT(DISTINCT role) > 1
                            ORDER BY role_count DESC, movie_count DESC
                        )
                        SELECT 
                            t.person_name,
                            t.movie_count,
                            t.roles,
                            t.role_count,
                            t.avg_rating,
                            (
                                SELECT jsonb_agg(
                                    jsonb_build_object(
                                        'id', m.id,
                                        'title', m.title,
                                        'vote_average', m.vote_average,
                                        'release_date', m.release_date,
                                        'roles', (
                                            SELECT array_agg(at.role)
                                            FROM all_talents at
                                            WHERE at.id = m.id AND at.person_name = t.person_name
                                        )
                                    )
                                )
                                FROM (
                                    SELECT DISTINCT id
                                    FROM all_talents
                                    WHERE person_name = t.person_name
                                    ORDER BY id
                                    LIMIT 5
                                ) ids
                                JOIN "Movie" m ON m.id = ids.id
                            ) as movies
                        FROM role_counts t
                        LIMIT 10
                    `;
                } else if (limitNum <= 20) {
                    query = `
                        WITH all_talents AS (
                            SELECT id, title, release_date, vote_average, 
                                unnest(director) as person_name, 'director' as role
                            FROM "Movie"
                            WHERE array_length(director, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(writers) as person_name, 'writer' as role
                            FROM "Movie"
                            WHERE array_length(writers, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(producers) as person_name, 'producer' as role
                            FROM "Movie"
                            WHERE array_length(producers, 1) > 0
                        ),
                        role_counts AS (
                            SELECT 
                                person_name,
                                COUNT(DISTINCT id)::integer as movie_count,
                                array_agg(DISTINCT role) as roles,
                                COUNT(DISTINCT role)::integer as role_count,
                                ROUND(AVG(vote_average)::numeric, 2) as avg_rating
                            FROM all_talents
                            GROUP BY person_name
                            HAVING 
                                COUNT(DISTINCT id) >= $1 AND
                                COUNT(DISTINCT role) > 1
                            ORDER BY role_count DESC, movie_count DESC
                        )
                        SELECT 
                            t.person_name,
                            t.movie_count,
                            t.roles,
                            t.role_count,
                            t.avg_rating,
                            (
                                SELECT jsonb_agg(
                                    jsonb_build_object(
                                        'id', m.id,
                                        'title', m.title,
                                        'vote_average', m.vote_average,
                                        'release_date', m.release_date,
                                        'roles', (
                                            SELECT array_agg(at.role)
                                            FROM all_talents at
                                            WHERE at.id = m.id AND at.person_name = t.person_name
                                        )
                                    )
                                )
                                FROM (
                                    SELECT DISTINCT id
                                    FROM all_talents
                                    WHERE person_name = t.person_name
                                    ORDER BY id
                                    LIMIT 5
                                ) ids
                                JOIN "Movie" m ON m.id = ids.id
                            ) as movies
                        FROM role_counts t
                        LIMIT 20
                    `;
                } else {
                    query = `
                        WITH all_talents AS (
                            SELECT id, title, release_date, vote_average, 
                                unnest(director) as person_name, 'director' as role
                            FROM "Movie"
                            WHERE array_length(director, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(writers) as person_name, 'writer' as role
                            FROM "Movie"
                            WHERE array_length(writers, 1) > 0
                            UNION ALL
                            SELECT id, title, release_date, vote_average, 
                                unnest(producers) as person_name, 'producer' as role
                            FROM "Movie"
                            WHERE array_length(producers, 1) > 0
                        ),
                        role_counts AS (
                            SELECT 
                                person_name,
                                COUNT(DISTINCT id)::integer as movie_count,
                                array_agg(DISTINCT role) as roles,
                                COUNT(DISTINCT role)::integer as role_count,
                                ROUND(AVG(vote_average)::numeric, 2) as avg_rating
                            FROM all_talents
                            GROUP BY person_name
                            HAVING 
                                COUNT(DISTINCT id) >= $1 AND
                                COUNT(DISTINCT role) > 1
                            ORDER BY role_count DESC, movie_count DESC
                        )
                        SELECT 
                            t.person_name,
                            t.movie_count,
                            t.roles,
                            t.role_count,
                            t.avg_rating,
                            (
                                SELECT jsonb_agg(
                                    jsonb_build_object(
                                        'id', m.id,
                                        'title', m.title,
                                        'vote_average', m.vote_average,
                                        'release_date', m.release_date,
                                        'roles', (
                                            SELECT array_agg(at.role)
                                            FROM all_talents at
                                            WHERE at.id = m.id AND at.person_name = t.person_name
                                        )
                                    )
                                )
                                FROM (
                                    SELECT DISTINCT id
                                    FROM all_talents
                                    WHERE person_name = t.person_name
                                    ORDER BY id
                                    LIMIT 5
                                ) ids
                                JOIN "Movie" m ON m.id = ids.id
                            ) as movies
                        FROM role_counts t
                        LIMIT 50
                    `;
                }

                // 执行查询
                const crossRoleTalents = await prisma.$queryRawUnsafe(query, minMoviesNum);

                return {
                    data: Array.isArray(crossRoleTalents)
                        ? crossRoleTalents.map((talent: any) => ({
                            ...talent,
                            movie_count: talent.movie_count?.toString() || "0",
                            role_count: talent.role_count?.toString() || "0",
                            avg_rating: talent.avg_rating?.toString() || "0",
                            movies: Array.isArray(talent.movies)
                                ? talent.movies.map((movie: any) => ({
                                    ...movie,
                                    vote_average: movie.vote_average?.toString() || "0",
                                    release_date: movie.release_date
                                        ? new Date(movie.release_date).toISOString().split("T")[0]
                                        : null,
                                }))
                                : [],
                        }))
                        : [],
                    meta: {
                        limit: limitNum,
                        minMovies,
                        count: Array.isArray(crossRoleTalents) ? crossRoleTalents.length : 0,
                    },
                };
            } catch (error: any) {
                logger.error("获取跨领域电影工作者分析失败:", error);
                throw error;
            }
        },
    );
}
