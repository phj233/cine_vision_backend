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
Object.defineProperty(exports, "__esModule", { value: true });
exports.MovieService = void 0;
const client_1 = require("@prisma/client");
const prisma = new client_1.PrismaClient();
class MovieService {
    static getPaginatedMovies(query) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c, _d, _e;
            const where = this.buildWhereClause(query);
            const orderBy = this.buildOrderBy(query.sortBy);
            const page = (_a = query.page) !== null && _a !== void 0 ? _a : 1;
            const pageSize = (_b = query.pageSize) !== null && _b !== void 0 ? _b : 20;
            const [movies, total] = yield Promise.all([
                prisma.movie.findMany({
                    where,
                    skip: (page - 1) * pageSize,
                    take: pageSize,
                    orderBy,
                    select: this.getListFields()
                }),
                prisma.movie.count({ where })
            ]);
            return {
                data: movies,
                meta: {
                    total,
                    page: (_c = query.page) !== null && _c !== void 0 ? _c : 1,
                    pageSize: (_d = query.pageSize) !== null && _d !== void 0 ? _d : 20,
                    totalPages: Math.ceil(total / ((_e = query.pageSize) !== null && _e !== void 0 ? _e : 20))
                }
            };
        });
    }
    static getMovieDetails(id) {
        return __awaiter(this, void 0, void 0, function* () {
            return prisma.movie.findUnique({
                where: { id },
                select: this.getDetailFields()
            });
        });
    }
    static buildWhereClause(query) {
        const where = {};
        if (query.genre) {
            where.genres = { has: query.genre };
        }
        if (query.minRating) {
            where.vote_average = { gte: query.minRating };
        }
        if (query.year) {
            where.release_date = {
                gte: new Date(`${query.year}-01-01`),
                lte: new Date(`${query.year}-12-31`)
            };
        }
        if (query.search) {
            where.OR = [
                { title: { contains: query.search, mode: 'insensitive' } },
                { overview: { contains: query.search, mode: 'insensitive' } }
            ];
        }
        return where;
    }
    static buildOrderBy(sortBy) {
        const allowedSortFields = [
            'release_date',
            'vote_average',
            'popularity',
            'runtime'
        ];
        // 使用Prisma的SortOrder类型
        return sortBy && allowedSortFields.includes(sortBy)
            ? { [sortBy]: 'desc' }
            : { release_date: 'desc' };
    }
    static getListFields() {
        return {
            id: true,
            title: true,
            release_date: true,
            vote_average: true,
            genres: true,
            poster_path: true,
            runtime: true,
            director: true
        };
    }
    static getDetailFields() {
        return {
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
            writers: true
        };
    }
}
exports.MovieService = MovieService;
