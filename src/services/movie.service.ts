import { PrismaClient } from '@prisma/client';
import { MovieQueryParams } from '@/types';
import config from '@/config';

const prisma = new PrismaClient();

export class MovieService {
    static async getPaginatedMovies(query: MovieQueryParams) {
        const where = this.buildWhereClause(query);
        const orderBy = this.buildOrderBy(query.sortBy);

        const page = query.page ?? 1;
        const pageSize = query.pageSize ?? 20;
        
        const [movies, total] = await Promise.all([
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
                page: query.page ?? 1,
                pageSize: query.pageSize ?? 20,
                totalPages: Math.ceil(total / (query.pageSize ?? 20))
            }
        };
    }

    static async getMovieDetails(id: string) {
        return prisma.movie.findUnique({
            where: { id },
            select: this.getDetailFields()
        });
    }

    private static buildWhereClause(query: MovieQueryParams) {
        const where: any = {};

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

    private static buildOrderBy(sortBy?: string) {
        const allowedSortFields = [
            'release_date',
            'vote_average',
            'popularity',
            'runtime'
        ];

        // 使用Prisma的SortOrder类型
        return sortBy && allowedSortFields.includes(sortBy)
            ? { [sortBy]: 'desc' as const }
            : { release_date: 'desc' as const };
    }

    private static getListFields() {
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

    private static getDetailFields() {
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
