import { FastifyInstance } from 'fastify';
import { MovieController } from '@/controllers/movie.controller';
import { validateMovieQueryParams } from '@/utils/validator';

export async function movieRoutes(fastify: FastifyInstance) {
    fastify.get('/', {
        schema: {
            querystring: {
                type: 'object',
                properties: {
                    page: { type: 'number', default: 1 },
                    pageSize: { type: 'number', default: 20 },
                    genre: { type: 'string' },
                    minRating: { type: 'number' },
                    year: { type: 'number' },
                    sortBy: { type: 'string' },
                    search: { type: 'string' }
                }
            }
        },
        handler: MovieController.getMovies
    });

    fastify.get('/:id', {
        schema: {
            params: {
                type: 'object',
                properties: {
                    id: { type: 'string' }
                },
                required: ['id']
            }
        },
        handler: MovieController.getMovieDetails
    });
}
