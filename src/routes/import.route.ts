import { FastifyInstance } from 'fastify';
import { ImportController } from '@/controllers/import.controller';

export async function importRoutes(fastify: FastifyInstance) {
    fastify.post('/', {
        config: {
            rateLimit: {
                max: 3,
                timeWindow: '1 hour'
            }
        },
        handler: ImportController.handleImport
    });
}
