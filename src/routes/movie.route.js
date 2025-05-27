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
exports.movieRoutes = movieRoutes;
const movie_controller_1 = require("@/controllers/movie.controller");
function movieRoutes(fastify) {
    return __awaiter(this, void 0, void 0, function* () {
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
            handler: movie_controller_1.MovieController.getMovies
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
            handler: movie_controller_1.MovieController.getMovieDetails
        });
    });
}
