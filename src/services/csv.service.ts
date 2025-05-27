import { createReadStream } from 'fs';
import { parse } from 'csv-parse';
import { pipeline } from 'stream/promises';
import { Transform } from 'stream';
import { PrismaClient } from '@prisma/client';
import { parseProductionCompanies, parseCast } from '@/utils/parser';
import logger from '@/utils/logger';
import config from "@/config";
import { DatabaseError, ValidationError } from '@/utils/error-handler';

const prisma = new PrismaClient({
  log: [
    { level: 'query', emit: 'event' },
    { level: 'error', emit: 'event' }
  ]
});

prisma.$on('query', (e) => {
  logger.debug(`Query: ${e.query} | Params: ${e.params}`);
});

prisma.$on('error', (e) => {
  logger.error(`Prisma Error: ${e.message}`);
});

export class CsvService {
  static async importMovies(filePath: string) {
    let processedCount = 0;
    let batch: any[] = [];
    const startTime = Date.now();
    let errorRows: { row: number, error: string }[] = [];

    try {
      logger.info(`开始处理文件: ${filePath}`);

      await pipeline(
        createReadStream(filePath),
        this.createCSVParser(),
        this.createTransformer(batch, errorRows),
        this.createBatchProcessor(batch, processedCount)
      );

      // 如果有错误行，记录但继续处理
      if (errorRows.length > 0) {
        logger.warn(`文件处理完成，但有 ${errorRows.length} 行数据无法处理`, {
          errors: errorRows.slice(0, 10) // 只记录前10个错误，避免日志过大
        });
      }

      logger.info(`文件处理完成，共处理 ${processedCount} 条记录，耗时 ${Date.now() - startTime}ms`);
      return {
        success: true,
        count: processedCount,
        errorCount: errorRows.length,
        errors: errorRows.slice(0, 10)
      };
    } catch (error: any) {
      logger.error(`文件处理失败: ${error.message}`);
      throw new DatabaseError(`CSV导入失败: ${error.message}`, { cause: error });
    } finally {
      await prisma.$disconnect();
      logger.info('数据库连接已关闭');
    }
  }

  private static createCSVParser() {
    return parse({
      columns: true,
      trim: true,
      skip_empty_lines: true,
      cast: (value, context) => {
        if (context.header) return value;
        if (context.column === 'release_date') return new Date(value);
        return value;
      }
    });
  }

  private static createTransformer(batch: any[], errorRows: { row: number, error: string }[]) {
    let rowNumber = 0;

    return new Transform({
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
        } catch (error: any) {
          errorRows.push({
            row: rowNumber,
            error: error.message
          });
          logger.error(`数据转换失败 [行 ${rowNumber}]: ${error.message}`);
          callback();
        }
      }
    });
  }

  private static createBatchProcessor(batch: any[], processedCount: number) {
    return new Transform({
      objectMode: true,
      async transform(_chunk, _encoding, callback) {
        if (batch.length >= config.csv.batchSize) {
          try {
            await CsvService.processBatch(batch);
            processedCount += batch.length;
            batch.length = 0; // 清空数组但保持引用
          } catch (error) {
            logger.error(`批量处理失败:`, error);
            // 我们在这里只记录错误，但不阻止处理继续进行
          }
        }
        callback();
      },
      async flush(callback) {
        if (batch.length > 0) {
          try {
            await CsvService.processBatch(batch);
            processedCount += batch.length;
          } catch (error) {
            logger.error(`最后批次处理失败:`, error);
          }
        }
        callback();
      }
    });
  }

  private static async processBatch(batch: any[]) {
    try {
      const start = Date.now();
      await prisma.$transaction([
        prisma.movie.createMany({
          data: batch,
          skipDuplicates: true
        })
      ]);
      logger.debug(`已插入 ${batch.length} 条记录，耗时 ${Date.now() - start}ms`);
    } catch (error: any) {
      logger.error(`批量插入失败: ${error.message}`);
      throw new DatabaseError(`数据库插入失败: ${error.message}`);
    }
  }

  private static transformRow(row: any) {
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
        genres: row.genres?.split(/, */).filter(Boolean) || [],
        production_companies: parseProductionCompanies(row.production_companies),
        production_countries: row.production_countries?.split(/, */) || [],
        spoken_languages: row.spoken_languages?.split(/, */) || [],
        cast: parseCast(row.cast),
        director: row.director?.split(/, */) || [],
        director_of_photography: row.director_of_photography?.split(/, */) || [],
        writers: row.writers?.split(/, */) || [],
        producers: row.producers?.split(/, */) || [],
        music_composer: row.music_composer?.split(/, */) || [],
        imdb_rating: row.imdb_rating ? parseFloat(row.imdb_rating) : null,
        imdb_votes: row.imdb_votes ? parseInt(row.imdb_votes) : null,
        poster_path: row.poster_path || null
      };
    } catch (error: any) {
      logger.error('行数据转换失败:', {
        error,
        row: JSON.stringify(row)
      });
      throw new ValidationError(`数据格式无效: ${error.message}`);
    }
  }
}
