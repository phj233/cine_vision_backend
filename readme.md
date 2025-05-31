# Cine Vision Backend 项目文档

## 1. 项目概述
这是一个基于 TypeScript Prisma 和 Fastify 的后端服务，用于管理和分析电影数据。主要功能包括：
- **电影数据管理**：提供电影数据的增删改查操作。
- **数据可视化**：支持多种电影数据的统计和可视化分析。
- **CSV 数据导入**：允许通过 CSV 文件批量导入电影数据。

项目使用模块化设计，包含控制器、服务、工具类等模块，确保代码的可维护性和扩展性。

## 2. 项目结构
```
.
├── logs
│   ├── error.log
│   └── info.log
├── prisma
│   ├── migrations
│   │   ├── 20250517054126_init
│   │   │   └── migration.sql
│   │   ├── 20250517142038_change_cast_to_string_array
│   │   │   └── migration.sql
│   │   └── migration_lock.toml
│   └── schema.prisma
├── src
│   ├── config
│   │   ├── index.js
│   │   └── index.ts
│   ├── controllers
│   │   ├── import.controller.js
│   │   ├── import.controller.ts
│   │   ├── movie.controller.js
│   │   ├── movie.controller.ts
│   │   ├── visualization.controller.js
│   │   └── visualization.controller.ts
│   ├── logs
│   │   └── error.log
│   ├── routes
│   │   ├── import.route.js
│   │   ├── import.route.ts
│   │   ├── movie.route.js
│   │   └── movie.route.ts
│   ├── services
│   │   ├── csv.service.js
│   │   ├── csv.service.ts
│   │   ├── movie.service.js
│   │   └── movie.service.ts
│   ├── types
│   ├── utils
│   │   ├── advanced-clean-database.js
│   │   ├── advanced-clean-database.ts
│   │   ├── clean-database.js
│   │   ├── clean-database.ts
│   │   ├── db-test.js
│   │   ├── db-test.ts
│   │   ├── error-handler.js
│   │   ├── error-handler.ts
│   │   ├── logger.js
│   │   ├── logger.ts
│   │   ├── parser.js
│   │   ├── parser.ts
│   │   ├── stream-processor.js
│   │   ├── stream-processor.ts
│   │   ├── validator.js
│   │   └── validator.ts
│   ├── alias.js
│   ├── alias.ts
│   ├── app.js
│   ├── app.ts
│   ├── types.js
│   └── types.ts
├── tmp
├── package-lock.json
├── package.json
└── tsconfig.json
```

### 目录说明
- **logs**: 存放日志文件。
- **prisma**: 包含 Prisma 配置和数据库迁移文件。
- **src**: 源代码目录，包含配置、控制器、路由、服务、工具类等。
- **tmp**: 临时文件存储目录。
- **package.json**: 项目依赖和脚本配置。
- **tsconfig.json**: TypeScript 编译配置。

## 3. 依赖管理
### 主要依赖
```json
{
  "dependencies": {
    "@fastify/cors": "^11.0.1",
    "@fastify/multipart": "^9.0.3",
    "@prisma/client": "^6.8.2",
    "csv-parse": "^5.6.0",
    "dotenv": "^16.5.0",
    "fastify": "^5.3.3",
    "fs-extra": "^11.3.0",
    "pino": "^9.6.0"
  },
  "devDependencies": {
    "@types/fs-extra": "^11.0.4",
    "@types/node": "^22.15.18",
    "typescript": "^5.8.3"
  }
}
```

### 脚本命令
```json
{
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "start": "npm run build && node src/app.js",
    "dev": "tsc -w & nodemon src/app.js",
    "test": "echo \"Error: no test specified\" && exit 1"
  }
}
```

## 4. 配置文件
### `src/config/index.ts`
```ts
import dotenv from 'dotenv';

dotenv.config();

export default {
  port: process.env.PORT || 3000,
  database: {
    url: process.env.DATABASE_URL || 'postgresql://postgre:postgre@localhost:5432/movies'
  },
  csv: {
    batchSize: parseInt(process.env.CSV_BATCH_SIZE || '1000'),
    maxFileSize: parseInt(process.env.CSV_MAX_FILE_SIZE || '1073741824'), // 1GB
    timeout: parseInt(process.env.CSV_TIMEOUT || '3600000'), // 1小时
    retryCount: parseInt(process.env.CSV_RETRY_COUNT || '3'),
    retryDelay: parseInt(process.env.CSV_RETRY_DELAY || '1000') // 1秒
  },
  logging: {
    level: process.env.LOG_LEVEL || 'debug',
    file: {
      enabled: process.env.LOG_FILE_ENABLED === 'true',
      path: process.env.LOG_FILE_PATH || 'logs/app.log'
    }
  }
};
```

### `tsconfig.json`
- 使用 CommonJS 模块系统。
- 启用严格类型检查。
- 支持别名路径（`@/*` 映射到 `src/*`）。

## 5. 数据库模型
### `prisma/schema.prisma`
```prisma
model Movie {
  id                       String    @id @unique // 电影的唯一标识符
  title                    String // 电影标题
  vote_average             Float // 平均评分
  vote_count               Int // 评分人数
  status                   String // 电影状态（如上映、未上映等）
  release_date             DateTime? // 发行日期
  revenue                  BigInt // 票房收入
  runtime                  Int? // 时长（分钟）
  budget                   BigInt // 预算
  imdb_id                  String? // IMDb ID
  original_language        String? // 原始语言
  original_title           String // 原始标题
  overview                 String? // 概述/简介
  popularity               Float? // 流行度评分
  tagline                  String? // 标语
  genres                   String[] // 类型数组
  production_companies     Json // 制作公司信息（JSON格式）
  production_countries     String[] // 制作国家/地区
  spoken_languages         String[] // 使用的语言
  cast                     String[] // 演员列表
  director                 String[] // 导演列表
  director_of_photography  String[] // 摄影指导列表
  writers                  String[] // 编剧列表
  producers                String[] // 制片人列表
  music_composer           String[] // 音乐作曲家列表
  imdb_rating              Float? // IMDb 评分
  imdb_votes               Int? // IMDb 评分人数
  poster_path              String? // 海报图片路径

  @@index([release_date]) // 在发行日期上创建索引
  @@index([genres], type: Gin) // 在类型上创建Gin索引
  @@index([vote_average]) // 在平均评分上创建索引
}
```

字段说明：
- **id**: 电影的唯一标识符
- **title**: 电影标题
- **vote_average**: 平均评分
- **vote_count**: 评分人数
- **status**: 电影状态（如上映、未上映等）
- **release_date**: 发行日期
- **revenue**: 票房收入
- **runtime**: 时长（分钟）
- **budget**: 预算
- **imdb_id**: IMDb ID
- **original_language**: 原始语言
- **original_title**: 原始标题
- **overview**: 概述/简介
- **popularity**: 流行度评分
- **tagline**: 标语
- **genres**: 类型数组
- **production_companies**: 制作公司信息（JSON格式）
- **production_countries**: 制作国家/地区
- **spoken_languages**: 使用的语言
- **cast**: 演员列表
- **director**: 导演列表
- **director_of_photography**: 摄影指导列表
- **writers**: 编剧列表
- **producers**: 制片人列表
- **music_composer**: 音乐作曲家列表
- **imdb_rating**: IMDb 评分
- **imdb_votes**: IMDb 评分人数
- **poster_path**: 海报图片路径

索引说明：
- **release_date**: 在发行日期上创建索引以加速按年份查询
- **genres**: 在类型上创建Gin索引以加速按类型查询
- **vote_average**: 在平均评分上创建索引以加速按评分排序

## 6. 核心模块
### 控制器
#### `src/controllers/movie.controller.ts`
- 提供电影数据的查询接口。
- 支持分页、过滤、排序等功能。
- 实现了高级查询功能，如相似电影推荐、随机推荐等。

#### `src/controllers/visualization.controller.ts`
- 提供电影数据的可视化接口。
- 支持评分分布、年度趋势、类型对比等多种分析。

#### `src/controllers/import.controller.ts`
- 实现了 CSV 文件的上传和处理。
- 支持大文件流式处理，避免内存溢出。

### 工具类
#### `src/utils/stream-processor.ts`
- 实现了 CSV 流的解析和数据库写入。
- 支持批处理和错误重试机制。

#### `src/utils/parser.ts`
- 解析制作公司和演员信息。
- 支持多种格式的输入。

#### `src/utils/error-handler.ts`
- 自定义错误类（如 ValidationError, NotFoundError）。
- 异步错误处理包装器。

#### `src/utils/logger.ts`
- 日志记录器，支持多级日志输出（info, error, warn, debug）。

## 7. API 接口
### `/api/v1/movies`
- `GET /movies`: 获取电影列表（支持分页、过滤、排序）。
- `GET /movies/all`: 获取所有电影。
- `GET /movies/:id`: 获取单部电影详情。
- `GET /movies/search`: 搜索电影（支持关键词搜索）。
- `GET /recommendations/random`: 获取随机推荐电影。
- `GET /genres/stats`: 获取电影类型统计信息。
- `GET /stats/years`: 按年份分组的电影数量。
- `GET /stats/summary`: 电影数据统计摘要。
- `GET /visualization/rating-distribution`: 电影评分分布。
- `GET /visualization/yearly-trends`: 年度趋势分析。
- `GET /visualization/genre-comparison`: 类型对比分析。
- `GET /visualization/runtime-distribution`: 电影时长分布。
- `GET /visualization/top-production-companies`: 制作公司排名。
- `GET /visualization/actor-collaborations`: 演员合作网络分析。
- `GET /visualization/budget-revenue`: 预算与票房关系分析。

### `/api/v1/import`
- `POST /import`: 上传并处理 CSV 文件。

## 8. 日志与测试工具
- 日志文件存放在 `logs` 目录中。
- 日志级别可通过环境变量配置。
- 支持开发环境下的调试日志。

## 9. 构建与部署
### 开发环境
- 使用 `npm run dev` 启动开发服务器。
- 自动监听文件变化并重启服务。

### 生产环境
- 使用 `npm run build` 构建生产版本。
- 使用 `npm start` 启动生产服务。

### 部署建议
- 使用 PM2 等进程管理工具确保服务稳定性。
- 配置反向代理（如 Nginx）以提高性能和安全性。

## 10. 总结
该项目是一个功能齐全的电影数据分析平台，具备强大的数据导入、查询和可视化能力。通过合理的模块划分和良好的错误处理机制，确保了系统的稳定性和可维护性。