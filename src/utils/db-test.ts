import { PrismaClient } from '@prisma/client';
import fs from 'fs-extra';
import path from 'path';

/**
 * 解决BigInt序列化问题
 */
// 处理BigInt序列化问题的替代方案
const replaceBigInt = (key: string, value: any) => {
    if (typeof value === 'bigint') {
        return Number(value);
    }
    return value;
};

/**
 * 数据库连接测试工具
 * 用于验证数据库连接和ID使用情况
 */
async function testDatabaseConnection() {
    console.log('开始测试数据库连接...');

    // 从.env文件读取数据库连接信息
    try {
        const envPath = path.resolve(process.cwd(), '.env');
        const envExists = await fs.pathExists(envPath);

        if (envExists) {
            const envContent = await fs.readFile(envPath, 'utf8');
            console.log('\n数据库连接信息:');
            const dbUrlMatch = envContent.match(/DATABASE_URL=(.+)/);
            if (dbUrlMatch) {
                // 隐藏密码
                const dbUrl = dbUrlMatch[1].replace(/"/g, '');
                const maskedUrl = dbUrl.replace(/\/\/[^:]+:[^@]+@/, '//[username]:[password]@');
                console.log(`DATABASE_URL=${maskedUrl}`);

                // 解析连接字符串获取更多信息
                try {
                    const urlParts = new URL(dbUrl);
                    console.log(`- 主机: ${urlParts.hostname}`);
                    console.log(`- 端口: ${urlParts.port || '5432 (默认)'}`);
                    console.log(`- 数据库: ${urlParts.pathname.replace('/', '')}`);
                    console.log(`- 用户名: ${urlParts.username}`);
                    console.log(`- SSL模式: ${urlParts.searchParams.get('sslmode') || 'prefer (默认)'}`);
                    console.log(`- Schema: ${urlParts.searchParams.get('schema') || 'public (默认)'}`);
                } catch (parseError) {
                    console.log('无法解析数据库URL，请确保格式正确:', parseError);
                }
            } else {
                console.log('在.env文件中未找到DATABASE_URL');
            }
        } else {
            console.log('.env文件不存在');
        }
    } catch (envError) {
        console.error('读取环境文件失败:', envError);
    }

    // 连接到数据库并查询数据
    const prisma = new PrismaClient();

    try {
        console.log('\n尝试连接到数据库...');
        // 检查能否执行基本查询
        const result = await prisma.$queryRaw`SELECT 1 as connected`;
        console.log('数据库连接成功! 结果:', result);

        // 检查Movie表是否存在并获取记录数
        const countResult = await prisma.movie.count();
        console.log(`Movie表中有 ${countResult} 条记录`);

        if (countResult > 0) {
            // 获取几条记录的ID样本
            const movies = await prisma.movie.findMany({
                select: { id: true, title: true },
                take: 5
            });

            console.log('\nID样本:');
            movies.forEach(movie => {
                console.log(`- ID: ${movie.id} (${typeof movie.id}), 标题: ${movie.title}`);
            });

            // 检查ID的数据类型
            const firstMovie = await prisma.movie.findFirst({
                where: { id: movies[0].id }
            });

            if (firstMovie) {
                console.log('\n第一条记录的详细信息:');
                try {
                    // 使用自定义replacer处理BigInt
                    console.log(JSON.stringify(firstMovie, replaceBigInt, 2));
                } catch (jsonError) {
                    console.error('序列化记录时出错:', jsonError);
                    console.log('记录ID:', firstMovie.id);
                    console.log('记录标题:', firstMovie.title);
                }
            }

            // 尝试检查这些ID在原始CSV中是否存在
            console.log('\n数据库查询示例:');
            console.log(`SELECT * FROM "Movie" WHERE id = '${movies[0].id}';`);

            // 获取表结构信息
            const tableInfo = await prisma.$queryRaw`
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'Movie' 
                ORDER BY ordinal_position
            `;

            console.log('\n表结构信息:');
            console.log(tableInfo);
        }

        // 输出IDEA连接配置建议
        console.log('\n=============================================');
        console.log('IDEA数据库连接配置指南:');
        console.log('=============================================');
        console.log('1. 在IDEA中打开"Database"工具窗口');
        console.log('2. 点击"+"添加新数据源，选择"PostgreSQL"');
        console.log('3. 填写连接信息:');
        console.log('   - 主机: localhost (或数据库服务器IP)');
        console.log('   - 端口: 5432 (PostgreSQL默认端口)');
        console.log('   - 数据库: movies (与Prisma配置中相同)');
        console.log('   - 用户名和密码: 与Prisma配置相同');
        console.log('4. 展开高级选项:');
        console.log('   - 在URL参数中添加: ?schema=public');
        console.log('5. 测试连接，确保连接成功');
        console.log('6. 确认连接后，展开数据库，查看"public"架构下的表');
        console.log('7. 如果看不到表，尝试:');
        console.log('   - 右键点击数据库连接，选择"刷新"');
        console.log('   - 确保用户有足够权限访问表');
        console.log('   - 检查表是否在"public"架构中');
        console.log('注: 如果使用的是IDEA 2021或更新版本，可能需要在');
        console.log('数据库工具中手动添加表，方法是右键点击"public"架构');
        console.log('选择"New > Table"，然后输入表名"Movie"');
        console.log('=============================================');

    } catch (error) {
        console.error('数据库测试失败:', error);
    } finally {
        await prisma.$disconnect();
        console.log('\n数据库连接已关闭');
    }
}

// 执行测试
testDatabaseConnection()
    .then(() => console.log('测试完成'))
    .catch(error => console.error('测试过程中发生错误:', error)); 