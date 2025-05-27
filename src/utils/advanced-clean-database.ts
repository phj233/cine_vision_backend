import { PrismaClient, Prisma } from '@prisma/client';
import { exit } from 'process';
import readline from 'readline';

/**
 * 高级数据库清理工具
 * 可以删除各种不完整或异常的电影记录
 */

// 创建一个交互式命令行界面
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

// 提问助手函数
const question = (query: string): Promise<string> => {
    return new Promise((resolve) => {
        rl.question(query, (answer) => {
            resolve(answer);
        });
    });
};

async function advancedCleanDatabase() {
    console.log('========================================');
    console.log('高级数据库清理工具');
    console.log('========================================');

    const prisma = new PrismaClient();

    try {
        // 获取当前记录总数
        const totalMovies = await prisma.movie.count();
        console.log(`数据库中目前有 ${totalMovies} 条电影记录`);

        console.log('\n可执行的清理操作:');
        console.log('1. 删除发布日期为空的记录');
        console.log('2. 删除标题为空的记录');
        console.log('3. 删除评分为0且评分数为0的记录');
        console.log('4. 删除预算和收入均为0的记录');
        console.log('5. 删除没有任何类型(genres)的记录');
        console.log('6. 删除没有演员(cast)的记录');
        console.log('7. 执行所有清理操作');
        console.log('0. 退出');

        const choice = await question('\n请选择要执行的操作 [0-7]: ');

        switch (choice.trim()) {
            case '1':
                await deleteEmptyReleaseDate(prisma);
                break;
            case '2':
                await deleteEmptyTitles(prisma);
                break;
            case '3':
                await deleteZeroRatings(prisma);
                break;
            case '4':
                await deleteZeroBudgetAndRevenue(prisma);
                break;
            case '5':
                await deleteEmptyGenres(prisma);
                break;
            case '6':
                await deleteEmptyCast(prisma);
                break;
            case '7':
                await executeAllCleanings(prisma);
                break;
            case '0':
                console.log('已取消操作');
                break;
            default:
                console.log('无效的选择');
                break;
        }

        // 获取清理后的记录总数
        const remainingMovies = await prisma.movie.count();
        console.log(`\n清理完成，数据库中现在有 ${remainingMovies} 条电影记录`);
        console.log(`共删除了 ${totalMovies - remainingMovies} 条记录`);

    } catch (error) {
        console.error('清理数据库时出错:', error);
    } finally {
        await prisma.$disconnect();
        console.log('\n数据库连接已关闭');
        rl.close();
    }
}

// 删除发布日期为空的记录
async function deleteEmptyReleaseDate(prisma: PrismaClient) {
    console.log('\n开始删除发布日期为空的记录...');

    const count = await prisma.movie.count({
        where: { release_date: null }
    });

    if (count === 0) {
        console.log('没有找到发布日期为空的记录');
        return;
    }

    console.log(`找到 ${count} 条发布日期为空的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        const result = await prisma.movie.deleteMany({
            where: { release_date: null }
        });
        console.log(`成功删除 ${result.count} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 删除标题为空的记录
async function deleteEmptyTitles(prisma: PrismaClient) {
    console.log('\n开始删除标题为空的记录...');

    // 在Prisma中，String类型不允许为null，但可以是空字符串
    const count = await prisma.movie.count({
        where: {
            title: { equals: '' }
        }
    });

    if (count === 0) {
        console.log('没有找到标题为空的记录');
        return;
    }

    console.log(`找到 ${count} 条标题为空的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        const result = await prisma.movie.deleteMany({
            where: {
                title: { equals: '' }
            }
        });
        console.log(`成功删除 ${result.count} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 删除评分为0且评分数为0的记录
async function deleteZeroRatings(prisma: PrismaClient) {
    console.log('\n开始删除评分为0且评分数为0的记录...');

    const count = await prisma.movie.count({
        where: {
            vote_average: { equals: 0 },
            vote_count: { equals: 0 }
        }
    });

    if (count === 0) {
        console.log('没有找到评分为0且评分数为0的记录');
        return;
    }

    console.log(`找到 ${count} 条评分为0且评分数为0的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        const result = await prisma.movie.deleteMany({
            where: {
                vote_average: { equals: 0 },
                vote_count: { equals: 0 }
            }
        });
        console.log(`成功删除 ${result.count} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 删除预算和收入均为0的记录
async function deleteZeroBudgetAndRevenue(prisma: PrismaClient) {
    console.log('\n开始删除预算和收入均为0的记录...');

    const count = await prisma.movie.count({
        where: {
            budget: { equals: BigInt(0) },
            revenue: { equals: BigInt(0) }
        }
    });

    if (count === 0) {
        console.log('没有找到预算和收入均为0的记录');
        return;
    }

    console.log(`找到 ${count} 条预算和收入均为0的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        const result = await prisma.movie.deleteMany({
            where: {
                budget: { equals: BigInt(0) },
                revenue: { equals: BigInt(0) }
            }
        });
        console.log(`成功删除 ${result.count} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 删除没有任何类型(genres)的记录
async function deleteEmptyGenres(prisma: PrismaClient) {
    console.log('\n开始删除没有任何类型(genres)的记录...');

    // 使用数组长度来检查空数组
    const emptyGenresMovies = await prisma.$queryRaw`
        SELECT id FROM "Movie" WHERE array_length(genres, 1) IS NULL OR array_length(genres, 1) = 0
    `;

    const count = Array.isArray(emptyGenresMovies) ? emptyGenresMovies.length : 0;

    if (count === 0) {
        console.log('没有找到没有任何类型的记录');
        return;
    }

    console.log(`找到 ${count} 条没有任何类型的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        // 获取所有空类型电影的ID
        const idsToDelete = Array.isArray(emptyGenresMovies)
            ? emptyGenresMovies.map((movie: any) => movie.id)
            : [];

        // 批量删除
        let deletedCount = 0;
        if (idsToDelete.length > 0) {
            for (const id of idsToDelete) {
                try {
                    await prisma.movie.delete({
                        where: { id }
                    });
                    deletedCount++;
                } catch (err) {
                    console.error(`删除ID为 ${id} 的记录时出错:`, err);
                }
            }
        }

        console.log(`成功删除 ${deletedCount} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 删除没有演员(cast)的记录
async function deleteEmptyCast(prisma: PrismaClient) {
    console.log('\n开始删除没有演员(cast)的记录...');

    // 使用数组长度来检查空数组
    const emptyCastMovies = await prisma.$queryRaw`
        SELECT id FROM "Movie" WHERE array_length(cast, 1) IS NULL OR array_length(cast, 1) = 0
    `;

    const count = Array.isArray(emptyCastMovies) ? emptyCastMovies.length : 0;

    if (count === 0) {
        console.log('没有找到没有演员的记录');
        return;
    }

    console.log(`找到 ${count} 条没有演员的记录`);
    const confirm = await question(`确认删除这 ${count} 条记录? (y/n): `);

    if (confirm.toLowerCase() === 'y') {
        // 获取所有空演员电影的ID
        const idsToDelete = Array.isArray(emptyCastMovies)
            ? emptyCastMovies.map((movie: any) => movie.id)
            : [];

        // 批量删除
        let deletedCount = 0;
        if (idsToDelete.length > 0) {
            for (const id of idsToDelete) {
                try {
                    await prisma.movie.delete({
                        where: { id }
                    });
                    deletedCount++;
                } catch (err) {
                    console.error(`删除ID为 ${id} 的记录时出错:`, err);
                }
            }
        }

        console.log(`成功删除 ${deletedCount} 条记录`);
    } else {
        console.log('已取消删除');
    }
}

// 执行所有清理操作
async function executeAllCleanings(prisma: PrismaClient) {
    console.log('\n将执行所有清理操作...');
    const confirm = await question('确认执行所有清理操作? 这将删除所有不完整的记录 (y/n): ');

    if (confirm.toLowerCase() !== 'y') {
        console.log('已取消操作');
        return;
    }

    await deleteEmptyReleaseDate(prisma);
    await deleteEmptyTitles(prisma);
    await deleteZeroRatings(prisma);
    await deleteZeroBudgetAndRevenue(prisma);
    await deleteEmptyGenres(prisma);
    await deleteEmptyCast(prisma);

    console.log('\n所有清理操作已完成');
}

// 执行清理
advancedCleanDatabase()
    .then(() => {
        console.log('高级清理操作完成');
        exit(0);
    })
    .catch(error => {
        console.error('清理过程中发生错误:', error);
        exit(1);
    }); 