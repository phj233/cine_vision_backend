import { PrismaClient } from '@prisma/client';
import { exit } from 'process';

/**
 * 数据库清理工具
 * 删除发布日期为空的电影记录
 */
async function cleanDatabase() {
    console.log('开始清理数据库...');
    console.log('准备删除发布日期为空的电影记录...');

    const prisma = new PrismaClient();

    try {
        // 首先查询有多少条发布日期为空的记录
        const emptyDateCount = await prisma.movie.count({
            where: {
                release_date: null
            }
        });

        if (emptyDateCount === 0) {
            console.log('没有找到发布日期为空的记录，无需清理');
            return;
        }

        console.log(`找到 ${emptyDateCount} 条发布日期为空的记录`);

        // 获取几条示例记录以供确认
        const sampleRecords = await prisma.movie.findMany({
            where: {
                release_date: null
            },
            select: {
                id: true,
                title: true,
                release_date: true
            },
            take: 5
        });

        console.log('\n示例记录:');
        sampleRecords.forEach(movie => {
            console.log(`- ID: ${movie.id}, 标题: ${movie.title}, 发布日期: ${movie.release_date}`);
        });

        // 询问确认
        console.log(`\n确认: 您将删除 ${emptyDateCount} 条发布日期为空的记录`);
        console.log('按Ctrl+C取消，或等待5秒自动继续...');

        // 等待5秒，给用户时间取消操作
        await new Promise(resolve => setTimeout(resolve, 5000));

        // 执行删除操作
        console.log('\n开始删除记录...');
        const deleteResult = await prisma.movie.deleteMany({
            where: {
                release_date: null
            }
        });

        console.log(`成功删除 ${deleteResult.count} 条记录`);

        // 验证删除结果
        const remainingCount = await prisma.movie.count({
            where: {
                release_date: null
            }
        });

        if (remainingCount === 0) {
            console.log('所有发布日期为空的记录已删除');
        } else {
            console.log(`警告: 仍有 ${remainingCount} 条发布日期为空的记录未被删除`);
        }

        // 统计剩余记录总数
        const totalCount = await prisma.movie.count();
        console.log(`数据库中现在共有 ${totalCount} 条电影记录`);

    } catch (error) {
        console.error('清理数据库时出错:', error);
    } finally {
        await prisma.$disconnect();
        console.log('\n数据库连接已关闭');
    }
}

// 执行清理
cleanDatabase()
    .then(() => {
        console.log('清理操作完成');
        exit(0);
    })
    .catch(error => {
        console.error('清理过程中发生错误:', error);
        exit(1);
    }); 