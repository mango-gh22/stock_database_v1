# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\clean_database.py
# File Name: clean_database
# @ Author: mango-gh22
# @ Date：2026/1/6 21:07
"""
desc 清空数据库中全部数据，保留字段
"""

# File Path: E:/MyFile/stock_database_v1/scripts/clean_database.py
"""
安全清空数据库中的数据
"""

import sys
import os
import logging
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.config.logging_config import setup_logging

logger = setup_logging()


def clean_database_data():
    """清空数据库中的股票数据"""
    print("\n" + "=" * 60)
    print("⚠️  WARNING: 即将清空数据库中的股票数据")
    print("=" * 60)

    try:
        # 确认
        confirmation = input("⚠️  确认要清空数据库中的所有股票数据吗？(输入 'YES' 继续): ")
        if confirmation != 'YES':
            print("操作已取消")
            return False

        # 再次确认
        confirmation2 = input("⚠️  ⚠️  再次确认！这将删除所有数据，无法恢复！(输入 'CONFIRM' 继续): ")
        if confirmation2 != 'CONFIRM':
            print("操作已取消")
            return False

        # 连接到数据库
        db = DatabaseConnector()

        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                print("\n正在清空数据...")

                # 1. 先统计现有数据
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
                total_count = cursor.fetchone()[0]
                print(f"当前数据量: {total_count:,} 条记录")

                # 2. 清空表
                start_time = time.time()
                cursor.execute("TRUNCATE TABLE stock_daily_data")
                conn.commit()
                end_time = time.time()

                # 3. 验证
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
                after_count = cursor.fetchone()[0]

                print(f"\n✅ 数据清空完成！")
                print(f"   删除记录: {total_count:,} 条")
                print(f"   剩余记录: {after_count:,} 条")
                print(f"   耗时: {end_time - start_time:.2f} 秒")

                if after_count == 0:
                    print("\n🎉 数据库已清空，可以开始批量下载测试")
                    return True
                else:
                    print("\n❌ 清空失败，仍有数据")
                    return False

    except Exception as e:
        logger.error(f"清空数据库失败: {e}")
        print(f"\n❌ 错误: {e}")
        return False


def main():
    """主函数"""
    print("数据库清理工具")
    print("-" * 40)

    # 显示当前数据统计
    try:
        db = DatabaseConnector()
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(DISTINCT symbol) as symbol_count,
                        MIN(trade_date) as earliest_date,
                        MAX(trade_date) as latest_date
                    FROM stock_daily_data
                """)

                result = cursor.fetchone()
                total, symbols, earliest, latest = result

                print("📊 当前数据统计:")
                print(f"   总记录数: {total:,} 条")
                print(f"   股票数量: {symbols} 只")
                print(f"   最早日期: {earliest}")
                print(f"   最新日期: {latest}")
    except Exception as e:
        print(f"获取统计数据失败: {e}")

    # 清空数据
    success = clean_database_data()

    if success:
        print("\n" + "=" * 60)
        print("✅ 数据库清理完成")
        print("=" * 60)
        print("\n💡 下一步:")
        print("1. 运行批量下载测试: python scripts/full_batch_test.py")
        print("2. 验证数据是否成功下载")
    else:
        print("\n❌ 数据库清理失败")

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)