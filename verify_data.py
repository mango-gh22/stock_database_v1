# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\verify_data.py
# File Name: verify_data
# @ Author: mango-gh22
# @ Date：2025/12/31 22:27
"""
desc 快速查询脚本
"""
# verify_data.py
import sys

sys.path.insert(0, r"E:\MyFile\stock_database_v1")

from src.database.db_connector import DatabaseConnector

db = DatabaseConnector()

print("🔍 验证数据库中的数据")
print("=" * 50)

# 1. 查询今天新增的数据
today = '2025-12-31'  # 如果是今天的话
query = """
    SELECT symbol, trade_date, open_price, close_price, volume, created_time 
    FROM stock_daily_data 
    WHERE symbol LIKE '%600000%' 
    ORDER BY created_time DESC 
    LIMIT 5
"""

print("查询 sh600000 的最新数据:")
with db.get_connection() as conn:
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        if results:
            for i, row in enumerate(results, 1):
                print(f"{i}. {row['symbol']} | {row['trade_date']} | "
                      f"收盘: {row['close_price']} | 成交量: {row['volume']:,} | "
                      f"创建: {row['created_time']}")
        else:
            print("未找到 sh600000 的数据")

# 2. 查询表统计
print(f"\n📊 数据表统计:")
with db.get_connection() as conn:
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
        total = cursor.fetchone()['total']
        print(f"总记录数: {total:,}")

        cursor.execute("SELECT COUNT(DISTINCT symbol) as symbols FROM stock_daily_data")
        symbols = cursor.fetchone()['symbols']
        print(f"股票数量: {symbols}")

        # 查询今天新增的记录
        cursor.execute("""
            SELECT COUNT(*) as today_count 
            FROM stock_daily_data 
            WHERE DATE(created_time) = CURDATE()
        """)
        today_count = cursor.fetchone()['today_count']
        print(f"今日新增: {today_count}")