# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\check_existing_data.py
# File Name: check_existing_data
# @ Author: mango-gh22
# @ Date：2026/1/1 13:22
"""
desc 
"""
# check_existing_data.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("🔍 检查数据库中现有数据")
print("=" * 60)

from src.database.db_connector import DatabaseConnector
import pandas as pd

db = DatabaseConnector()
conn = db.get_connection()
cursor = conn.cursor(dictionary=True)

# 检查 sh.600000
symbol = "sh600000"  # 注意：数据库中存储的是没有点的格式
print(f"\n检查 {symbol} 的数据:")

cursor.execute("""
    SELECT trade_date, open_price, close_price, volume, created_time
    FROM stock_daily_data 
    WHERE symbol = %s
    ORDER BY trade_date DESC
    LIMIT 10
""", (symbol,))

records = cursor.fetchall()
print(f"找到 {len(records)} 条记录:")

for record in records:
    print(f"  {record['trade_date']}: 开盘{record['open_price']}, 收盘{record['close_price']}, 创建于{record['created_time']}")

# 检查最近的创建时间
print(f"\n检查今天 {pd.Timestamp.now().date()} 的数据:")
cursor.execute("""
    SELECT COUNT(*) as today_count 
    FROM stock_daily_data 
    WHERE DATE(created_time) = CURDATE()
""")
today_count = cursor.fetchone()['today_count']
print(f"今天创建的数据: {today_count} 条")

# 检查今天创建的 sh.600000 数据
cursor.execute("""
    SELECT trade_date, created_time
    FROM stock_daily_data 
    WHERE symbol = %s AND DATE(created_time) = CURDATE()
    ORDER BY created_time DESC
""", (symbol,))

today_records = cursor.fetchall()
print(f"今天创建的 {symbol} 数据: {len(today_records)} 条")

for record in today_records:
    print(f"  交易日期: {record['trade_date']}, 创建时间: {record['created_time']}")

conn.close()

print("\n" + "=" * 60)
print("💡 分析结果：")
print(f"1. {symbol} 已经有 {len(records)} 条历史数据")
print(f"2. 今天创建了 {today_count} 条新数据")
print(f"3. {symbol} 今天创建了 {len(today_records)} 条数据")