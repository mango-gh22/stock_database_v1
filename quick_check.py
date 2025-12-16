# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\quick_check.py
# File Name: quick_check
# @ Author: mango-gh22
# @ Date：2025/12/14 18:26
"""
desc 
"""
# quick_check.py
"""
快速检查列名映射
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from src.database.db_connector import DatabaseConnector

db = DatabaseConnector()

print("🔍 快速检查表结构和列名")
print("=" * 60)

# 1. 检查表结构
print("\n1. stock_daily_data 表列名:")
result = db.execute_query("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'stock_daily_data'
    ORDER BY ORDINAL_POSITION
""")

important_cols = ['open_price', 'high_price', 'low_price', 'close_price',
                  'pre_close_price', 'change_percent', 'volume', 'amount']

for row in result:
    col_name = row['COLUMN_NAME']
    if col_name in important_cols:
        print(f"  ✓ {col_name:20} {row['DATA_TYPE']}")

# 2. 检查数据示例
print("\n2. 数据示例（最新3条）:")
result = db.execute_query("""
    SELECT 
        trade_date,
        symbol,
        open_price,
        high_price, 
        low_price,
        close_price,
        change_percent,
        volume
    FROM stock_daily_data 
    ORDER BY trade_date DESC 
    LIMIT 3
""")

if result:
    for i, row in enumerate(result):
        print(f"\n  记录 {i+1}:")
        print(f"    日期: {row['trade_date']}")
        print(f"    代码: {row['symbol']}")
        print(f"    价格: {row['open_price']} / {row['high_price']} / {row['low_price']} / {row['close_price']}")
        print(f"    涨跌: {row.get('change_percent', 0):+.2f}%")
        print(f"    成交量: {row.get('volume', 0):,}")

db.close_all_connections()
print("\n" + "=" * 60)
print("✅ 检查完成")