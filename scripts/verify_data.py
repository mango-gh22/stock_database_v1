# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\verify_data.py
# File Name: verify_data
# @ Author: mango-gh22
# @ Date：2026/1/10 23:16
"""
desc Python实时查询（最可靠）数据库
"""

import pandas as pd
from src.database.db_connector import DatabaseConnector

db = DatabaseConnector()

# 查询sh600036所有数据
query = """
    SELECT trade_date, open_price, close_price, volume 
    FROM stock_daily_data 
    WHERE symbol = 'sh600036'
    ORDER BY trade_date DESC
    LIMIT 20
"""

with db.get_connection() as conn:
    df = pd.read_sql(query, conn)
    print(f"\n✅ sh600036 数据确认：{len(df)} 条记录")
    print(df.to_string(index=False))