# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnostic_table_columns.py
# File Name: diagnose_table_columns
# @ Author: mango-gh22
# @ Date：2026/1/1 23:26
"""
desc 
"""
# diagnostic_table_columns.py
import sys
sys.path.append('../..')

from src.data.adaptive_storage import AdaptiveDataStorage

storage = AdaptiveDataStorage()
print(f"Table name: {storage.table_name}")
print(f"Table columns count: {len(storage.table_columns)}")
print("Table columns:", storage.table_columns)

# 测试列名是否匹配
test_cols = ['trade_date', 'symbol', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
matches = [col for col in test_cols if col in storage.table_columns]
print(f"\n匹配列: {len(matches)}/{len(test_cols)}")
print("匹配详情:", matches)