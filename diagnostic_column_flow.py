# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnostic_column_flow.py
# File Name: diagnostic_column_flow
# @ Author: mango-gh22
# @ Date：2026/1/1 23:18
"""
desc 
"""
# diagnostic_column_flow.py
import sys
sys.path.append('.')

from src.data.baostock_collector import BaostockCollector
from src.data.enhanced_processor import EnhancedDataProcessor

# 1. 采集层原始列名
collector = BaostockCollector()
raw_df = collector.fetch_daily_data('sh600519', '20240101', '20240105')
print("=== Baostock 原始列名 ===")
print(raw_df.columns.tolist())
print(raw_df.head(2))

# 2. 处理后列名（如果存在enhanced_processor）
processor = EnhancedDataProcessor()
processed_df, _ = processor.process_stock_data(raw_df, 'sh600519', 'baostock')
print("\n=== Processor 处理后列名 ===")
print(processed_df.columns.tolist())
print(processed_df.head(2))