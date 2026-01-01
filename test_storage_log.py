# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_storage_log.py
# File Name: test_stroage_log
# @ Author: mango-gh22
# @ Date：2025/12/31 21:24
"""
desc 
"""
# test_storage_log.py
import sys
sys.path.insert(0, r"E:\MyFile\stock_database_v1")

from src.data.data_storage import DataStorage

storage = DataStorage()

# 测试1：最简单的调用（DataScheduler 使用的格式）
print("测试1: DataScheduler 格式")
result1 = storage.log_data_update('daily', 'sh600000', 5, 'success')
print(f"结果: {result1}")

# 测试2：完整参数调用（DataPipeline 使用的格式）
print("\n测试2: DataPipeline 格式")
result2 = storage.log_data_update(
    data_type='daily',
    symbol='sh600000',
    start_date='20251201',
    end_date='20251228',
    rows_affected=10,
    status='success',
    error_message=None,
    execution_time=1.5
)
print(f"结果: {result2}")

# 测试3：处理元组参数（模拟实际存储返回值）
print("\n测试3: 处理元组参数")
result3 = storage.log_data_update(
    data_type='daily',
    symbol='sh600000',
    start_date='20251201',
    end_date='20251228',
    rows_affected=(3, {'status': 'success', 'table': 'stock_daily_data'}),  # 元组！
    status='success'
)
print(f"结果: {result3}")