# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_pipeline_simple.py
# File Name: test_pipeline_simple
# @ Author: mango-gh22
# @ Date：2025/12/31 21:25
"""
desc 
"""
# test_pipeline_simple.py
import sys
sys.path.insert(0, r"E:\MyFile\stock_database_v1")

from src.data.data_pipeline import DataPipeline
from src.data.baostock_collector import BaostockCollector
from src.data.data_storage import DataStorage

# 创建组件
collector = BaostockCollector()
storage = DataStorage()

print("创建数据管道...")
pipeline = DataPipeline(collector=collector, storage=storage)

# 测试单只股票处理
print("\n测试单只股票处理...")
result = pipeline.fetch_and_store_daily_data(
    symbol='sh600000',
    start_date='20251225',
    end_date='20251228'
)

print(f"处理结果: {result}")
print(f"状态: {result.get('status')}")
print(f"存储记录数: {result.get('records_stored')}")
print(f"是否为整数: {isinstance(result.get('records_stored'), int)}")