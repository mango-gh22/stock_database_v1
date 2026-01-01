# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_raw_data.py
# File Name: test_raw_data
# @ Author: mango-gh22
# @ Date：2025/12/28 22:13
"""
desc 
"""
import logging
# 文件路径: E:\MyFile\stock_database_v1\test_raw_data.py
import sys
sys.path.insert(0, r"E:\MyFile\stock_database_v1")

from src.data.baostock_collector import BaostockCollector
import pandas as pd

logging.basicConfig(level=logging.DEBUG)

collector = BaostockCollector()
collector.login()

# 测试采集单只股票的最原始数据
test_symbol = "sh.600000"  # 使用带点的格式试试
start_date = "2025-12-25"
end_date = "2025-12-28"

print(f"正在采集 {test_symbol} 的原始数据...")
raw_df = collector.query_history_k_data(
    symbol=test_symbol,
    start_date=start_date,
    end_date=end_date
)

collector.logout()

print("采集完成！")
print(f"数据形状: {raw_df.shape}")
print("列名:", list(raw_df.columns) if not raw_df.empty else "空")
if not raw_df.empty:
    print("前3行数据预览:")
    print(raw_df.head(3).to_string())