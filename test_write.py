# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_write.py
# File Name: test_write
# @ Author: mango-gh22
# @ Date：2025/12/27 20:59
"""
desc 
"""
# test_write.py
import pandas as pd
from src.data.data_storage import DataStorage

# 构造测试数据（使用原始列名，如 'open'）
df = pd.DataFrame({
    'symbol': ['sh600519'],
    'trade_date': ['20251227'],
    'open': [1800.0],
    'high': [1820.0],
    'low': [1790.0],
    'close': [1810.0],
    'volume': [100000],
    'amount': [1.81e9],
    'data_source': ['test'],
    'processed_time': [pd.Timestamp.now()],
    'quality_grade': ['A']
})

storage = DataStorage('config/database.yaml')  # 确保路径正确
rows, report = storage.store_daily_data(df)
print("✅ 写入结果:", report)

# 查询验证
conn = storage.db_connector.get_connection()
result = pd.read_sql("""
    SELECT symbol, trade_date, open_price, close_price, volume 
    FROM stock_daily_data 
    WHERE symbol='sh600519' AND trade_date='20251227'
""", conn)
print("\n🔍 数据库实际内容:")
print(result)
conn.close()