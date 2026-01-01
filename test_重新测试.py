# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_重新测试.py
# File Name: test_重新测试
# @ Author: mango-gh22
# @ Date：2026/1/1 23:42
"""
desc 
"""
import sys
sys.path.append('.')

from src.data.integrated_pipeline import IntegratedDataPipeline

pipeline = IntegratedDataPipeline()
result = pipeline.process_single_stock('sh600519', '20240101', '20240105')

print("\n" + "="*50)
print("存储结果:", result)
print("="*50)

# 如果成功，验证数据库
if result['status'] == 'success':
    with pipeline.storage.db_connector.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s", ('sh600519',))
            count = cursor.fetchone()[0]
            print(f"\n✅ 数据库验证: sh600519 有 {count} 条记录")