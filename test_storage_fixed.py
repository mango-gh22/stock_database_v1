# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_storage_fixed.py
# File Name: test_storage_fixed
# @ Author: mango-gh22
# @ Date：2026/1/1 23:33
"""
desc 
"""
# test_storage_fixed.py
import sys

sys.path.append('.')

from src.data.integrated_pipeline import IntegratedDataPipeline

pipeline = IntegratedDataPipeline()
result = pipeline.process_single_stock('sh600036', '20240101', '20240115')

print("\n" + "=" * 50)
print("存储结果:", result)
print("=" * 50)

# 验证数据库
if result['status'] == 'success':
    print("\n验证数据库...")
    with pipeline.storage.db_connector.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s", ('sh600519',))
            count = cursor.fetchone()[0]
            print(f"数据库中 sh600519 的记录数: {count}")

            if count > 0:
                cursor.execute(
                    "SELECT trade_date, open_price, close_price FROM stock_daily_data WHERE symbol = %s ORDER BY trade_date DESC LIMIT 2",
                    ('sh600519',))
                rows = cursor.fetchall()
                print("\n最新2条数据:")
                for row in rows:
                    print(f"  {row}")