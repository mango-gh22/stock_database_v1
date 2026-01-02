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
# 验证数据库 - 修复：查询实际处理的symbol
# 验证数据库 - 防崩溃版
# 验证数据库 - 查询实际处理的日期范围
if result['status'] == 'success':
    print("\n验证数据库...")
    symbol_to_check = result['symbol']

    # 查询我们实际下载的日期范围
    start_date_query = '2024-01-01'
    end_date_query = '2024-01-15'

    with pipeline.storage.db_connector.get_connection() as conn:
        with conn.cursor() as cursor:
            # 查询指定日期范围的记录数
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily_data 
                WHERE symbol = %s 
                AND trade_date BETWEEN %s AND %s
            """, (symbol_to_check, start_date_query, end_date_query))
            count = cursor.fetchone()[0]
            print(f"数据库中 {symbol_to_check} 在{start_date_query}~{end_date_query}的记录数: {count}")

            if count > 0:
                cursor.execute("""
                    SELECT trade_date, open_price, close_price 
                    FROM stock_daily_data 
                    WHERE symbol = %s 
                    AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                """, (symbol_to_check, start_date_query, end_date_query))
                rows = cursor.fetchall()
                print(f"\n{symbol_to_check} 数据预览:")
                for row in rows:
                    print(f"  {row}")
            else:
                print(f"⚠️ 数据库中未找到 {symbol_to_check} 在指定日期的数据")