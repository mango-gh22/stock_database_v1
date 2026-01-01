# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_safe_storage.py
# File Name: test_safe_storage
# @ Author: mango-gh22
# @ Date：2025/12/28 17:48
"""
desc 
"""
# test_safe_storage_v2.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage
import pandas as pd
from datetime import datetime


def test_safe_storage():
    """测试安全存储"""
    print("🧪 测试安全存储")
    print("=" * 50)

    storage = DataStorage()

    try:
        # 创建测试数据
        test_data = pd.DataFrame({
            'symbol': ['SAFETEST001', 'SAFETEST001'],
            'trade_date': ['2025-12-28', '2025-12-29'],
            'open_price': [100.0, 101.0],
            'close_price': [102.0, 103.0],
            'volume': [1000000, 1200000]
        })

        print(f"测试数据: {len(test_data)} 条")
        print(f"数据字段: {list(test_data.columns)}")

        # 先清理可能存在的旧数据
        with storage.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol = 'SAFETEST001'")
                conn.commit()
                print("清理旧测试数据")

        # 测试安全存储
        print("\n开始安全存储测试...")
        result = storage.safe_store_daily_data(test_data)

        print(f"\n安全存储结果:")
        print(f"  行数: {result[0]}")
        print(f"  状态: {result[1]['status']}")
        print(f"  表名: {result[1].get('table', 'N/A')}")

        # 立即验证
        print("\n立即验证数据库...")
        with storage.db_connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT symbol, trade_date, close_price, volume 
                    FROM stock_daily_data 
                    WHERE symbol = 'SAFETEST001'
                    ORDER BY trade_date
                """)
                rows = cursor.fetchall()

                if rows:
                    print(f"✅ 数据库中找到 {len(rows)} 条记录:")
                    for row in rows:
                        print(f"   {row['symbol']} | {row['trade_date']} | {row['close_price']} | {row['volume']}")
                else:
                    print("❌ 数据库中未找到测试数据")

        # 清理
        print("\n清理测试数据...")
        with storage.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol = 'SAFETEST001'")
                conn.commit()
                print("测试数据已清理")

        print("\n✅ 测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_safe_storage()