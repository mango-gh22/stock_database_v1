# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_direct_insert.py
# File Name: test_direct_insert
# @ Author: mango-gh22
# @ Date：2025/12/28 17:25
"""
desc 
"""
# test_direct_insert.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage
from src.database.db_connector import DatabaseConnector
import pandas as pd
from datetime import datetime
import time


def test_direct_insert():
    """直接测试数据存储"""
    print("🧪 直接测试数据存储")
    print("=" * 50)

    try:
        # 1. 直接测试数据库连接
        print("1. 测试数据库连接...")
        connector = DatabaseConnector()

        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
                result = cursor.fetchone()
                print(f"   当前数据表记录数: {result['count']}")

        # 2. 创建测试数据
        print("\n2. 创建测试数据...")
        test_data = pd.DataFrame({
            'symbol': ['TEST001', 'TEST001'],
            'trade_date': ['2025-12-28', '2025-12-29'],
            'open_price': [100.0, 101.0],
            'high_price': [105.0, 106.0],
            'low_price': [99.0, 100.0],
            'close_price': [102.0, 103.0],
            'pre_close_price': [100.0, 102.0],
            'volume': [1000000, 1200000],
            'amount': [102000000.0, 123600000.0],
            'change_percent': [2.0, 0.98],
            'turnover_rate': [1.5, 1.8],
            'amplitude': [6.0, 6.0],
            'data_source': ['test', 'test'],
            'processed_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 2
        })

        print(f"   测试数据字段: {list(test_data.columns)}")
        print(f"   测试数据行数: {len(test_data)}")

        # 3. 直接SQL插入（绕过存储层）
        print("\n3. 直接SQL插入测试...")
        with connector.get_connection() as conn:
            with conn.cursor() as cursor:
                # 先删除可能存在的测试数据
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol = 'TEST001'")
                conn.commit()
                print("   清理旧的测试数据")

                # 直接插入
                insert_sql = """
                    INSERT INTO stock_daily_data 
                    (symbol, trade_date, open_price, high_price, low_price, close_price, 
                     pre_close_price, volume, amount, change_percent, turnover_rate, 
                     amplitude, data_source, processed_time, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """

                for _, row in test_data.iterrows():
                    params = (
                        row['symbol'], row['trade_date'],
                        float(row['open_price']), float(row['high_price']),
                        float(row['low_price']), float(row['close_price']),
                        float(row['pre_close_price']), int(row['volume']),
                        float(row['amount']), float(row['change_percent']),
                        float(row['turnover_rate']), float(row['amplitude']),
                        row['data_source'], row['processed_time']
                    )
                    cursor.execute(insert_sql, params)

                conn.commit()
                print(f"   直接插入 {len(test_data)} 条记录")

        # 4. 验证插入
        print("\n4. 验证插入结果...")
        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT symbol, trade_date, close_price, volume 
                    FROM stock_daily_data 
                    WHERE symbol = 'TEST001' 
                    ORDER BY trade_date
                """)
                results = cursor.fetchall()

                if results:
                    print(f"   查询到 {len(results)} 条记录:")
                    for row in results:
                        print(f"     {row['symbol']} | {row['trade_date']} | {row['close_price']} | {row['volume']}")
                else:
                    print("   ❌ 未查询到测试数据")

        # 5. 使用存储层测试
        print("\n5. 使用DataStorage测试...")
        storage = DataStorage()

        # 创建另一批测试数据
        test_data2 = pd.DataFrame({
            'symbol': ['TEST002', 'TEST002'],
            'trade_date': ['2025-12-28', '2025-12-29'],
            'open_price': [200.0, 201.0],
            'close_price': [202.0, 203.0],
            'volume': [2000000, 2200000],
            'amount': [202000000.0, 223600000.0]
        })

        print(f"   存储测试数据: {len(test_data2)} 条")

        # 使用存储层方法
        rows_affected, status = storage.store_daily_data(test_data2)
        print(f"   存储层返回: rows={rows_affected}, status={status['status']}")

        # 验证存储层插入
        time.sleep(1)  # 等待一下
        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = 'TEST002'")
                result = cursor.fetchone()
                print(f"   验证TEST002记录数: {result['count']}")

        # 6. 最终统计
        print("\n6. 最终统计...")
        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT symbol) as symbols,
                        MIN(trade_date) as earliest,
                        MAX(trade_date) as latest
                    FROM stock_daily_data
                """)
                stats = cursor.fetchone()
                print(f"   总记录数: {stats['total']}")
                print(f"   股票数: {stats['symbols']}")
                print(f"   日期范围: {stats['earliest']} 到 {stats['latest']}")

        # 7. 清理测试数据
        print("\n7. 清理测试数据...")
        with connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol IN ('TEST001', 'TEST002')")
                conn.commit()
                print("   测试数据已清理")

        print("\n✅ 测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


def check_table_structure():
    """检查表结构"""
    print("\n🔍 检查表结构")
    print("=" * 50)

    try:
        connector = DatabaseConnector()

        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                # 查看表结构
                cursor.execute("DESCRIBE stock_daily_data")
                columns = cursor.fetchall()

                print(f"表 stock_daily_data 结构 ({len(columns)} 列):")
                print("-" * 80)
                print(f"{'字段名':<20} {'类型':<20} {'可为空':<8} {'键':<8} {'默认值':<15}")
                print("-" * 80)

                for col in columns:
                    print(
                        f"{col['Field']:<20} {col['Type']:<20} {col['Null']:<8} {col['Key']:<8} {str(col['Default'] or ''):<15}")

                # 查看唯一键约束
                cursor.execute("SHOW INDEX FROM stock_daily_data WHERE Non_unique = 0")
                unique_indexes = cursor.fetchall()

                print(f"\n唯一键约束 ({len(unique_indexes)} 个):")
                for idx in unique_indexes:
                    print(f"  索引名: {idx['Key_name']}, 列: {idx['Column_name']}, 顺序: {idx['Seq_in_index']}")

    except Exception as e:
        print(f"❌ 检查表结构失败: {e}")


if __name__ == "__main__":
    test_direct_insert()
    check_table_structure()