# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\check_database_reality.py
# File Name: check_database_reality
# @ Author: mango-gh22
# @ Date：2025/12/28 18:00
"""
desc 
"""
# check_database_reality.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.db_connector import DatabaseConnector
import pandas as pd
import mysql.connector
from datetime import datetime


def check_reality():
    """检查数据库现实情况"""
    print("🔍 数据库现实检查")
    print("=" * 50)

    try:
        # 方法1：使用项目中的连接器
        print("1. 使用项目DatabaseConnector检查:")
        connector = DatabaseConnector()

        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                # 检查总记录数
                cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
                total1 = cursor.fetchone()['total']
                print(f"   总记录数（通过项目连接器）: {total1}")

                # 检查特定股票
                cursor.execute("""
                    SELECT symbol, COUNT(*) as count 
                    FROM stock_daily_data 
                    WHERE symbol LIKE 'sh6%' 
                    GROUP BY symbol 
                    ORDER BY count DESC 
                    LIMIT 5
                """)
                stocks1 = cursor.fetchall()
                print(f"   前5只sh6开头股票: {[(s['symbol'], s['count']) for s in stocks1]}")

        # 方法2：直接连接（绕过项目配置）
        print("\n2. 直接MySQL连接检查:")
        try:
            # 使用你的实际数据库配置
            direct_conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",  # 你的用户名
                password="",  # 你的密码
                database="stock_database"
            )

            with direct_conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
                total2 = cursor.fetchone()['total']
                print(f"   总记录数（直接连接）: {total2}")

                cursor.execute("SELECT DATABASE() as db, USER() as user")
                info = cursor.fetchone()
                print(f"   数据库: {info['db']}, 用户: {info['user']}")

                # 检查最近的数据
                cursor.execute("""
                    SELECT symbol, trade_date, close_price, created_time 
                    FROM stock_daily_data 
                    ORDER BY created_time DESC 
                    LIMIT 5
                """)
                recent = cursor.fetchall()
                print(f"   最近5条数据:")
                for row in recent:
                    print(f"     {row['symbol']} | {row['trade_date']} | {row['close_price']} | {row['created_time']}")

            direct_conn.close()

        except Exception as e:
            print(f"   直接连接失败: {e}")

        # 方法3：执行一个真实的插入测试
        print("\n3. 执行真实插入测试:")
        test_data = pd.DataFrame({
            'symbol': ['REALTEST001'],
            'trade_date': [datetime.now().strftime('%Y-%m-%d')],
            'open_price': [999.99],
            'close_price': [1000.00],
            'volume': [999999]
        })

        with connector.get_connection() as conn:
            with conn.cursor() as cursor:
                # 先清理
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol = 'REALTEST001'")
                conn.commit()

                # 插入
                insert_sql = """
                    INSERT INTO stock_daily_data 
                    (symbol, trade_date, open_price, close_price, volume, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                """
                cursor.execute(insert_sql, (
                    'REALTEST001',
                    datetime.now().strftime('%Y-%m-%d'),
                    999.99, 1000.00, 999999
                ))
                conn.commit()
                print(f"   插入测试数据完成")

                # 立即查询
                cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = 'REALTEST001'")
                result = cursor.fetchall()
                print(f"   查询结果: {len(result)} 条记录")

                # 在其他连接中查询
                cursor2 = conn.cursor(dictionary=True)
                cursor2.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = 'REALTEST001'")
                count_result = cursor2.fetchone()
                print(f"   验证查询: {count_result['count']} 条记录")

                # 清理
                cursor.execute("DELETE FROM stock_daily_data WHERE symbol = 'REALTEST001'")
                conn.commit()
                print(f"   清理测试数据")

        # 方法4：检查表大小
        print("\n4. 检查表大小:")
        with connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT 
                        table_name,
                        table_rows,
                        data_length / 1024 / 1024 as data_mb,
                        index_length / 1024 / 1024 as index_mb
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name = 'stock_daily_data'
                """)
                table_info = cursor.fetchone()
                if table_info:
                    print(f"   表名: {table_info['table_name']}")
                    print(f"   行数: {table_info['table_rows']:,}")
                    print(f"   数据大小: {table_info['data_mb']:.2f} MB")
                    print(f"   索引大小: {table_info['index_mb']:.2f} MB")

        print("\n✅ 检查完成")

    except Exception as e:
        print(f"❌ 检查失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_reality()