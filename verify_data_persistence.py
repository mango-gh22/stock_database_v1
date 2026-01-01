# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\verify_data_persistence.py
# File Name: verify_data_persistence
# @ Author: mango-gh22
# @ Date：2025/12/28 20:14
"""
desc 
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\verify_data_persistence.py
# File Name: verify_data_persistence
# @ Author: mango-gh22
# @ Date：2025/12/28 20:06
"""
验证数据持久化 - 测试数据是否真的存储到数据库
"""

import sys
import os
import time
import mysql.connector
from datetime import datetime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def verify_data_persistence():
    """验证数据是否真的持久化到数据库"""
    print("🔍 数据持久化验证")
    print("=" * 50)

    # 测试数据
    test_symbol = f"VERIFY_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    test_date = datetime.now().strftime('%Y-%m-%d')

    print(f"测试数据: {test_symbol} {test_date}")

    try:
        # 1. 直接连接到数据库（不通过项目代码）
        print("\n1. 📊 直接MySQL连接验证")
        try:
            conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="stock_user",
                password="",  # 你的实际密码
                database="stock_database",
                autocommit=True
            )

            cursor = conn.cursor(dictionary=True)

            # 插入测试数据
            insert_sql = """
                INSERT INTO stock_daily_data 
                (symbol, trade_date, open_price, close_price, volume, created_time, updated_time)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            """

            cursor.execute(insert_sql, (test_symbol, test_date, 100.0, 101.0, 1000000))
            inserted_id = cursor.lastrowid

            print(f"   ✅ 直接插入成功，行ID: {inserted_id}")

            # 立即查询验证
            cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
            results = cursor.fetchall()
            print(f"   📋 查询结果: {len(results)} 条记录")

            for row in results:
                print(f"      ID: {row['id']}, Symbol: {row['symbol']}, Date: {row['trade_date']}")

            # 保持数据不删除，稍后验证
            print(f"   💾 数据保留供后续验证")

            conn.close()

        except Exception as e:
            print(f"   ❌ 直接连接失败: {e}")
            return False

        print("\n2. 🕐 等待3秒...")
        time.sleep(3)

        # 2. 再次连接验证数据是否还在
        print("\n3. 🔄 重新连接验证数据持久性")
        try:
            conn2 = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="stock_user",
                password="",  # 你的实际密码
                database="stock_database",
                autocommit=True
            )

            cursor2 = conn2.cursor(dictionary=True)

            # 重新查询
            cursor2.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
            persistent_results = cursor2.fetchall()

            print(f"   📋 持久化查询结果: {len(persistent_results)} 条记录")

            if len(persistent_results) > 0:
                print(f"   ✅ 数据持久化验证成功！")
                for row in persistent_results:
                    print(
                        f"      ID: {row['id']}, Symbol: {row['symbol']}, Date: {row['trade_date']}, Created: {row['created_time']}")
            else:
                print(f"   ❌ 数据未持久化，可能被回滚或清理")

            # 3. 使用项目中的DataStorage验证
            print("\n4. 🔧 使用项目DataStorage验证")
            from src.data.data_storage import DataStorage

            storage = DataStorage()
            is_verified = storage.verify_data_insertion(test_symbol, test_date)

            if is_verified:
                print(f"   ✅ DataStorage验证成功")
            else:
                print(f"   ❌ DataStorage验证失败")

            # 4. 统计表中有多少数据
            print("\n5. 📈 统计数据表信息")
            cursor2.execute("SELECT COUNT(*) as total FROM stock_daily_data")
            total_count = cursor2.fetchone()['total']
            print(f"   📊 表中总记录数: {total_count:,}")

            # 检查最近的数据
            cursor2.execute(
                "SELECT symbol, trade_date, created_time FROM stock_daily_data ORDER BY created_time DESC LIMIT 5")
            recent_data = cursor2.fetchall()

            print(f"   🕒 最近5条数据:")
            for i, row in enumerate(recent_data, 1):
                print(f"      {i}. {row['symbol']} | {row['trade_date']} | {row['created_time']}")

            # 5. 清理测试数据（可选）
            print("\n6. 🧹 清理测试数据")
            cleanup = input("是否清理测试数据？(y/n): ").strip().lower()

            if cleanup == 'y':
                cursor2.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
                deleted_count = cursor2.rowcount
                conn2.commit()
                print(f"   🗑️  清理完成，删除了 {deleted_count} 条记录")
            else:
                print(f"   📝 测试数据保留在数据库中")
                print(f"     符号: {test_symbol}")
                print(f"     日期: {test_date}")

            conn2.close()

            return len(persistent_results) > 0

        except Exception as e:
            print(f"   ❌ 重新连接验证失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    except Exception as e:
        print(f"❌ 验证过程异常: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_database_status():
    """检查数据库状态"""
    print("\n" + "=" * 50)
    print("📋 数据库状态检查")
    print("=" * 50)

    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="stock_user",
            password="",  # 你的实际密码
            database="stock_database"
        )

        cursor = conn.cursor(dictionary=True)

        # 1. 检查表大小
        print("\n1. 📊 表大小检查")
        cursor.execute("""
            SELECT 
                table_name,
                table_rows,
                ROUND(data_length / 1024 / 1024, 2) as data_mb,
                ROUND(index_length / 1024 / 1024, 2) as index_mb,
                ROUND((data_length + index_length) / 1024 / 1024, 2) as total_mb,
                create_time,
                update_time
            FROM information_schema.tables 
            WHERE table_schema = DATABASE()
            ORDER BY table_rows DESC
        """)

        tables = cursor.fetchall()

        for table in tables:
            print(f"   {table['table_name']:25} {table['table_rows']:12,} 行 | {table['total_mb']:6.2f} MB")

        # 2. 检查 stock_daily_data 表的详细信息
        print("\n2. 📈 stock_daily_data 表详情")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date,
                COUNT(DISTINCT symbol) as distinct_symbols
            FROM stock_daily_data
        """)

        stats = cursor.fetchone()

        if stats:
            print(f"   总记录数: {stats['total_records']:,}")
            print(f"   最早日期: {stats['earliest_date']}")
            print(f"   最新日期: {stats['latest_date']}")
            print(f"   股票数量: {stats['distinct_symbols']}")

            # 检查是否有今天的数据
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("SELECT COUNT(*) as today_count FROM stock_daily_data WHERE trade_date = %s", (today,))
            today_count = cursor.fetchone()['today_count']
            print(f"   今日数据: {today_count} 条")

        # 3. 检查最近添加的数据
        print("\n3. 🕒 最近添加的数据")
        cursor.execute("""
            SELECT 
                symbol,
                trade_date,
                created_time,
                open_price,
                close_price,
                volume
            FROM stock_daily_data 
            ORDER BY created_time DESC 
            LIMIT 10
        """)

        recent = cursor.fetchall()

        for i, row in enumerate(recent, 1):
            created_time = row['created_time'].strftime('%H:%M:%S') if row['created_time'] else 'N/A'
            print(f"   {i:2}. {row['symbol']:15} {row['trade_date']} {created_time} | {row['close_price']:8.2f}")

        # 4. 检查数据源分布
        print("\n4. 📊 数据源分布")
        cursor.execute("""
            SELECT 
                COALESCE(data_source, 'unknown') as source,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM stock_daily_data), 2) as percentage
            FROM stock_daily_data
            GROUP BY data_source
            ORDER BY count DESC
        """)

        sources = cursor.fetchall()

        for source in sources:
            print(f"   {source['source']:15} {source['count']:10,} 条 ({source['percentage']:5.2f}%)")

        conn.close()

        print("\n✅ 数据库状态检查完成")

    except Exception as e:
        print(f"❌ 数据库状态检查失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("数据持久化验证工具")
    print("=" * 60)

    # 先验证数据持久化
    persistence_success = verify_data_persistence()

    if persistence_success:
        print("\n" + "=" * 50)
        print("🎉 数据持久化验证成功！")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("❌ 数据持久化验证失败")
        print("=" * 50)

    # 检查数据库状态
    check_database_status()

    # 最终建议
    print("\n" + "=" * 60)
    print("💡 下一步建议")
    print("=" * 60)

    if persistence_success:
        print("1. ✅ 数据存储功能正常，可以开始数据采集")
        print("2. 📊 使用 MySQL Workbench 或 phpMyAdmin 查看数据库")
        print("3. 🔧 运行实际的数据采集脚本测试")
        print("4. 📈 检查日志文件了解详细执行情况")
    else:
        print("1. 🔍 检查 MySQL 用户权限")
        print("2. ⚙️  检查数据库配置文件")
        print("3. 🔄 检查是否有自动清理脚本")
        print("4. 📝 查看 MySQL 错误日志")

    print("\n📝 验证完成时间: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    main()