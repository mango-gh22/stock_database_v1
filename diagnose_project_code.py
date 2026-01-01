# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_project_code.py
# File Name: diagnose_project_code
# @ Author: mango-gh22
# @ Date：2026/1/1 9:39
"""
desc 
"""
# diagnose_project_code.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("🔍 项目代码数据流详细诊断")
print("=" * 60)

# 1. 测试 BaostockCollector
print("1. 测试 BaostockCollector...")
try:
    from src.data.baostock_collector import BaostockCollector

    collector = BaostockCollector()
    print("   ✅ BaostockCollector 初始化成功")

    # 测试采集
    test_data = collector.fetch_daily_data(
        symbol="sh.600028",
        start_date="2025-12-29",
        end_date="2025-12-31"
    )

    # 修复诊断脚本
    # 在 BaostockCollector 测试部分
    if test_data is not None and not test_data.empty:  # 检查 DataFrame
        print(f"     采集到 {len(test_data)} 条记录")
        print(f"     第一条记录字段: {list(test_data.columns)}")
        print(f"     示例数据: {test_data.iloc[0].to_dict()}")


    # print(f"   ✅ 数据采集成功")
    # print(f"     采集到 {len(test_data)} 条记录")
    #
    # if test_data:
    #     print(f"     第一条记录字段: {list(test_data[0].keys())}")
    #     print(f"     示例数据: {test_data[0]}")

except Exception as e:
    print(f"   ❌ BaostockCollector 失败: {e}")
    import traceback

    traceback.print_exc()

# 2. 测试 DataStorage
print("\n2. 测试 DataStorage...")
try:
    from src.data.data_storage import DataStorage

    storage = DataStorage()
    print("   ✅ DataStorage 初始化成功")

    # 创建测试数据（符合 Baostock 格式）
    test_records = [{
        'code': 'sh.600028',
        'date': '2025-12-29',
        'open': 5.21,
        'high': 5.25,
        'low': 5.18,
        'close': 5.23,
        'volume': 50000000,
        'amount': 260000000.0,
        'preclose': 5.20,
        'turnover': 1.5,
        'pctChg': 0.58
    }, {
        'code': 'sh.600028',
        'date': '2025-12-30',
        'open': 5.23,
        'high': 5.28,
        'low': 5.20,
        'close': 5.25,
        'volume': 52000000,
        'amount': 271000000.0,
        'preclose': 5.23,
        'turnover': 1.6,
        'pctChg': 0.38
    }]

    print(f"   准备存储 {len(test_records)} 条测试记录...")

    # 存储数据
    result = storage.store_daily_data(test_records)

    print(f"   ✅ 存储完成")
    print(f"     结果: {result}")

    # 验证存储
    from src.database.db_connector import DatabaseConnector

    db = DatabaseConnector()
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = 'sh.600028'")
    count = cursor.fetchone()['count']
    print(f"     验证: 数据库中有 {count} 条 sh.600028 记录")

    # 清理测试数据
    if 'test_records' in locals():
        cursor.execute(
            "DELETE FROM stock_daily_data WHERE symbol = 'sh.600028' AND trade_date IN ('2025-12-29', '2025-12-30')")
        conn.commit()
        print(f"     清理了 {cursor.rowcount} 条测试记录")

    conn.close()

except Exception as e:
    print(f"   ❌ DataStorage 失败: {e}")
    import traceback

    traceback.print_exc()

# 3. 测试完整的 DataPipeline
print("\n3. 测试完整的 DataPipeline...")
try:
    from src.data.data_pipeline import DataPipeline
    from src.data.baostock_collector import BaostockCollector
    from src.data.data_storage import DataStorage

    # 初始化
    collector = BaostockCollector()
    storage = DataStorage()
    pipeline = DataPipeline(collector=collector, storage=storage)

    print("   ✅ DataPipeline 初始化成功")

    # 获取数据库连接用于监控
    from src.database.db_connector import DatabaseConnector

    db = DatabaseConnector()
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True)

    # 记录执行前状态
    cursor.execute("SELECT COUNT(*) as before_count FROM stock_daily_data WHERE symbol = 'sh.600036'")
    before_count = cursor.fetchone()['before_count']
    print(f"   执行前 sh.600036 记录数: {before_count}")

    # 执行管道（使用不同的股票避免冲突）
    print("   执行管道...")
    result = pipeline.fetch_and_store_daily_data(
        symbol="sh.600036",  # 招商银行
        start_date="2025-12-25",
        end_date="2025-12-31"
    )

    print(f"   ✅ 管道执行完成")
    print(f"     结果: {result}")

    # 记录执行后状态
    cursor.execute("SELECT COUNT(*) as after_count FROM stock_daily_data WHERE symbol = 'sh.600036'")
    after_count = cursor.fetchone()['after_count']
    print(f"   执行后 sh.600036 记录数: {after_count}")
    print(f"   实际增加: {after_count - before_count} 条")

    # 查看具体插入了哪些数据
    if after_count > before_count:
        cursor.execute("""
            SELECT trade_date, open_price, close_price, volume, created_time
            FROM stock_daily_data 
            WHERE symbol = 'sh.600036' 
            ORDER BY trade_date DESC 
            LIMIT 5
        """)
        new_records = cursor.fetchall()
        print(f"   最新插入的记录:")
        for record in new_records:
            print(
                f"     {record['trade_date']}: 开盘{record['open_price']}, 收盘{record['close_price']}, 成交量{record['volume']:,}")

    conn.close()

except Exception as e:
    print(f"   ❌ DataPipeline 失败: {e}")
    import traceback

    traceback.print_exc()

print("\n" + "=" * 60)
print("📋 诊断完成")