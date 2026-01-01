# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\precise_diagnosis.py
# File Name: precise_diagnosis
# @ Author: mango-gh22
# @ Date：2026/1/1 11:06
"""
desc 
"""
# precise_diagnosis.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("🎯 精确问题诊断")
print("=" * 60)

# 1. 检查 DataStorage 类的方法
print("1. 🔍 检查 DataStorage 类...")
try:
    from src.data.data_storage import DataStorage
    import inspect

    storage = DataStorage()
    methods = [m for m in dir(storage) if not m.startswith('_')]

    print(f"   DataStorage 有 {len(methods)} 个公共方法:")
    for method in sorted(methods):
        print(f"     - {method}")

    # 特别检查是否有 get_last_update_date
    if 'get_last_update_date' in methods:
        print("   ✅ 找到 get_last_update_date 方法")
    else:
        print("   ❌ 缺少 get_last_update_date 方法！")

except Exception as e:
    print(f"   ❌ 检查失败: {e}")

# 2. 手动模拟完整流程
print("\n2. 🔄 手动模拟完整流程...")
try:
    # 创建一个简化的存储函数，绕开所有复杂逻辑
    def simple_store_daily_data(symbol, start_date, end_date):
        """绕过所有管道逻辑的直接存储"""
        print(f"   测试 {symbol} [{start_date} 到 {end_date}]")

        # 导入必要的模块
        from src.data.baostock_collector import BaostockCollector
        from src.database.db_connector import DatabaseConnector
        import mysql.connector
        from datetime import datetime

        # 1. 采集数据
        print("   a. 采集数据...")
        collector = BaostockCollector()
        data = collector.fetch_daily_data(symbol, start_date, end_date)

        if data is None or data.empty:
            print("   ❌ 没有采集到数据")
            return False

        print(f"   ✅ 采集到 {len(data)} 条数据")

        # 2. 直接存储到数据库（绕过所有逻辑）
        print("   b. 直接存储到数据库...")
        db = DatabaseConnector()
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)

        # 查看当前状态
        clean_symbol = symbol.replace('.', '')
        cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        before = cursor.fetchone()['count']
        print(f"      存储前: {before} 条记录")

        # 插入数据
        inserted = 0
        for _, row in data.iterrows():
            try:
                # 构建插入语句（只包含基本字段）
                sql = """
                    INSERT INTO stock_daily_data 
                    (symbol, trade_date, open_price, close_price, high_price, low_price, volume, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """

                cursor.execute(sql, (
                    clean_symbol,
                    row.get('date', row.get('trade_date')),
                    row.get('open', row.get('open_price')),
                    row.get('close', row.get('close_price')),
                    row.get('high', row.get('high_price')),
                    row.get('low', row.get('low_price')),
                    row.get('volume', 0)
                ))
                inserted += 1

            except mysql.connector.errors.IntegrityError as e:
                # 重复数据，跳过
                if "Duplicate entry" in str(e):
                    pass
                else:
                    raise

        conn.commit()

        # 查看存储后状态
        cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        after = cursor.fetchone()['count']

        cursor.close()
        conn.close()

        print(f"      存储后: {after} 条记录")
        print(f"      实际插入: {inserted} 条")

        return inserted > 0


    # 测试几个股票
    test_cases = [
        ("sh.600000", "2025-12-25", "2025-12-31"),
        ("sz.000001", "2025-12-25", "2025-12-31"),
    ]

    for symbol, start, end in test_cases:
        print(f"\n   测试 {symbol}...")
        result = simple_store_daily_data(symbol, start, end)
        if result:
            print(f"   ✅ {symbol} 测试成功")
        else:
            print(f"   ❌ {symbol} 测试失败")

except Exception as e:
    print(f"   ❌ 手动模拟失败: {e}")
    import traceback

    traceback.print_exc()

# 3. 检查 DataPipeline 的核心逻辑
print("\n3. ⚙️ 检查 DataPipeline 逻辑...")
try:
    # 查看 DataPipeline 的 fetch_and_store_daily_data 方法
    import inspect
    from src.data.data_pipeline import DataPipeline
    from src.data.baostock_collector import BaostockCollector
    from src.data.data_storage import DataStorage

    # 获取方法源码
    source = inspect.getsource(DataPipeline.fetch_and_store_daily_data)

    print("   DataPipeline.fetch_and_store_daily_data 方法概要:")

    # 查找关键逻辑
    lines = source.split('\n')
    for i, line in enumerate(lines):
        if 'get_last_update_date' in line:
            print(f"      第{i + 1}行: {line.strip()}")
        if 'skip' in line.lower() or 'duplicate' in line.lower():
            print(f"      第{i + 1}行: {line.strip()}")
        if 'store_daily_data' in line:
            print(f"      第{i + 1}行: {line.strip()}")

    print(f"   方法总行数: {len(lines)}")

except Exception as e:
    print(f"   ❌ 检查失败: {e}")

print("\n" + "=" * 60)
print("📋 诊断完成")