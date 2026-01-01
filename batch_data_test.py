# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\batch_data_test.py
# File Name: batch_data_test
# @ Author: mango-gh22
# @ Date：2026/1/1 9:50
"""
desc 
"""
# batch_data_test.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("📊 批量数据采集测试")
print("=" * 60)

# 导入模块
from src.data.baostock_collector import BaostockCollector
from src.data.data_storage import DataStorage
from src.data.data_pipeline import DataPipeline
from src.database.db_connector import DatabaseConnector

# 初始化
collector = BaostockCollector()
storage = DataStorage()
pipeline = DataPipeline(collector=collector, storage=storage)
db = DatabaseConnector()

# 测试股票列表（A股蓝筹股）
test_symbols = [
    "sh.600000",  # 浦发银行
    "sh.600036",  # 招商银行
    "sh.600519",  # 贵州茅台
    "sz.000002",  # 万科A
    "sz.000858",  # 五粮液
]

test_start = "2025-12-25"
test_end = "2025-12-31"

print(f"测试 {len(test_symbols)} 只股票")
print(f"测试日期: {test_start} 到 {test_end}")
print("-" * 60)

# 获取数据库连接
conn = db.get_connection()
cursor = conn.cursor(dictionary=True)

total_added = 0
success_count = 0
failed_count = 0

for symbol in test_symbols:
    try:
        # 记录执行前状态
        cursor.execute("SELECT COUNT(*) as before FROM stock_daily_data WHERE symbol = %s", (symbol,))
        before = cursor.fetchone()['before']

        print(f"\n📈 处理 {symbol}...")
        print(f"   执行前: {before} 条记录")

        # 执行数据管道
        result = pipeline.fetch_and_store_daily_data(
            symbol=symbol,
            start_date=test_start,
            end_date=test_end
        )

        # 记录执行后状态
        cursor.execute("SELECT COUNT(*) as after FROM stock_daily_data WHERE symbol = %s", (symbol,))
        after = cursor.fetchone()['after']
        added = after - before

        status = result.get('status', 'unknown')
        records_stored = result.get('records_stored', 0)

        if status == 'success' and added > 0:
            print(f"   ✅ 成功: 增加 {added} 条记录 (报告: {records_stored} 条)")
            success_count += 1
            total_added += added
        elif status == 'no_data':
            print(f"   ⚠️  无数据: 可能日期范围无效或市场休市")
        else:
            print(f"   ❌ 失败: 状态={status}, 增加={added}")
            failed_count += 1

    except Exception as e:
        print(f"   ❌ 异常: {e}")
        failed_count += 1

# 关闭连接
conn.close()

# 最终统计
print("\n" + "=" * 60)
print("📊 批量测试结果")
print("=" * 60)

print(f"测试股票数: {len(test_symbols)}")
print(f"成功: {success_count}")
print(f"失败: {failed_count}")
print(f"新增记录总数: {total_added}")

# 查看总体状态
conn = db.get_connection()
cursor = conn.cursor(dictionary=True)
cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
total = cursor.fetchone()['total']

cursor.execute("""
    SELECT 
        DATE(created_time) as date,
        COUNT(*) as count
    FROM stock_daily_data 
    WHERE created_time >= CURDATE()
    GROUP BY DATE(created_time)
    ORDER BY date DESC
""")
today_stats = cursor.fetchall()

conn.close()

print(f"\n📈 数据库总体统计:")
print(f"   总记录数: {total:,}")
if today_stats:
    print("   今日新增统计:")
    for stat in today_stats:
        print(f"     {stat['date']}: {stat['count']:,} 条")
else:
    print("   今日暂无新增记录")

print("\n🎯 结论:")
if success_count > 0:
    print(f"   ✅ 数据采集系统工作正常！成功采集 {success_count} 只股票的数据")
else:
    print("   ⚠️  数据采集可能需要调试")