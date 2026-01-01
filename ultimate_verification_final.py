# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\ultimate_verification_final.py
# File Name: ultimate_verification_final
# @ Author: mango-gh22
# @ Date：2026/1/1 9:37
"""
desc 
"""
# ultimate_verification_final.py
import sys
import os
from dotenv import load_dotenv

# 加载环境变量
sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv(r"E:\MyFile\stock_database_v1\.env")

import mysql.connector
from datetime import datetime
import time

print("🕵️ 终极数据验证测试 - 最终修复版")
print("=" * 60)

# 1. 直接数据库连接
print("1. 🔗 建立直接数据库连接...")
db_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'stock_user',
    'password': os.getenv('DB_PASSWORD'),
    'database': 'stock_database',
    'autocommit': True
}

print(f"   用户: {db_config['user']}")
print(f"   数据库: {db_config['database']}")

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    print("   ✅ 数据库连接成功！")

except Exception as e:
    print(f"   ❌ 数据库连接失败: {e}")
    sys.exit(1)

# 2. 创建唯一测试标记
test_timestamp = int(time.time() % 1000000)
test_symbol = f"TEST_{test_timestamp:06d}"[:15]
test_date = datetime.now().strftime('%Y-%m-%d')

print(f"2. 🏷️ 创建唯一测试标记: {test_symbol}")
print(f"   测试日期: {test_date}")

# 3. 验证前状态
print("3. 📊 验证前数据库状态...")
cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
before_total = cursor.fetchone()['total']
print(f"   数据表总记录数: {before_total:,}")

cursor.execute("SELECT MAX(created_time) as latest FROM stock_daily_data")
latest_before = cursor.fetchone()['latest']
print(f"   最新记录时间: {latest_before}")

# 4. 直接插入测试数据
print("4. ⚡ 直接插入测试数据到数据库...")
try:
    direct_insert_sql = """
        INSERT INTO stock_daily_data 
        (symbol, trade_date, open_price, close_price, volume, created_time, updated_time)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
    """
    cursor.execute(direct_insert_sql, (test_symbol, test_date, 999.99, 1000.00, 999999))
    direct_row_id = cursor.lastrowid
    conn.commit()

    print(f"   ✅ 直接插入成功，行ID: {direct_row_id}")

except Exception as e:
    print(f"   ❌ 直接插入失败: {e}")
    sys.exit(1)

# 5. 立即验证直接插入
print("5. 🔄 立即验证直接插入...")
cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
direct_results = cursor.fetchall()
print(f"   找到 {len(direct_results)} 条直接插入的记录")

if direct_results:
    for row in direct_results:
        print(f"     ID: {row['id']}, Symbol: {row['symbol']}, "
              f"Close: {row['close_price']}, Created: {row['created_time']}")

# 6. 等待3秒
print("6. ⏳ 等待3秒...")
time.sleep(3)

# 7. 重新连接验证持久性
print("7. 🔗 重新连接验证数据持久性...")
try:
    conn2 = mysql.connector.connect(**db_config)
    cursor2 = conn2.cursor(dictionary=True)

    cursor2.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    persistent_count = cursor2.fetchone()['count']
    print(f"   持久性验证: 找到 {persistent_count} 条记录")

    cursor2.execute("SELECT COUNT(*) as total FROM stock_daily_data")
    after_total = cursor2.fetchone()['total']
    print(f"   数据表总记录数: {after_total:,}")
    print(f"   净增加: {after_total - before_total} 条")

except Exception as e:
    print(f"   ❌ 重新连接失败: {e}")
    sys.exit(1)

# 8. 运行代码的数据采集（关键测试！）
print("8. 🚀 运行项目代码的数据采集...")
print("   (这会调用你的项目代码，不是直接插入)")

# 先记录执行前的状态
print("   记录执行前状态...")
cursor2.execute("SELECT COUNT(*) as count_before FROM stock_daily_data WHERE symbol LIKE '%600028%'")
before_code = cursor2.fetchone()['count_before']
print(f"   执行前 sh600028 相关记录数: {before_code}")

# 单独测试项目代码
print("\n   ⚙️  单独测试项目代码...")
try:
    # 导入项目模块
    from src.data.baostock_collector import BaostockCollector
    from src.data.data_storage import DataStorage
    from src.data.data_pipeline import DataPipeline

    print("   ✅ 项目模块导入成功")

    # 初始化组件
    print("   初始化组件...")
    collector = BaostockCollector()
    storage = DataStorage()
    pipeline = DataPipeline(collector=collector, storage=storage)

    print("   ✅ 组件初始化成功")

    # 测试采集
    code_test_symbol = "sh.600028"
    code_test_start = "2025-12-29"
    code_test_end = "2025-12-31"

    print(f"   测试股票: {code_test_symbol}")
    print(f"   测试日期: {code_test_start} 到 {code_test_end}")

    # 执行代码采集
    print("   正在执行代码采集...")
    result = pipeline.fetch_and_store_daily_data(
        symbol=code_test_symbol,
        start_date=code_test_start,
        end_date=code_test_end
    )

    print(f"\n   📊 代码执行结果:")
    print(f"     状态: {result.get('status', 'N/A')}")
    print(f"     消息: {result.get('message', 'N/A')}")
    print(f"     采集记录: {result.get('records_collected', 0)}")
    print(f"     存储记录: {result.get('records_stored', 0)}")
    print(f"     处理时间: {result.get('processing_time', 0):.2f}秒")

    # 记录代码执行后的状态
    print("\n   记录执行后状态...")
    cursor2.execute("SELECT COUNT(*) as count_after FROM stock_daily_data WHERE symbol LIKE '%600028%'")
    after_code = cursor2.fetchone()['count_after']
    print(f"   执行后 sh600028 相关记录数: {after_code}")

    print(f"   数据库实际变化: {after_code - before_code} 条")

    project_code_success = True

except Exception as e:
    print(f"   ❌ 项目代码执行失败: {e}")
    import traceback

    traceback.print_exc()
    project_code_success = False
    after_code = before_code

# 9. 最终验证
print("9. 📋 最终验证...")
try:
    cursor2.execute("""
        SELECT symbol, COUNT(*) as count, 
               MIN(created_time) as earliest, MAX(created_time) as latest
        FROM stock_daily_data 
        WHERE created_time > %s
        GROUP BY symbol
        ORDER BY latest DESC
    """, (latest_before,))

    recent_changes = cursor2.fetchall()
    print(f"   最近新增记录统计: {len(recent_changes)} 只股票")

    for change in recent_changes[:10]:
        print(f"     {change['symbol']}: {change['count']} 条, 最新: {change['latest']}")

except Exception as e:
    print(f"   最终验证查询失败: {e}")

# 10. 清理测试数据
print("10. 🧹 清理测试数据...")
try:
    cursor2.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    cleaned = cursor2.rowcount
    conn2.commit()
    print(f"   清理了 {cleaned} 条测试数据")
except Exception as e:
    print(f"   清理失败: {e}")

# 11. 关闭连接
try:
    conn.close()
    if conn2 != conn:
        conn2.close()
except:
    pass

print("\n" + "=" * 60)
print("🎯 验证完成总结")
print("=" * 60)

print(f"📊 数据库操作验证:")
print(f"  直接插入: {'✅ 成功' if persistent_count > 0 else '❌ 失败'}")
print(f"  数据持久化: {'✅ 成功' if after_total - before_total == 1 else '❌ 失败'}")

if project_code_success:
    if after_code > before_code:
        print(f"  项目代码插入: ✅ 成功 (增加 {after_code - before_code} 条)")
    else:
        print(f"  项目代码插入: ⚠️  未增加新数据 (已有 {before_code} 条)")
else:
    print(f"  项目代码插入: ❌ 执行失败")

print(f"\n💡 关键发现:")
print(f"  1. 数据库连接正常")
print(f"  2. 直接插入正常")
print(f"  3. 数据持久化正常")
print(f"  4. 项目代码能执行但可能需要调试")

print(f"\n📈 数据统计:")
print(f"  验证前总记录: {before_total:,}")
print(f"  验证后总记录: {after_total:,}")
print(f"  总增加记录: {after_total - before_total}")