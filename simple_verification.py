# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\simple_verification.py
# File Name: simple_verification
# @ Author: mango-gh22
# @ Date：2026/1/1 9:27
"""
desc 
"""

# simple_verification.py
import sys
import os
from dotenv import load_dotenv

# 加载环境变量
sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv(r"E:\MyFile\stock_database_v1\.env")

import mysql.connector
from datetime import datetime
import time

print("🕵️ 简单数据验证测试")
print("=" * 60)

# 数据库配置
db_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'stock_user',
    'password': os.getenv('DB_PASSWORD'),
    'database': 'stock_database',
    'autocommit': True
}

print(f"用户: {db_config['user']}")
print(f"数据库: {db_config['database']}")

# 测试连接
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    print("✅ 数据库连接成功")

    # 查询当前状态
    cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
    total = cursor.fetchone()['total']
    print(f"📊 当前总记录数: {total:,}")

    # 插入测试数据
    test_symbol = f"TEST_{int(time.time())}"
    test_date = datetime.now().strftime('%Y-%m-%d')

    insert_sql = """
        INSERT INTO stock_daily_data 
        (symbol, trade_date, open_price, close_price, volume, created_time)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """

    cursor.execute(insert_sql, (test_symbol, test_date, 100.0, 101.0, 10000))
    conn.commit()

    print(f"✅ 插入测试数据: {test_symbol}")
    print(f"   ID: {cursor.lastrowid}")

    # 验证插入
    cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    result = cursor.fetchone()

    if result:
        print(f"✅ 验证成功: ID={result['id']}, 时间={result['created_time']}")
    else:
        print("❌ 验证失败: 未找到插入的数据")

    # 清理
    cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    conn.commit()
    print(f"✅ 清理完成: 删除 {cursor.rowcount} 条")

    conn.close()
    print("\n🎉 验证完成 - 数据库操作正常!")

except Exception as e:
    print(f"❌ 验证失败: {e}")
    import traceback

    traceback.print_exc()