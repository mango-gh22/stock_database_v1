# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_fixed_connection.py
# File Name: test_fixed_connection
# @ Author: mango-gh22
# @ Date：2026/1/1 9:05
"""
desc 
"""
# test_fixed_connection.py
import os
from dotenv import load_dotenv
import mysql.connector

# 加载环境变量
load_dotenv()

print("🔧 测试修复后的连接")
print("=" * 40)

# 方法1：使用项目代码的连接方式
from src.database.db_connector import DatabaseConnector

print("1. 测试项目 DatabaseConnector:")
try:
    db = DatabaseConnector()
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True)

    # 测试查询
    cursor.execute("SELECT DATABASE() as db, USER() as user")
    result = cursor.fetchone()
    print(f"   ✅ 连接成功!")
    print(f"   数据库: {result['db']}")
    print(f"   用户: {result['user']}")

    # 测试插入
    test_symbol = f"FIX_TEST_{int(os.times().elapsed)}"
    insert_sql = """
        INSERT INTO stock_daily_data 
        (symbol, trade_date, open_price, close_price, volume, created_time)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    cursor.execute(insert_sql, (test_symbol, '2025-12-31', 100.0, 101.0, 10000))
    conn.commit()

    # 查询确认
    cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    result = cursor.fetchone()
    print(f"   ✅ 插入成功! ID: {result['id'] if result else 'N/A'}")

    # 清理
    cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    conn.commit()
    print(f"   ✅ 清理完成")

    conn.close()
    print("   🎉 DatabaseConnector 测试通过!")

except Exception as e:
    print(f"   ❌ DatabaseConnector 失败: {e}")

print("\n2. 测试直接连接:")
try:
    # 直接从环境变量获取密码
    password = os.getenv('DB_PASSWORD')
    print(f"   从环境变量获取的密码长度: {len(password) if password else 0}")

    conn = mysql.connector.connect(
        host='localhost',
        port=3306,
        user='stock_user',
        password=password,
        database='stock_database',
        autocommit=True
    )

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
    result = cursor.fetchone()
    print(f"   ✅ 直接连接成功!")
    print(f"   数据表记录数: {result['count']:,}")

    conn.close()

except Exception as e:
    print(f"   ❌ 直接连接失败: {e}")

print("\n" + "=" * 40)
print("📋 验证完成")