# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\quick_root_test.py
# File Name: quick_root_test
# @ Author: mango-gh22
# @ Date：2026/1/1 8:03
"""
desc 
"""
# quick_root_test.py
import mysql.connector
from datetime import datetime

print("🔧 使用 root 账户测试数据库连接")
print("=" * 50)

try:
    # 使用 root 账户（你知道密码的）
    conn = mysql.connector.connect(
        host='localhost',
        port=3306,
        user='root',  # 使用 root
        password='',  # 你的 root 密码
        database='stock_database'
    )

    cursor = conn.cursor(dictionary=True)

    # 测试插入
    test_symbol = f"ROOT_TEST_{int(datetime.now().timestamp())}"
    sql = """
        INSERT INTO stock_daily_data 
        (symbol, trade_date, open_price, close_price, volume, created_time, updated_time)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
    """

    cursor.execute(sql, (test_symbol, '2025-12-31', 100.0, 101.0, 1000000))
    conn.commit()

    print(f"✅ 使用 root 插入成功: {test_symbol}")

    # 立即查询
    cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    results = cursor.fetchall()

    print(f"   查询到 {len(results)} 条记录")
    for row in results:
        print(f"     ID: {row['id']}, Created: {row['created_time']}")

    # 清理
    cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
    print(f"   清理测试数据: 删除 {cursor.rowcount} 条")
    conn.commit()

    conn.close()
    print("\n🎉 root 账户测试完成 - 数据库连接和写入正常")

except Exception as e:
    print(f"❌ root 账户测试失败: {e}")