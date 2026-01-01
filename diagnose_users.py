# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_users.py
# File Name: diagnose_users
# @ Author: mango-gh22
# @ Date：2026/1/1 8:44
"""
desc 
"""

# diagnose_users.py
import mysql.connector
from getpass import getpass

print("🔍 MySQL 用户诊断工具")
print("=" * 50)

# 使用你知道的账户
username = input("MySQL 用户名 (如 root): ")
password = getpass("密码: ")

try:
    conn = mysql.connector.connect(
        host='localhost',
        port=3306,
        user=username,
        password=password
    )

    cursor = conn.cursor(dictionary=True)

    print("\n✅ 连接成功！")
    print("\n1. 查看所有用户:")
    print("-" * 40)
    cursor.execute("SELECT user, host, authentication_string FROM mysql.user")
    for user in cursor.fetchall():
        print(f"   {user['user']}@{user['host']}")

    print("\n2. 查看 stock_user 权限:")
    print("-" * 40)
    try:
        cursor.execute("SHOW GRANTS FOR 'stock_user'@'localhost'")
        for grant in cursor.fetchall():
            print(f"   {list(grant.values())[0]}")
    except:
        print("   ❌ stock_user@localhost 不存在")

    print("\n3. 测试 stock_database 访问:")
    print("-" * 40)
    try:
        cursor.execute("USE stock_database")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"   ✅ 可以访问 stock_database")
        print(f"   包含 {len(tables)} 个表:")
        for table in tables:
            print(f"     - {list(table.values())[0]}")
    except Exception as e:
        print(f"   ❌ 无法访问 stock_database: {e}")

    conn.close()

except mysql.connector.Error as err:
    print(f"\n❌ 连接失败: {err}")
    print("\n💡 建议:")
    print("1. 确认 MySQL 服务正在运行")
    print("2. 确认用户名密码正确")
    print("3. 尝试在 MySQL Workbench 中用相同凭证连接")