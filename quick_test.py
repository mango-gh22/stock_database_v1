# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\quick_test.py
# File Name: quick_test
# @ File: quick_test.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 22:36
"""
desc 简化main.py直接测试
"""

# quick_test.py
import sys
sys.path.insert(0, '.')

from src.database.db_connector import DatabaseConnector

db = DatabaseConnector()
if db.test_connection():
    print("✅ 数据库连接成功")
    if db.create_database_if_not_exists():
        print("✅ 数据库创建成功")
        # 执行SQL文件
        import subprocess
        result = subprocess.run([
            'mysql', '-u', 'stock_user', '-proot1234', 'stock_database',
            '-e', 'source scripts/schema/create_tables.sql'
        ])
        print(f"✅ SQL执行结果: {result.returncode}")
else:
    print("❌ 数据库连接失败")