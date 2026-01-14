# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\setup_mysql.py
# @ Author: mango-gh22
# @ Date：2025/12/5 21:44

# scripts/setup_mysql.py
import mysql.connector
from mysql.connector import Error
import yaml
import os

def setup_database():
    """设置MySQL数据库和用户"""
    try:
        # 读取配置
        with open('config/database.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # 提取配置
        db_config = config['database']['mysql']
        host = db_config['host']
        port = db_config['port']
        user = 'root'  # 使用root用户创建
        password = input("请输入MySQL root密码: ")  # 或者从环境变量获取

        # 连接到MySQL
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

        cursor = connection.cursor()

        # 1. 创建数据库
        cursor.execute(f"""
            CREATE DATABASE IF NOT EXISTS {db_config['database']} 
            DEFAULT CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
        """)
        print(f"✅ 数据库 '{db_config['database']}' 创建完成")

        # 2. 创建用户
        cursor.execute(
            f"CREATE USER IF NOT EXISTS '{db_config['user']}'@'localhost' IDENTIFIED BY '{db_config['password']}'")
        print(f"✅ 用户 '{db_config['user']}' 创建完成")

        # 3. 授予权限
        cursor.execute(f"GRANT ALL PRIVILEGES ON {db_config['database']}.* TO '{db_config['user']}'@'localhost'")
        print(f"✅ 权限授予完成")

        # 4. 刷新权限
        cursor.execute("FLUSH PRIVILEGES")
        print("✅ 权限刷新完成")

        cursor.close()
        connection.close()

        print("\n🎉 MySQL数据库设置完成！")
        print(f"   数据库: {db_config['database']}")
        print(f"   用户: {db_config['user']}")
        print(f"   主机: {db_config['host']}:{db_config['port']}")

    except Error as e:
        print(f"❌ 错误: {e}")
    except Exception as e:
        print(f"❌ 错误: {e}")


if __name__ == "__main__":
    setup_database()