# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\test_mysql_connector.py
# File Name: test_mysql_connector
# @ Author: mango-gh22
# @ Date：2025/12/31 23:10
"""
desc 
"""

import mysql.connector
import os
import sys
from configparser import ConfigParser


def test_database_connection():
    """测试数据库连接配置"""
    print("🔍 数据库连接配置验证")
    print("=" * 50)

    # 1. 检查环境变量配置
    print("1. 检查环境变量配置...")
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'stock_db')
    }

    print(f"   Host: {db_config['host']}")
    print(f"   Port: {db_config['port']}")
    print(f"   User: {db_config['user']}")
    print(f"   Password: {'*' * len(db_config['password']) if db_config['password'] else 'None'}")
    print(f"   Database: {db_config['database']}")

    # 2. 尝试连接数据库
    print("\n2. 尝试连接数据库...")
    try:
        conn = mysql.connector.connect(**db_config)
        print("✅ 数据库连接成功!")

        # 3. 执行基本查询
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE() AS current_db, USER() AS current_user, VERSION() AS version")
        result = cursor.fetchone()
        print(f"   当前数据库: {result[0]}")
        print(f"   当前用户: {result[1]}")
        print(f"   MySQL版本: {result[2]}")

        # 4. 检查表是否存在
        print("\n3. 检查数据表...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = 'stock_daily_data'
        """, (db_config['database'],))

        table_exists = cursor.fetchone()
        if table_exists:
            print("✅ 数据表 stock_daily_data 存在")
            # 查询表结构
            cursor.execute("DESCRIBE stock_daily_data")
            columns = cursor.fetchall()
            print(f"   表字段数: {len(columns)}")
        else:
            print("⚠️  数据表 stock_daily_data 不存在")

        conn.close()
        return True

    except mysql.connector.Error as err:
        print(f"❌ 数据库连接失败: {err}")
        if err.errno == 1045:
            print("   解决方案:")
            print("   1. 检查数据库用户名和密码是否正确")
            print("   2. 确认用户 'stock_user' 是否存在并有连接权限")
            print("   3. 验证MySQL服务是否正常运行")
        return False
    except Exception as e:
        print(f"❌ 连接过程中发生未知错误: {e}")
        return False


def create_database_user():
    """创建数据库用户脚本"""
    print("\n📝 创建数据库用户SQL脚本:")
    print("-" * 30)
    print("请在MySQL命令行中执行以下SQL语句:")
    print("")
    print("CREATE DATABASE IF NOT EXISTS stock_db;")
    print("CREATE USER IF NOT EXISTS 'stock_user'@'localhost' IDENTIFIED BY 'your_password_here';")
    print("GRANT ALL PRIVILEGES ON stock_db.* TO 'stock_user'@'localhost';")
    print("FLUSH PRIVILEGES;")
    print("")


def check_env_file():
    """检查环境配置文件"""
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"✅ 环境配置文件 {env_file} 存在")
        with open(env_file, 'r') as f:
            content = f.read()
            print("   配置内容预览:")
            for line in content.split('\n')[:5]:
                if line.strip():
                    print(f"   {line}")
    else:
        print(f"⚠️  环境配置文件 {env_file} 不存在")
        print("   建议创建 .env 文件并添加以下配置:")
        print("   DB_HOST=localhost")
        print("   DB_PORT=3306")
        print("   DB_USER=stock_user")
        print("   DB_PASSWORD=your_password")
        print("   DB_NAME=stock_db")


if __name__ == "__main__":
    print("🕵️ 股票数据库连接验证工具")
    print("=" * 60)

    # 执行各项检查
    check_env_file()
    print()
    success = test_database_connection()

    if not success:
        create_database_user()

    print("\n📊 验证完成")
    sys.exit(0 if success else 1)
