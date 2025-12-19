# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/database/connection.py
# @ Author: mango-gh22
# @ Date：2025/12/5 20:20
"""
数据库连接管理 - 适配您的配置
"""

import pymysql
import yaml

def get_connection():
    """获取pymysql原生连接（安全版，从环境变量读取密码）"""
    try:
        # ============ 已验证成功的环境变量加载代码 ============
        import os
        from dotenv import load_dotenv

        # 确保从项目根目录加载 .env 文件
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        env_path = os.path.join(project_root, '.env')

        load_dotenv(dotenv_path=env_path, override=True)

        db_password = os.getenv('DB_PASSWORD')
        if not db_password:
            raise ValueError(
                "❌ 未找到数据库密码。请确保：\n"
                "1. 在项目根目录的 .env 文件中设置 DB_PASSWORD=你的密码\n"
                "2. 或者在系统环境变量中设置 DB_PASSWORD"
            )
        # ===================================================

        # 加载YAML配置
        config_path = os.path.join('config', 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            mysql_config = config['database']['mysql']

            print(f"连接数据库: {mysql_config['database']}@{mysql_config['host']}")

            connection = pymysql.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                user=mysql_config['user'],
                password=db_password,  # 使用从环境变量读取的密码
                database=mysql_config['database'],
                charset=mysql_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        else:
            # 如果配置文件不存在，使用环境变量中的完整配置
            print("⚠️ 配置文件不存在，使用环境变量配置")
            host = os.getenv('DB_HOST', 'localhost')
            port = int(os.getenv('DB_PORT', 3306))
            user = os.getenv('DB_USER', 'stock_user')
            database = os.getenv('DB_NAME', 'stock_database')

            return pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=db_password,
                database=database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
    except Exception as e:
        print(f"连接数据库失败: {e}")
        raise

def test_connection():
    """测试连接"""
    print("🧪 测试数据库连接")
    print("-" * 40)

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 测试查询
        cursor.execute("SELECT VERSION() as version")
        version = cursor.fetchone()['version']
        print(f"✅ MySQL版本: {version}")

        # 显示所有表
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [table['Tables_in_stock_database'] for table in tables]
        print(f"📊 数据库表 ({len(table_names)}个):")
        for table in table_names:
            print(f"  - {table}")

        cursor.close()
        conn.close()

        # 检查关键表是否存在
        required_tables = ['stock_basic_info', 'stock_daily_data']
        missing_tables = [t for t in required_tables if t not in table_names]

        if missing_tables:
            print(f"⚠️  缺少表: {missing_tables}")
        else:
            print("✅ 所有必需表都存在")

        return True

    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False

if __name__ == "__main__":
    test_connection()
