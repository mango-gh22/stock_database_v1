
"""
数据库连接管理 - 适配您的配置
"""
import pymysql
import yaml
import os

def get_connection():
    """获取pymysql原生连接"""
    try:
        # 加载配置
        config_path = os.path.join('config', 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 适配您的配置结构
            mysql_config = config['database']['mysql']

            print(f"连接数据库: {mysql_config['database']}@{mysql_config['host']}")

            connection = pymysql.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database'],
                charset=mysql_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor
            )

            return connection

        else:
            # 如果配置文件不存在，使用默认配置
            print("⚠️ 配置文件不存在，使用默认配置")
            return pymysql.connect(
                host='localhost',
                port=3306,
                user='root',
                password='root1234',
                database='stock_database',
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
        required_tables = ['stock_basic', 'stock_daily_data']
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
