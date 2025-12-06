# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_import_and_config.py
# File Name: fix_import_and_config
# @ Author: mango-gh22
# @ Date：2025/12/6 20:20
"""
desc 
"""
"""
修复导入问题和数据库配置
"""
import os
import sys


def fix_logger_import():
    """修复logger导入问题"""
    print("🔧 修复logger导入")
    print("=" * 40)

    # 1. 检查logger.py文件
    logger_path = 'src/utils/logger.py'

    if not os.path.exists(logger_path):
        print(f"❌ logger.py不存在: {logger_path}")
        return False

    # 读取logger.py内容
    with open(logger_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否有setup_logger函数
    if 'def setup_logger' not in content:
        print("⚠️ logger.py中没有setup_logger函数")

        # 创建简单的logger.py
        simple_logger = '''
"""
简单日志工具
"""
import logging
import os

def get_logger(name='stock_database', level=logging.INFO):
    """获取logger"""
    logger = logging.getLogger(name)

    if not logger.handlers:
        # 设置级别
        logger.setLevel(level)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # 格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

        # 文件处理器
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f'{name}.log')

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger

# 为向后兼容性添加别名
setup_logger = get_logger
'''

        with open(logger_path, 'w', encoding='utf-8') as f:
            f.write(simple_logger)

        print("✅ 已创建简单的logger.py")
        return True
    else:
        print("✅ logger.py已包含setup_logger函数")
        return True


def fix_connection_module():
    """修复connection.py模块"""
    print("\n🔧 修复connection.py模块")
    print("=" * 40)

    connection_path = 'src/database/connection.py'

    # 创建简化的connection.py
    simple_connection = '''
"""
数据库连接管理 - 极简版本
"""
import pymysql
import yaml
import os

def get_connection():
    """获取数据库连接"""
    try:
        # 加载配置
        config_path = os.path.join('config', 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            db_config = config.get('development', {})
        else:
            # 默认配置
            db_config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'root',  # 默认密码，请修改
                'database': 'stock_database',
                'charset': 'utf8mb4'
            }

        print(f"连接数据库: {db_config.get('database')}@{db_config.get('host')}")

        return pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config.get('charset', 'utf8mb4'),
            cursorclass=pymysql.cursors.DictCursor
        )

    except Exception as e:
        print(f"连接数据库失败: {e}")
        raise

def test_connection():
    """测试连接"""
    print("测试数据库连接...")

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✅ 连接测试成功: {result}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False

if __name__ == "__main__":
    test_connection()
'''

    # 备份原文件
    if os.path.exists(connection_path):
        backup_path = connection_path + '.backup2'
        with open(connection_path, 'r', encoding='utf-8') as f:
            original = f.read()
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(original)
        print(f"✅ 已备份原文件到: {backup_path}")

    # 写入新文件
    with open(connection_path, 'w', encoding='utf-8') as f:
        f.write(simple_connection)

    print("✅ 已更新connection.py为极简版本")
    return True


def check_database_config():
    """检查数据库配置"""
    print("\n🔍 检查数据库配置")
    print("=" * 40)

    config_path = 'config/database.yaml'

    if not os.path.exists(config_path):
        print(f"❌ 配置文件不存在: {config_path}")

        # 创建默认配置
        default_config = '''# 数据库配置
development:
  host: localhost
  port: 3306
  user: root
  password: root  # 请修改为您的MySQL密码
  database: stock_database
  charset: utf8mb4

test:
  host: localhost
  port: 3306
  user: root
  password: root
  database: stock_database_test
  charset: utf8mb4

production:
  host: localhost
  port: 3306
  user: root
  password: root
  database: stock_database_prod
  charset: utf8mb4
'''

        os.makedirs('config', exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(default_config)

        print(f"✅ 已创建默认配置文件: {config_path}")
        print("⚠️  请修改config/database.yaml中的数据库密码")
        return False
    else:
        print(f"✅ 配置文件存在: {config_path}")

        # 读取配置检查密码
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            db_config = config.get('development', {})
            password = db_config.get('password', '')

            if not password or password == 'root':
                print("⚠️  警告: 数据库密码为空或为默认值'root'")
                print("请修改config/database.yaml中的password字段")
                return False
            else:
                print("✅ 数据库密码已配置")
                return True

        except Exception as e:
            print(f"❌ 读取配置文件失败: {e}")
            return False


def fix_simple_query_engine():
    """修复简单查询引擎"""
    print("\n🔧 修复简单查询引擎")
    print("=" * 40)

    query_path = 'src/query/simple_query.py'

    # 创建更简单的版本
    super_simple_query = '''
"""
超级简单查询引擎
"""
import pandas as pd
import pymysql
import yaml
import os

class SuperSimpleQuery:
    """超级简单查询"""

    def __init__(self):
        self.conn = self._get_connection()

    def _get_connection(self):
        """获取连接"""
        # 读取配置
        config_path = os.path.join('config', 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            db_config = config.get('development', {})
        else:
            db_config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'root',
                'database': 'stock_database',
                'charset': 'utf8mb4'
            }

        return pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config.get('charset', 'utf8mb4'),
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_stats(self):
        """获取统计"""
        cursor = self.conn.cursor()

        # 股票统计
        cursor.execute("SELECT COUNT(*) as count FROM stock_basic")
        stock_count = cursor.fetchone()['count']

        # 日线统计
        cursor.execute("""
            SELECT COUNT(*) as total,
                   MIN(trade_date) as earliest,
                   MAX(trade_date) as latest
            FROM stock_daily_data
        """)
        daily_stats = cursor.fetchone()

        cursor.close()

        return {
            'stock_count': stock_count,
            'daily_total': daily_stats['total'],
            'earliest_date': str(daily_stats['earliest']),
            'latest_date': str(daily_stats['latest'])
        }

    def query_daily(self, symbol=None, limit=5):
        """查询日线"""
        if symbol:
            sql = """
                SELECT trade_date, symbol, `close`, volume, 
                       COALESCE(price_change, `change`) as price_change
                FROM stock_daily_data
                WHERE symbol = %s
                ORDER BY trade_date DESC
                LIMIT %s
            """
            params = (symbol, limit)
        else:
            sql = """
                SELECT trade_date, symbol, `close`, volume, 
                       COALESCE(price_change, `change`) as price_change
                FROM stock_daily_data
                ORDER BY trade_date DESC
                LIMIT %s
            """
            params = (limit,)

        return pd.read_sql(sql, self.conn, params=params)

    def get_stock_list(self):
        """获取股票列表"""
        sql = "SELECT symbol, name FROM stock_basic ORDER BY symbol"
        return pd.read_sql(sql, self.conn)

    def close(self):
        """关闭连接"""
        self.conn.close()

def quick_test():
    """快速测试"""
    print("🧪 超级简单查询测试")
    print("=" * 50)

    query = SuperSimpleQuery()

    try:
        # 统计
        stats = query.get_stats()
        print(f"📊 统计:")
        print(f"  股票: {stats.get('stock_count', 0)}")
        print(f"  日线: {stats.get('daily_total', 0)}")

        if stats.get('daily_total', 0) > 0:
            # 股票列表
            stocks = query.get_stock_list()
            if not stocks.empty:
                symbol = stocks.iloc[0]['symbol']
                name = stocks.iloc[0]['name']
                print(f"\\n📈 测试股票: {symbol} ({name})")

                # 查询
                data = query.query_daily(symbol=symbol, limit=3)
                if not data.empty:
                    print(f"✅ 查询到 {len(data)} 条记录:")
                    for idx, row in data.iterrows():
                        print(f"  {row['trade_date']}: {row['close']:.2f}")
                else:
                    print("⚠️  无数据")

        print("\\n🎉 测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        query.close()

if __name__ == "__main__":
    quick_test()
'''

    # 写入文件
    with open(query_path, 'w', encoding='utf-8') as f:
        f.write(super_simple_query)

    print("✅ 已更新简单查询引擎")
    return True


def create_final_test():
    """创建最终测试脚本"""
    print("\n📝 创建最终测试脚本")
    print("=" * 40)

    test_script = '''
"""
最终P4测试脚本
"""
import sys
import os
sys.path.insert(0, '.')

def test_all():
    """测试所有功能"""
    print("🚀 P4最终测试")
    print("=" * 60)

    # 1. 测试数据库连接
    print("\\n1. 🔗 测试数据库连接...")
    try:
        from src.database.connection import get_connection
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SHOW TABLES")
        tables = [row['Tables_in_stock_database'] for row in cursor.fetchall()]
        print(f"✅ 连接成功! 数据库表: {tables}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False

    # 2. 测试数据统计
    print("\\n2. 📊 测试数据统计...")
    try:
        from src.query.simple_query import SuperSimpleQuery
        query = SuperSimpleQuery()

        stats = query.get_stats()
        print(f"✅ 统计成功:")
        print(f"   股票数量: {stats.get('stock_count', 0)}")
        print(f"   日线记录: {stats.get('daily_total', 0)}")

        query.close()
    except Exception as e:
        print(f"❌ 统计失败: {e}")
        return False

    # 3. 测试查询功能
    print("\\n3. 📈 测试查询功能...")
    try:
        query = SuperSimpleQuery()

        # 获取股票列表
        stocks = query.get_stock_list()
        if not stocks.empty:
            symbol = stocks.iloc[0]['symbol']
            name = stocks.iloc[0]['name']
            print(f"   测试股票: {symbol} ({name})")

            # 查询数据
            data = query.query_daily(symbol=symbol, limit=2)
            if not data.empty:
                print(f"✅ 查询成功: {len(data)}条记录")
                for idx, row in data.iterrows():
                    print(f"   {row['trade_date']}: {row['close']:.2f}")
            else:
                print("⚠️  无数据")
        else:
            print("⚠️  无股票数据")

        query.close()
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return False

    # 4. 测试main.py命令
    print("\\n4. 📝 测试main.py命令...")
    try:
        import subprocess

        # 测试validate命令
        result = subprocess.run(
            ['python', 'main.py', '--action', 'validate'],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✅ validate命令执行成功")
            # 检查关键信息
            if '数据验证报告' in result.stdout:
                print("✅ 数据验证功能正常")
        else:
            print(f"❌ validate命令失败")

    except Exception as e:
        print(f"⚠️  命令测试异常: {e}")

    print("\\n" + "=" * 60)
    print("🎉 P4最终测试完成!")
    return True

if __name__ == "__main__":
    test_all()
'''

    with open('final_p4_test.py', 'w', encoding='utf-8') as f:
        f.write(test_script)

    print("✅ 已创建最终测试脚本: final_p4_test.py")
    return True


def main():
    """主函数"""
    print("🔧 P4阶段完整修复")
    print("=" * 60)

    # 执行修复步骤
    steps = [
        ("修复logger导入", fix_logger_import),
        ("检查数据库配置", check_database_config),
        ("修复connection.py", fix_connection_module),
        ("修复查询引擎", fix_simple_query_engine),
        ("创建测试脚本", create_final_test)
    ]

    for step_name, step_func in steps:
        print(f"\n📋 步骤: {step_name}")
        print("-" * 40)
        step_func()

    print("\n" + "=" * 60)
    print("🎉 修复完成!")
    print("\n下一步:")
    print("1. 请检查config/database.yaml中的数据库密码")
    print("2. 运行测试: python final_p4_test.py")
    print("3. 如果测试成功，创建Git标签")


if __name__ == "__main__":
    main()