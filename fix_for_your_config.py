# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_for_your_config.py
# File Name: fix_for_your_config
# @ Author: mango-gh22
# @ Date：2025/12/6 20:25
"""
desc 
"""
"""
适配您的database.yaml配置
"""
import os
import yaml

print("🔧 适配您的数据库配置")
print("=" * 60)

# 1. 创建适配的连接模块
connection_code = '''
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
'''

# 写入connection.py
connection_path = 'src/database/connection.py'
os.makedirs(os.path.dirname(connection_path), exist_ok=True)

with open(connection_path, 'w', encoding='utf-8') as f:
    f.write(connection_code)

print(f"✅ 已创建适配的连接模块: {connection_path}")

# 2. 创建简单的查询引擎
query_engine_code = '''
"""
简单查询引擎 - P4阶段核心
"""
import pandas as pd
import pymysql
import yaml
import os
from datetime import datetime

class SimpleQueryEngine:
    """简单查询引擎"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()
        print("🚀 查询引擎初始化完成")

    def _get_connection(self):
        """获取数据库连接"""
        try:
            # 读取配置
            config_path = os.path.join('config', 'database.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            mysql_config = config['database']['mysql']

            return pymysql.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database'],
                charset=mysql_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor
            )

        except Exception as e:
            print(f"❌ 连接数据库失败: {e}")
            raise

    def get_statistics(self):
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票基本信息统计
            cursor.execute("SELECT COUNT(*) as count FROM stock_basic")
            stats['total_stocks'] = cursor.fetchone()['count']

            # 日线数据统计
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    MIN(trade_date) as earliest,
                    MAX(trade_date) as latest,
                    COUNT(DISTINCT symbol) as symbols
                FROM stock_daily_data
            """)
            result = cursor.fetchone()
            stats['total_daily_records'] = result['total']
            stats['earliest_date'] = str(result['earliest']) if result['earliest'] else None
            stats['latest_date'] = str(result['latest']) if result['latest'] else None
            stats['stocks_with_data'] = result['symbols']

            # 股票列表
            cursor.execute("SELECT symbol, name FROM stock_basic ORDER BY symbol")
            stocks = cursor.fetchall()
            stats['stock_list'] = [stock['symbol'] for stock in stocks]
            stats['stock_details'] = {stock['symbol']: stock['name'] for stock in stocks}

            cursor.close()

            print(f"📊 数据统计完成: {stats['total_daily_records']}条日线记录")
            return stats

        except Exception as e:
            print(f"❌ 获取统计失败: {e}")
            return {}

    def query_daily_data(self, symbol=None, limit=10):
        """查询日线数据"""
        try:
            if symbol:
                # 使用COALESCE处理可能的列名变化
                sql = """
                    SELECT 
                        trade_date, symbol,
                        `open`, `high`, `low`, `close`,
                        volume, amount, pct_change,
                        COALESCE(price_change, `change`) as price_change,
                        pre_close, turnover_rate, amplitude
                    FROM stock_daily_data
                    WHERE symbol = %s
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                params = (symbol, limit)
            else:
                sql = """
                    SELECT 
                        trade_date, symbol,
                        `open`, `high`, `low`, `close`,
                        volume, amount, pct_change,
                        COALESCE(price_change, `change`) as price_change,
                        pre_close, turnover_rate, amplitude
                    FROM stock_daily_data
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                params = (limit,)

            df = pd.read_sql(sql, self.conn, params=params)

            if not df.empty:
                # 转换数据类型
                df['trade_date'] = pd.to_datetime(df['trade_date'])

                # 转换数值列
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                              'pct_change', 'price_change', 'pre_close', 
                              'turnover_rate', 'amplitude']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            print(f"✅ 查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            print(f"❌ 查询失败: {e}")
            return pd.DataFrame()

    def export_to_csv(self, symbol=None, filename=None):
        """导出数据到CSV"""
        try:
            # 查询数据
            df = self.query_daily_data(symbol=symbol, limit=1000)

            if df.empty:
                return "无数据可导出"

            # 生成文件名
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                symbol_part = f"_{symbol}" if symbol else "_all"
                filename = f"stock_data{symbol_part}_{timestamp}.csv"

            # 确保导出目录存在
            export_dir = "data/exports"
            os.makedirs(export_dir, exist_ok=True)

            filepath = os.path.join(export_dir, filename)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            print(f"💾 导出成功: {filepath} ({len(df)}条记录)")
            return filepath

        except Exception as e:
            print(f"❌ 导出失败: {e}")
            return str(e)

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            print("🔌 数据库连接已关闭")

def run_p4_test():
    """运行P4测试"""
    print("🧪 P4查询引擎测试")
    print("=" * 50)

    engine = SimpleQueryEngine()

    try:
        # 1. 数据统计
        print("\\n📊 1. 数据统计测试")
        stats = engine.get_statistics()

        if stats:
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")
            print(f"   数据范围: {stats.get('earliest_date')} 到 {stats.get('latest_date')}")

        # 2. 查询测试
        print("\\n📈 2. 查询功能测试")
        if stats.get('stock_list'):
            test_symbol = stats['stock_list'][0]
            stock_name = stats['stock_details'].get(test_symbol, '未知')
            print(f"   测试股票: {test_symbol} ({stock_name})")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = row['trade_date'].strftime('%Y-%m-%d')
                    print(f"     {date_str}: 收盘价 {row['close']:.2f} 涨跌 {row.get('price_change', 0):+.2f}")
            else:
                print("   未查询到数据")

        # 3. 导出测试
        print("\\n💾 3. 数据导出测试")
        if stats.get('stock_list'):
            export_file = engine.export_to_csv(
                symbol=stats['stock_list'][0],
                filename="p4_test_export.csv"
            )
            print(f"   导出结果: {export_file}")

        print("\\n🎉 P4查询引擎测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()

if __name__ == "__main__":
    run_p4_test()
'''

# 写入query_engine.py
query_engine_path = 'src/query/query_engine.py'
os.makedirs(os.path.dirname(query_engine_path), exist_ok=True)

with open(query_engine_path, 'w', encoding='utf-8') as f:
    f.write(query_engine_code)

print(f"✅ 已创建查询引擎: {query_engine_path}")

# 3. 创建测试脚本
test_script = '''
"""
P4阶段最终测试脚本
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 P4阶段最终测试")
    print("=" * 60)

    try:
        # 1. 测试数据库连接
        print("\\n🔗 1. 测试数据库连接...")
        from src.database.connection import test_connection
        if not test_connection():
            print("❌ 数据库连接失败，终止测试")
            return

        # 2. 测试查询引擎
        print("\\n🚀 2. 测试查询引擎...")
        from src.query.query_engine import run_p4_test
        run_p4_test()

        # 3. 测试main.py命令
        print("\\n📝 3. 测试main.py命令...")
        import subprocess

        # 测试validate命令
        print("   运行: python main.py --action validate")
        result = subprocess.run(
            ['python', 'main.py', '--action', 'validate'],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✅ validate命令执行成功")
            # 显示关键信息
            lines = result.stdout.split('\\n')
            for line in lines:
                if any(keyword in line for keyword in ['股票总数', '日线数据', '总记录数', '数据验证报告']):
                    print(f"   {line}")
        else:
            print(f"❌ validate命令失败: {result.stderr[:200]}")

        print("\\n" + "=" * 60)
        print("🎉 P4阶段测试完成!")

    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
'''

with open('test_p4_final.py', 'w', encoding='utf-8') as f:
    f.write(test_script)

print(f"✅ 已创建测试脚本: test_p4_final.py")

# 4. 更新main.py添加p4_test命令
print("\n📝 检查main.py是否需要更新...")

main_py_path = 'main.py'
if os.path.exists(main_py_path):
    with open(main_py_path, 'r', encoding='utf-8') as f:
        main_content = f.read()

    # 检查是否已有p4_test
    if 'p4_test' not in main_content:
        # 找到action参数定义
        import re

        # 在choices中添加p4_test
        pattern = r"choices=\[([^\]]+)\]"
        match = re.search(pattern, main_content)

        if match:
            current_actions = match.group(1)
            new_actions = current_actions.rstrip()
            if not new_actions.endswith(','):
                new_actions += ','
            new_actions += " 'p4_test'"

            new_content = main_content.replace(current_actions, new_actions)

            # 在validate之前添加p4_test处理逻辑
            if 'elif action == "validate":' in new_content:
                p4_code = '''
    elif action == "p4_test":
        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)

        try:
            from src.query.query_engine import run_p4_test
            run_p4_test()
        except Exception as e:
            print(f"❌ P4测试失败: {e}")
            import traceback
            traceback.print_exc()'''

                new_content = new_content.replace(
                    'elif action == "validate":',
                    f'{p4_code}\\n    elif action == "validate":'
                )

            with open(main_py_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已更新main.py，添加p4_test命令")
        else:
            print("⚠️  无法更新main.py的action参数")
    else:
        print("✅ main.py中已有p4_test命令")
else:
    print("⚠️  main.py文件不存在")

print("\n" + "=" * 60)
print("🎉 P4阶段适配完成!")
print("\n运行测试:")
print("1. 首先测试连接: python src/database/connection.py")
print("2. 运行完整测试: python test_p4_final.py")
print("3. 使用main.py命令: python main.py --action p4_test")
print("4. 验证数据: python main.py --action validate")