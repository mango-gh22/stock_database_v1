# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_table_names.py
# File Name: fix_table_names
# @ Author: mango-gh22
# @ Date：2025/12/6 20:33
"""
desc 
"""
"""
修复表名不一致问题
"""
import pymysql
import yaml
import os

print("🔧 修复表名不一致问题")
print("=" * 60)


def get_connection():
    """获取数据库连接"""
    config_path = 'config/database.yaml'
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


def check_tables():
    """检查表结构"""
    conn = get_connection()
    cursor = conn.cursor()

    print("📊 当前数据库表结构:")
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    table_mapping = {}
    for table in tables:
        table_name = table['Tables_in_stock_database']
        print(f"  - {table_name}")

        # 检查表结构
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        column_names = [col['Field'] for col in columns]

        table_mapping[table_name] = column_names

        # 显示关键表的列
        if table_name in ['stock_basic_info', 'stock_daily_data']:
            print(f"    列: {column_names}")

    cursor.close()
    conn.close()

    return table_mapping


def update_query_engine():
    """更新查询引擎使用正确的表名"""
    print("\n📝 更新查询引擎...")

    query_engine_code = '''
"""
查询引擎 - 适配实际表名
"""
import pandas as pd
import pymysql
import yaml
import os
from datetime import datetime

class QueryEngine:
    """查询引擎 - 使用实际的表名"""

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

    def get_data_statistics(self):
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票基本信息统计 - 使用实际的表名 stock_basic_info
            cursor.execute("SELECT COUNT(*) as count FROM stock_basic_info")
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
            cursor.execute("SELECT symbol, name FROM stock_basic_info ORDER BY symbol")
            stocks = cursor.fetchall()
            stats['stock_list'] = [stock['symbol'] for stock in stocks]
            stats['stock_details'] = {stock['symbol']: stock['name'] for stock in stocks}

            # 行业统计
            cursor.execute("SELECT COUNT(DISTINCT industry) FROM stock_basic_info")
            stats['industry_count'] = cursor.fetchone()['count']

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
                # 使用实际的表名和列名
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

    def query_stock_basic(self, symbol=None):
        """查询股票基本信息 - 使用正确的表名"""
        try:
            if symbol:
                sql = "SELECT symbol, name, industry FROM stock_basic_info WHERE symbol = %s"
                params = (symbol,)
            else:
                sql = "SELECT symbol, name, industry FROM stock_basic_info ORDER BY symbol"
                params = None

            df = pd.read_sql(sql, self.conn, params=params)
            print(f"✅ 查询股票信息: {len(df)}条记录")
            return df

        except Exception as e:
            print(f"❌ 查询股票信息失败: {e}")
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

def test_query_engine():
    """测试查询引擎"""
    print("🧪 测试查询引擎")
    print("=" * 50)

    engine = QueryEngine()

    try:
        # 1. 数据统计
        print("\\n📊 1. 数据统计测试")
        stats = engine.get_data_statistics()

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
                    close_price = row['close'] if 'close' in row else 'N/A'
                    price_change = row.get('price_change', 0)
                    print(f"     {date_str}: 收盘价 {close_price} 涨跌 {price_change:+.2f}")
            else:
                print("   未查询到数据")

        # 3. 导出测试
        print("\\n💾 3. 数据导出测试")
        if stats.get('stock_list'):
            export_file = engine.export_to_csv(
                symbol=stats['stock_list'][0],
                filename="p4_fixed_test.csv"
            )
            print(f"   导出结果: {export_file}")

        print("\\n🎉 查询引擎测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()

if __name__ == "__main__":
    test_query_engine()
'''

    # 写入query_engine.py
    query_engine_path = 'src/query/query_engine.py'
    os.makedirs(os.path.dirname(query_engine_path), exist_ok=True)

    with open(query_engine_path, 'w', encoding='utf-8') as f:
        f.write(query_engine_code)

    print(f"✅ 已更新查询引擎: {query_engine_path}")
    return True


def update_main_py():
    """更新main.py"""
    print("\n📝 更新main.py...")

    main_py_path = 'main.py'
    if not os.path.exists(main_py_path):
        print(f"❌ main.py不存在: {main_py_path}")
        return False

    with open(main_py_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. 添加p4_test到action列表
    import re

    # 查找action参数定义
    pattern = r"choices=\[([^\]]+)\]"
    match = re.search(pattern, content)

    if match:
        current_actions = match.group(1)
        print(f"当前actions: {current_actions}")

        # 添加p4_test到列表
        if "'p4_test'" not in current_actions:
            new_actions = current_actions.rstrip()
            if not new_actions.endswith(','):
                new_actions += ','
            new_actions += " 'p4_test'"

            new_content = content.replace(current_actions, new_actions)

            with open(main_py_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已添加p4_test到action列表")
        else:
            print("✅ p4_test已在action列表中")
            new_content = content
    else:
        print("❌ 未找到action参数定义")
        return False

    # 2. 在validate之前添加p4_test处理逻辑
    if 'elif action == "validate":' in new_content:
        p4_code = '''
    elif action == "p4_test":
        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)

        try:
            from src.query.query_engine import test_query_engine
            test_query_engine()
        except Exception as e:
            print(f"❌ P4测试失败: {e}")
            import traceback
            traceback.print_exc()'''

        # 在validate之前插入
        updated_content = new_content.replace(
            'elif action == "validate":',
            f'{p4_code}\\n    elif action == "validate":'
        )

        with open(main_py_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)

        print("✅ 已添加p4_test处理逻辑")

    return True


def create_validate_fix():
    """创建validate修复"""
    print("\n🔧 创建validate修复...")

    # 创建新的validate函数
    validate_code = '''
def validate_data():
    """验证数据"""
    print("🔍 数据验证报告")
    print("=" * 50)

    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        try:
            # 获取统计信息
            stats = engine.get_data_statistics()

            if not stats:
                print("❌ 无法获取数据统计")
                return

            print(f"\\n📊 股票基本信息:")
            print(f"  总股票数: {stats.get('total_stocks', 0)}")
            print(f"  行业数量: {stats.get('industry_count', 0)}")

            print(f"\\n📅 日线数据:")
            print(f"  总记录数: {stats.get('total_daily_records', 0)}")
            print(f"  最早日期: {stats.get('earliest_date', '未知')}")
            print(f"  最新日期: {stats.get('latest_date', '未知')}")
            print(f"  有数据的股票: {stats.get('stocks_with_data', 0)}")

            if stats.get('stock_list'):
                print(f"\\n📋 股票列表 ({len(stats['stock_list'])} 只):")
                for i, symbol in enumerate(stats['stock_list'][:10], 1):
                    name = stats['stock_details'].get(symbol, '')
                    print(f"  {i:2}. {symbol} {name}")
                if len(stats['stock_list']) > 10:
                    print(f"  ... 还有 {len(stats['stock_list']) - 10} 只股票")

            print("\\n✅ 数据验证完成")

        finally:
            engine.close()

    except Exception as e:
        print(f"❌ 数据验证失败: {e}")
        import traceback
        traceback.print_exc()
'''

    # 读取main.py并替换validate_data函数
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找现有的validate_data函数
    import re
    pattern = r'def validate_data\(\):[^{]+\{[^}]+\}'

    # 简单替换：找到validate_data函数定义的位置
    if 'def validate_data():' in content:
        # 找到def validate_data():到下一个def或文件结束
        start = content.find('def validate_data():')
        # 查找下一个def或文件结束
        next_def = content.find('\\ndef ', start + 1)
        if next_def == -1:
            next_def = len(content)

        # 替换这部分内容
        before = content[:start]
        after = content[next_def:]

        new_content = before + validate_code + after

        with open('main.py', 'w', encoding='utf-8') as f:
            f.write(new_content)

        print("✅ 已更新validate_data函数")
    else:
        print("⚠️  未找到validate_data函数，将在合适位置插入")

        # 在main函数中找到合适位置插入
        if 'def main():' in content:
            # 在main函数开始后插入
            main_start = content.find('def main():')
            # 找到main函数的第一个缩进行
            lines = content[main_start:].split('\\n')

            # 重建内容
            new_lines = []
            for line in lines:
                new_lines.append(line)
                if line.strip().startswith('logger.info') and '执行动作' in line:
                    # 在此之后插入validate_data函数定义
                    new_lines.append('')
                    new_lines.append(validate_code.strip())

            new_content = content[:main_start] + '\\n'.join(new_lines)

            with open('main.py', 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已插入validate_data函数")


def main():
    """主函数"""
    print("🔧 全面修复P4问题")
    print("=" * 60)

    # 1. 检查表结构
    print("\\n1. 检查数据库表结构...")
    table_mapping = check_tables()

    # 2. 更新查询引擎
    print("\\n2. 更新查询引擎...")
    update_query_engine()

    # 3. 更新main.py
    print("\\n3. 更新main.py...")
    update_main_py()

    # 4. 修复validate
    print("\\n4. 修复validate函数...")
    create_validate_fix()

    print("\\n" + "=" * 60)
    print("🎉 修复完成!")
    print("\\n运行测试:")
    print("1. 测试查询引擎: python src/query/query_engine.py")
    print("2. 测试main.py命令: python main.py --action p4_test")
    print("3. 验证数据: python main.py --action validate")


if __name__ == "__main__":
    main()