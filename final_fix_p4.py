# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\final_fix_p4.py
# File Name: final_fix_p4
# @ Author: mango-gh22
# @ Date：2025/12/6 20:40
"""
desc 
"""
"""
最终P4修复脚本
"""
import os
import sys

print("🔧 最终P4修复")
print("=" * 60)


def fix_query_engine():
    """修复查询引擎"""
    print("📝 修复查询引擎...")

    query_engine_code = '''
"""
查询引擎 - 最终修复版本
"""
import pandas as pd
import pymysql
import yaml
import os
from datetime import datetime

class QueryEngine:
    """查询引擎 - 使用正确的cursor返回类型"""

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

            # 注意：这里使用普通cursor，不是DictCursor，因为我们的代码使用数字索引
            return pymysql.connect(
                host=mysql_config['host'],
                port=mysql_config['port'],
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database'],
                charset=mysql_config.get('charset', 'utf8mb4')
                # 移除了cursorclass=pymysql.cursors.DictCursor
            )

        except Exception as e:
            print(f"❌ 连接数据库失败: {e}")
            raise

    def get_data_statistics(self):
        """获取数据统计 - 修复fetchone()返回类型"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票基本信息统计 - 使用stock_basic_info表
            cursor.execute("SELECT COUNT(*) FROM stock_basic_info")
            result = cursor.fetchone()
            stats['total_stocks'] = result[0] if result else 0

            # 日线数据统计
            cursor.execute("""
                SELECT 
                    COUNT(*),
                    MIN(trade_date),
                    MAX(trade_date),
                    COUNT(DISTINCT symbol)
                FROM stock_daily_data
            """)
            result = cursor.fetchone()
            if result:
                stats['total_daily_records'] = result[0]
                stats['earliest_date'] = str(result[1]) if result[1] else 'N/A'
                stats['latest_date'] = str(result[2]) if result[2] else 'N/A'
                stats['stocks_with_data'] = result[3]
            else:
                stats['total_daily_records'] = 0
                stats['earliest_date'] = 'N/A'
                stats['latest_date'] = 'N/A'
                stats['stocks_with_data'] = 0

            # 股票列表
            cursor.execute("SELECT symbol, name FROM stock_basic_info ORDER BY symbol")
            stocks = cursor.fetchall()
            stats['stock_list'] = [stock[0] for stock in stocks]
            stats['stock_details'] = {stock[0]: stock[1] for stock in stocks}

            # 行业统计
            cursor.execute("SELECT COUNT(DISTINCT industry) FROM stock_basic_info")
            result = cursor.fetchone()
            stats['industry_count'] = result[0] if result else 0

            cursor.close()

            print(f"📊 数据统计完成: {stats.get('total_daily_records', 0)}条日线记录")
            return stats

        except Exception as e:
            print(f"❌ 获取统计失败: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def query_daily_data(self, symbol=None, limit=10):
        """查询日线数据"""
        try:
            if symbol:
                # 注意：这里使用change_amount而不是change
                sql = """
                    SELECT 
                        trade_date, symbol,
                        `open`, `high`, `low`, `close`,
                        volume, amount, pct_change,
                        change_amount as price_change,
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
                        change_amount as price_change,
                        pre_close, turnover_rate, amplitude
                    FROM stock_daily_data
                    ORDER BY trade_date DESC
                    LIMIT %s
                """
                params = (limit,)

            df = pd.read_sql(sql, self.conn, params=params)

            if not df.empty:
                # 转换数据类型
                if 'trade_date' in df.columns:
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
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def get_stock_list(self):
        """获取股票列表 - 新增方法"""
        try:
            sql = "SELECT symbol, name FROM stock_basic_info ORDER BY symbol"
            df = pd.read_sql(sql, self.conn)
            print(f"📋 获取股票列表: {len(df)}只股票")
            return df
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()

    def query_stock_basic(self, symbol=None):
        """查询股票基本信息"""
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
                print("⚠️  无数据可导出")
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
            print(f"   数据范围: {stats.get('earliest_date', 'N/A')} 到 {stats.get('latest_date', 'N/A')}")

        # 2. 查询测试
        print("\\n📈 2. 查询功能测试")
        if stats and stats.get('stock_list'):
            test_symbol = stats['stock_list'][0]
            stock_name = stats['stock_details'].get(test_symbol, '未知')
            print(f"   测试股票: {test_symbol} ({stock_name})")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = str(row['trade_date'])[:10] if 'trade_date' in row else '未知日期'
                    close_price = row.get('close', 'N/A')
                    price_change = row.get('price_change', 0)
                    print(f"     {date_str}: 收盘价 {close_price} 涨跌 {price_change:+.2f}")
            else:
                print("   未查询到数据")
        else:
            print("   无股票数据")

        # 3. 导出测试
        print("\\n💾 3. 数据导出测试")
        if stats and stats.get('stock_list'):
            export_file = engine.export_to_csv(
                symbol=stats['stock_list'][0],
                filename="p4_final_test.csv"
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

    print(f"✅ 已修复查询引擎: {query_engine_path}")
    return True


def fix_main_py():
    """修复main.py"""
    print("\n📝 修复main.py...")

    main_py_path = 'main.py'
    if not os.path.exists(main_py_path):
        print(f"❌ main.py不存在")
        return False

    with open(main_py_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 修复1：更新action列表，确保有p4_test
    import re

    # 查找action参数
    pattern = r"--action\s+\{([^}]+)\}"
    match = re.search(pattern, content)

    if match:
        current_actions = match.group(1)
        print(f"当前action列表: {current_actions}")

        # 确保有p4_test
        if 'p4_test' not in current_actions:
            # 在列表末尾添加
            if current_actions.endswith(','):
                new_actions = current_actions + " 'p4_test'"
            else:
                new_actions = current_actions + ", 'p4_test'"

            new_content = content.replace(current_actions, new_actions)

            with open(main_py_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已添加p4_test到action列表")
            content = new_content
        else:
            print("✅ p4_test已在action列表中")
    else:
        print("❌ 未找到action参数")

    # 修复2：确保有p4_test的处理逻辑
    if 'elif action == "p4_test":' not in content:
        print("添加p4_test处理逻辑...")

        # 找到validate的位置
        validate_pos = content.find('elif action == "validate":')
        if validate_pos > 0:
            # 在validate之前插入p4_test
            p4_test_code = '''
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

            new_content = content[:validate_pos] + p4_test_code + '\\n' + content[validate_pos:]

            with open(main_py_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已添加p4_test处理逻辑")
            content = new_content

    # 修复3：更新validate_data函数
    print("\n🔧 更新validate_data函数...")

    # 创建新的validate_data函数
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
            print(f"  最早日期: {stats.get('earliest_date', 'N/A')}")
            print(f"  最新日期: {stats.get('latest_date', 'N/A')}")
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

    # 替换或添加validate_data函数
    if 'def validate_data():' in content:
        # 找到函数开始和结束
        start = content.find('def validate_data():')
        # 找到下一个def或文件结束
        next_def = content.find('\\ndef ', start + 1)
        if next_def == -1:
            next_def = len(content)

        # 替换这部分
        new_content = content[:start] + validate_code + content[next_def:]
    else:
        # 在main函数中找到合适位置插入
        main_start = content.find('def main():')
        if main_start > 0:
            # 在logger.info("执行动作:")之后插入
            action_pos = content.find('logger.info("执行动作:")', main_start)
            if action_pos > 0:
                # 找到该行的结束
                line_end = content.find('\\n', action_pos)
                insert_pos = line_end + 1

                new_content = content[:insert_pos] + '\\n' + validate_code + '\\n' + content[insert_pos:]
            else:
                # 直接插入到main函数开始处
                main_body_start = content.find('\\n', main_start) + 1
                new_content = content[:main_body_start] + validate_code + '\\n' + content[main_body_start:]
        else:
            print("❌ 未找到main函数")
            return False

    with open(main_py_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print("✅ 已更新validate_data函数")
    return True


def create_simple_test():
    """创建简单测试脚本"""
    print("\n🧪 创建简单测试脚本...")

    test_code = '''
"""
简单测试脚本
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 简单测试P4功能")
    print("=" * 50)

    try:
        # 直接导入测试
        print("\\n1. 导入QueryEngine...")
        from src.query.query_engine import QueryEngine
        print("✅ 导入成功")

        # 测试引擎
        print("\\n2. 创建查询引擎...")
        engine = QueryEngine()

        # 测试统计
        print("\\n3. 测试数据统计...")
        stats = engine.get_data_statistics()

        if stats:
            print(f"📊 统计结果:")
            print(f"  股票数量: {stats.get('total_stocks', 0)}")
            print(f"  日线记录: {stats.get('total_daily_records', 0)}")

            if stats.get('total_daily_records', 0) > 0:
                print("✅ 数据库中有数据!")

                # 测试查询
                print("\\n4. 测试查询...")
                if stats.get('stock_list'):
                    test_symbol = stats['stock_list'][0]
                    print(f"  查询股票: {test_symbol}")

                    data = engine.query_daily_data(symbol=test_symbol, limit=2)
                    if not data.empty:
                        print(f"✅ 查询成功: {len(data)}条记录")
                        print(data[['trade_date', 'symbol', 'close', 'price_change']].to_string())
                    else:
                        print("⚠️  未查询到数据")

        engine.close()
        print("\\n🎉 简单测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
'''

    with open('simple_p4_test.py', 'w', encoding='utf-8') as f:
        f.write(test_code)

    print("✅ 已创建简单测试脚本: simple_p4_test.py")
    return True


def main():
    """主函数"""
    print("🔧 执行最终修复...")

    # 执行修复
    fix_query_engine()
    fix_main_py()
    create_simple_test()

    print("\n" + "=" * 60)
    print("🎉 最终修复完成!")
    print("\n运行测试:")
    print("1. 简单测试: python simple_p4_test.py")
    print("2. 查询引擎测试: python src/query/query_engine.py")
    print("3. main.py测试: python main.py --action p4_test")
    print("4. 验证数据: python main.py --action validate")


if __name__ == "__main__":
    main()