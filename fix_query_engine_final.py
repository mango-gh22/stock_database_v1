# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_query_engine_final.py
# File Name: fix_query_engine_final
# @ Author: mango-gh22
# @ Date：2025/12/6 21:47
"""
desc 
"""
"""
最终修复query_engine.py
"""
import os

print("🔧 最终修复query_engine.py")
print("=" * 60)

# 创建修复后的query_engine.py
fixed_query_engine = '''"""
查询引擎 - 最终修复版本
"""
import pandas as pd
import pymysql
import yaml
import os
from datetime import datetime

class QueryEngine:
    """查询引擎 - 简化稳定版本"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()

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
                charset=mysql_config.get('charset', 'utf8mb4')
            )

        except Exception as e:
            print(f"❌ 连接数据库失败: {e}")
            raise

    def get_data_statistics(self):
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票基本信息统计
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

            return stats

        except Exception as e:
            print(f"❌ 获取统计失败: {e}")
            return {}

    def query_daily_data(self, symbol=None, limit=10):
        """查询日线数据"""
        try:
            if symbol:
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

            # 使用pandas读取数据
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

            return filepath

        except Exception as e:
            print(f"❌ 导出失败: {e}")
            return str(e)

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()

def test_query_engine():
    """测试查询引擎"""
    print("🧪 查询引擎测试")
    print("=" * 50)

    engine = QueryEngine()

    try:
        # 1. 数据统计
        print("\\n📊 1. 数据统计")
        stats = engine.get_data_statistics()

        if stats:
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")
            print(f"   数据范围: {stats.get('earliest_date', 'N/A')} 到 {stats.get('latest_date', 'N/A')}")
        else:
            print("   统计失败")
            return False

        # 2. 查询测试
        print("\\n📈 2. 查询测试")
        if stats.get('stock_list'):
            test_symbol = stats['stock_list'][0]
            stock_name = stats['stock_details'].get(test_symbol, '')
            print(f"   测试股票: {test_symbol} ({stock_name})")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = str(row['trade_date'])[:10]
                    close_price = row.get('close', 'N/A')
                    price_change = row.get('price_change', 0)
                    print(f"     {date_str}: {close_price} ({price_change:+.2f})")
            else:
                print("   未查询到数据")

        # 3. 导出测试
        print("\\n💾 3. 导出测试")
        if stats.get('stock_list'):
            export_file = engine.export_to_csv(
                symbol=stats['stock_list'][0],
                filename="test_export.csv"
            )
            print(f"   导出文件: {export_file}")

        print("\\n✅ 查询引擎测试完成!")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

    finally:
        engine.close()

if __name__ == "__main__":
    test_query_engine()
'''

# 备份原文件
query_engine_path = 'src/query/query_engine.py'
if os.path.exists(query_engine_path):
    import shutil

    shutil.copy2(query_engine_path, query_engine_path + '.backup')
    print(f"✅ 已备份原文件: {query_engine_path}.backup")

# 写入修复版本
os.makedirs(os.path.dirname(query_engine_path), exist_ok=True)
with open(query_engine_path, 'w', encoding='utf-8') as f:
    f.write(fixed_query_engine)

print(f"✅ 已修复: {query_engine_path}")

# 立即测试
print("\n🔧 立即测试修复版本...")

import subprocess

# 测试查询引擎
print("测试: python src/query/query_engine.py")
result = subprocess.run(
    ['python', 'src/query/query_engine.py'],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✅ 查询引擎测试通过!")
    if result.stdout:
        lines = result.stdout.split('\n')
        for line in lines[:20]:
            if line.strip():
                print(f"  {line}")
else:
    print(f"❌ 查询引擎测试失败")
    if result.stderr:
        print(f"错误: {result.stderr[:500]}")

print("\n" + "=" * 60)
print("🎉 query_engine.py修复完成!")