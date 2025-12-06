
"""
简单查询引擎 - 直接使用pymysql
"""
import pandas as pd
import pymysql
import yaml
import os
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('simple_query')

class SimpleQuery:
    """简单查询类"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()
        logger.info("简单查询引擎初始化完成")

    def _get_connection(self):
        """获取数据库连接"""
        try:
            # 读取配置
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
                    'password': '',  # 需要填写您的密码
                    'database': 'stock_database',
                    'charset': 'utf8mb4'
                }

            logger.info(f"连接数据库: {db_config['database']}@{db_config['host']}")

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
            logger.error(f"连接数据库失败: {e}")
            raise

    def get_stats(self):
        """获取统计信息"""
        try:
            cursor = self.conn.cursor()

            # 股票统计
            cursor.execute("SELECT COUNT(*) as count FROM stock_basic")
            stock_count = cursor.fetchone()['count']

            # 日线统计
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    MIN(trade_date) as earliest,
                    MAX(trade_date) as latest,
                    COUNT(DISTINCT symbol) as symbols
                FROM stock_daily_data
            """)
            daily_stats = cursor.fetchone()

            cursor.close()

            return {
                'stock_count': stock_count,
                'daily_total': daily_stats['total'],
                'earliest_date': str(daily_stats['earliest']),
                'latest_date': str(daily_stats['latest']),
                'symbols_with_data': daily_stats['symbols']
            }

        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return {}

    def query_daily(self, symbol=None, limit=10):
        """查询日线数据"""
        try:
            if symbol:
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
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                              'pct_change', 'price_change', 'pre_close', 
                              'turnover_rate', 'amplitude']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询失败: {e}")
            return pd.DataFrame()

    def get_stock_list(self):
        """获取股票列表"""
        try:
            sql = "SELECT symbol, name FROM stock_basic ORDER BY symbol"
            df = pd.read_sql(sql, self.conn)
            return df
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("连接已关闭")

def quick_test():
    """快速测试"""
    print("🧪 简单查询引擎测试")
    print("=" * 50)

    query = SimpleQuery()

    try:
        # 1. 测试统计
        print("\n📊 1. 数据统计")
        stats = query.get_stats()
        if stats:
            print(f"   股票数量: {stats.get('stock_count', 0)}")
            print(f"   日线记录: {stats.get('daily_total', 0)}")
            print(f"   数据范围: {stats.get('earliest_date')} 到 {stats.get('latest_date')}")

        # 2. 测试查询
        print("\n📈 2. 日线查询")
        # 先获取股票列表
        stocks_df = query.get_stock_list()
        if not stocks_df.empty:
            test_symbol = stocks_df.iloc[0]['symbol']
            test_name = stocks_df.iloc[0]['name']
            print(f"   测试股票: {test_symbol} ({test_name})")

            data = query.query_daily(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = row['trade_date'].strftime('%Y-%m-%d')
                    print(f"     {date_str}: {row['close']:.2f} ({row.get('price_change', 0):+.2f})")
            else:
                print("   未查询到数据")
        else:
            print("   未找到股票数据")

        # 3. 导出测试
        print("\n💾 3. 数据导出")
        if not stocks_df.empty and not data.empty if 'data' in locals() else False:
            export_dir = 'data/exports'
            os.makedirs(export_dir, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"test_export_{timestamp}.csv"
            filepath = os.path.join(export_dir, filename)

            data.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"   导出成功: {filepath}")

        print("\n✅ 简单查询引擎测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        query.close()

if __name__ == "__main__":
    quick_test()
