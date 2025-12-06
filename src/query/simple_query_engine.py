
"""
安全查询引擎 - P4阶段最终版本
自动适应表结构变化
"""
import pandas as pd
import pymysql
import yaml
import os
from typing import Dict, List, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('safe_query_engine')

class SafeQueryEngine:
    """安全查询引擎 - 自动检测列名"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()
        self.column_info = self._detect_columns()

    def _get_connection(self):
        """获取数据库连接"""
        try:
            # 读取配置
            config_path = os.path.join('config', 'database.yaml')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    db_config = yaml.safe_load(f)['development']
            else:
                # 默认配置
                db_config = {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': '您的密码',
                    'database': 'stock_database',
                    'charset': 'utf8mb4'
                }

            return pymysql.connect(**db_config)
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise

    def _detect_columns(self) -> Dict:
        """检测表列名"""
        column_info = {
            'daily_table': 'stock_daily_data',
            'basic_table': 'stock_basic',
            'daily_columns': [],
            'change_column': None
        }

        try:
            cursor = self.conn.cursor()

            # 检测日线表列
            cursor.execute("SHOW COLUMNS FROM stock_daily_data")
            daily_columns = [row[0] for row in cursor.fetchall()]
            column_info['daily_columns'] = daily_columns

            # 检测价格变化列名
            if 'price_change' in daily_columns:
                column_info['change_column'] = 'price_change'
            elif 'change' in daily_columns:
                column_info['change_column'] = '`change`'  # 使用反引号
            else:
                column_info['change_column'] = 'NULL as price_change'

            cursor.close()

            logger.info(f"检测到列信息: {column_info}")
            return column_info

        except Exception as e:
            logger.error(f"检测列名失败: {e}")
            return column_info

    def get_data_statistics(self) -> Dict:
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票统计
            cursor.execute("SELECT COUNT(*) FROM stock_basic")
            stats['total_stocks'] = cursor.fetchone()[0]

            # 日线统计 - 使用检测到的列名
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(trade_date) as earliest_date,
                    MAX(trade_date) as latest_date,
                    COUNT(DISTINCT symbol) as stocks_with_data
                FROM {self.column_info['daily_table']}
            """)
            result = cursor.fetchone()
            stats['total_daily_records'] = result[0]
            stats['earliest_date'] = str(result[1]) if result[1] else None
            stats['latest_date'] = str(result[2]) if result[2] else None
            stats['stocks_with_data'] = result[3]

            # 股票列表
            cursor.execute("SELECT symbol FROM stock_basic ORDER BY symbol")
            stats['stock_list'] = [row[0] for row in cursor.fetchall()]

            cursor.close()
            logger.info(f"数据统计: {stats.get('total_daily_records', 0)}条记录")
            return stats

        except Exception as e:
            logger.error(f"数据统计失败: {e}")
            return {}

    def query_daily_data(self, symbol: str = None, limit: int = 10) -> pd.DataFrame:
        """查询日线数据 - 安全版本"""
        try:
            # 构建SELECT子句
            select_columns = [
                "trade_date", "symbol",
                "`open`", "`high`", "`low`", "`close`",
                "volume", "amount", "pct_change",
                f"{self.column_info['change_column']} as price_change",
                "pre_close", "turnover_rate", "amplitude"
            ]

            select_clause = ", ".join(select_columns)

            # 构建WHERE子句
            where_clause = ""
            params = []

            if symbol:
                where_clause = "WHERE symbol = %s"
                params.append(symbol)

            # 构建完整SQL
            sql = f"""
                SELECT {select_clause}
                FROM {self.column_info['daily_table']}
                {where_clause}
                ORDER BY trade_date DESC
                LIMIT %s
            """

            params.append(limit)

            logger.debug(f"执行SQL: {sql}")
            logger.debug(f"参数: {params}")

            # 执行查询
            df = pd.read_sql(sql, self.conn, params=params if params else None)

            if not df.empty:
                # 转换数据类型
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                              'pct_change', 'price_change', 'pre_close', 'turnover_rate', 'amplitude']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")

def test_safe_engine():
    """测试安全引擎"""
    print("🧪 测试安全查询引擎")
    print("=" * 50)

    engine = SafeQueryEngine()

    try:
        # 1. 测试统计
        print("📊 1. 数据统计测试")
        stats = engine.get_data_statistics()
        print(f"   股票总数: {stats.get('total_stocks', 0)}")
        print(f"   日线记录: {stats.get('total_daily_records', 0)}")

        # 2. 测试查询
        print("📈 2. 日线查询测试")
        if stats.get('stock_list'):
            test_symbol = stats['stock_list'][0]
            print(f"   测试股票: {test_symbol}")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录")
                for idx, row in data.iterrows():
                    print(f"     {row['trade_date']}: {row['close']:.2f} ({row.get('price_change', 0):+.2f})")
            else:
                print("   未查询到数据")

        print("✅ 安全查询引擎测试通过!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()

if __name__ == "__main__":
    test_safe_engine()
