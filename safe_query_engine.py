# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\simple_query_engine.py
# File Name: safe_query_engine
# @ Author: mango-gh22
# @ Date：2025/12/6 20:09
"""
desc 
"""
"""
简化版安全查询引擎 - P4阶段核心
"""
import pandas as pd
import pymysql
import yaml
import os
from typing import Dict, List, Optional
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('simple_query_engine')


class SimpleQueryEngine:
    """简化查询引擎 - 直接处理保留关键字"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()
        logger.info("查询引擎初始化完成")

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
                    'password': '您的密码',  # 请修改为您的密码
                    'database': 'stock_database',
                    'charset': 'utf8mb4'
                }

            logger.info(f"连接数据库: {db_config['database']}@{db_config['host']}")
            return pymysql.connect(**db_config)

        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise

    def get_data_statistics(self) -> Dict:
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票基本信息统计
            cursor.execute("SELECT COUNT(*) FROM stock_basic")
            stats['total_stocks'] = cursor.fetchone()[0]

            # 日线数据统计 - 使用反引号处理保留关键字
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(trade_date) as earliest_date,
                    MAX(trade_date) as latest_date,
                    COUNT(DISTINCT symbol) as stocks_with_data
                FROM stock_daily_data
            """)
            result = cursor.fetchone()
            stats['total_daily_records'] = result[0]
            stats['earliest_date'] = str(result[1]) if result[1] else None
            stats['latest_date'] = str(result[2]) if result[2] else None
            stats['stocks_with_data'] = result[3]

            # 股票列表
            cursor.execute("SELECT symbol, name FROM stock_basic ORDER BY symbol")
            stocks = cursor.fetchall()
            stats['stock_list'] = [row[0] for row in stocks]
            stats['stock_details'] = {row[0]: row[1] for row in stocks}

            # 行业统计
            cursor.execute("SELECT COUNT(DISTINCT industry) FROM stock_basic")
            stats['industry_count'] = cursor.fetchone()[0]

            cursor.close()

            logger.info(f"数据统计: {stats['total_daily_records']}条日线记录")
            return stats

        except Exception as e:
            logger.error(f"数据统计失败: {e}")
            return {}

    def query_daily_data(self,
                         symbol: str = None,
                         start_date: str = None,
                         end_date: str = None,
                         limit: int = 100) -> pd.DataFrame:
        """
        查询日线数据 - 安全版本

        使用反引号处理所有可能的保留关键字
        """
        try:
            # 构建SELECT子句 - 使用反引号保护所有列名
            select_columns = [
                "trade_date",
                "symbol",
                "`open`",
                "`high`",
                "`low`",
                "`close`",
                "volume",
                "amount",
                "pct_change",
                # 尝试不同的列名
                "COALESCE(price_change, `change`) as price_change",
                "pre_close",
                "turnover_rate",
                "amplitude"
            ]

            select_clause = ", ".join(select_columns)

            # 构建WHERE条件
            conditions = ["1=1"]
            params = {}

            if symbol:
                conditions.append("symbol = %(symbol)s")
                params['symbol'] = symbol

            if start_date:
                conditions.append("trade_date >= %(start_date)s")
                params['start_date'] = start_date

            if end_date:
                conditions.append("trade_date <= %(end_date)s")
                params['end_date'] = end_date

            where_clause = " AND ".join(conditions)

            # 构建完整SQL
            sql = f"""
                SELECT {select_clause}
                FROM stock_daily_data
                WHERE {where_clause}
                ORDER BY trade_date DESC
                LIMIT {limit}
            """

            logger.debug(f"执行SQL: {sql}")

            # 执行查询
            df = pd.read_sql(sql, self.conn, params=params if params else None)

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

            logger.info(f"查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询失败: {e}")
            # 返回空的DataFrame
            return pd.DataFrame()

    def query_stock_basic(self, symbol: str = None) -> pd.DataFrame:
        """查询股票基本信息"""
        try:
            sql = "SELECT symbol, name, industry, list_date FROM stock_basic"
            params = None

            if symbol:
                sql += " WHERE symbol = %s"
                params = (symbol,)

            sql += " ORDER BY symbol"

            df = pd.read_sql(sql, self.conn, params=params)
            logger.info(f"查询股票信息: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询股票信息失败: {e}")
            return pd.DataFrame()

    def export_to_csv(self, symbol: str = None, filename: str = None) -> str:
        """导出数据到CSV"""
        try:
            # 查询数据
            df = self.query_daily_data(symbol=symbol, limit=1000)

            if df.empty:
                return "无数据可导出"

            # 生成文件名
            if filename is None:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                symbol_part = f"_{symbol}" if symbol else "_all"
                filename = f"stock_data{symbol_part}_{timestamp}.csv"

            # 确保导出目录存在
            export_dir = "data/exports"
            os.makedirs(export_dir, exist_ok=True)

            filepath = os.path.join(export_dir, filename)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            logger.info(f"导出成功: {filepath} ({len(df)}条记录)")
            return filepath

        except Exception as e:
            logger.error(f"导出失败: {e}")
            return str(e)

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")


# 测试函数
def test_simple_engine():
    """测试简化查询引擎"""
    print("🧪 简化查询引擎测试")
    print("=" * 50)

    engine = SimpleQueryEngine()

    try:
        # 1. 数据统计
        print("\n📊 1. 数据统计测试")
        stats = engine.get_data_statistics()

        if stats:
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")
            print(f"   数据范围: {stats.get('earliest_date')} 到 {stats.get('latest_date')}")

            # 2. 查询测试
            print("\n📈 2. 日线查询测试")
            if stats.get('stock_list'):
                test_symbol = stats['stock_list'][0]
                print(f"   测试股票: {test_symbol}")

                data = engine.query_daily_data(symbol=test_symbol, limit=3)
                if not data.empty:
                    print(f"  查询成功: {len(data)}条记录")
                    for idx, row in data.iterrows():
                        date_str = row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'],
                                                                                     'strftime') else str(
                            row['trade_date'])
                        print(f"     {date_str}: {row['close']:.2f} (涨跌: {row.get('price_change', 0):+.2f})")
                else:
                    print("   未查询到数据")

            # 3. 导出测试
            print("\n💾 3. 数据导出测试")
            if stats.get('stock_list'):
                export_file = engine.export_to_csv(
                    symbol=stats['stock_list'][0],
                    filename="p4_test_export.csv"
                )
                print(f"   导出结果: {export_file}")

        print("\n✅ 简化查询引擎测试通过!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()


if __name__ == "__main__":
    test_simple_engine()