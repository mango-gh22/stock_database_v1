# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query/query_engine.py
# @ Author: mango-gh22
# @ Date：2025/12/5 20:20

"""
查询引擎 - 适配新版数据库连接器
"""

import pandas as pd
from datetime import datetime
import os
from pathlib import Path
import logging

# 使用新的数据库连接器
from src.database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class QueryEngine:
    """查询引擎 - 适配新版数据库连接器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化查询引擎

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.db_connector = DatabaseConnector(config_path)
        logger.info("查询引擎初始化完成")

    def get_data_statistics(self) -> dict:
        """获取数据统计"""
        stats = {
            'total_stocks': 0,
            'total_daily_records': 0,
            'earliest_date': 'N/A',
            'latest_date': 'N/A',
            'stocks_with_data': 0,
            'industry_count': 0,
            'stock_list': [],
            'stock_details': {},
            'table_info': {}
        }

        try:
            # 1. 获取数据库信息
            db_info = self.db_connector.get_database_info()
            stats['database'] = db_info['database']
            stats['version'] = db_info['version']
            stats['tables'] = db_info['tables']

            # 2. 股票基本信息统计
            result = self.db_connector.execute_query(
                "SELECT COUNT(*) as count FROM stock_basic_info"
            )
            if result:
                stats['total_stocks'] = result[0]['count']

            # 3. 行业统计
            result = self.db_connector.execute_query(
                "SELECT COUNT(DISTINCT industry) as count FROM stock_basic_info WHERE industry IS NOT NULL AND industry != ''"
            )
            if result:
                stats['industry_count'] = result[0]['count']

            # 4. 股票列表
            result = self.db_connector.execute_query(
                "SELECT symbol, name FROM stock_basic_info ORDER BY symbol"
            )
            stats['stock_list'] = [row['symbol'] for row in result]
            stats['stock_details'] = {row['symbol']: row['name'] for row in result}

            # 5. 日线数据统计
            result = self.db_connector.execute_query("""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(trade_date) as earliest_date,
                    MAX(trade_date) as latest_date,
                    COUNT(DISTINCT symbol) as stocks_count
                FROM stock_daily_data
            """)

            if result and result[0]:
                row = result[0]
                stats['total_daily_records'] = row['total_records']
                stats['stocks_with_data'] = row['stocks_count']

                if row['earliest_date']:
                    stats['earliest_date'] = row['earliest_date'].strftime('%Y-%m-%d')
                if row['latest_date']:
                    stats['latest_date'] = row['latest_date'].strftime('%Y-%m-%d')

            # 6. 表信息统计
            table_counts = {}
            for table in db_info['tables']:
                try:
                    result = self.db_connector.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                    if result:
                        table_counts[table] = result[0]['count']
                except:
                    table_counts[table] = 0

            stats['table_counts'] = table_counts

            logger.info(f"数据统计完成: {stats['total_daily_records']}条日线记录")
            return stats

        except Exception as e:
            logger.error(f"获取统计失败: {e}")
            return stats

    def query_daily_data(self, symbol: str = None, start_date: str = None,
                         end_date: str = None, limit: int = 100) -> pd.DataFrame:
        """
        查询日线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            limit: 限制返回条数

        Returns:
            日线数据DataFrame
        """
        try:
            # 构建查询
            where_conditions = []
            params = []

            if symbol:
                where_conditions.append("symbol = %s")
                params.append(symbol)

            if start_date:
                where_conditions.append("trade_date >= %s")
                params.append(start_date)

            if end_date:
                where_conditions.append("trade_date <= %s")
                params.append(end_date)

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            # 构建查询语句 - 适配您的表结构
            query = f"""
                SELECT 
                    trade_date, 
                    symbol,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume,
                    amount,
                    pct_change,
                    change_amount as price_change,
                    pre_close_price as pre_close,
                    turnover_rate,
                    amplitude,
                    ma5, ma10, ma20
                FROM stock_daily_data
                {where_clause}
                ORDER BY trade_date DESC
                LIMIT %s
            """
            params.append(limit)

            # 执行查询
            result = self.db_connector.execute_query(query, tuple(params))

            # 转换为DataFrame
            df = pd.DataFrame(result) if result else pd.DataFrame()

            if not df.empty:
                # 转换日期类型
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])

                # 转换数值类型
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount',
                                'pct_change', 'price_change', 'pre_close',
                                'turnover_rate', 'amplitude', 'ma5', 'ma10', 'ma20']

                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"查询日线数据成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询日线数据失败: {e}")
            return pd.DataFrame()

    def query_stock_basic(self, symbol: str = None, industry: str = None) -> pd.DataFrame:
        """
        查询股票基本信息

        Args:
            symbol: 股票代码
            industry: 行业

        Returns:
            股票基本信息DataFrame
        """
        try:
            where_conditions = []
            params = []

            if symbol:
                where_conditions.append("symbol = %s")
                params.append(symbol)

            if industry:
                where_conditions.append("industry LIKE %s")
                params.append(f"%{industry}%")

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT 
                    symbol,
                    name,
                    industry,
                    area,
                    market,
                    list_date,
                    exchange,
                    list_status
                FROM stock_basic_info
                {where_clause}
                ORDER BY symbol
            """

            result = self.db_connector.execute_query(query, tuple(params) if params else None)
            df = pd.DataFrame(result) if result else pd.DataFrame()

            logger.info(f"查询股票基本信息成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询股票基本信息失败: {e}")
            return pd.DataFrame()

    def get_stock_list(self, market: str = None) -> pd.DataFrame:
        """
        获取股票列表

        Args:
            market: 市场类型

        Returns:
            股票列表DataFrame
        """
        try:
            where_conditions = []
            params = []

            if market:
                if market.upper() == 'SH':
                    where_conditions.append("exchange = 'SH'")
                elif market.upper() == 'SZ':
                    where_conditions.append("exchange = 'SZ'")
                elif market.upper() == 'BJ':
                    where_conditions.append("exchange = 'BJ'")

            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT 
                    symbol,
                    name,
                    industry,
                    market,
                    exchange
                FROM stock_basic_info
                {where_clause}
                ORDER BY symbol
            """

            result = self.db_connector.execute_query(query, tuple(params) if params else None)
            df = pd.DataFrame(result) if result else pd.DataFrame()

            logger.info(f"获取股票列表成功: {len(df)}只股票")
            return df

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def export_to_csv(self, symbol: str = None, start_date: str = None,
                      end_date: str = None, filename: str = None) -> str:
        """
        导出数据到CSV

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            filename: 文件名

        Returns:
            导出文件路径
        """
        try:
            # 查询数据
            df = self.query_daily_data(symbol, start_date, end_date, limit=5000)

            if df.empty:
                logger.warning("无数据可导出")
                return "无数据可导出"

            # 生成文件名
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                symbol_part = f"_{symbol}" if symbol else "_all"
                date_part = ""
                if start_date and end_date:
                    date_part = f"_{start_date}_{end_date}"
                elif start_date:
                    date_part = f"_{start_date}"
                filename = f"stock_data{symbol_part}{date_part}_{timestamp}.csv"

            # 确保导出目录存在
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)

            filepath = export_dir / filename

            # 导出到CSV
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            logger.info(f"导出成功: {filepath} ({len(df)}条记录)")
            return str(filepath)

        except Exception as e:
            logger.error(f"导出失败: {e}")
            return str(e)

    def execute_custom_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        执行自定义查询

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果DataFrame
        """
        try:
            result = self.db_connector.execute_query(query, params)
            df = pd.DataFrame(result) if result else pd.DataFrame()

            logger.info(f"执行自定义查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"执行自定义查询失败: {e}")
            return pd.DataFrame()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            表结构信息
        """
        try:
            query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """

            result = self.db_connector.execute_query(query, (table_name,))
            df = pd.DataFrame(result) if result else pd.DataFrame()

            logger.info(f"获取表结构成功: {table_name}")
            return df

        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return pd.DataFrame()

    def close(self):
        """关闭连接"""
        self.db_connector.close_all_connections()
        logger.info("数据库连接已关闭")


def test_query_engine():
    """测试查询引擎"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试查询引擎")
    print("=" * 50)

    engine = QueryEngine()

    try:
        # 1. 数据统计
        print("\n📊 1. 数据统计测试")
        stats = engine.get_data_statistics()

        if stats:
            print(f"   数据库: {stats.get('database', 'Unknown')}")
            print(f"   版本: {stats.get('version', 'Unknown')}")
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")
            print(f"   数据范围: {stats.get('earliest_date', 'N/A')} 到 {stats.get('latest_date', 'N/A')}")
            print(f"   行业数量: {stats.get('industry_count', 0)}")

        # 2. 获取股票列表
        print("\n📋 2. 获取股票列表")
        stock_df = engine.get_stock_list()
        if not stock_df.empty:
            print(f"   获取到 {len(stock_df)} 只股票")
            print("   前5只股票:")
            for i, (_, row) in enumerate(stock_df.head().iterrows()):
                print(f"     {i + 1}. {row['symbol']} - {row['name']} ({row.get('industry', 'N/A')})")

        # 3. 查询具体股票数据
        print("\n📈 3. 查询股票数据")
        if not stock_df.empty:
            test_symbol = stock_df.iloc[0]['symbol']
            test_name = stock_df.iloc[0]['name']
            print(f"   测试股票: {test_symbol} ({test_name})")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = str(row['trade_date'])[:10] if 'trade_date' in row else '未知日期'
                    close_price = row.get('close', 'N/A')
                    price_change = row.get('price_change', 0)
                    pct_change = row.get('pct_change', 0)
                    print(f"     {date_str}: 收盘价 {close_price} 涨跌 {price_change:+.2f} ({pct_change:+.2f}%)")
            else:
                print("   未查询到数据")
        else:
            print("   无股票数据")

        # 4. 表结构查看
        print("\n🏗️  4. 表结构查看")
        table_schema = engine.get_table_schema('stock_daily_data')
        if not table_schema.empty:
            print(f"   stock_daily_data 表结构 ({len(table_schema)}列):")
            for i, (_, row) in enumerate(table_schema.head(5).iterrows()):
                print(
                    f"     {row['COLUMN_NAME']}: {row['DATA_TYPE']} {'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")
            if len(table_schema) > 5:
                print(f"     ... 还有 {len(table_schema) - 5} 列")

        # 5. 导出测试
        print("\n💾 5. 数据导出测试")
        if not stock_df.empty:
            export_file = engine.export_to_csv(
                symbol=stock_df.iloc[0]['symbol'],
                filename="test_export.csv"
            )
            print(f"   导出结果: {export_file}")

        print("\n🎉 查询引擎测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()


if __name__ == "__main__":
    test_query_engine()