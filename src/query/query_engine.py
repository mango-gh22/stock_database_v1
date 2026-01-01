# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query/query_engine.py
# @ Author: mango-gh22
# @ Date：2025/12/5 20:20

"""
查询引擎 - 适配新版数据库连接器
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query/query_engine.py
# @ Author: mango-gh22
# @ Date：2025/12/27 10:30

"""
查询引擎 - 适配新版数据库连接器 + 支持多格式股票代码输入（如 '000001.SZ'）
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_connector import DatabaseConnector

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

    def _normalize_symbol(self, symbol: str) -> str:
        """
        将多种股票代码格式统一转换为数据库格式 (sz000001 / sh600519)

        支持输入格式：
          - '000001.SZ' → 'sz000001'
          - '600519.SH' → 'sh600519'
          - 'SZ000001'  → 'sz000001'
          - 'sh600519'  → 'sh600519' (不变)
          - '600519'    → 'sh600519' (启发式)
          - '000001'    → 'sz000001' (启发式)
          - '688001'    → 'sh688001' (科创板)

        Args:
            symbol: 原始股票代码

        Returns:
            标准化后的 symbol（若无法识别则原样返回）
        """
        if not isinstance(symbol, str):
            return symbol

        sym = symbol.strip().lower()

        # 已是目标格式：sz000001 / sh600519
        if sym.startswith(('sz', 'sh')) and len(sym) == 8:
            return sym

        # 处理 Tushare 格式：000001.SZ 或 600519.SH
        if '.' in sym:
            parts = sym.split('.', 1)
            code_part = parts[0].zfill(6)
            market_part = parts[1].lower()
            if market_part in ('sz', 'sh'):
                return market_part + code_part
            elif market_part == 'xshe':
                return 'sz' + code_part
            elif market_part == 'xshg':
                return 'sh' + code_part

        # 处理纯数字代码（启发式）
        if sym.isdigit() and len(sym) == 6:
            if sym.startswith(('00', '30')):
                return 'sz' + sym
            elif sym.startswith(('60', '68')):  # 沪市主板 + 科创板
                return 'sh' + sym
            # 可扩展北交所：elif sym.startswith('8'): return 'bj' + sym

        # 无法识别，原样返回
        logger.warning(f"无法标准化股票代码: '{symbol}'，将按原值查询")
        return symbol

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
            'table_counts': {}  # 替代原 table_info
        }

        try:
            db_info = self.db_connector.get_database_info()
            stats['database'] = db_info['database']
            stats['version'] = db_info['version']
            stats['tables'] = db_info['tables']

            result = self.db_connector.execute_query(
                "SELECT COUNT(*) as count FROM stock_basic_info"
            )
            if result:
                stats['total_stocks'] = result[0]['count']

            result = self.db_connector.execute_query(
                "SELECT COUNT(DISTINCT industry) as count FROM stock_basic_info WHERE industry IS NOT NULL AND industry != ''"
            )
            if result:
                stats['industry_count'] = result[0]['count']

            result = self.db_connector.execute_query(
                "SELECT symbol, name FROM stock_basic_info ORDER BY symbol"
            )
            stats['stock_list'] = [row['symbol'] for row in result]
            stats['stock_details'] = {row['symbol']: row['name'] for row in result}

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

            table_counts = {}
            for table in db_info['tables']:
                try:
                    result = self.db_connector.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                    if result:
                        table_counts[table] = result[0]['count']
                except Exception:
                    table_counts[table] = 0
            stats['table_counts'] = table_counts

            logger.info(f"数据统计完成: {stats['total_daily_records']}条日线记录")
            return stats

        except Exception as e:
            logger.error(f"获取统计失败: {e}", exc_info=True)
            return stats

    def query_daily_data(self, symbol: str = None, start_date: str = None,
                         end_date: str = None, limit: int = 100) -> pd.DataFrame:
        """
        查询日线数据

        Args:
            symbol: 股票代码（支持 '000001.SZ', 'sh600519' 等多种格式）
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            limit: 限制返回条数

        Returns:
            日线数据DataFrame
        """
        try:
            where_conditions = []
            params = []

            if symbol:
                normalized = self._normalize_symbol(symbol)
                where_conditions.append("symbol = %s")
                params.append(normalized)
                if normalized != symbol:
                    logger.debug(f"Symbol 标准化: '{symbol}' → '{normalized}'")

            if start_date:
                where_conditions.append("trade_date >= %s")
                params.append(start_date)

            if end_date:
                where_conditions.append("trade_date <= %s")
                params.append(end_date)

            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

            # ✅ 关键修正：price_change = close_price - pre_close_price
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
                    (close_price - pre_close_price) as price_change,
                    change_percent as pct_change,
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

            result = self.db_connector.execute_query(query, tuple(params))
            df = pd.DataFrame(result) if result else pd.DataFrame()

            if not df.empty:
                # ✅ 保留为 datetime 类型，不转字符串
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])

                numeric_cols = [
                    'open', 'high', 'low', 'close', 'pre_close',
                    'price_change', 'pct_change',
                    'volume', 'amount',
                    'turnover_rate', 'amplitude',
                    'ma5', 'ma10', 'ma20'
                ]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"查询日线数据成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询日线数据失败: {e}", exc_info=True)
            return pd.DataFrame()

    def query_stock_basic(self, symbol: str = None, industry: str = None) -> pd.DataFrame:
        try:
            where_conditions = []
            params = []

            if symbol:
                normalized = self._normalize_symbol(symbol)
                where_conditions.append("symbol = %s")
                params.append(normalized)
                if normalized != symbol:
                    logger.debug(f"Symbol 标准化: '{symbol}' → '{normalized}'")

            if industry:
                where_conditions.append("industry LIKE %s")
                params.append(f"%{industry}%")

            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

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
            logger.error(f"查询股票基本信息失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_stock_list(self, market: str = None) -> pd.DataFrame:
        try:
            where_conditions = []
            params = []

            if market:
                market_upper = market.upper()
                if market_upper == 'SH':
                    where_conditions.append("exchange = 'SH'")
                elif market_upper == 'SZ':
                    where_conditions.append("exchange = 'SZ'")
                elif market_upper == 'BJ':
                    where_conditions.append("exchange = 'BJ'")

            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

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
            logger.error(f"获取股票列表失败: {e}", exc_info=True)
            return pd.DataFrame()

    def export_to_csv(self, symbol: str = None, start_date: str = None,
                      end_date: str = None, filename: str = None) -> str:
        try:
            df = self.query_daily_data(symbol, start_date, end_date, limit=5000)

            if df.empty:
                logger.warning("无数据可导出")
                return "无数据可导出"

            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                symbol_part = f"_{symbol}" if symbol else "_all"
                date_part = ""
                if start_date and end_date:
                    date_part = f"_{start_date}_{end_date}"
                elif start_date:
                    date_part = f"_{start_date}"
                filename = f"stock_data{symbol_part}{date_part}_{timestamp}.csv"

            # ✅ 使用项目根目录
            project_root = Path(__file__).parent.parent.parent
            export_dir = project_root / "data" / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)

            filepath = export_dir / filename
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            logger.info(f"导出成功: {filepath} ({len(df)}条记录)")
            return str(filepath)

        except Exception as e:
            logger.error(f"导出失败: {e}", exc_info=True)
            return str(e)

    def execute_custom_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        try:
            result = self.db_connector.execute_query(query, params)
            df = pd.DataFrame(result) if result else pd.DataFrame()
            logger.info(f"执行自定义查询成功: {len(df)}条记录")
            return df
        except Exception as e:
            logger.error(f"执行自定义查询失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        try:
            query = """
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
            logger.error(f"获取表结构失败: {e}", exc_info=True)
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
        print("\n📊 1. 数据统计测试")
        stats = engine.get_data_statistics()
        if stats:
            print(f"   数据库: {stats.get('database', 'Unknown')}")
            print(f"   版本: {stats.get('version', 'Unknown')}")
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")
            print(f"   数据范围: {stats.get('earliest_date', 'N/A')} 到 {stats.get('latest_date', 'N/A')}")
            print(f"   行业数量: {stats.get('industry_count', 0)}")

        print("\n📋 2. 获取股票列表")
        stock_df = engine.get_stock_list()
        if not stock_df.empty:
            print(f"   获取到 {len(stock_df)} 只股票")
            print("   前5只股票:")
            for i, (_, row) in enumerate(stock_df.head().iterrows()):
                print(f"     {i + 1}. {row['symbol']} - {row['name']} ({row.get('industry', 'N/A')})")

        print("\n📈 3. 查询股票数据（测试 '000001.SZ'）")
        if not stock_df.empty:
            test_symbol_tushare = '000001.SZ'
            print(f"   测试股票 (Tushare格式): {test_symbol_tushare}")

            data = engine.query_daily_data(symbol=test_symbol_tushare, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = row['trade_date'].strftime('%Y-%m-%d')
                    close_price = row.get('close', 'N/A')
                    price_change = row.get('price_change', 0)
                    pct_change = row.get('pct_change', 0)
                    print(f"     {date_str}: 收盘价 {close_price} 涨跌 {price_change:+.2f} ({pct_change:+.2f}%)")
            else:
                print("   未查询到数据")

        print("\n🏗️  4. 表结构查看")
        table_schema = engine.get_table_schema('stock_daily_data')
        if not table_schema.empty:
            print(f"   stock_daily_data 表结构 ({len(table_schema)}列):")
            for i, (_, row) in enumerate(table_schema.head(5).iterrows()):
                print(
                    f"     {row['COLUMN_NAME']}: {row['DATA_TYPE']} {'NULL' if row['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")
            if len(table_schema) > 5:
                print(f"     ... 还有 {len(table_schema) - 5} 列")

        print("\n💾 5. 数据导出测试")
        if not stock_df.empty:
            export_file = engine.export_to_csv(
                symbol='600519.SH',
                filename="test_export_sh600519.csv"
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