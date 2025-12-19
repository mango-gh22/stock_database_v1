# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\dynamic_query_engine.py
# File Name: debug_tablename_query
# @ Author: mango-gh22
# @ Date：2025/12/6 17:54
"""
desc 
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
动态表名查询引擎
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text
from src.database.connection import engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DynamicQueryEngine:
    """动态表名查询引擎"""

    def __init__(self, config_path: str = None):
        """
        初始化查询引擎

        Args:
            config_path: 配置文件路径
        """
        self.engine = engine
        self.logger = get_logger(__name__)

        # 自动检测表名
        self.table_mapping = self._detect_table_names()
        self.logger.info(f"表名映射: {self.table_mapping}")

        self._init_cache()

    def _detect_table_names(self) -> Dict[str, str]:
        """自动检测表名"""
        mapping = {}

        try:
            with self.engine.connect() as conn:
                # 获取所有表
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]

                # 根据表名模式匹配
                for table in tables:
                    table_lower = table.lower()

                    if 'basic' in table_lower and ('info' in table_lower or 'basic' in table_lower):
                        if 'stock_basic' not in mapping:
                            mapping['stock_basic'] = table

                    elif 'daily' in table_lower and 'data' in table_lower:
                        if 'daily_data' not in mapping:
                            mapping['daily_data'] = table

                    elif 'index' in table_lower and 'info' in table_lower:
                        if 'index_info' not in mapping:
                            mapping['index_info'] = table

                    elif 'index' in table_lower and ('constituent' in table_lower or 'component' in table_lower):
                        if 'index_components' not in mapping:
                            mapping['index_components'] = table

                    elif 'financial' in table_lower:
                        if 'financial_data' not in mapping:
                            mapping['financial_data'] = table

                    elif 'minute' in table_lower and 'data' in table_lower:
                        if 'minute_data' not in mapping:
                            mapping['minute_data'] = table

                # 设置默认映射
                if 'stock_basic' not in mapping:
                    mapping['stock_basic'] = 'stock_basic'
                if 'daily_data' not in mapping:
                    mapping['daily_data'] = 'daily_data'

        except Exception as e:
            self.logger.error(f"检测表名失败: {e}")
            # 使用默认表名
            mapping = {
                'stock_basic': 'stock_basic_info',
                'daily_data': 'stock_daily_data',
                'index_info': 'index_info',
                'index_components': 'stock_index_constituent',
                'financial_data': 'stock_financial_indicators',
                'minute_data': 'stock_minute_data'
            }

        return mapping

    def _init_cache(self):
        """初始化缓存"""
        self._stock_cache = {}
        self._index_cache = {}

    def _execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """执行SQL查询"""
        try:
            if params:
                df = pd.read_sql_query(text(query), self.engine, params=params)
            else:
                df = pd.read_sql_query(text(query), self.engine)
            return df
        except Exception as e:
            self.logger.error(f"执行查询失败: {e}")
            self.logger.error(f"查询语句: {query[:200]}...")
            return pd.DataFrame()

    def _get_table(self, table_key: str) -> str:
        """获取实际表名"""
        return self.table_mapping.get(table_key, table_key)

    def get_stock_basic(self,
                        symbol: str = None,
                        exchange: str = None,
                        industry: str = None) -> pd.DataFrame:
        """
        查询股票基本信息
        """
        table_name = self._get_table('stock_basic')

        # 尝试不同的列名
        column_tests = [
            ('name', 'stock_name'),  # 可能是stock_name
            ('name', 'name'),  # 或者就是name
            ('name', 'stock_name, name')  # 或者两个都有
        ]

        for test_name, column_expr in column_tests:
            try:
                query = f"""
                SELECT 
                    symbol, {column_expr} as name, exchange, industry, 
                    listing_date, is_active, created_at
                FROM {table_name}
                WHERE 1=1
                """
                params = {}

                if symbol:
                    query += " AND symbol = :symbol"
                    params['symbol'] = symbol
                if exchange:
                    query += " AND exchange = :exchange"
                    params['exchange'] = exchange
                if industry:
                    query += " AND industry LIKE :industry"
                    params['industry'] = f"%{industry}%"

                query += " ORDER BY symbol"

                df = self._execute_query(query, params)
                if not df.empty:
                    self.logger.info(f"查询股票基本信息成功，使用列表达式: {column_expr}")
                    return df
            except:
                continue

        # 如果都失败，尝试获取表结构
        self.logger.warning(f"所有列名测试失败，尝试获取{table_name}表结构")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                columns = [row[0] for row in result.fetchall()]
                self.logger.info(f"表{table_name}的实际列: {columns}")

                # 构建动态查询
                select_cols = ['symbol']
                if 'stock_name' in columns:
                    select_cols.append('stock_name as name')
                elif 'name' in columns:
                    select_cols.append('name')

                if 'exchange' in columns:
                    select_cols.append('exchange')
                if 'industry' in columns:
                    select_cols.append('industry')
                if 'listing_date' in columns:
                    select_cols.append('listing_date')
                if 'is_active' in columns:
                    select_cols.append('is_active')
                if 'created_at' in columns:
                    select_cols.append('created_at')

                query = f"""
                SELECT {', '.join(select_cols)}
                FROM {table_name}
                """

                df = self._execute_query(query)
                return df

        except Exception as e:
            self.logger.error(f"获取表结构失败: {e}")

        return pd.DataFrame()

    def get_daily_data(self,
                       symbol: str = None,
                       start_date: str = None,
                       end_date: str = None,
                       fields: List[str] = None,
                       limit: int = None) -> pd.DataFrame:
        """
        查询日线行情数据
        """
        table_name = self._get_table('daily_data')

        # 默认字段（尝试匹配实际列名）
        if fields is None:
            # 先尝试获取实际列
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"DESCRIBE {table_name}"))
                    actual_columns = [row[0] for row in result.fetchall()]

                    # 使用实际存在的列
                    default_fields = []
                    for field in ['trade_date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']:
                        if field in actual_columns:
                            default_fields.append(field)

                    if not default_fields:
                        default_fields = ['*']  # 如果都不存在，使用*
                    fields = default_fields
            except:
                fields = ['trade_date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']

        field_str = ', '.join(fields)

        query = f"""
        SELECT {field_str}
        FROM {table_name}
        WHERE 1=1
        """
        params = {}

        if symbol:
            query += " AND symbol = :symbol"
            params['symbol'] = symbol
        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND trade_date <= :end_date"
            params['end_date'] = end_date

        query += " ORDER BY trade_date DESC"

        if limit:
            query += f" LIMIT {limit}"

        df = self._execute_query(query, params)

        if not df.empty and 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')

        return df

    def get_data_statistics(self) -> Dict:
        """
        获取数据统计信息
        """
        stats = {}

        try:
            # 股票数量统计
            basic_table = self._get_table('stock_basic')
            stock_query = f"""
            SELECT 
                COUNT(*) as total_stocks,
                COUNT(DISTINCT industry) as total_industries,
                exchange,
                COUNT(*) as count_by_exchange
            FROM {basic_table}
            GROUP BY exchange
            """

            # 日线数据统计
            daily_table = self._get_table('daily_data')
            daily_query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date,
                COUNT(DISTINCT symbol) as stocks_with_data
            FROM {daily_table}
            """

            # 股票统计
            stock_stats = self._execute_query(stock_query)
            daily_stats = self._execute_query(daily_query)

            if not stock_stats.empty:
                stats['stock_basic'] = {
                    'total_stocks': int(stock_stats['total_stocks'].iloc[0]),
                    'total_industries': int(
                        stock_stats['total_industries'].iloc[0]) if 'total_industries' in stock_stats.columns else 0,
                    'exchange_distribution': stock_stats.set_index('exchange')['count_by_exchange'].to_dict()
                }
            else:
                stats['stock_basic'] = {
                    'total_stocks': 0,
                    'total_industries': 0,
                    'exchange_distribution': {}
                }

            if not daily_stats.empty:
                stats['daily_data'] = {
                    'total_records': int(daily_stats['total_records'].iloc[0]),
                    'earliest_date': str(daily_stats['earliest_date'].iloc[0]) if pd.notna(
                        daily_stats['earliest_date'].iloc[0]) else None,
                    'latest_date': str(daily_stats['latest_date'].iloc[0]) if pd.notna(
                        daily_stats['latest_date'].iloc[0]) else None,
                    'stocks_with_data': int(
                        daily_stats['stocks_with_data'].iloc[0]) if 'stocks_with_data' in daily_stats.columns else 0
                }
            else:
                stats['daily_data'] = {
                    'total_records': 0,
                    'earliest_date': None,
                    'latest_date': None,
                    'stocks_with_data': 0
                }

            self.logger.info("数据统计查询成功")

        except Exception as e:
            self.logger.error(f"数据统计查询失败: {e}")

        return stats

    def get_stock_list(self) -> List[str]:
        """获取股票代码列表"""
        table_name = self._get_table('stock_basic')
        query = f"SELECT symbol FROM {table_name} ORDER BY symbol"

        try:
            df = self._execute_query(query)
            if not df.empty and 'symbol' in df.columns:
                return df['symbol'].tolist()
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")

        return []

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("数据库连接测试成功")
                return True
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")
            return False

    def close(self):
        """关闭连接"""
        pass


def test_dynamic_engine():
    """测试动态引擎"""
    print("🧪 测试动态查询引擎...")

    engine = DynamicQueryEngine()

    print("1️⃣ 表名映射:", engine.table_mapping)

    print("\n2️⃣ 测试连接...")
    if engine.test_connection():
        print("✅ 连接成功")
    else:
        print("❌ 连接失败")
        return

    print("\n3️⃣ 测试数据统计...")
    stats = engine.get_data_statistics()
    print(f"📊 股票数量: {stats.get('stock_basic', {}).get('total_stocks', 0)}")
    print(f"📅 日线记录: {stats.get('daily_data', {}).get('total_records', 0)}")

    print("\n4️⃣ 测试股票列表...")
    stocks = engine.get_stock_list()
    print(f"📋 股票列表: {len(stocks)} 只")

    if stocks:
        print("\n5️⃣ 测试股票基本信息...")
        df = engine.get_stock_basic(stocks[0])
        if not df.empty:
            print(f"✅ 成功获取 {stocks[0]} 基本信息")
            print(df.head())

    print("\n🎉 动态引擎测试完成!")


if __name__ == "__main__":
    test_dynamic_engine()