# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\standardize_tables.py
# File Name: standardize_tables
# @ Author: mango-gh22
# @ Date：2025/12/6 18:49
"""
desc 创建表结构修复脚本
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复表结构 - 将非标准列名改为标准列名
"""

import sys
import os
import pandas as pd
from typing import Dict, List, Optional, Union, Tuple



sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import engine
from sqlalchemy import text
from src.utils.logger import get_logger

logger = get_logger(__name__)


def rename_columns_in_daily_table():
    """重命名日线数据表的列"""
    print("🔄 修复日线数据表列名")
    print("=" * 60)

    # 列名映射：旧列名 -> 新列名
    column_renames = {
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'pre_close_price': 'pre_close',
        'change_amount': 'change',
        'created_time': 'created_at',
        'updated_time': 'updated_at'
    }

    try:
        with engine.connect() as conn:
            # 1. 检查当前表结构
            print("📋 当前表结构:")
            result = conn.execute(text("DESCRIBE stock_daily_data"))
            current_columns = {}
            for row in result:
                print(f"  {row[0]:20} {row[1]:20}")
                current_columns[row[0]] = row[1]

            # 2. 检查需要重命名的列是否存在
            rename_operations = []
            for old_name, new_name in column_renames.items():
                if old_name in current_columns:
                    rename_operations.append((old_name, new_name))
                    print(f"  🔄 {old_name} → {new_name}")
                else:
                    print(f"  ⚠️  {old_name} 不存在，跳过")

            if not rename_operations:
                print("✅ 所有列名已经是标准名称")
                return True

            # 3. 执行重命名
            print(f"\n🔄 执行 {len(rename_operations)} 个重命名操作...")

            for old_name, new_name in rename_operations:
                try:
                    # 获取列定义
                    column_type = current_columns[old_name]

                    # 执行重命名
                    rename_sql = f"ALTER TABLE stock_daily_data CHANGE COLUMN {old_name} {new_name} {column_type}"
                    conn.execute(text(rename_sql))

                    print(f"  ✅ {old_name} → {new_name}")

                except Exception as e:
                    print(f"  ❌ 重命名 {old_name} 失败: {e}")

            conn.commit()

            # 4. 验证重命名结果
            print("\n🔍 验证重命名结果:")
            result = conn.execute(text("DESCRIBE stock_daily_data"))
            final_columns = [row[0] for row in result]

            print(f"📝 最终列名 ({len(final_columns)}):")
            for i, col in enumerate(final_columns):
                print(f"  {i + 1:2d}. {col}")

            # 检查是否包含标准列名
            standard_cols = ['open', 'high', 'low', 'close']
            missing = [col for col in standard_cols if col not in final_columns]

            if not missing:
                print("✅ 所有标准列名已就位")
            else:
                print(f"⚠️  缺少标准列: {missing}")

            return True

    except Exception as e:
        print(f"❌ 修复表结构失败: {e}")
        return False


def create_standardized_table():
    """创建标准化的日线数据表（备用方案）"""
    print("\n📋 创建标准化表（备用方案）")
    print("=" * 60)

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_data_standard (
        id INT AUTO_INCREMENT PRIMARY KEY,
        trade_date DATE NOT NULL COMMENT '交易日期',
        symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
        open DECIMAL(10, 4) COMMENT '开盘价',
        high DECIMAL(10, 4) COMMENT '最高价',
        low DECIMAL(10, 4) COMMENT '最低价',
        close DECIMAL(10, 4) COMMENT '收盘价',
        volume BIGINT COMMENT '成交量(股)',
        amount DECIMAL(20, 4) COMMENT '成交额(元)',
        change DECIMAL(10, 4) COMMENT '涨跌额',
        pct_change DECIMAL(10, 4) COMMENT '涨跌幅(%)',
        pre_close DECIMAL(10, 4) COMMENT '前收盘价',
        turnover_rate DECIMAL(10, 4) COMMENT '换手率(%)',
        amplitude DECIMAL(10, 4) COMMENT '振幅(%)',
        ma5 DECIMAL(10, 4) COMMENT '5日均线',
        ma10 DECIMAL(10, 4) COMMENT '10日均线',
        ma20 DECIMAL(10, 4) COMMENT '20日均线',
        ma30 DECIMAL(10, 4) COMMENT '30日均线',
        ma60 DECIMAL(10, 4) COMMENT '60日均线',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_date_symbol (trade_date, symbol),
        INDEX idx_symbol (symbol),
        INDEX idx_trade_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='标准日线行情数据表'
    """

    copy_data_sql = """
    INSERT INTO daily_data_standard (
        trade_date, symbol, 
        open, high, low, close,
        volume, amount, 
        change, pct_change, pre_close,
        turnover_rate, amplitude,
        ma5, ma10, ma20, ma30, ma60,
        created_at, updated_at
    )
    SELECT 
        trade_date, symbol,
        open_price, high_price, low_price, close_price,
        volume, amount,
        change_amount, pct_change, pre_close_price,
        turnover_rate, amplitude,
        ma5, ma10, ma20, ma30, ma60,
        created_time, updated_time
    FROM stock_daily_data
    ON DUPLICATE KEY UPDATE
        open = VALUES(open),
        high = VALUES(high),
        low = VALUES(low),
        close = VALUES(close)
    """

    try:
        with engine.connect() as conn:
            # 1. 创建标准化表
            print("创建标准化表...")
            conn.execute(text(create_table_sql))

            # 2. 复制数据
            print("复制数据到标准化表...")
            result = conn.execute(text(copy_data_sql))
            rows_affected = result.rowcount

            conn.commit()

            print(f"✅ 创建标准化表完成，复制 {rows_affected} 行数据")

            # 3. 验证
            result = conn.execute(text("SELECT COUNT(*) FROM daily_data_standard"))
            count = result.scalar()
            print(f"📊 标准化表数据量: {count}")

            return True

    except Exception as e:
        print(f"❌ 创建标准化表失败: {e}")
        return False


def update_query_engine_for_standard_tables():
    """更新查询引擎使用标准表"""
    print("\n🔧 更新查询引擎使用标准表")
    print("=" * 60)

    query_engine_file = 'src/query/query_engine.py'

    # 检查是否存在标准化表
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'daily_data_standard'"))
            has_standard_table = result.fetchone() is not None

            if has_standard_table:
                print("✅ 发现标准化表 daily_data_standard")
                target_table = 'daily_data_standard'
            else:
                print("⚠️  未发现标准化表，使用 stock_daily_data")
                target_table = 'stock_daily_data'

    except Exception as e:
        print(f"❌ 检查表失败: {e}")
        target_table = 'stock_daily_data'

    # 创建修复版的查询引擎
    fixed_code = f'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询引擎 - v0.4.0（标准化版）
使用标准列名：open, high, low, close 等
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text
from src.database.connection import engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class QueryEngine:
    """股票数据查询引擎（标准化版）"""

    def __init__(self, config_path: str = None):
        self.engine = engine
        self.logger = get_logger(__name__)
        self.table_name = '{target_table}'  # 自动选择表
        self._init_cache()

    def _init_cache(self):
        self._stock_cache = {{}}
        self._index_cache = {{}}

    def _execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        try:
            if params:
                df = pd.read_sql_query(text(query), self.engine, params=params)
            else:
                df = pd.read_sql_query(text(query), self.engine)
            return df
        except Exception as e:
            self.logger.error(f"执行查询失败: {{e}}")
            return pd.DataFrame()

    def get_daily_data(self,
                      symbol: str = None,
                      start_date: str = None,
                      end_date: str = None,
                      fields: List[str] = None,
                      limit: int = None) -> pd.DataFrame:
        """
        查询日线行情数据（使用标准列名）
        """
        # 默认字段（标准列名）
        if fields is None:
            fields = [
                'trade_date', 'symbol', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'pct_change', 'change',
                'pre_close', 'turnover_rate', 'amplitude'
            ]

        field_str = ', '.join(fields)

        query = f"""
        SELECT {{field_str}}
        FROM {{self.table_name}}
        WHERE 1=1
        """
        params = {{}}

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
            query += f" LIMIT {{limit}}"

        try:
            df = self._execute_query(query, params)

            if not df.empty and 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df = df.sort_values('trade_date')

            self.logger.info(f"查询日线数据，返回{{len(df)}}条记录")
            return df

        except Exception as e:
            self.logger.error(f"查询日线数据失败: {{e}}")
            return pd.DataFrame()

    # 其他方法保持不变...
    def get_stock_basic(self, 
                       symbol: str = None,
                       exchange: str = None,
                       industry: str = None) -> pd.DataFrame:
        query = """
        SELECT 
            symbol, stock_name as name, exchange, industry, 
            listing_date, is_active, created_at
        FROM stock_basic_info
        WHERE 1=1
        """
        params = {{}}

        if symbol:
            query += " AND symbol = :symbol"
            params['symbol'] = symbol
        if exchange:
            query += " AND exchange = :exchange"
            params['exchange'] = exchange
        if industry:
            query += " AND industry LIKE :industry"
            params['industry'] = f"%{{industry}}%"

        query += " ORDER BY symbol"

        try:
            df = self._execute_query(query, params)
            self.logger.info(f"查询股票基本信息，返回{{len(df)}}条记录")
            return df
        except Exception as e:
            self.logger.error(f"查询股票基本信息失败: {{e}}")
            return pd.DataFrame()

    def get_data_statistics(self) -> Dict:
        stats = {{}}

        # 股票数量统计
        stock_query = """
        SELECT 
            COUNT(*) as total_stocks,
            COUNT(DISTINCT industry) as total_industries,
            exchange,
            COUNT(*) as count_by_exchange
        FROM stock_basic_info
        GROUP BY exchange
        """

        # 日线数据统计
        daily_query = f"""
        SELECT 
            COUNT(*) as total_records,
            MIN(trade_date) as earliest_date,
            MAX(trade_date) as latest_date,
            COUNT(DISTINCT symbol) as stocks_with_data
        FROM {{self.table_name}}
        """

        try:
            stock_stats = pd.read_sql_query(text(stock_query), self.engine)
            daily_stats = pd.read_sql_query(text(daily_query), self.engine)

            stats['stock_basic'] = {{
                'total_stocks': int(stock_stats['total_stocks'].iloc[0]) if len(stock_stats) > 0 else 0,
                'total_industries': int(stock_stats['total_industries'].iloc[0]) if len(stock_stats) > 0 else 0,
                'exchange_distribution': stock_stats.set_index('exchange')['count_by_exchange'].to_dict()
            }}

            stats['daily_data'] = {{
                'total_records': int(daily_stats['total_records'].iloc[0]) if len(daily_stats) > 0 else 0,
                'earliest_date': str(daily_stats['earliest_date'].iloc[0]) if len(daily_stats) > 0 else None,
                'latest_date': str(daily_stats['latest_date'].iloc[0]) if len(daily_stats) > 0 else None,
                'stocks_with_data': int(daily_stats['stocks_with_data'].iloc[0]) if len(daily_stats) > 0 else 0
            }}

            self.logger.info("数据统计查询成功")
            return stats

        except Exception as e:
            self.logger.error(f"数据统计查询失败: {{e}}")
            return {{}}

    def get_stock_list(self) -> List[str]:
        query = "SELECT symbol FROM stock_basic_info ORDER BY symbol"
        try:
            df = pd.read_sql_query(text(query), self.engine)
            return df['symbol'].tolist()
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {{e}}")
            return []

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("数据库连接测试成功")
                return True
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {{e}}")
            return False

    def close(self):
        pass

if __name__ == "__main__":
    engine = QueryEngine()
    stats = engine.get_data_statistics()
    print(f"数据统计: {{stats}}")
    engine.close()
'''

    # 备份原文件
    import shutil
    shutil.copy2(query_engine_file, query_engine_file + '.backup_standard')

    # 写入新文件
    with open(query_engine_file, 'w', encoding='utf-8') as f:
        f.write(fixed_code)

    print(f"✅ 查询引擎已更新，使用表: {target_table}")

    # 测试新引擎
    print("\n🧪 测试新查询引擎...")
    try:
        exec(fixed_code.replace('if __name__ == "__main__":', 'if True:'))
        print("✅ 新引擎测试通过")
    except Exception as e:
        print(f"❌ 新引擎测试失败: {e}")

    return True


def main():
    """主函数"""
    print("🔧 股票数据库标准化修复")
    print("=" * 60)

    print("请选择修复方案:")
    print("1. 重命名现有表的列（推荐）")
    print("2. 创建新的标准化表并复制数据")
    print("3. 仅更新查询引擎适配现有表")
    print("4. 全部执行")

    choice = input("\n请输入选择 (1-4): ").strip()

    if choice == '1':
        rename_columns_in_daily_table()
        update_query_engine_for_standard_tables()
    elif choice == '2':
        create_standardized_table()
        update_query_engine_for_standard_tables()
    elif choice == '3':
        update_query_engine_for_standard_tables()
    elif choice == '4':
        rename_columns_in_daily_table()
        create_standardized_table()
        update_query_engine_for_standard_tables()
    else:
        print("❌ 无效选择")
        return

    print("\n" + "=" * 60)
    print("📋 标准化修复完成!")
    print("\n🎉 现在可以测试:")
    print("  python main.py --action p4_query_test")
    print("  python main.py --action validate")


if __name__ == "__main__":
    main()