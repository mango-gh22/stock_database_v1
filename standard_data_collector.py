# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\standard_data_collector.py
# File Name: standard_data_collector
# @ Author: mango-gh22
# @ Date：2025/12/6 19:00
"""
desc 数据采集器的标准化版本
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标准化数据采集器 - 确保使用标准列名
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import get_session
from sqlalchemy import text
from src.utils.logger import get_logger
import pandas as pd
from datetime import datetime

logger = get_logger(__name__)


class StandardDataCollector:
    """标准化数据采集器"""

    def __init__(self):
        self.session = get_session()
        self.logger = get_logger(__name__)

        # 标准列名定义
        self.standard_columns = {
            'trade_date': 'DATE',
            'symbol': 'VARCHAR(20)',
            'open': 'DECIMAL(10,4)',
            'high': 'DECIMAL(10,4)',
            'low': 'DECIMAL(10,4)',
            'close': 'DECIMAL(10,4)',
            'volume': 'BIGINT',
            'amount': 'DECIMAL(20,4)',
            'pct_change': 'DECIMAL(10,4)',
            'change': 'DECIMAL(10,4)',
            'pre_close': 'DECIMAL(10,4)',
            'turnover_rate': 'DECIMAL(10,4)',
            'amplitude': 'DECIMAL(10,4)',
            'created_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP'
        }

    def ensure_standard_table(self):
        """确保存在标准化的表"""
        try:
            # 检查是否存在标准化表
            result = self.session.execute(text("SHOW TABLES LIKE 'daily_data_standard'"))
            if result.fetchone():
                self.logger.info("标准化表已存在")
                return True

            # 创建标准化表
            self.logger.info("创建标准化表...")

            columns_sql = []
            for col_name, col_type in self.standard_columns.items():
                if col_name == 'trade_date':
                    columns_sql.append(f"{col_name} {col_type} NOT NULL COMMENT '交易日期'")
                elif col_name == 'symbol':
                    columns_sql.append(f"{col_name} {col_type} NOT NULL COMMENT '股票代码'")
                elif col_name == 'created_at':
                    columns_sql.append(f"{col_name} TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                elif col_name == 'updated_at':
                    columns_sql.append(f"{col_name} TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
                else:
                    columns_sql.append(f"{col_name} {col_type} COMMENT ''")

            create_sql = f"""
            CREATE TABLE daily_data_standard (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns_sql)},
                UNIQUE KEY uk_date_symbol (trade_date, symbol),
                INDEX idx_symbol (symbol),
                INDEX idx_trade_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='标准日线行情数据表'
            """

            self.session.execute(text(create_sql))
            self.session.commit()

            self.logger.info("✅ 标准化表创建成功")
            return True

        except Exception as e:
            self.logger.error(f"创建标准化表失败: {e}")
            self.session.rollback()
            return False

    def insert_standard_data(self, data_dict):
        """插入标准化数据"""
        try:
            # 确保表存在
            self.ensure_standard_table()

            # 准备数据
            trade_date = data_dict.get('trade_date')
            symbol = data_dict.get('symbol')

            if not trade_date or not symbol:
                self.logger.error("缺少必要字段: trade_date 或 symbol")
                return False

            # 构建INSERT语句
            columns = []
            values = []
            params = {}

            for col in self.standard_columns.keys():
                if col in ['created_at', 'updated_at']:
                    continue  # 数据库自动处理

                if col in data_dict:
                    columns.append(col)
                    values.append(f":{col}")
                    params[col] = data_dict[col]

            if not columns:
                self.logger.error("没有有效数据可插入")
                return False

            insert_sql = f"""
            INSERT INTO daily_data_standard ({', '.join(columns)})
            VALUES ({', '.join(values)})
            ON DUPLICATE KEY UPDATE
                {', '.join([f"{col}=VALUES({col})" for col in columns if col not in ['trade_date', 'symbol']])}
            """

            self.session.execute(text(insert_sql), params)
            self.session.commit()

            self.logger.info(f"✅ 插入数据: {symbol} {trade_date}")
            return True

        except Exception as e:
            self.logger.error(f"插入数据失败: {e}")
            self.session.rollback()
            return False

    def migrate_existing_data(self):
        """迁移现有数据到标准化表"""
        try:
            # 检查是否存在非标准表
            result = self.session.execute(text("SHOW TABLES LIKE 'stock_daily_data'"))
            if not result.fetchone():
                self.logger.warning("非标准表不存在，无需迁移")
                return True

            self.logger.info("开始迁移现有数据到标准化表...")

            # 确保标准化表存在
            self.ensure_standard_table()

            # 迁移数据
            migrate_sql = """
            INSERT INTO daily_data_standard (
                trade_date, symbol, 
                open, high, low, close,
                volume, amount, 
                pct_change, change, pre_close,
                turnover_rate, amplitude,
                ma5, ma10, ma20, ma30, ma60,
                created_at, updated_at
            )
            SELECT 
                trade_date, symbol,
                CASE 
                    WHEN COLUMN_NAME = 'open_price' THEN open_price
                    WHEN COLUMN_NAME = 'open' THEN open
                    ELSE NULL
                END as open,
                CASE 
                    WHEN COLUMN_NAME = 'high_price' THEN high_price
                    WHEN COLUMN_NAME = 'high' THEN high
                    ELSE NULL
                END as high,
                CASE 
                    WHEN COLUMN_NAME = 'low_price' THEN low_price
                    WHEN COLUMN_NAME = 'low' THEN low
                    ELSE NULL
                END as low,
                CASE 
                    WHEN COLUMN_NAME = 'close_price' THEN close_price
                    WHEN COLUMN_NAME = 'close' THEN close
                    ELSE NULL
                END as close,
                volume, amount,
                pct_change,
                CASE 
                    WHEN COLUMN_NAME = 'change_amount' THEN change_amount
                    WHEN COLUMN_NAME = 'change' THEN change
                    ELSE NULL
                END as change,
                CASE 
                    WHEN COLUMN_NAME = 'pre_close_price' THEN pre_close_price
                    WHEN COLUMN_NAME = 'pre_close' THEN pre_close
                    ELSE NULL
                END as pre_close,
                turnover_rate, amplitude,
                ma5, ma10, ma20, ma30, ma60,
                CASE 
                    WHEN COLUMN_NAME = 'created_time' THEN created_time
                    ELSE CURRENT_TIMESTAMP
                END as created_at,
                CASE 
                    WHEN COLUMN_NAME = 'updated_time' THEN updated_time
                    ELSE CURRENT_TIMESTAMP
                END as updated_at
            FROM stock_daily_data,
            (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_NAME = 'stock_daily_data' AND TABLE_SCHEMA = DATABASE()) as cols
            ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                updated_at = CURRENT_TIMESTAMP
            """

            result = self.session.execute(text(migrate_sql))
            rows_migrated = result.rowcount

            self.session.commit()

            self.logger.info(f"✅ 迁移完成: {rows_migrated} 行数据")

            # 验证迁移结果
            result = self.session.execute(text("SELECT COUNT(*) FROM daily_data_standard"))
            count = result.scalar()
            self.logger.info(f"📊 标准化表数据量: {count}")

            return True

        except Exception as e:
            self.logger.error(f"迁移数据失败: {e}")
            self.session.rollback()
            return False

    def close(self):
        """关闭会话"""
        self.session.close()


def main():
    """主函数"""
    print("📡 标准化数据采集器")
    print("=" * 60)

    collector = StandardDataCollector()

    try:
        print("1️⃣ 检查标准化表...")
        collector.ensure_standard_table()

        print("\n2️⃣ 检查现有数据...")
        result = collector.session.execute(text("SELECT COUNT(*) FROM stock_daily_data"))
        old_count = result.scalar()
        print(f"   非标准表数据: {old_count} 行")

        result = collector.session.execute(text("SELECT COUNT(*) FROM daily_data_standard"))
        new_count = result.scalar()
        print(f"   标准化表数据: {new_count} 行")

        if old_count > 0 and new_count == 0:
            choice = input(f"\n是否迁移 {old_count} 行数据到标准化表？(y/n): ")
            if choice.lower() == 'y':
                print("\n3️⃣ 迁移数据...")
                collector.migrate_existing_data()

        print("\n✅ 标准化设置完成")

    finally:
        collector.close()


if __name__ == "__main__":
    main()