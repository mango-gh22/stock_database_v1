# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\rollback_table_schema.py
# File Name: rollback_table_schema
# @ Author: mango-gh22
# @ Date：2026/1/11 9:35
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回退表结构：删除新增的业务时间字段
"""

import sys
import os

# 将项目根目录添加到 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def rollback_schema():
    """回退表结构到原始状态"""
    db = DatabaseConnector()

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:

                # 检查并删除新增字段（如果存在）
                fields_to_remove = ['listing_date', 'data_start_date', 'data_end_date']

                for field in fields_to_remove:
                    try:
                        cursor.execute(f"ALTER TABLE stock_daily_data DROP COLUMN IF EXISTS {field}")
                        logger.info(f"✅ 删除字段: {field}")
                    except Exception as e:
                        logger.warning(f"删除字段失败 {field}: {e}")

                # 验证回退后的表结构
                cursor.execute("DESCRIBE stock_daily_data")
                columns = [col[0] for col in cursor.fetchall()]

                logger.info(f"回退后表字段数: {len(columns)}")
                logger.info(f"字段列表: {columns}")

                conn.commit()
                logger.info("✅ 表结构回退完成")

                return True

    except Exception as e:
        logger.error(f"回退表结构失败: {e}", exc_info=True)
        return False


def verify_data_integrity():
    """验证数据完整性"""
    db = DatabaseConnector()

    with db.get_connection() as conn:
        import pandas as pd

        # 检查数据是否完整
        cursor = conn.cursor()

        # 检查关键字段是否存在
        cursor.execute("DESCRIBE stock_daily_data")
        columns = [col[0] for col in cursor.fetchall()]

        required_fields = ['symbol', 'trade_date', 'pb', 'pe_ttm', 'close_price']
        missing_fields = [f for f in required_fields if f not in columns]

        if missing_fields:
            logger.error(f"❌ 缺少关键字段: {missing_fields}")
            return False

        logger.info("✅ 所有关键字段存在")

        # 检查是否有数据错乱（简单验证）
        cursor.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN pb > 0 AND pb < 100 THEN 1 ELSE 0 END) as valid_pb,
                   SUM(CASE WHEN close_price > 0 THEN 1 ELSE 0 END) as valid_price
            FROM stock_daily_data
        """)

        result = cursor.fetchone()
        total = result[0]
        valid_pb = result[1]
        valid_price = result[2]

        logger.info(f"总记录数: {total:,}")
        logger.info(f"有效PB记录: {valid_pb:,} ({valid_pb / total * 100:.1f}%)")
        logger.info(f"有效价格记录: {valid_price:,} ({valid_price / total * 100:.1f}%)")

        if valid_pb < total * 0.5:
            logger.warning("⚠️  PB数据异常，可能存在错位")
            return False

        if valid_price < total * 0.5:
            logger.warning("⚠️  价格数据异常，可能存在错位")
            return False

        logger.info("✅ 数据完整性验证通过")
        return True


if __name__ == "__main__":
    print("⚠️  此操作将删除新增字段，请在执行前备份数据库！")
    confirm = input("确定回退表结构吗？(yes/no): ")

    if confirm.lower() == 'yes':
        success = rollback_schema()
        if success:
            print("\n📊 验证数据完整性...")
            verify_data_integrity()
        exit(0 if success else 1)
    else:
        print("操作已取消")
        exit(0)