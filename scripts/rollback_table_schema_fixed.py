# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\rollback_table_schema_fixed.py
# File Name: rollback_table_schema_fixed
# @ Author: mango-gh22
# @ Date：2026/1/11 10:06
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版：回退表结构 - 兼容 MySQL 语法
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def rollback_schema():
    """回退表结构到原始状态，删除新增字段"""
    db = DatabaseConnector()

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:

                # 需要删除的新增字段
                fields_to_remove = ['listing_date', 'data_start_date', 'data_end_date']

                for field in fields_to_remove:
                    try:
                        # MySQL 不支持 IF EXISTS，直接删除
                        cursor.execute(f"ALTER TABLE stock_daily_data DROP COLUMN {field}")
                        logger.info(f"✅ 成功删除字段: {field}")
                    except Exception as e:
                        # 字段不存在或其他错误，记录但不中断
                        logger.warning(f"删除字段 {field} 失败: {e}")

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


if __name__ == "__main__":
    print("⚠️  此操作将删除新增字段，请在执行前备份数据库！")
    confirm = input("确定回退表结构吗？(yes/no): ")

    if confirm.lower() == 'yes':
        success = rollback_schema()
        exit(0 if success else 1)
    else:
        print("操作已取消")
        exit(0)