# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\update_table_schema_final.py
# File Name: update_table_schema_final
# @ Author: mango-gh22
# @ Date：2026/1/11 9:16
"""
desc 修正时间字段语义的表结构更新
"""

import sys
import os

# 将项目根目录添加到 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def update_schema_semantics():
    """修正时间字段语义"""
    db = DatabaseConnector()

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 添加业务时间字段
                logger.info("添加业务时间字段...")

                # 数据首次上市日期
                cursor.execute("""
                    ALTER TABLE stock_daily_data 
                    ADD COLUMN listing_date DATE COMMENT '股票上市日期' 
                    AFTER symbol
                """)

                # 数据实际开始日期（数据库中最早的数据）
                cursor.execute("""
                    ALTER TABLE stock_daily_data 
                    ADD COLUMN data_start_date DATE COMMENT '数据起始日期' 
                    AFTER updated_time
                """)

                # 数据实际结束日期（数据库中最新数据）
                cursor.execute("""
                    ALTER TABLE stock_daily_data 
                    ADD COLUMN data_end_date DATE COMMENT '数据截止日期' 
                    AFTER data_start_date
                """)

                # 2. 重命名操作时间字段（可选，保留原有字段）
                logger.info("更新元数据字段...")

                # 3. 更新业务时间数据
                logger.info("初始化业务时间数据...")

                # 为每只股票计算 data_start_date 和 data_end_date
                cursor.execute("""
                    UPDATE stock_daily_data s
                    INNER JOIN (
                        SELECT symbol, 
                               MIN(trade_date) as min_date,
                               MAX(trade_date) as max_date
                        FROM stock_daily_data
                        GROUP BY symbol
                    ) t ON s.symbol = t.symbol
                    SET s.data_start_date = t.min_date,
                        s.data_end_date = t.max_date
                """)

                conn.commit()
                logger.info("✅ 表结构语义修正完成")

                # 4. 显示统计
                cursor.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN data_start_date IS NOT NULL THEN 1 ELSE 0 END) as initialized
                    FROM stock_daily_data
                """)

                result = cursor.fetchone()
                logger.info(f"总记录: {result[0]}, 已初始化业务时间: {result[1]}")

                return True

    except Exception as e:
        logger.error(f"更新表结构失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    print("此操作将修改表结构，建议在执行前备份数据库！")
    confirm = input("继续吗？(yes/no): ")

    if confirm.lower() == 'yes':
        success = update_schema_semantics()
        exit(0 if success else 1)
    else:
        print("操作已取消")
        exit(0)