# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\update_table_schema.py
# File Name: update_table_schema
# @ Author: mango-gh22
# @ Date：2025/12/10 22:03
"""
desc 
"""
# scripts/update_table_schema.py
"""
更新数据库表结构，添加缺失的列
"""

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def update_table_schema():
    """更新表结构"""
    db = DatabaseConnector()

    # 需要添加的列定义
    additional_columns = [
        "data_source VARCHAR(50) DEFAULT 'baostock' COMMENT '数据源'",
        "processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '处理时间'",
        "quality_grade VARCHAR(20) DEFAULT 'unknown' COMMENT '质量等级'",
        "created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'",
        "updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'"
    ]

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 检查并添加缺失的列
                cursor.execute("DESCRIBE stock_daily_data")
                existing_columns = [col[0] for col in cursor.fetchall()]

                for column_def in additional_columns:
                    column_name = column_def.split()[0]

                    if column_name not in existing_columns:
                        try:
                            alter_sql = f"ALTER TABLE stock_daily_data ADD COLUMN {column_def}"
                            cursor.execute(alter_sql)
                            logger.info(f"✅ 添加列: {column_name}")
                        except Exception as e:
                            logger.warning(f"添加列失败 {column_name}: {e}")
                    else:
                        logger.debug(f"⏩ 列已存在: {column_name}")

                conn.commit()
                logger.info("✅ 表结构更新完成")

    except Exception as e:
        logger.error(f"更新表结构失败: {e}")


if __name__ == "__main__":
    update_table_schema()