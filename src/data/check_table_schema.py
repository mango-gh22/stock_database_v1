# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\check_table_schema.py
# File Name: check_table_schema
# @ Author: mango-gh22
# @ Date：2025/12/10 21:53
"""
desc 
"""
# src/data/check_table_schema.py
"""
检查数据库表结构
"""

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def check_table_schema():
    """检查表结构"""
    try:
        db = DatabaseConnector()

        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 查看 stock_daily_data 表结构
                cursor.execute("DESCRIBE stock_daily_data")
                columns = cursor.fetchall()

                print("📊 stock_daily_data 表结构:")
                print("-" * 80)
                print(f"{'字段名':<20} {'类型':<20} {'允许空':<10} {'键':<10} {'默认值':<15} {'额外':<10}")
                print("-" * 80)

                for col in columns:
                    print(f"{col[0]:<20} {col[1]:<20} {col[2]:<10} {col[3]:<10} {str(col[4]):<15} {col[5]:<10}")

                # 列出所有列名
                column_names = [col[0] for col in columns]
                print(f"\n🔢 总列数: {len(column_names)}")
                print(f"📋 列名列表: {column_names}")

                return column_names

    except Exception as e:
        logger.error(f"检查表结构失败: {e}")
        return []


if __name__ == "__main__":
    check_table_schema()