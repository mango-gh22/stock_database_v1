# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\rellback_daily_table_full.py
# File Name: rellback_daily_table_full
# @ Author: mango-gh22
# @ Date：2025/12/12 20:27
"""
desc 删除scripts/update_daily_table_full.py添加的列（推荐）
"""

# scripts/rollback_daily_table_full.py
from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def rollback_daily_table_full():
    """回退日线数据表结构更新"""
    db = DatabaseConnector()

    # 脚本中添加的所有列名
    added_columns = [
        # 基础价格数据
        'open_price', 'high_price', 'low_price', 'close_price',
        'pre_close_price', 'change_percent', 'change', 'volume',
        'amount', 'amplitude',

        # 技术指标
        'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
        'volume_ma5', 'volume_ma10', 'volume_ma20',

        # 高级指标
        'rsi', 'bb_middle', 'bb_upper', 'bb_lower', 'volatility_20d',

        # 财务指标
        'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe',
        'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',

        # 市值数据
        'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',

        # 元数据
        'data_source', 'processed_time', 'quality_grade',
        'created_time', 'updated_time'
    ]

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 检查表是否存在
                cursor.execute("SHOW TABLES LIKE 'stock_daily_data'")
                if not cursor.fetchone():
                    logger.error("表 stock_daily_data 不存在")
                    return False

                # 获取现有列（排除基础列）
                cursor.execute("DESCRIBE stock_daily_data")
                existing_columns = {col[0] for col in cursor.fetchall()}

                # 基础列（不能删除的）
                base_columns = {'id', 'symbol', 'trade_date', 'ts_code'}

                removed_count = 0

                # 删除脚本添加的列
                for column_name in added_columns:
                    if column_name in existing_columns and column_name not in base_columns:
                        try:
                            alter_sql = f"ALTER TABLE stock_daily_data DROP COLUMN {column_name}"
                            cursor.execute(alter_sql)
                            removed_count += 1
                            logger.info(f"✅ 删除列: {column_name}")
                        except Exception as e:
                            logger.warning(f"删除列失败 {column_name}: {e}")

                # 删除索引（如果需要）
                try:
                    cursor.execute("SHOW INDEX FROM stock_daily_data WHERE Key_name = 'idx_unique_symbol_date'")
                    if cursor.fetchone():
                        cursor.execute("ALTER TABLE stock_daily_data DROP INDEX idx_unique_symbol_date")
                        logger.info("✅ 删除索引: idx_unique_symbol_date")
                except Exception as e:
                    logger.warning(f"删除索引失败: {e}")

                conn.commit()

                if removed_count > 0:
                    logger.info(f"✅ 表结构回退完成，删除{removed_count}列")
                else:
                    logger.info("✅ 未发现需要删除的列")

                return True

    except Exception as e:
        logger.error(f"回退表结构失败: {e}")
        return False


if __name__ == "__main__":
    success = rollback_daily_table_full()
    exit(0 if success else 1)