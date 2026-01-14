# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\update_daily_table_full.py
# File Name: update_daily_table_full
# @ Author: mango-gh22
# @ Date：2025/12/12 20:07
"""
desc 动态添加缺失列（逐步更新）
python -m scripts.update_daily_table_full
完整更新stock_daily_data表结构
"""
from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def update_daily_table_full():
    """完整更新日线数据表结构"""
    db = DatabaseConnector()

    # 所有需要的列定义（按类别分组）
    required_columns = {
        # 基础价格数据
        'price': [
            ("open_price", "DECIMAL(10,4) COMMENT '开盘价'"),
            ("high_price", "DECIMAL(10,4) COMMENT '最高价'"),
            ("low_price", "DECIMAL(10,4) COMMENT '最低价'"),
            ("close_price", "DECIMAL(10,4) COMMENT '收盘价'"),
            ("pre_close_price", "DECIMAL(10,4) COMMENT '前收盘价'"),
            ("change_percent", "DECIMAL(10,4) COMMENT '涨跌幅(%)'"),
            ("change", "DECIMAL(10,4) COMMENT '涨跌额'"),
            ("volume", "BIGINT COMMENT '成交量(手)'"),
            ("amount", "DECIMAL(20,4) COMMENT '成交额(万元)'"),
            ("amplitude", "DECIMAL(10,4) COMMENT '振幅(%)'"),
        ],

        # 技术指标
        'technical': [
            ("ma5", "DECIMAL(10,4) COMMENT '5日均线'"),
            ("ma10", "DECIMAL(10,4) COMMENT '10日均线'"),
            ("ma20", "DECIMAL(10,4) COMMENT '20日均线'"),
            ("ma30", "DECIMAL(10,4) COMMENT '30日均线'"),
            ("ma60", "DECIMAL(10,4) COMMENT '60日均线'"),
            ("ma120", "DECIMAL(10,4) COMMENT '120日均线'"),
            ("ma250", "DECIMAL(10,4) COMMENT '250日均线'"),
            ("volume_ma5", "BIGINT COMMENT '5日成交量均线'"),
            ("volume_ma10", "BIGINT COMMENT '10日成交量均线'"),
            ("volume_ma20", "BIGINT COMMENT '20日成交量均线'"),
        ],

        # 高级指标
        'advanced': [
            ("rsi", "DECIMAL(10,4) COMMENT 'RSI相对强弱指标'"),
            ("bb_middle", "DECIMAL(10,4) COMMENT '布林带中轨'"),
            ("bb_upper", "DECIMAL(10,4) COMMENT '布林带上轨'"),
            ("bb_lower", "DECIMAL(10,4) COMMENT '布林带下轨'"),
            ("volatility_20d", "DECIMAL(10,4) COMMENT '20日波动率'"),
        ],

        # 财务指标
        'financial': [
            ("turnover_rate", "DECIMAL(10,4) COMMENT '换手率(%)'"),
            ("turnover_rate_f", "DECIMAL(10,4) COMMENT '换手率(自由流通股)'"),
            ("volume_ratio", "DECIMAL(10,4) COMMENT '量比'"),
            ("pe", "DECIMAL(10,4) COMMENT '市盈率'"),
            ("pe_ttm", "DECIMAL(10,4) COMMENT '市盈率(TTM)'"),
            ("pb", "DECIMAL(10,4) COMMENT '市净率'"),
            ("ps", "DECIMAL(10,4) COMMENT '市销率'"),
            ("ps_ttm", "DECIMAL(10,4) COMMENT '市销率(TTM)'"),
            ("dv_ratio", "DECIMAL(10,4) COMMENT '股息率'"),
            ("dv_ttm", "DECIMAL(10,4) COMMENT '股息率(TTM)'"),
        ],

        # 市值数据
        'market_value': [
            ("total_share", "DECIMAL(20,4) COMMENT '总股本'"),
            ("float_share", "DECIMAL(20,4) COMMENT '流通股本'"),
            ("free_share", "DECIMAL(20,4) COMMENT '自由流通股本'"),
            ("total_mv", "DECIMAL(20,4) COMMENT '总市值'"),
            ("circ_mv", "DECIMAL(20,4) COMMENT '流通市值'"),
        ],

        # 元数据
        'metadata': [
            ("data_source", "VARCHAR(50) DEFAULT 'baostock' COMMENT '数据源'"),
            ("processed_time", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '处理时间'"),
            ("quality_grade", "VARCHAR(20) DEFAULT 'unknown' COMMENT '质量等级'"),
            ("created_time", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'"),
            ("updated_time", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'"),
        ]
    }

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 检查表是否存在
                cursor.execute("SHOW TABLES LIKE 'stock_daily_data'")
                if not cursor.fetchone():
                    logger.error("表 stock_daily_data 不存在，请先创建表")
                    return False

                # 检查现有列
                cursor.execute("DESCRIBE stock_daily_data")
                existing_columns = {col[0] for col in cursor.fetchall()}

                added_count = 0

                # 按类别添加缺失列
                for category, columns in required_columns.items():
                    logger.info(f"检查 {category} 类别列...")
                    for column_name, column_def in columns:
                        if column_name not in existing_columns:
                            try:
                                alter_sql = f"ALTER TABLE stock_daily_data ADD COLUMN {column_name} {column_def}"
                                cursor.execute(alter_sql)
                                added_count += 1
                                logger.info(f"✅ 添加列: {column_name}")
                            except Exception as e:
                                logger.warning(f"添加列失败 {column_name}: {e}")
                        else:
                            logger.debug(f"⏩ 列已存在: {column_name}")

                # 创建索引（如果不存在）
                try:
                    # 检查唯一索引是否存在
                    cursor.execute("SHOW INDEX FROM stock_daily_data WHERE Key_name = 'idx_unique_symbol_date'")
                    if not cursor.fetchone():
                        cursor.execute(
                            "ALTER TABLE stock_daily_data ADD UNIQUE INDEX idx_unique_symbol_date (symbol, trade_date)")
                        logger.info("✅ 添加唯一索引: idx_unique_symbol_date")
                except Exception as e:
                    logger.warning(f"创建索引失败: {e}")

                conn.commit()

                if added_count > 0:
                    logger.info(f"✅ 表结构更新完成，新增{added_count}列")
                else:
                    logger.info("✅ 所有列已存在，无需更新")

                return True

    except Exception as e:
        logger.error(f"更新表结构失败: {e}")
        return False


if __name__ == "__main__":
    success = update_daily_table_full()
    exit(0 if success else 1)