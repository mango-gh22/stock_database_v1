# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\fix_column_mapping.py
# File Name: fix_column_mapping
# @ Author: mango-gh22
# @ Date：2025/12/12 21:50
"""
desc 检查和更新列名映射
"""

# scripts/fix_column_mapping.py
"""
修复列名映射问题 - 确保EnhancedDataProcessor的输出列名与数据库表列名匹配
"""
import pandas as pd
from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_database_columns(table_name: str = 'stock_daily_data') -> list:
    """获取数据库表的实际列名"""
    db = DatabaseConnector()
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                columns = [col[0] for col in cursor.fetchall()]
                logger.info(f"数据库表 {table_name} 有 {len(columns)} 列")
                return columns
    except Exception as e:
        logger.error(f"获取数据库列失败: {e}")
        return []


def check_column_mapping():
    """检查并修复列名映射"""
    # 1. 获取数据库实际列名
    db_columns = get_database_columns()

    if not db_columns:
        logger.error("无法获取数据库列，请检查数据库连接")
        return

    logger.info("=" * 60)
    logger.info("数据库实际列名（前20个）：")
    for col in db_columns[:20]:
        logger.info(f"  - {col}")

    # 2. EnhancedDataProcessor 生成的典型列名
    processor_columns = [
        'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'pct_change', 'change', 'amplitude',
        'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
        'volume_ma5', 'volume_ma10', 'volume_ma20',
        'rsi', 'bb_middle', 'bb_upper', 'bb_lower', 'volatility_20d',
        'trade_date', 'symbol', 'data_source', 'processed_time', 'quality_grade'
    ]

    # 3. DataStorage 中的列映射
    storage_mapping = {
        'open': 'open_price',
        'high': 'high_price',
        'low': 'low_price',
        'close': 'close_price',
        'pre_close': 'pre_close_price',
        'change': 'change',
        'pct_change': 'change_percent',
        'volume': 'volume',
        'amount': 'amount',
        'amplitude': 'amplitude',
        'turnover_rate': 'turnover_rate',
        'turnover_rate_f': 'turnover_rate_f',
        'volume_ratio': 'volume_ratio',
        'pe': 'pe',
        'pe_ttm': 'pe_ttm',
        'pb': 'pb',
        'ps': 'ps',
        'ps_ttm': 'ps_ttm',
        'dv_ratio': 'dv_ratio',
        'dv_ttm': 'dv_ttm',
        'total_share': 'total_share',
        'float_share': 'float_share',
        'free_share': 'free_share',
        'total_mv': 'total_mv',
        'circ_mv': 'circ_mv'
    }

    logger.info("\n" + "=" * 60)
    logger.info("检查列名映射：")

    # 检查映射是否完整
    for src_col in processor_columns:
        if src_col in storage_mapping:
            target_col = storage_mapping[src_col]
            if target_col in db_columns:
                logger.info(f"✅ {src_col} -> {target_col} (数据库中存在)")
            else:
                logger.warning(f"❌ {src_col} -> {target_col} (数据库中不存在!)")
        else:
            if src_col in ['trade_date', 'symbol', 'data_source', 'processed_time', 'quality_grade']:
                if src_col in db_columns:
                    logger.info(f"✅ {src_col} (直接使用，存在于数据库)")
                else:
                    logger.warning(f"❌ {src_col} (直接使用，但数据库中不存在!)")
            elif src_col in db_columns:
                logger.info(f"⚠️ {src_col} (无映射，但直接存在于数据库)")
            else:
                logger.error(f"❌ {src_col} (无映射且数据库中不存在!)")

    # 4. 建议修复方案
    logger.info("\n" + "=" * 60)
    logger.info("修复建议：")

    # 方案1: 临时修复 - 添加缺失的列到数据库
    missing_in_db = []
    for src_col in processor_columns:
        target_col = storage_mapping.get(src_col, src_col)
        if target_col not in db_columns and src_col not in ['trade_date', 'symbol', 'data_source', 'processed_time',
                                                            'quality_grade']:
            missing_in_db.append(target_col)

    if missing_in_db:
        logger.warning(f"数据库中缺失 {len(missing_in_db)} 列:")
        for col in missing_in_db:
            logger.warning(f"  - {col}")

    # 方案2: 修复DataStorage的_preprocess_data方法
    logger.info("\n修复DataStorage的_preprocess_data方法：")
    logger.info("在_preprocess_data方法中，确保重命名生效")

    return True


def create_fix_sql():
    """创建修复SQL，添加缺失的列或重命名列"""
    db = DatabaseConnector()

    # 需要添加或检查的列
    columns_to_add = [
        ("open", "DECIMAL(10,4) COMMENT '开盘价(临时兼容)'"),
        ("high", "DECIMAL(10,4) COMMENT '最高价(临时兼容)'"),
        ("low", "DECIMAL(10,4) COMMENT '最低价(临时兼容)'"),
        ("close", "DECIMAL(10,4) COMMENT '收盘价(临时兼容)'"),
        ("pre_close", "DECIMAL(10,4) COMMENT '前收盘价(临时兼容)'"),
        ("pct_change", "DECIMAL(10,4) COMMENT '涨跌幅(%)(临时兼容)'"),
    ]

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # 获取现有列
                cursor.execute("DESCRIBE stock_daily_data")
                existing_columns = {col[0] for col in cursor.fetchall()}

                added = 0
                for col_name, col_def in columns_to_add:
                    if col_name not in existing_columns:
                        try:
                            alter_sql = f"ALTER TABLE stock_daily_data ADD COLUMN {col_name} {col_def}"
                            cursor.execute(alter_sql)
                            added += 1
                            logger.info(f"✅ 添加兼容列: {col_name}")
                        except Exception as e:
                            logger.warning(f"添加兼容列失败 {col_name}: {e}")

                conn.commit()

                if added > 0:
                    logger.info(f"✅ 添加了 {added} 个兼容列")
                else:
                    logger.info("⏩ 所有兼容列已存在")

                return True

    except Exception as e:
        logger.error(f"创建修复SQL失败: {e}")
        return False


def test_column_mapping():
    """测试列名映射是否正确"""
    from src.data.enhanced_processor import create_test_data
    from src.data.enhanced_processor import EnhancedDataProcessor

    logger.info("=" * 60)
    logger.info("测试列名映射...")

    try:
        # 1. 创建测试数据
        test_data = create_test_data()
        logger.info(f"测试数据列名: {list(test_data.columns)}")

        # 2. 处理数据
        processor = EnhancedDataProcessor()
        symbol = 'sh600519'

        df_processed, quality_report = processor.process_stock_data(
            test_data, symbol, 'test'
        )

        logger.info(f"处理后的列名: {list(df_processed.columns)}")

        # 3. 检查哪些列需要映射
        mapping_needed = []
        direct_columns = []

        for col in df_processed.columns:
            if col in ['open', 'high', 'low', 'close', 'pre_close', 'pct_change']:
                mapping_needed.append(col)
            elif col in ['trade_date', 'symbol', 'data_source', 'processed_time', 'quality_grade']:
                direct_columns.append(col)

        if mapping_needed:
            logger.warning(f"需要映射的列: {mapping_needed}")
        if direct_columns:
            logger.info(f"直接使用的列: {direct_columns}")

        return True

    except Exception as e:
        logger.error(f"测试失败: {e}")
        return False


if __name__ == "__main__":
    print("🔧 检查并修复列名映射问题")
    print("=" * 60)

    # 1. 检查列映射
    check_column_mapping()

    print("\n" + "=" * 60)
    user_input = input("是否要添加兼容列到数据库? (y/n): ")
    if user_input.lower() == 'y':
        create_fix_sql()

    print("\n" + "=" * 60)
    user_input = input("是否要测试列名映射? (y/n): ")
    if user_input.lower() == 'y':
        test_column_mapping()

    print("\n" + "=" * 60)
    print("💡 根本解决方案:")
    print("1. 修改 EnhancedDataProcessor.prepare_for_storage() 方法")
    print("2. 确保列名映射正确应用到 DataFrame")
    print("3. 或者在 DataStorage 中修复列名映射")