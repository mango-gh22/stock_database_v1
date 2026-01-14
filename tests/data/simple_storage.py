# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\simple_storage.py
# File Name: simple_storage
# @ Author: mango-gh22
# @ Date：2025/12/10 22:04
"""
desc 简化版数据存储器 - 只插入表中已有的列
"""

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Dict

logger = get_logger(__name__)


class SimpleDataStorage:
    """简化版数据存储器 - 只处理表中已有的列"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        self.db_connector = DatabaseConnector(config_path)
        self._load_table_columns()
        logger.info("简化版数据存储器初始化完成")

    def _load_table_columns(self):
        """加载表中实际存在的列"""
        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DESCRIBE stock_daily_data")
                    columns = cursor.fetchall()
                    self.table_columns = [col[0] for col in columns]
                    logger.info(f"表列加载完成: {len(self.table_columns)}列")
        except Exception as e:
            logger.error(f"加载表列失败: {e}")
            # 默认列（根据常见表结构）
            self.table_columns = [
                'symbol', 'trade_date', 'open', 'high', 'low', 'close',
                'pre_close', 'volume', 'amount', 'pct_change', 'change',
                'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
                'amplitude'
            ]

    def store_daily_data(self, df: pd.DataFrame) -> Tuple[int, Dict]:
        """存储日线数据 - 只插入表中已有的列"""
        if df.empty:
            return 0, {'status': 'skipped', 'reason': 'empty_data'}

        try:
            # 1. 准备数据
            df_processed = self._prepare_data(df)

            # 2. 构建SQL（只包含表中存在的列）
            sql, records = self._build_insert_sql(df_processed)

            if not records:
                return 0, {'status': 'skipped', 'reason': 'no_valid_data'}

            # 3. 执行插入
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, records)
                    affected_rows = cursor.rowcount
                    conn.commit()

            symbol = df_processed['symbol'].iloc[0] if 'symbol' in df_processed.columns else 'unknown'
            logger.info(f"存储完成: {symbol}, {affected_rows}行")

            return affected_rows, {
                'status': 'success',
                'records': len(records),
                'affected': affected_rows,
                'symbol': symbol
            }

        except Exception as e:
            logger.error(f"存储失败: {e}")
            return 0, {'status': 'error', 'reason': str(e)}

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """准备数据 - 只保留表中有的列"""
        df_processed = df.copy()

        # 列名标准化
        column_mapping = {
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'pre_close_price': 'pre_close',
            'change_percent': 'pct_change',
            'symbol': 'symbol',
            'trade_date': 'trade_date'
        }

        # 重命名列
        for old_name, new_name in column_mapping.items():
            if old_name in df_processed.columns and new_name not in df_processed.columns:
                df_processed.rename(columns={old_name: new_name}, inplace=True)

        # 只保留表中存在的列
        valid_columns = [col for col in df_processed.columns if col in self.table_columns]
        df_processed = df_processed[valid_columns]

        # 确保数据类型
        if 'trade_date' in df_processed.columns:
            df_processed['trade_date'] = pd.to_datetime(
                df_processed['trade_date'], errors='coerce'
            ).dt.strftime('%Y%m%d')

        return df_processed

    def _build_insert_sql(self, df: pd.DataFrame) -> Tuple[str, list]:
        """构建插入SQL"""
        # 获取表中存在的列
        available_columns = [col for col in df.columns if col in self.table_columns]

        if not available_columns:
            return "", []

        # 基础插入SQL
        columns_str = ', '.join(available_columns)
        placeholders = ', '.join(['%s'] * len(available_columns))

        base_sql = f"""
            INSERT INTO stock_daily_data ({columns_str})
            VALUES ({placeholders})
        """

        # 更新部分（排除唯一键）
        unique_columns = ['symbol', 'trade_date']
        update_columns = [col for col in available_columns if col not in unique_columns]

        if update_columns:
            update_sql = "ON DUPLICATE KEY UPDATE " + \
                         ', '.join([f"{col} = VALUES({col})" for col in update_columns])
            full_sql = base_sql + " " + update_sql
        else:
            full_sql = base_sql

        # 准备记录
        records = []
        for _, row in df.iterrows():
            record = []
            for col in available_columns:
                val = row[col]
                if pd.isna(val):
                    record.append(None)
                elif isinstance(val, (np.integer, np.int64)):
                    record.append(int(val))
                elif isinstance(val, (np.floating, np.float64)):
                    record.append(float(val))
                else:
                    record.append(val)
            records.append(tuple(record))

        return full_sql, records

    def save_daily_data(self, df: pd.DataFrame) -> bool:
        """兼容接口"""
        affected, _ = self.store_daily_data(df)
        return affected > 0


# 测试
def test_simple_storage():
    """测试简化版存储器"""
    print("🧪 测试简化版数据存储器")
    print("=" * 50)

    try:
        storage = SimpleDataStorage()
        print("✅ 初始化成功")

        # 测试数据
        import pandas as pd
        test_data = pd.DataFrame({
            'symbol': ['sh600519'] * 3,
            'trade_date': ['20241201', '20241202', '20241203'],
            'open_price': [100.0, 101.0, 102.0],
            'high_price': [105.0, 106.0, 107.0],
            'low_price': [98.0, 99.0, 100.0],
            'close_price': [103.0, 104.0, 105.0],
            'volume': [1000000, 2000000, 3000000]
        })

        print(f"📊 测试数据: {len(test_data)}条")

        # 测试存储
        affected, report = storage.store_daily_data(test_data)
        print(f"✅ 存储结果: {affected}行影响, 状态: {report['status']}")

        # 测试兼容方法
        saved = storage.save_daily_data(test_data)
        print(f"✅ 兼容方法: {saved}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        print(traceback.format_exc())
        return False


if __name__ == "__main__":
    test_simple_storage()