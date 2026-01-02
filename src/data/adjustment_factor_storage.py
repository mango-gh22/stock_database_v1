# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\adjustment_factor_storage.py
# File Name: adjustment_factor_storage
# @ Author: mango-gh22
# @ Date：2026/1/2 18:46
"""
desc 复权因子存储管理器 - 适配 adjust_factors 表结构
继承 DataStorage 核心逻辑，定制复权因子专用预处理

生产运行：
python -m src.data.adjustment_factor_manager --mode incremental
incremental 增加的，递增的，（运行生产任务，生产运行）
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging
import sys

sys.path.append('E:\\MyFile\\stock_database_v1')  # 添加项目根目录

from src.data.data_storage import DataStorage
from src.config.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


class AdjustmentFactorStorage(DataStorage):
    """复权因子存储管理器 - 增强版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化

        Args:
            config_path: 数据库配置路径
        """
        super().__init__(config_path)

        # 表名固定
        self.factor_table = 'adjust_factors'

        # 验证表结构
        self._validate_table_structure()

        logger.info("✅ 复权因子存储器初始化完成")

    def _validate_table_structure(self):
        """验证 adjust_factors 表结构"""
        try:
            required_columns = {
                'symbol', 'ex_date', 'cash_div', 'shares_div',
                'allotment_ratio', 'allotment_price', 'split_ratio',
                'forward_factor', 'backward_factor', 'total_factor'
            }

            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {self.factor_table}")
                    actual_columns = {row[0] for row in cursor.fetchall()}

            missing_columns = required_columns - actual_columns
            if missing_columns:
                logger.error(f"❌ {self.factor_table} 表缺少字段: {missing_columns}")
                raise ValueError(f"表结构不匹配: {missing_columns}")

        except Exception as e:
            logger.error(f"验证表结构失败: {e}")
            raise

    def _preprocess_adjustment_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理复权因子数据

        Args:
            df: 原始DataFrame

        Returns:
            预处理后的DataFrame
        """
        if df.empty:
            logger.warning("输入DataFrame为空")
            return df

        df_processed = df.copy()

        # 1. 确保必需字段存在
        required_fields = [
            'symbol', 'ex_date', 'cash_div', 'shares_div',
            'allotment_ratio', 'allotment_price', 'split_ratio',
            'forward_factor', 'backward_factor', 'total_factor'
        ]

        for field in required_fields:
            if field not in df_processed.columns:
                if field in ['cash_div', 'shares_div', 'allotment_ratio',
                             'allotment_price', 'split_ratio']:
                    df_processed[field] = 0.0
                else:
                    df_processed[field] = None

        # 2. 标准化股票代码
        if 'symbol' in df_processed.columns:
            df_processed['symbol'] = df_processed['symbol'].apply(
                lambda x: str(x).replace('.', '').lower() if pd.notna(x) else None
            )

        # 3. 日期格式转换
        if 'ex_date' in df_processed.columns:
            df_processed['ex_date'] = pd.to_datetime(df_processed['ex_date'], errors='coerce')

        # 4. 数值字段转换
        numeric_fields = [
            'cash_div', 'shares_div', 'allotment_ratio',
            'allotment_price', 'split_ratio',
            'forward_factor', 'backward_factor', 'total_factor'
        ]

        for field in numeric_fields:
            if field in df_processed.columns:
                df_processed[field] = pd.to_numeric(df_processed[field], errors='coerce')

        # 5. 处理空值
        df_processed['cash_div'] = df_processed['cash_div'].fillna(0.0)
        df_processed['shares_div'] = df_processed['shares_div'].fillna(0.0)
        df_processed['allotment_ratio'] = df_processed['allotment_ratio'].fillna(0.0)
        df_processed['allotment_price'] = df_processed['allotment_price'].fillna(0.0)
        df_processed['split_ratio'] = df_processed['split_ratio'].fillna(1.0)
        df_processed['forward_factor'] = df_processed['forward_factor'].fillna(1.0)
        df_processed['backward_factor'] = df_processed['backward_factor'].fillna(1.0)
        df_processed['total_factor'] = df_processed['total_factor'].fillna(1.0)

        # 6. 添加时间戳
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'updated_time' in df_processed.columns:
            df_processed['updated_time'] = current_time

        logger.info(f"✅ 预处理完成: {len(df_processed)} 条记录")
        return df_processed

    def store_adjustment_factors(self, data: Any) -> Tuple[int, Dict]:
        """
        存储复权因子数据

        Args:
            data: DataFrame或字典列表

        Returns:
            (影响行数, 详细信息字典)
        """
        try:
            # 转换为DataFrame
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError(f"不支持的数据类型: {type(data)}")

            if df.empty:
                logger.warning("复权因子数据为空，跳过存储")
                return 0, {'status': 'skipped', 'reason': 'empty_data'}

            logger.info(f"开始存储复权因子数据: {len(df)} 条记录")

            # 预处理
            df_processed = self._preprocess_adjustment_factors(df)

            if df_processed.empty:
                logger.error("预处理后无有效数据")
                return 0, {'status': 'skipped', 'reason': 'preprocess_failed'}

            # 构建动态SQL
            insert_sql, update_sql, valid_columns = self._build_dynamic_sql(
                df_processed, self.factor_table
            )

            # 准备记录
            records = self._prepare_records(df_processed, valid_columns)

            if not records:
                logger.error("无有效记录可插入")
                return 0, {'status': 'skipped', 'reason': 'no_valid_records'}

            # 批量插入
            affected_rows = 0
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.executemany(insert_sql, records)
                        affected_rows = cursor.rowcount
                        conn.commit()
                        logger.info(f"✅ 复权因子存储成功: {affected_rows}/{len(records)} 条")
                    except Exception as insert_error:
                        conn.rollback()
                        logger.error(f"❌ 批量插入失败: {insert_error}")

                        # 单条重试
                        logger.info("尝试单条插入模式...")
                        success_count = 0
                        for record in records:
                            try:
                                cursor.execute(insert_sql, record)
                                success_count += 1
                            except Exception as single_error:
                                logger.debug(f"单条插入失败: {record[:2]} - {single_error}")

                        conn.commit()
                        affected_rows = success_count
                        logger.warning(f"单条插入完成: {success_count}/{len(records)} 条")

            # 记录日志
            symbol = df_processed['symbol'].iloc[0] if 'symbol' in df_processed.columns else 'unknown'
            self._log_data_update(
                data_type='adjustment_factor',
                symbol=symbol,
                table_name=self.factor_table,
                records_processed=len(records),
                records_affected=affected_rows,
                status='success'
            )

            return affected_rows, {
                'status': 'success',
                'table': self.factor_table,
                'records_processed': len(records),
                'records_affected': affected_rows,
                'symbol': symbol,
                'columns': valid_columns
            }

        except Exception as e:
            logger.error(f"存储复权因子失败: {e}", exc_info=True)
            return 0, {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            }

    def get_factors_by_symbol(self, symbol: str, start_date: str = None,
                              end_date: str = None) -> pd.DataFrame:
        """
        查询指定股票的复权因子

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            复权因子DataFrame
        """
        normalized_symbol = str(symbol).replace('.', '').lower()

        query = f"SELECT * FROM {self.factor_table} WHERE symbol = %s"
        params = [normalized_symbol]

        if start_date:
            query += " AND ex_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND ex_date <= %s"
            params.append(end_date)

        query += " ORDER BY ex_date DESC"

        try:
            with self.db_connector.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
            logger.info(f"查询复权因子成功: {symbol}, {len(df)} 条记录")
            return df
        except Exception as e:
            logger.error(f"查询复权因子失败 {symbol}: {e}")
            return pd.DataFrame()

    # 定位到第280-290行附近，找到 get_latest_factor_date 方法

    def get_latest_factor_date(self, symbol: str) -> Optional[str]:
        """获取最新复权因子日期"""
        df = self.get_factors_by_symbol(symbol)
        if df.empty:
            return None

        # ✨ 修复后的正确代码
        latest_date = df['ex_date'].iloc[0]
        if isinstance(latest_date, pd.Timestamp):
            return latest_date.strftime('%Y-%m-%d')
        else:
            return str(latest_date)

    def clear_cache(self, symbol: Optional[str] = None):
        """清理缓存（P8阶段实现）"""
        logger.info(f"清理缓存功能将在P8阶段实现: {symbol}")
        pass

if __name__ == "__main__":
    test_adjustment_factor_downloader()