# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\data_preparer.py
# File Name: data_preparer
# @ Author: mango-gh22
# @ Date：2025/12/20 20:02
"""
desc 
"""
# 保存到：src/utils/data_preparer.py
"""
数据准备工具 - 处理字段缺失和None值问题
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DataPreparer:
    """数据准备工具类"""

    # 字段回退策略
    FIELD_FALLBACKS = {
        'close_price': {
            'primary': 'close_price',
            'alternatives': ['ma20', 'ma10', 'ma5', 'ma_close', 'close'],
            'description': '收盘价'
        },
        'open_price': {
            'primary': 'open_price',
            'alternatives': ['close_price', 'ma20', 'ma10', 'ma5', 'open'],
            'description': '开盘价'
        },
        'high_price': {
            'primary': 'high_price',
            'alternatives': ['close_price', 'ma20', 'ma10', 'ma5', 'high'],
            'description': '最高价'
        },
        'low_price': {
            'primary': 'low_price',
            'alternatives': ['close_price', 'ma20', 'ma10', 'ma5', 'low'],
            'description': '最低价'
        }
    }

    @staticmethod
    def prepare_stock_data(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        准备股票数据，处理字段缺失问题

        Args:
            df: 原始股票数据
            symbol: 股票代码

        Returns:
            处理后的数据
        """
        if df.empty:
            return df

        result_df = df.copy()
        symbol_prefix = f"[{symbol}] " if symbol else ""

        # 1. 列名标准化
        result_df = DataPreparer._standardize_column_names(result_df, symbol)

        # 2. 处理None值
        result_df = DataPreparer._handle_none_values(result_df, symbol)

        # 3. 数据质量报告
        DataPreparer._report_data_quality(result_df, symbol)

        return result_df

    @staticmethod
    def _standardize_column_names(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """标准化列名"""
        column_mapping = {
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'volume': 'volume',
            'amount': 'amount'
        }

        result_df = df.copy()
        renamed_cols = {}

        for old_name, new_name in column_mapping.items():
            if old_name in result_df.columns and new_name not in result_df.columns:
                result_df[new_name] = result_df[old_name]
                renamed_cols[old_name] = new_name

        if renamed_cols and symbol:
            logger.info(f"[{symbol}] 列名映射: {renamed_cols}")

        return result_df

    @staticmethod
    def _handle_none_values(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """处理None值"""
        result_df = df.copy()
        symbol_prefix = f"[{symbol}] " if symbol else ""

        for field, config in DataPreparer.FIELD_FALLBACKS.items():
            if field in result_df.columns:
                null_count = result_df[field].isnull().sum()

                if null_count > 0:
                    # 尝试使用替代字段
                    alternative_used = None

                    for alternative in config['alternatives']:
                        if (alternative in result_df.columns and
                                alternative != field and
                                not result_df[alternative].isnull().all()):
                            # 填充None值
                            mask = result_df[field].isnull()
                            result_df.loc[mask, field] = result_df.loc[mask, alternative]

                            alternative_used = alternative
                            break

                    if alternative_used and symbol:
                        logger.info(
                            f"{symbol_prefix}使用 {alternative_used} "
                            f"填充 {null_count} 条 {config['description']} 缺失值"
                        )
                    elif null_count > 0 and symbol:
                        logger.warning(
                            f"{symbol_prefix}{config['description']} "
                            f"有 {null_count} 条缺失且无合适替代"
                        )

        return result_df

    @staticmethod
    def _report_data_quality(df: pd.DataFrame, symbol: str = None):
        """报告数据质量"""
        if not symbol:
            return

        key_fields = ['close_price', 'open_price', 'high_price', 'low_price']
        report_lines = []

        for field in key_fields:
            if field in df.columns:
                non_null = df[field].notnull().sum()
                total = len(df)
                coverage = (non_null / total * 100) if total > 0 else 0
                report_lines.append(f"{field}: {coverage:.1f}%")

        if report_lines:
            logger.info(f"[{symbol}] 数据覆盖率: {', '.join(report_lines)}")

            # 检查close_price的覆盖率
            if 'close_price' in df.columns:
                close_coverage = df['close_price'].notnull().sum() / len(df) * 100
                if close_coverage < 60:
                    logger.warning(f"[{symbol}] close_price覆盖率较低: {close_coverage:.1f}%")