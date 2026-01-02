# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\robust_data_preparer.py
# File Name: robust_data_preparer
# @ Author: mango-gh22
# @ Date：2025/12/20 19:55
"""
desc 
"""
# src/data/robust_data_preparer.py
"""
健壮性数据准备器 - 处理字段缺失问题
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class RobustDataPreparer:
    """处理数据字段缺失问题的健壮性数据准备器"""

    # 定义字段回退优先级
    FIELD_FALLBACK_PRIORITY = {
        'close_price': ['close_price', 'ma20', 'ma10', 'ma5', 'ma_close', 'close'],
        'open_price': ['open_price', 'close_price', 'ma20', 'ma10', 'ma5', 'open'],
        'high_price': ['high_price', 'close_price', 'ma20', 'ma10', 'ma5', 'high'],
        'low_price': ['low_price', 'close_price', 'ma20', 'ma10', 'ma5', 'low'],
        'volume': ['volume', 'amount'],  # 如果成交量缺失，尝试用成交额间接推断
    }

    @classmethod
    def prepare_price_data(cls, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        准备价格数据，处理字段缺失问题

        Args:
            df: 原始数据DataFrame
            symbol: 股票代码（用于日志记录）

        Returns:
            处理后的DataFrame，包含必要的价格字段
        """
        if df.empty:
            return df

        result_df = df.copy()
        symbol_info = f"[{symbol}]" if symbol else ""

        # 1. 检查并回退关键价格字段
        for target_field, candidates in cls.FIELD_FALLBACK_PRIORITY.items():
            if target_field not in result_df.columns or result_df[target_field].isnull().all():
                # 尝试从候选字段中获取数据
                for candidate in candidates:
                    if (candidate in result_df.columns and
                            not result_df[candidate].isnull().all()):
                        result_df[target_field] = result_df[candidate]
                        logger.warning(
                            f"{symbol_info} 使用 {candidate} 作为 {target_field} 的替代"
                        )
                        break
                else:
                    # 所有候选字段都无效
                    logger.error(f"{symbol_info} 无法为 {target_field} 找到有效数据")
                    result_df[target_field] = np.nan

        # 2. 数据质量评估
        quality_report = cls._assess_data_quality(result_df)
        if symbol:
            logger.info(f"{symbol_info} 数据质量评估: {quality_report}")

        # 3. 添加数据缺失标记（供后续策略使用）
        result_df['_data_missing'] = result_df['close_price'].isnull()

        return result_df

    @classmethod
    def _assess_data_quality(cls, df: pd.DataFrame) -> Dict[str, float]:
        """评估数据质量"""
        quality = {}
        required_fields = ['close_price', 'high_price', 'low_price', 'open_price']

        for field in required_fields:
            if field in df.columns:
                non_null_ratio = df[field].notnull().sum() / len(df)
                quality[f"{field}_coverage"] = round(non_null_ratio * 100, 1)
            else:
                quality[f"{field}_coverage"] = 0.0

        return quality

    @classmethod
    def validate_for_indicators(cls, df: pd.DataFrame,
                                min_coverage: float = 80.0) -> bool:
        """
        验证数据是否足够进行指标计算

        Args:
            df: 数据DataFrame
            min_coverage: 最小数据覆盖率（%）

        Returns:
            是否足够计算指标
        """
        if df.empty:
            return False

        # 检查close_price的覆盖率
        if 'close_price' in df.columns:
            coverage = df['close_price'].notnull().sum() / len(df) * 100
            return coverage >= min_coverage

        return False