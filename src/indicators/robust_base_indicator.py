# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\robust_base_indicator.py
# @ Author: mango-gh22
# @ Date：2025/12/20 20:07
"""
desc
增强的技术指标基类 - 集成数据兼容性
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Optional, Any
from enum import Enum
import logging

from .base_indicator import BaseIndicator as OriginalBaseIndicator
from ..utils.data_preparer import DataPreparer

logger = logging.getLogger(__name__)


class RobustBaseIndicator(OriginalBaseIndicator):
    """增强的技术指标基类 - 自动处理数据缺失"""

    def prepare_data(self, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        增强的数据准备方法

        Args:
            df: 原始数据
            symbol: 股票代码（用于日志）

        Returns:
            处理后的数据
        """
        # 使用DataPreparer处理数据
        df_processed = DataPreparer.prepare_stock_data(df, symbol)

        # 检查数据是否足够
        self._validate_data_sufficiency(df_processed, symbol)

        return df_processed

    def _validate_data_sufficiency(self, df: pd.DataFrame, symbol: str = None):
        """验证数据是否足够"""
        # 检查数据点数
        if len(df) < self.min_data_points:
            msg = f"数据点数不足: {len(df)} < {self.min_data_points}"
            if symbol:
                msg = f"[{symbol}] {msg}"
            logger.warning(msg)

        # 检查close_price覆盖率
        if 'close_price' in df.columns:
            coverage = df['close_price'].notnull().sum() / len(df) * 100
            if coverage < 50 and symbol:
                logger.warning(f"[{symbol}] close_price覆盖率较低: {coverage:.1f}%")

                # 如果覆盖率太低，尝试给出建议
                if coverage < 30:
                    logger.warning(f"[{symbol}] 建议补充数据或使用其他时间段")

    def calculate(self, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        增强的计算方法（带symbol参数）

        Args:
            df: 数据
            symbol: 股票代码

        Returns:
            计算结果
        """
        # 准备数据
        df_prepared = self.prepare_data(df, symbol)

        # 调用原始计算方法
        return super().calculate(df_prepared)