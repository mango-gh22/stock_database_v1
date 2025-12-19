# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\moving_arerage.py
# File Name: moving_arerage
# @ Author: mango-gh22
# @ Date：2025/12/17 6:15
"""
desc 
"""

"""
移动平均线指标计算
版本: v0.6.0-P6
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional


class MovingAverage:
    """移动平均线指标"""

    def __init__(self, periods: List[int] = None):
        """
        初始化移动平均线计算器

        Args:
            periods: 移动平均周期列表，默认[5, 10, 20, 30, 60]
        """
        self.periods = periods or [5, 10, 20, 30, 60]

    def calculate_sma(self, data: pd.Series, period: int) -> pd.Series:
        """计算简单移动平均线"""
        return data.rolling(window=period).mean()

    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """计算指数移动平均线"""
        return data.ewm(span=period, adjust=False).mean()

    def calculate_all(self, close_prices: pd.Series) -> Dict[str, pd.Series]:
        """计算所有周期的移动平均线"""
        results = {}

        for period in self.periods:
            # 简单移动平均
            sma_key = f'MA{period}'
            results[sma_key] = self.calculate_sma(close_prices, period)

            # 指数移动平均
            ema_key = f'EMA{period}'
            results[ema_key] = self.calculate_ema(close_prices, period)

        return results