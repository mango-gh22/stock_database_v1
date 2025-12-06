# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\indicators.py
# File Name: indicators
# @ Author: m_mango
# @ Date：2025/12/6 16:27
"""
desc 技术指标计算模块
"""

"""
技术指标计算模块 - v0.4.0
功能：实现常用的技术分析指标计算
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import warnings

warnings.filterwarnings('ignore')


class TechnicalIndicators:
    """技术指标计算器"""

    @staticmethod
    def calculate_ma(data: pd.DataFrame,
                     periods: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """
        计算移动平均线

        Args:
            data: 包含'close'列的DataFrame
            periods: 周期列表

        Returns:
            DataFrame: 包含MA指标的DataFrame
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame必须包含'close'列")

        result = data.copy()
        for period in periods:
            if len(data) >= period:
                result[f'MA{period}'] = data['close'].rolling(window=period).mean()
            else:
                result[f'MA{period}'] = np.nan
        return result

    @staticmethod
    def calculate_rsi(data: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        计算相对强弱指数(RSI)

        Args:
            data: 包含'close'列的DataFrame
            period: RSI周期

        Returns:
            DataFrame: 包含RSI指标的DataFrame
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame必须包含'close'列")

        result = data.copy()
        if len(data) < period + 1:
            result['RSI'] = np.nan
            return result

        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        result['RSI'] = rsi
        return result

    @staticmethod
    def calculate_macd(data: pd.DataFrame,
                       fast: int = 12,
                       slow: int = 26,
                       signal: int = 9) -> pd.DataFrame:
        """
        计算MACD指标

        Args:
            data: 包含'close'列的DataFrame
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期

        Returns:
            DataFrame: 包含MACD指标的DataFrame
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame必须包含'close'列")

        result = data.copy()
        if len(data) < slow:
            result['MACD'] = np.nan
            result['MACD_signal'] = np.nan
            result['MACD_hist'] = np.nan
            return result

        # 计算EMA
        ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = data['close'].ewm(span=slow, adjust=False).mean()

        # 计算MACD线和信号线
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        hist_line = macd_line - signal_line

        result['MACD'] = macd_line
        result['MACD_signal'] = signal_line
        result['MACD_hist'] = hist_line

        return result

    @staticmethod
    def calculate_bollinger_bands(data: pd.DataFrame,
                                  period: int = 20,
                                  std_dev: float = 2.0) -> pd.DataFrame:
        """
        计算布林带

        Args:
            data: 包含'close'列的DataFrame
            period: 移动平均周期
            std_dev: 标准差倍数

        Returns:
            DataFrame: 包含布林带的DataFrame
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame必须包含'close'列")

        result = data.copy()
        if len(data) < period:
            result['BB_middle'] = np.nan
            result['BB_upper'] = np.nan
            result['BB_lower'] = np.nan
            result['BB_width'] = np.nan
            return result

        # 计算中轨和标准差
        middle_band = data['close'].rolling(window=period).mean()
        rolling_std = data['close'].rolling(window=period).std()

        # 计算上下轨
        upper_band = middle_band + (rolling_std * std_dev)
        lower_band = middle_band - (rolling_std * std_dev)

        # 计算带宽
        bb_width = (upper_band - lower_band) / middle_band

        result['BB_middle'] = middle_band
        result['BB_upper'] = upper_band
        result['BB_lower'] = lower_band
        result['BB_width'] = bb_width

        return result

    @staticmethod
    def calculate_volume_indicators(data: pd.DataFrame,
                                    period: int = 20) -> pd.DataFrame:
        """
        计算成交量指标

        Args:
            data: 包含'volume'列的DataFrame
            period: 移动平均周期

        Returns:
            DataFrame: 包含成交量指标的DataFrame
        """
        if 'volume' not in data.columns:
            raise ValueError("DataFrame必须包含'volume'列")

        result = data.copy()
        if len(data) < period:
            result['VOL_MA'] = np.nan
            result['VOL_RATIO'] = np.nan
            return result

        # 成交量移动平均
        vol_ma = data['volume'].rolling(window=period).mean()

        # 成交量比率（当日成交量/平均成交量）
        vol_ratio = data['volume'] / vol_ma

        result['VOL_MA'] = vol_ma
        result['VOL_RATIO'] = vol_ratio

        return result

    @staticmethod
    def calculate_all_indicators(data: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有技术指标

        Args:
            data: 包含'close'和'volume'列的DataFrame

        Returns:
            DataFrame: 包含所有技术指标的DataFrame
        """
        if data.empty:
            return data

        result = data.copy()

        # 计算MA
        result = TechnicalIndicators.calculate_ma(result)

        # 计算RSI
        result = TechnicalIndicators.calculate_rsi(result)

        # 计算MACD
        result = TechnicalIndicators.calculate_macd(result)

        # 计算布林带
        result = TechnicalIndicators.calculate_bollinger_bands(result)

        # 计算成交量指标
        if 'volume' in result.columns:
            result = TechnicalIndicators.calculate_volume_indicators(result)

        return result