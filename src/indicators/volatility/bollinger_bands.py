# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/volatility\bollinger_bands.py
# File Name: bollinger_bands
# @ Author: mango-gh22
# @ Date：2025/12/20 22:54
"""
File: src/indicators/volatility/bollinger_bands.py (修复)
Desc: 布林带指标 - 修复参数验证问题
"""
import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class BollingerBands(BaseIndicator):
    """布林带指标"""

    # 类属性
    name = "bollinger_bands"
    indicator_type = IndicatorType.VOLATILITY
    description = "布林带波动率指标"

    # 默认参数
    default_parameters = {
        'period': 20,
        'std_dev': 2,
        'price_column': 'close_price'
    }

    def __init__(self, **parameters):
        """
        初始化布林带指标

        Args:
            period: 移动平均周期，默认20
            std_dev: 标准差倍数，默认2
            price_column: 价格列名
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.period = self.parameters['period']
        self.std_dev = self.parameters['std_dev']
        self.price_column = self.parameters['price_column']
        self.requires_adjusted_price = True
        self.min_data_points = self.period + 10
        self.description = f"布林带，周期: {self.period}，标准差倍数: {self.std_dev}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算布林带指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含布林带指标的DataFrame
        """
        logger.info(f"计算布林带，周期: {self.period}，标准差: {self.std_dev}")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 计算中轨（移动平均）
        middle_band = price_series.rolling(window=self.period).mean()
        result_df['BB_Middle'] = middle_band

        # 计算标准差
        std = price_series.rolling(window=self.period).std()

        # 计算上轨和下轨
        upper_band = middle_band + (std * self.std_dev)
        lower_band = middle_band - (std * self.std_dev)

        result_df['BB_Upper'] = upper_band
        result_df['BB_Lower'] = lower_band

        # 添加信号
        self._add_bollinger_signals(result_df, price_series)

        return result_df

    def _add_bollinger_signals(self, df: pd.DataFrame, price_series: pd.Series):
        """添加布林带信号"""
        # 价格与布林带的关系
        df['Price_Above_Upper'] = price_series > df['BB_Upper']
        df['Price_Below_Lower'] = price_series < df['BB_Lower']
        df['Price_In_Band'] = ~(df['Price_Above_Upper'] | df['Price_Below_Lower'])

        # 布林带收窄/扩张
        band_width = df['BB_Upper'] - df['BB_Lower']
        band_width_pct_change = band_width.pct_change() * 100
        df['BB_Squeeze'] = band_width_pct_change < 0  # 带宽缩小
        df['BB_Expansion'] = band_width_pct_change > 0  # 带宽扩大

        # 极端挤压信号（带宽历史低位）
        if len(df) > self.period * 2:
            width_rank = band_width.rolling(window=self.period * 2).rank(pct=True)
            df['BB_Extreme_Squeeze'] = width_rank < 0.2

        # 突破信号
        df['BB_Breakout_Upper'] = (price_series > df['BB_Upper']) & \
                                  (price_series.shift(1) <= df['BB_Upper'].shift(1))
        df['BB_Breakout_Lower'] = (price_series < df['BB_Lower']) & \
                                  (price_series.shift(1) >= df['BB_Lower'].shift(1))

        # 回归信号
        df['BB_Return_From_Upper'] = (price_series <= df['BB_Upper']) & \
                                     (price_series.shift(1) > df['BB_Upper'].shift(1))
        df['BB_Return_From_Lower'] = (price_series >= df['BB_Lower']) & \
                                     (price_series.shift(1) < df['BB_Lower'].shift(1))

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        period = self.parameters.get('period')
        std_dev = self.parameters.get('std_dev')

        if not isinstance(period, int) or period <= 0:
            logger.error("周期必须是正整数")
            return False

        if std_dev <= 0:
            logger.error("标准差倍数必须大于0")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'BB_Middle', 'BB_Upper', 'BB_Lower',
            'Price_Above_Upper', 'Price_Below_Lower', 'Price_In_Band',
            'BB_Squeeze', 'BB_Expansion', 'BB_Extreme_Squeeze',
            'BB_Breakout_Upper', 'BB_Breakout_Lower',
            'BB_Return_From_Upper', 'BB_Return_From_Lower'
        ]