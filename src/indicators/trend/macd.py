# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\macd.py
# File Name: macd
# @ Author: mango-gh22
# @ Date：2025/12/20 19:20
"""
File: src/indicators/trend/macd.py (修复)
Desc: MACD指标 - 修复参数验证问题
"""
import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class MACD(BaseIndicator):
    """MACD指标"""

    # 类属性
    name = "macd"
    indicator_type = IndicatorType.TREND
    description = "指数平滑异同移动平均线(MACD)"

    # 默认参数
    default_parameters = {
        'fast_period': 12,
        'slow_period': 26,
        'signal_period': 9,
        'price_column': 'close_price'
    }

    def __init__(self, **parameters):
        """
        初始化MACD指标

        Args:
            fast_period: 快速EMA周期，默认12
            slow_period: 慢速EMA周期，默认26
            signal_period: 信号线周期，默认9
            price_column: 价格列名
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.fast_period = self.parameters['fast_period']
        self.slow_period = self.parameters['slow_period']
        self.signal_period = self.parameters['signal_period']
        self.price_column = self.parameters['price_column']
        self.requires_adjusted_price = True
        self.min_data_points = self.slow_period + self.signal_period + 10
        self.description = f"MACD指标，快线: {self.fast_period}，慢线: {self.slow_period}，信号: {self.signal_period}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算MACD指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含MACD指标的DataFrame
        """
        logger.info(f"计算MACD指标，快线: {self.fast_period}, 慢线: {self.slow_period}, 信号: {self.signal_period}")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 计算快速EMA
        ema_fast = price_series.ewm(span=self.fast_period, adjust=False).mean()

        # 计算慢速EMA
        ema_slow = price_series.ewm(span=self.slow_period, adjust=False).mean()

        # 计算DIF（差离值）
        dif = ema_fast - ema_slow
        result_df['MACD_DIF'] = dif

        # 计算DEA（信号线）
        dea = dif.ewm(span=self.signal_period, adjust=False).mean()
        result_df['MACD_DEA'] = dea

        # 计算MACD柱状图
        histogram = (dif - dea) * 2
        result_df['MACD_HIST'] = histogram

        # 添加MACD信号
        self._add_macd_signals(result_df)

        return result_df

    def _add_macd_signals(self, df: pd.DataFrame):
        """添加MACD交易信号"""
        # MACD金叉：DIF上穿DEA
        df['MACD_GOLDEN_CROSS'] = (df['MACD_DIF'] > df['MACD_DEA']) & \
                                  (df['MACD_DIF'].shift(1) <= df['MACD_DEA'].shift(1))

        # MACD死叉：DIF下穿DEA
        df['MACD_DEATH_CROSS'] = (df['MACD_DIF'] < df['MACD_DEA']) & \
                                 (df['MACD_DIF'].shift(1) >= df['MACD_DEA'].shift(1))

        # 柱状图变化信号
        hist_diff = df['MACD_HIST'].diff()
        df['MACD_HIST_TURNING_UP'] = (hist_diff > 0) & (hist_diff.shift(1) <= 0)
        df['MACD_HIST_TURNING_DOWN'] = (hist_diff < 0) & (hist_diff.shift(1) >= 0)

        # 零轴信号
        df['MACD_DIF_ABOVE_ZERO'] = df['MACD_DIF'] > 0
        df['MACD_DIF_BELOW_ZERO'] = df['MACD_DIF'] < 0
        df['MACD_DIF_CROSS_ZERO_UP'] = (df['MACD_DIF'] > 0) & (df['MACD_DIF'].shift(1) <= 0)
        df['MACD_DIF_CROSS_ZERO_DOWN'] = (df['MACD_DIF'] < 0) & (df['MACD_DIF'].shift(1) >= 0)

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        fast_period = self.parameters.get('fast_period')
        slow_period = self.parameters.get('slow_period')
        signal_period = self.parameters.get('signal_period')

        if not all(isinstance(p, int) and p > 0 for p in [fast_period, slow_period, signal_period]):
            logger.error("MACD周期参数必须是正整数")
            return False

        if fast_period >= slow_period:
            logger.error("快速EMA周期必须小于慢速EMA周期")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'MACD_DIF', 'MACD_DEA', 'MACD_HIST',
            'MACD_GOLDEN_CROSS', 'MACD_DEATH_CROSS',
            'MACD_HIST_TURNING_UP', 'MACD_HIST_TURNING_DOWN',
            'MACD_DIF_ABOVE_ZERO', 'MACD_DIF_BELOW_ZERO',
            'MACD_DIF_CROSS_ZERO_UP', 'MACD_DIF_CROSS_ZERO_DOWN'
        ]