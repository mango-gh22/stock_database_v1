# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\parabolic_sar.py
# File Name: parabolic_sar
# @ Author: mango-gh22
# @ Date：2025/12/21 9:10
"""
desc 抛物线指标（Parabolic SAR）- 用于判断趋势反转点
"""

import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class ParabolicSAR(BaseIndicator):
    """抛物线指标（Parabolic SAR）"""

    # 类属性
    name = "parabolic_sar"
    indicator_type = IndicatorType.TREND
    description = "抛物线指标（Parabolic SAR）"

    # 默认参数
    default_parameters = {
        'acceleration_factor': 0.02,
        'acceleration_max': 0.2,
        'use_high_low': True
    }

    def __init__(self, **parameters):
        """
        初始化抛物线指标

        Args:
            acceleration_factor: 加速因子，默认0.02
            acceleration_max: 最大加速因子，默认0.2
            use_high_low: 是否使用最高最低价（True），或收盘价（False）
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.acceleration_factor = self.parameters['acceleration_factor']
        self.acceleration_max = self.parameters['acceleration_max']
        self.use_high_low = self.parameters['use_high_low']
        self.requires_adjusted_price = False
        self.min_data_points = 20
        self.description = f"抛物线指标，加速因子: {self.acceleration_factor}, 最大加速: {self.acceleration_max}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算抛物线指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含Parabolic SAR指标的DataFrame
        """
        logger.info(f"计算Parabolic SAR指标，加速因子: {self.acceleration_factor}, 最大加速: {self.acceleration_max}")

        # 准备数据
        df = self.prepare_data(df)

        # 验证必要列
        required_cols = ['close_price']
        if self.use_high_low:
            required_cols.extend(['high_price', 'low_price'])

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必要列: {missing_cols}")

        # 计算Parabolic SAR
        result_df = df.copy()
        sar_values = self._calculate_sar(df)
        result_df['Parabolic_SAR'] = sar_values

        # 添加信号
        self._add_sar_signals(result_df)

        return result_df

    def _calculate_sar(self, df: pd.DataFrame) -> pd.Series:
        """计算SAR值"""
        high = df['high_price'] if self.use_high_low else df['close_price']
        low = df['low_price'] if self.use_high_low else df['close_price']
        close = df['close_price']

        # 初始化数组
        sar = np.zeros(len(df))
        trend = np.zeros(len(df))  # 1表示上升趋势，-1表示下降趋势
        acceleration = np.zeros(len(df))
        extreme_point = np.zeros(len(df))

        # 初始化第一个值
        sar[0] = low[0] if close[1] > close[0] else high[0]
        trend[0] = 1 if close[1] > close[0] else -1
        acceleration[0] = self.acceleration_factor
        extreme_point[0] = high[0] if trend[0] == 1 else low[0]

        # 计算SAR
        for i in range(1, len(df)):
            prev_trend = trend[i - 1]

            if prev_trend == 1:  # 上升趋势
                # 计算当前SAR
                sar[i] = sar[i - 1] + acceleration[i - 1] * (extreme_point[i - 1] - sar[i - 1])

                # 检查是否需要反转
                if low[i] < sar[i]:
                    # 趋势反转
                    trend[i] = -1
                    sar[i] = max(high[i - 1], high[i])
                    acceleration[i] = self.acceleration_factor
                    extreme_point[i] = low[i]
                else:
                    # 继续上升趋势
                    trend[i] = 1
                    acceleration[i] = acceleration[i - 1]
                    extreme_point[i] = extreme_point[i - 1]

                    # 更新极值点
                    if high[i] > extreme_point[i - 1]:
                        extreme_point[i] = high[i]
                        acceleration[i] = min(acceleration[i - 1] + self.acceleration_factor,
                                              self.acceleration_max)

            else:  # 下降趋势
                # 计算当前SAR
                sar[i] = sar[i - 1] + acceleration[i - 1] * (extreme_point[i - 1] - sar[i - 1])

                # 检查是否需要反转
                if high[i] > sar[i]:
                    # 趋势反转
                    trend[i] = 1
                    sar[i] = min(low[i - 1], low[i])
                    acceleration[i] = self.acceleration_factor
                    extreme_point[i] = high[i]
                else:
                    # 继续下降趋势
                    trend[i] = -1
                    acceleration[i] = acceleration[i - 1]
                    extreme_point[i] = extreme_point[i - 1]

                    # 更新极值点
                    if low[i] < extreme_point[i - 1]:
                        extreme_point[i] = low[i]
                        acceleration[i] = min(acceleration[i - 1] + self.acceleration_factor,
                                              self.acceleration_max)

        return pd.Series(sar, index=df.index)

    def _add_sar_signals(self, df: pd.DataFrame):
        """添加SAR信号"""
        if 'Parabolic_SAR' not in df.columns:
            return

        # SAR与价格的位置关系
        if 'close_price' in df.columns:
            df['SAR_Above_Price'] = df['Parabolic_SAR'] > df['close_price']  # 看跌信号
            df['SAR_Below_Price'] = df['Parabolic_SAR'] < df['close_price']  # 看涨信号

        # SAR转折点
        sar_diff = df['Parabolic_SAR'].diff()
        df['SAR_Turning_Up'] = (sar_diff > 0) & (sar_diff.shift(1) <= 0)
        df['SAR_Turning_Down'] = (sar_diff < 0) & (sar_diff.shift(1) >= 0)

        # 趋势强度（基于加速度）
        if 'high_price' in df.columns and 'low_price' in df.columns:
            price_range = df['high_price'] - df['low_price']
            sar_to_price = abs(df['Parabolic_SAR'] - df['close_price'])
            df['SAR_Trend_Strength'] = sar_to_price / price_range.replace(0, 1)

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        acceleration_factor = self.parameters.get('acceleration_factor')
        acceleration_max = self.parameters.get('acceleration_max')

        if not (0 < acceleration_factor <= acceleration_max):
            logger.error("加速因子必须在0到最大加速因子之间")
            return False

        if acceleration_max <= acceleration_factor:
            logger.error("最大加速因子必须大于加速因子")
            return False

        if acceleration_max > 1.0:
            logger.warning("最大加速因子大于1.0可能导致异常结果")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        if self.use_high_low:
            return ['close_price', 'high_price', 'low_price']
        return ['close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        base_cols = ['Parabolic_SAR', 'SAR_Above_Price', 'SAR_Below_Price',
                     'SAR_Turning_Up', 'SAR_Turning_Down']

        if self.use_high_low:
            base_cols.append('SAR_Trend_Strength')

        return base_cols