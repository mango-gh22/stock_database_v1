# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/momentum\williams_r.py
# File Name: williams_r
# @ Author: mango-gh22
# @ Date：2025/12/21 9:49

"""
File: src/indicators/momentum/williams_r.py (修复)
Desc: 威廉指标（Williams %R）- 动量指标- 修复版
"""
import pandas as pd
import numpy as np
from typing import List, Dict
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class WilliamsR(BaseIndicator):
    """威廉指标（Williams %R）"""

    # 类属性
    name = "williams_r"
    indicator_type = IndicatorType.MOMENTUM
    description = "威廉指标（Williams %R）"

    # 默认参数
    default_parameters = {
        'period': 14,
        'overbought_level': -20,
        'oversold_level': -80
    }

    def __init__(self, **parameters):
        """
        初始化威廉指标

        Args:
            period: 计算周期，默认14
            overbought_level: 超买线，默认-20
            oversold_level: 超卖线，默认-80
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.period = self.parameters['period']
        self.overbought_level = self.parameters['overbought_level']
        self.oversold_level = self.parameters['oversold_level']
        self.requires_adjusted_price = False
        self.min_data_points = self.period + 10
        self.description = f"威廉指标，周期: {self.period}，超买: {self.overbought_level}，超卖: {self.oversold_level}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算威廉指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含Williams %R指标的DataFrame
        """
        logger.info(f"计算Williams %R指标，周期: {self.period}, "
                    f"超买: {self.overbought_level}, 超卖: {self.oversold_level}")

        # 准备数据
        df = self.prepare_data(df)

        # 验证必要列
        required_cols = ['high_price', 'low_price', 'close_price']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必要列: {missing_cols}")

        result_df = df.copy()
        high = df['high_price']
        low = df['low_price']
        close = df['close_price']

        # 计算最高价和最低价
        highest_high = high.rolling(window=self.period).max()
        lowest_low = low.rolling(window=self.period).min()

        # 计算Williams %R
        # 公式: %R = (最高价 - 收盘价) / (最高价 - 最低价) * -100
        price_range = highest_high - lowest_low
        price_range = price_range.replace(0, 1e-10)  # 避免除零

        williams_r = ((highest_high - close) / price_range) * -100
        result_df['Williams_R'] = williams_r

        # 添加信号
        self._add_williams_signals(result_df)

        return result_df

    def _add_williams_signals(self, df: pd.DataFrame):
        """添加威廉指标信号"""
        if 'Williams_R' not in df.columns:
            return

        williams_r = df['Williams_R']

        # 1. 超买超卖信号
        df['Williams_Overbought'] = williams_r >= self.overbought_level
        df['Williams_Oversold'] = williams_r <= self.oversold_level

        # 2. 极端超买超卖信号
        extreme_overbought = -10  # 比-20更极端
        extreme_oversold = -90  # 比-80更极端

        df['Williams_Extreme_Overbought'] = williams_r >= extreme_overbought
        df['Williams_Extreme_Oversold'] = williams_r <= extreme_oversold

        # 3. 零轴交叉信号（威廉指标的"零轴"是-50）
        df['Williams_Above_Mid'] = williams_r > -50
        df['Williams_Below_Mid'] = williams_r < -50
        df['Williams_Cross_Mid_Up'] = (williams_r > -50) & (williams_r.shift(1) <= -50)
        df['Williams_Cross_Mid_Down'] = (williams_r < -50) & (williams_r.shift(1) >= -50)

        # 4. 动量信号
        williams_diff = williams_r.diff()
        df['Williams_Momentum_Up'] = williams_diff > 0
        df['Williams_Momentum_Down'] = williams_diff < 0

        # 5. 超买超卖区突破信号
        df['Williams_Break_Overbought'] = (williams_r < self.overbought_level) & \
                                          (williams_r.shift(1) >= self.overbought_level)
        df['Williams_Break_Oversold'] = (williams_r > self.oversold_level) & \
                                        (williams_r.shift(1) <= self.oversold_level)

        # 6. 背离检测
        if 'close_price' in df.columns and len(df) > 30:
            df['Williams_Bullish_Divergence'] = False
            df['Williams_Bearish_Divergence'] = False

            for i in range(20, len(df)):
                # 底背离：价格创新低，指标未创新低
                if (df['close_price'].iloc[i] < df['close_price'].iloc[i - 10:i].min() and
                        williams_r.iloc[i] > williams_r.iloc[i - 10:i].min()):
                    df.loc[df.index[i], 'Williams_Bullish_Divergence'] = True

                # 顶背离：价格创新高，指标未创新高
                if (df['close_price'].iloc[i] > df['close_price'].iloc[i - 10:i].max() and
                        williams_r.iloc[i] < williams_r.iloc[i - 10:i].max()):
                    df.loc[df.index[i], 'Williams_Bearish_Divergence'] = True

        # 7. 超买超卖持续时间
        df['Williams_Overbought_Duration'] = 0
        df['Williams_Oversold_Duration'] = 0

        ob_count = 0
        os_count = 0

        for i in range(len(df)):
            if df['Williams_Overbought'].iloc[i]:
                ob_count += 1
                os_count = 0
            elif df['Williams_Oversold'].iloc[i]:
                os_count += 1
                ob_count = 0
            else:
                ob_count = 0
                os_count = 0

            df.loc[df.index[i], 'Williams_Overbought_Duration'] = ob_count
            df.loc[df.index[i], 'Williams_Oversold_Duration'] = os_count

        # 8. 趋势强度
        df['Williams_Trend_Strength'] = abs(williams_r).rolling(window=5).mean()

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        period = self.parameters.get('period')
        oversold_level = self.parameters.get('oversold_level')
        overbought_level = self.parameters.get('overbought_level')

        if not isinstance(period, int) or period <= 0:
            logger.error("周期必须是正整数")
            return False

        if not (-100 <= oversold_level < overbought_level <= 0):
            logger.error("超买超卖线必须在-100到0之间，且超卖线小于超买线")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return ['high_price', 'low_price', 'close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'Williams_R', 'Williams_Overbought', 'Williams_Oversold',
            'Williams_Extreme_Overbought', 'Williams_Extreme_Oversold',
            'Williams_Above_Mid', 'Williams_Below_Mid',
            'Williams_Cross_Mid_Up', 'Williams_Cross_Mid_Down',
            'Williams_Momentum_Up', 'Williams_Momentum_Down',
            'Williams_Break_Overbought', 'Williams_Break_Oversold',
            'Williams_Bullish_Divergence', 'Williams_Bearish_Divergence',
            'Williams_Overbought_Duration', 'Williams_Oversold_Duration',
            'Williams_Trend_Strength'
        ]