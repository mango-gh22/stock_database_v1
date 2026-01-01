# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/momentum\stochastic.py
# File Name: stochastic
# @ Author: mango-gh22
# @ Date：2025/12/21 9:47
"""
desc stochastic.py（随机指标）- 动量指标
"""
import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class Stochastic(BaseIndicator):
    """随机指标（Stochastic Oscillator）"""

    # 类属性
    name = "stochastic"
    indicator_type = IndicatorType.MOMENTUM
    description = "随机指标（Stochastic Oscillator）"

    # 默认参数
    default_parameters = {
        'k_period': 14,
        'd_period': 3,
        'smoothing': 3,
        'overbought_level': 80,
        'oversold_level': 20
    }

    def __init__(self, **parameters):
        """
        初始化随机指标

        Args:
            k_period: %K线周期，默认14
            d_period: %D线周期，默认3
            smoothing: 平滑周期，默认3
            overbought_level: 超买线，默认80
            oversold_level: 超卖线，默认20
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.k_period = self.parameters['k_period']
        self.d_period = self.parameters['d_period']
        self.smoothing = self.parameters['smoothing']
        self.overbought_level = self.parameters['overbought_level']
        self.oversold_level = self.parameters['oversold_level']
        self.requires_adjusted_price = False
        self.min_data_points = self.k_period + self.d_period + self.smoothing + 10
        self.description = f"随机指标，K={self.k_period}，D={self.d_period}，超买={self.overbought_level}，超卖={self.oversold_level}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算随机指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含随机指标的DataFrame
        """
        logger.info(f"计算Stochastic指标，参数: K={self.k_period}, D={self.d_period}, "
                    f"平滑={self.smoothing}, 超买={self.overbought_level}, 超卖={self.oversold_level}")

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

        # 计算%K线
        lowest_low = low.rolling(window=self.k_period).min()
        highest_high = high.rolling(window=self.k_period).max()

        # 避免除零
        price_range = highest_high - lowest_low
        price_range = price_range.replace(0, 1e-10)  # 避免除零

        k_raw = 100 * ((close - lowest_low) / price_range)

        # 平滑%K线
        k_fast = k_raw.rolling(window=self.smoothing).mean()
        result_df['Stochastic_K_Fast'] = k_fast

        # 计算%D线（%K线的移动平均）
        d_slow = k_fast.rolling(window=self.d_period).mean()
        result_df['Stochastic_D_Slow'] = d_slow

        # 添加慢速%K线（可选）
        k_slow = d_slow.rolling(window=self.d_period).mean()
        result_df['Stochastic_K_Slow'] = k_slow

        # 添加信号
        self._add_stochastic_signals(result_df)

        return result_df

    def _add_stochastic_signals(self, df: pd.DataFrame):
        """添加随机指标信号"""
        if 'Stochastic_K_Fast' not in df.columns or 'Stochastic_D_Slow' not in df.columns:
            return

        k_line = df['Stochastic_K_Fast']
        d_line = df['Stochastic_D_Slow']

        # 1. 超买超卖信号
        df['Stochastic_Overbought'] = k_line >= self.overbought_level
        df['Stochastic_Oversold'] = k_line <= self.oversold_level

        # 2. 金叉死叉信号
        df['Stochastic_Golden_Cross'] = (k_line > d_line) & (k_line.shift(1) <= d_line.shift(1))
        df['Stochastic_Death_Cross'] = (k_line < d_line) & (k_line.shift(1) >= d_line.shift(1))

        # 3. 背离检测
        if 'close_price' in df.columns and len(df) > 30:
            df['Stochastic_Bullish_Divergence'] = False
            df['Stochastic_Bearish_Divergence'] = False

            # 简单背离检测逻辑
            for i in range(20, len(df)):
                # 寻找局部极值点
                if (df['close_price'].iloc[i] < df['close_price'].iloc[i - 5:i].min() and
                        k_line.iloc[i] > k_line.iloc[i - 5:i].min()):
                    df.loc[df.index[i], 'Stochastic_Bullish_Divergence'] = True

                if (df['close_price'].iloc[i] > df['close_price'].iloc[i - 5:i].max() and
                        k_line.iloc[i] < k_line.iloc[i - 5:i].max()):
                    df.loc[df.index[i], 'Stochastic_Bearish_Divergence'] = True

        # 4. 动量信号
        k_momentum = k_line.diff()
        df['Stochastic_Momentum_Up'] = k_momentum > 0
        df['Stochastic_Momentum_Down'] = k_momentum < 0

        # 5. 交叉位置信号（在超买超卖区的交叉更有意义）
        df['Stochastic_Golden_in_Oversold'] = df['Stochastic_Golden_Cross'] & df['Stochastic_Oversold'].shift(1)
        df['Stochastic_Death_in_Overbought'] = df['Stochastic_Death_Cross'] & df['Stochastic_Overbought'].shift(1)

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        k_period = self.parameters.get('k_period')
        d_period = self.parameters.get('d_period')
        smoothing = self.parameters.get('smoothing')
        oversold_level = self.parameters.get('oversold_level')
        overbought_level = self.parameters.get('overbought_level')

        if not all(isinstance(p, int) and p > 0 for p in [k_period, d_period, smoothing]):
            logger.error("周期参数必须是正整数")
            return False

        if not (0 < oversold_level < overbought_level < 100):
            logger.error("超买超卖线必须在0-100之间，且超卖线小于超买线")
            return False

        if k_period <= smoothing:
            logger.warning("K周期应该大于平滑周期以获得更好效果")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return ['high_price', 'low_price', 'close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'Stochastic_K_Fast', 'Stochastic_D_Slow', 'Stochastic_K_Slow',
            'Stochastic_Overbought', 'Stochastic_Oversold',
            'Stochastic_Golden_Cross', 'Stochastic_Death_Cross',
            'Stochastic_Bullish_Divergence', 'Stochastic_Bearish_Divergence',
            'Stochastic_Momentum_Up', 'Stochastic_Momentum_Down',
            'Stochastic_Golden_in_Oversold', 'Stochastic_Death_in_Overbought'
        ]