# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/momentum\cci.py
# File Name: cci
# @ Author: mango-gh22
# @ Date：2025/12/21 9:48
"""
desc cci.py（商品通道指数）- 动量指标
"""
import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class CCI(BaseIndicator):
    """商品通道指数（Commodity Channel Index）"""

    # 类属性
    name = "cci"
    indicator_type = IndicatorType.MOMENTUM
    description = "商品通道指数（Commodity Channel Index）"

    # 默认参数
    default_parameters = {
        'period': 20,
        'constant': 0.015,
        'overbought_level': 100,
        'oversold_level': -100
    }

    def __init__(self, **parameters):
        """
        初始化CCI指标

        Args:
            period: 计算周期，默认20
            constant: 常数因子，默认0.015
            overbought_level: 超买线，默认100
            oversold_level: 超卖线，默认-100
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.period = self.parameters['period']
        self.constant = self.parameters['constant']
        self.overbought_level = self.parameters['overbought_level']
        self.oversold_level = self.parameters['oversold_level']
        self.requires_adjusted_price = False
        self.min_data_points = self.period + 10
        self.description = f"商品通道指数，周期={self.period}，超买={self.overbought_level}，超卖={self.oversold_level}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算CCI指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含CCI指标的DataFrame
        """
        logger.info(f"计算CCI指标，周期: {self.period}, 常数: {self.constant}, "
                    f"超买: {self.overbought_level}, 超卖: {self.oversold_level}")

        # 准备数据
        df = self.prepare_data(df)

        # 验证必要列
        required_cols = ['high_price', 'low_price', 'close_price']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必要列: {missing_cols}")

        result_df = df.copy()

        # 计算典型价格（Typical Price）
        typical_price = (df['high_price'] + df['low_price'] + df['close_price']) / 3

        # 计算简单移动平均
        sma = typical_price.rolling(window=self.period).mean()

        # 计算平均偏差
        mean_deviation = typical_price.rolling(window=self.period).apply(
            lambda x: np.mean(np.abs(x - np.mean(x)))
        )

        # 计算CCI值
        # 避免除零
        mean_deviation = mean_deviation.replace(0, 1e-10)
        cci = (typical_price - sma) / (self.constant * mean_deviation)

        result_df['CCI'] = cci

        # 添加信号
        self._add_cci_signals(result_df)

        return result_df

    def _add_cci_signals(self, df: pd.DataFrame):
        """添加CCI信号"""
        if 'CCI' not in df.columns:
            return

        cci = df['CCI']

        # 1. 超买超卖信号
        df['CCI_Overbought'] = cci >= self.overbought_level
        df['CCI_Oversold'] = cci <= self.oversold_level

        # 2. 零轴交叉信号
        df['CCI_Above_Zero'] = cci > 0
        df['CCI_Below_Zero'] = cci < 0
        df['CCI_Cross_Zero_Up'] = (cci > 0) & (cci.shift(1) <= 0)
        df['CCI_Cross_Zero_Down'] = (cci < 0) & (cci.shift(1) >= 0)

        # 3. 动量变化信号
        cci_diff = cci.diff()
        df['CCI_Momentum_Up'] = cci_diff > 0
        df['CCI_Momentum_Down'] = cci_diff < 0

        # 4. 背离检测
        if 'close_price' in df.columns and len(df) > 30:
            df['CCI_Bullish_Divergence'] = False
            df['CCI_Bearish_Divergence'] = False

            # 寻找局部极值点
            for i in range(20, len(df)):
                # 价格创新低但CCI未创新低（底背离）
                if (df['close_price'].iloc[i] < df['close_price'].iloc[i - 10:i].min() and
                        cci.iloc[i] > cci.iloc[i - 10:i].min()):
                    df.loc[df.index[i], 'CCI_Bullish_Divergence'] = True

                # 价格创新高但CCI未创新高（顶背离）
                if (df['close_price'].iloc[i] > df['close_price'].iloc[i - 10:i].max() and
                        cci.iloc[i] < cci.iloc[i - 10:i].max()):
                    df.loc[df.index[i], 'CCI_Bearish_Divergence'] = True

        # 5. 趋势强度
        cci_abs = abs(cci)
        df['CCI_Trend_Strength'] = cci_abs.rolling(window=5).mean()

        # 6. 超买超卖区间的持续时间
        df['CCI_Overbought_Duration'] = 0
        df['CCI_Oversold_Duration'] = 0

        overbought_count = 0
        oversold_count = 0

        for i in range(len(df)):
            if df['CCI_Overbought'].iloc[i]:
                overbought_count += 1
                oversold_count = 0
            elif df['CCI_Oversold'].iloc[i]:
                oversold_count += 1
                overbought_count = 0
            else:
                overbought_count = 0
                oversold_count = 0

            df.loc[df.index[i], 'CCI_Overbought_Duration'] = overbought_count
            df.loc[df.index[i], 'CCI_Oversold_Duration'] = oversold_count

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        period = self.parameters.get('period')
        constant = self.parameters.get('constant')
        oversold_level = self.parameters.get('oversold_level')
        overbought_level = self.parameters.get('overbought_level')

        if not isinstance(period, int) or period <= 0:
            logger.error("周期必须是正整数")
            return False

        if constant <= 0:
            logger.error("常数因子必须大于0")
            return False

        if oversold_level >= overbought_level:
            logger.error("超卖线必须小于超买线")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return ['high_price', 'low_price', 'close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'CCI', 'CCI_Overbought', 'CCI_Oversold',
            'CCI_Above_Zero', 'CCI_Below_Zero',
            'CCI_Cross_Zero_Up', 'CCI_Cross_Zero_Down',
            'CCI_Momentum_Up', 'CCI_Momentum_Down',
            'CCI_Bullish_Divergence', 'CCI_Bearish_Divergence',
            'CCI_Trend_Strength', 'CCI_Overbought_Duration',
            'CCI_Oversold_Duration'
        ]