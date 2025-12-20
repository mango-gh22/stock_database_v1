# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\macd.py
# File Name: macd
# @ Author: mango-gh22
# @ Date：2025/12/20 19:20
"""
desc 
"""

"""
MACD指标（指数平滑异同移动平均线）
用于判断股票价格趋势的强度和方向
"""
import pandas as pd
import numpy as np
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class MACD(BaseIndicator):
    """MACD指标"""

    def __init__(self, fast_period: int = 12,
                 slow_period: int = 26,
                 signal_period: int = 9,
                 price_column: str = 'close_price'):
        """
        初始化MACD指标

        Args:
            fast_period: 快速EMA周期，默认12
            slow_period: 慢速EMA周期，默认26
            signal_period: 信号线周期，默认9
            price_column: 价格列名
        """
        super().__init__("macd", IndicatorType.TREND)

        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.price_column = price_column
        self.requires_adjusted_price = True
        self.min_data_points = slow_period + signal_period + 10
        self.description = "MACD指标，包括DIF、DEA和MACD柱状图"

        # 设置参数
        self.parameters = {
            'fast_period': fast_period,
            'slow_period': slow_period,
            'signal_period': signal_period,
            'price_column': price_column
        }

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算MACD指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含MACD指标的DataFrame
        """
        logger.info(
            f"计算MACD指标，快线周期: {self.fast_period}, 慢线周期: {self.slow_period}, 信号周期: {self.signal_period}")

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

        # 计算MACD柱状图（histogram）
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

        # MACD背离检测（需要更多数据，这里简单实现）
        if len(df) > 30:
            # 价格创新高但MACD未创新高（顶背离）
            df['MACD_TOP_DIVERGENCE'] = False

            # 价格创新低但MACD未创新低（底背离）
            df['MACD_BOTTOM_DIVERGENCE'] = False

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        if not all(isinstance(p, int) and p > 0 for p in
                   [self.fast_period, self.slow_period, self.signal_period]):
            logger.error("MACD周期参数必须是正整数")
            return False

        if self.fast_period >= self.slow_period:
            logger.error("快速EMA周期必须小于慢速EMA周期")
            return False

        if self.signal_period <= 0:
            logger.error("信号线周期必须大于0")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return ['MACD_DIF', 'MACD_DEA', 'MACD_HIST',
                'MACD_GOLDEN_CROSS', 'MACD_DEATH_CROSS']