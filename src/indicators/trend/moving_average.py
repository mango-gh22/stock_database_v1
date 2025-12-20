# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\moving_average.py
# File Name: moving_arerage
# @ Author: mango-gh22
# @ Date：2025/12/17 6:15
"""
desc 
"""
"""
移动平均线指标
实现简单移动平均线(SMA)和指数移动平均线(EMA)
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))  # 添加项目根
from src.indicators.base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class MovingAverage(BaseIndicator):
    """移动平均线指标"""

    def __init__(self,
                 periods: List[int] = None,
                 ma_types: List[str] = None,
                 price_column: str = 'close_price'):
        """
        初始化移动平均线指标

        Args:
            periods: 计算周期列表，默认 [5, 10, 20, 30, 60, 120, 250]
            ma_types: 移动平均线类型，['sma', 'ema', 'wma']
            price_column: 价格列名
        """
        super().__init__("moving_average", IndicatorType.TREND)

        self.periods = periods or [5, 10, 20, 30, 60, 120, 250]
        self.ma_types = ma_types or ['sma', 'ema']
        self.price_column = price_column
        self.requires_adjusted_price = True
        self.min_data_points = max(self.periods) if self.periods else 250
        self.description = "移动平均线指标，包括SMA、EMA等"

        # 设置参数
        self.parameters = {
            'periods': self.periods,
            'ma_types': self.ma_types,
            'price_column': self.price_column
        }

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算移动平均线

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含移动平均线的DataFrame
        """
        logger.info(f"计算移动平均线，周期: {self.periods}, 类型: {self.ma_types}")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 计算各种移动平均线
        for period in self.periods:
            if period > len(price_series):
                logger.warning(f"周期 {period} 大于数据长度 {len(price_series)}，跳过")
                continue

            for ma_type in self.ma_types:
                column_name = self._get_column_name(ma_type, period)

                if ma_type == 'sma':
                    # 简单移动平均
                    result_df[column_name] = price_series.rolling(window=period).mean()

                elif ma_type == 'ema':
                    # 指数移动平均
                    result_df[column_name] = price_series.ewm(
                        span=period, adjust=False).mean()

                elif ma_type == 'wma':
                    # 加权移动平均
                    weights = np.arange(1, period + 1)
                    result_df[column_name] = price_series.rolling(window=period).apply(
                        lambda x: np.dot(x, weights) / weights.sum(), raw=True)

        # 计算移动平均线交叉信号
        if len(self.periods) >= 2:
            self._add_cross_signals(result_df)

        return result_df

    def _get_column_name(self, ma_type: str, period: int) -> str:
        """获取列名"""
        ma_type_map = {
            'sma': 'MA',
            'ema': 'EMA',
            'wma': 'WMA'
        }
        prefix = ma_type_map.get(ma_type, ma_type.upper())
        return f"{prefix}{period}"

    def _add_cross_signals(self, df: pd.DataFrame):
        """添加移动平均线交叉信号"""
        periods = sorted(self.periods)

        if len(periods) >= 2:
            # 计算短期和长期均线
            short_ma = self._get_column_name(self.ma_types[0], periods[0])
            long_ma = self._get_column_name(self.ma_types[0], periods[1])

            if short_ma in df.columns and long_ma in df.columns:
                # 金叉：短期均线上穿长期均线
                df['MA_CROSS_GOLDEN'] = (df[short_ma] > df[long_ma]) & \
                                        (df[short_ma].shift(1) <= df[long_ma].shift(1))

                # 死叉：短期均线下穿长期均线
                df['MA_CROSS_DEATH'] = (df[short_ma] < df[long_ma]) & \
                                       (df[short_ma].shift(1) >= df[long_ma].shift(1))

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        if not self.periods:
            logger.error("周期列表不能为空")
            return False

        if not all(isinstance(p, int) and p > 0 for p in self.periods):
            logger.error("周期必须是正整数")
            return False

        valid_ma_types = ['sma', 'ema', 'wma']
        if not all(mt in valid_ma_types for mt in self.ma_types):
            logger.error(f"移动平均线类型必须是: {valid_ma_types}")
            return False

        return True

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        columns = []
        for period in self.periods:
            for ma_type in self.ma_types:
                columns.append(self._get_column_name(ma_type, period))

        if len(self.periods) >= 2:
            columns.extend(['MA_CROSS_GOLDEN', 'MA_CROSS_DEATH'])

        return columns