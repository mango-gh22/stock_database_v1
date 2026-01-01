# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\moving_average.py
# File Name: moving_arerage
# @ Author: mango-gh22
# @ Date：2025/12/17 6:15
"""
File: src/indicators/trend/moving_average.py (更新)
Desc: 移动平均线指标 - 适配新设计
"""
import pandas as pd
import numpy as np
from typing import List, Dict
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class MovingAverage(BaseIndicator):
    """移动平均线指标"""

    # 类属性
    name = "moving_average"
    indicator_type = IndicatorType.TREND
    description = "移动平均线指标"

    # 默认参数
    default_parameters = {
        'periods': [5, 10, 20, 30, 60],
        'price_column': 'close_price',
        'ma_type': 'sma'  # 'sma'或'ema'
    }

    def __init__(self, **parameters):
        """
        初始化移动平均线

        Args:
            periods: 周期列表，默认[5, 10, 20, 30, 60]
            price_column: 价格列名
            ma_type: 移动平均类型，'sma'或'ema'
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        self.periods = self.parameters['periods']
        self.price_column = self.parameters['price_column']
        self.ma_type = self.parameters['ma_type']
        self.requires_adjusted_price = True
        self.min_data_points = max(self.periods) + 10

        # 更新描述
        self.description = f"移动平均线({self.ma_type.upper()})，周期: {self.periods}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算移动平均线

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含移动平均线的DataFrame
        """
        logger.info(f"计算{self.ma_type.upper()}移动平均线，周期: {self.periods}")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 计算移动平均线
        for period in self.periods:
            if self.ma_type.lower() == 'ema':
                # 指数移动平均
                ma_value = price_series.ewm(span=period, adjust=False).mean()
                column_name = f'EMA_{period}'
            else:
                # 简单移动平均
                ma_value = price_series.rolling(window=period).mean()
                column_name = f'MA_{period}'

            result_df[column_name] = ma_value

            # 添加价格与均线的关系
            if period <= 60:  # 只对短期均线添加信号
                result_df[f'{column_name}_Signal'] = price_series > ma_value

        # 添加均线交叉信号
        if len(self.periods) >= 2:
            self._add_cross_signals(result_df)

        return result_df

    def _add_cross_signals(self, df: pd.DataFrame):
        """添加均线交叉信号"""
        # 金叉：短期均线上穿长期均线
        # 死叉：短期均线下穿长期均线
        periods = sorted(self.periods)

        for i in range(len(periods)):
            for j in range(i + 1, len(periods)):
                short_period = periods[i]
                long_period = periods[j]

                if self.ma_type.lower() == 'ema':
                    short_col = f'EMA_{short_period}'
                    long_col = f'EMA_{long_period}'
                else:
                    short_col = f'MA_{short_period}'
                    long_col = f'MA_{long_period}'

                if short_col in df.columns and long_col in df.columns:
                    # 金叉
                    df[f'{short_col}_CROSS_{long_col}_GOLDEN'] = (
                        (df[short_col] > df[long_col]) &
                        (df[short_col].shift(1) <= df[long_col].shift(1))
                    )
                    # 死叉
                    df[f'{short_col}_CROSS_{long_col}_DEATH'] = (
                        (df[short_col] < df[long_col]) &
                        (df[short_col].shift(1) >= df[long_col].shift(1))
                    )

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        periods = self.parameters['periods']

        if not isinstance(periods, list) or len(periods) == 0:
            logger.error("periods必须是包含至少一个整数的列表")
            return False

        if not all(isinstance(p, int) and p > 0 for p in periods):
            logger.error("所有周期必须是正整数")
            return False

        ma_type = self.parameters.get('ma_type', 'sma')
        if ma_type not in ['sma', 'ema']:
            logger.error("ma_type必须是'sma'或'ema'")
            return False

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        columns = []
        for period in self.periods:
            if self.ma_type.lower() == 'ema':
                columns.append(f'EMA_{period}')
                if period <= 60:
                    columns.append(f'EMA_{period}_Signal')
            else:
                columns.append(f'MA_{period}')
                if period <= 60:
                    columns.append(f'MA_{period}_Signal')

        # 添加交叉信号列
        periods = sorted(self.periods)
        for i in range(len(periods)):
            for j in range(i + 1, len(periods)):
                short_period = periods[i]
                long_period = periods[j]
                if self.ma_type.lower() == 'ema':
                    prefix = 'EMA'
                else:
                    prefix = 'MA'

                columns.append(f'{prefix}_{short_period}_CROSS_{prefix}_{long_period}_GOLDEN')
                columns.append(f'{prefix}_{short_period}_CROSS_{prefix}_{long_period}_DEATH')

        return columns