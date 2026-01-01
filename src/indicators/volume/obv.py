# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/volume\obv.py
# File Name: obv
# @ Author: mango-gh22
# @ Date：2025/12/20 22:55
"""
File: src/indicators/volume/obv.py (更新)
Desc: OBV指标 - 适配新设计
"""

import pandas as pd
import numpy as np
from typing import List, Dict
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class OBV(BaseIndicator):
    """OBV指标（能量潮）"""

    # 类属性
    name = "obv"
    indicator_type = IndicatorType.VOLUME
    description = "能量潮指标(OBV)"

    # 默认参数
    default_parameters = {
        'price_column': 'close_price',
        'volume_column': 'volume'
    }

    def __init__(self, **parameters):
        """
        初始化OBV指标

        Args:
            price_column: 价格列名
            volume_column: 成交量列名
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        self.price_column = self.parameters['price_column']
        self.volume_column = self.parameters['volume_column']
        self.requires_adjusted_price = False
        self.min_data_points = 2
        self.description = "能量潮指标(OBV)"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算OBV指标

        Args:
            df: 包含价格和成交量数据的DataFrame

        Returns:
            包含OBV指标的DataFrame
        """
        logger.info("计算OBV指标")

        # 准备数据
        df = self.prepare_data(df)

        required_cols = [self.price_column, self.volume_column]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必要列: {missing_cols}")

        result_df = df.copy()
        price_series = df[self.price_column]
        volume_series = df[self.volume_column]

        # 计算价格变化
        price_change = price_series.diff()

        # 计算OBV
        obv = np.zeros(len(df))
        obv[0] = volume_series.iloc[0]

        for i in range(1, len(df)):
            if price_change.iloc[i] > 0:  # 价格上涨
                obv[i] = obv[i - 1] + volume_series.iloc[i]
            elif price_change.iloc[i] < 0:  # 价格下跌
                obv[i] = obv[i - 1] - volume_series.iloc[i]
            else:  # 价格不变
                obv[i] = obv[i - 1]

        result_df['OBV'] = obv

        # 添加信号
        self._add_obv_signals(result_df)

        return result_df

    def _add_obv_signals(self, df: pd.DataFrame):
        """添加OBV信号"""
        if 'OBV' not in df.columns:
            return

        obv = df['OBV']

        # OBV变化方向
        obv_change = obv.diff()
        df['OBV_Increasing'] = obv_change > 0
        df['OBV_Decreasing'] = obv_change < 0

        # OBV移动平均（用于趋势判断）
        if len(df) >= 20:
            obv_ma20 = obv.rolling(window=20).mean()
            df['OBV_MA20'] = obv_ma20
            df['OBV_Above_MA20'] = obv > obv_ma20
            df['OBV_Below_MA20'] = obv < obv_ma20

        # OBV与价格背离检测
        if self.price_column in df.columns and len(df) > 30:
            price = df[self.price_column]
            df['OBV_Bullish_Divergence'] = False
            df['OBV_Bearish_Divergence'] = False

            # 简单背离检测
            for i in range(20, len(df)):
                # 价格创新低但OBV未创新低（底背离）
                if (price.iloc[i] < price.iloc[i - 10:i].min() and
                        obv.iloc[i] > obv.iloc[i - 10:i].min()):
                    df.loc[df.index[i], 'OBV_Bullish_Divergence'] = True

                # 价格创新高但OBV未创新高（顶背离）
                if (price.iloc[i] > price.iloc[i - 10:i].max() and
                        obv.iloc[i] < obv.iloc[i - 10:i].max()):
                    df.loc[df.index[i], 'OBV_Bearish_Divergence'] = True

        # OBV突破信号
        if 'OBV_MA20' in df.columns:
            df['OBV_Break_Above_MA20'] = (obv > df['OBV_MA20']) & \
                                         (obv.shift(1) <= df['OBV_MA20'].shift(1))
            df['OBV_Break_Below_MA20'] = (obv < df['OBV_MA20']) & \
                                         (obv.shift(1) >= df['OBV_MA20'].shift(1))

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        return True  # OBV参数简单，总是有效

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column, self.volume_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        base_cols = ['OBV', 'OBV_Increasing', 'OBV_Decreasing']

        if len(df) >= 20:  # 动态添加MA相关列
            base_cols.extend(['OBV_MA20', 'OBV_Above_MA20', 'OBV_Below_MA20',
                              'OBV_Break_Above_MA20', 'OBV_Break_Below_MA20'])

        base_cols.extend(['OBV_Bullish_Divergence', 'OBV_Bearish_Divergence'])

        return base_cols
