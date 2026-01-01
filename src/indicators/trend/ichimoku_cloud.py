# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/trend\ichimoku_cloud.py
# File Name: ichimoku_cloud
# @ Author: mango-gh22
# @ Date：2025/12/21 9:43
"""
desc 一目均衡表（Ichimoku Cloud）- 日本技术分析指标
"""
import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class IchimokuCloud(BaseIndicator):
    """一目均衡表（Ichimoku Cloud）"""

    # 类属性
    name = "ichimoku_cloud"
    indicator_type = IndicatorType.TREND
    description = "一目均衡表（Ichimoku Cloud）"

    # 默认参数
    default_parameters = {
        'tenkan_period': 9,
        'kijun_period': 26,
        'senkou_span_b_period': 52,
        'displacement': 26
    }

    def __init__(self, **parameters):
        """
        初始化一目均衡表

        Args:
            tenkan_period: 转换线周期，默认9
            kijun_period: 基准线周期，默认26
            senkou_span_b_period: 先行带B周期，默认52
            displacement: 先行带位移，默认26
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}
        super().__init__(**merged_params)

        # 解包参数
        self.tenkan_period = self.parameters['tenkan_period']
        self.kijun_period = self.parameters['kijun_period']
        self.senkou_span_b_period = self.parameters['senkou_span_b_period']
        self.displacement = self.parameters['displacement']
        self.requires_adjusted_price = False
        self.min_data_points = max(self.tenkan_period, self.kijun_period,
                                   self.senkou_span_b_period) + self.displacement + 10
        self.description = f"一目均衡表，转换线={self.tenkan_period}，基准线={self.kijun_period}，先行带B={self.senkou_span_b_period}"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算一目均衡表

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含一目均衡表指标的DataFrame
        """
        logger.info(f"计算Ichimoku Cloud，参数: 转换线={self.tenkan_period}, 基准线={self.kijun_period}, "
                    f"先行带B={self.senkou_span_b_period}, 位移={self.displacement}")

        # 准备数据
        df = self.prepare_data(df)

        # 验证必要列
        required_cols = ['high_price', 'low_price', 'close_price']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少必要列: {missing_cols}")

        result_df = df.copy()

        # 计算5条线
        high = df['high_price']
        low = df['low_price']

        # 1. 转换线（Tenkan-sen）
        tenkan_sen = self._calculate_pivot_line(high, low, self.tenkan_period)
        result_df['Ichimoku_Tenkan'] = tenkan_sen

        # 2. 基准线（Kijun-sen）
        kijun_sen = self._calculate_pivot_line(high, low, self.kijun_period)
        result_df['Ichimoku_Kijun'] = kijun_sen

        # 3. 先行带A（Senkou Span A）- 提前26期
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(self.displacement)
        result_df['Ichimoku_Senkou_A'] = senkou_span_a

        # 4. 先行带B（Senkou Span B）- 提前26期
        senkou_span_b = self._calculate_pivot_line(high, low, self.senkou_span_b_period).shift(self.displacement)
        result_df['Ichimoku_Senkou_B'] = senkou_span_b

        # 5. 延迟线（Chikou Span）
        chikou_span = df['close_price'].shift(-self.displacement)
        result_df['Ichimoku_Chikou'] = chikou_span

        # 添加信号和分析
        self._add_ichimoku_signals(result_df)
        self._add_cloud_analysis(result_df)

        return result_df

    def _calculate_pivot_line(self, high: pd.Series, low: pd.Series, period: int) -> pd.Series:
        """计算枢轴线（最高价与最低价的平均值）"""
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        return (highest_high + lowest_low) / 2

    def _add_ichimoku_signals(self, df: pd.DataFrame):
        """添加一目均衡表信号"""
        # 1. 转换线与基准线的交叉
        df['Tenkan_Kijun_Cross_Up'] = (df['Ichimoku_Tenkan'] > df['Ichimoku_Kijun']) & \
                                      (df['Ichimoku_Tenkan'].shift(1) <= df['Ichimoku_Kijun'].shift(1))
        df['Tenkan_Kijun_Cross_Down'] = (df['Ichimoku_Tenkan'] < df['Ichimoku_Kijun']) & \
                                        (df['Ichimoku_Tenkan'].shift(1) >= df['Ichimoku_Kijun'].shift(1))

        # 2. 价格与云层的关系
        if 'Ichimoku_Senkou_A' in df.columns and 'Ichimoku_Senkou_B' in df.columns:
            # 计算云层顶部和底部
            df['Cloud_Top'] = df[['Ichimoku_Senkou_A', 'Ichimoku_Senkou_B']].max(axis=1)
            df['Cloud_Bottom'] = df[['Ichimoku_Senkou_A', 'Ichimoku_Senkou_B']].min(axis=1)

            # 价格与云层位置
            if 'close_price' in df.columns:
                df['Price_Above_Cloud'] = df['close_price'] > df['Cloud_Top']
                df['Price_Below_Cloud'] = df['close_price'] < df['Cloud_Bottom']
                df['Price_In_Cloud'] = (~df['Price_Above_Cloud']) & (~df['Price_Below_Cloud'])

        # 3. 延迟线信号
        if 'Ichimoku_Chikou' in df.columns and 'close_price' in df.columns:
            df['Chikou_Above_Price'] = df['Ichimoku_Chikou'] > df['close_price']
            df['Chikou_Below_Price'] = df['Ichimoku_Chikou'] < df['close_price']

    def _add_cloud_analysis(self, df: pd.DataFrame):
        """添加云层分析"""
        if 'Ichimoku_Senkou_A' in df.columns and 'Ichimoku_Senkou_B' in df.columns:
            # 云层厚度
            df['Cloud_Thickness'] = abs(df['Ichimoku_Senkou_A'] - df['Ichimoku_Senkou_B'])

            # 云层颜色（A在B之上为绿色看涨，反之为红色看跌）
            df['Cloud_Color_Green'] = df['Ichimoku_Senkou_A'] > df['Ichimoku_Senkou_B']
            df['Cloud_Color_Red'] = df['Ichimoku_Senkou_A'] < df['Ichimoku_Senkou_B']

            # 云层未来形态
            df['Future_Cloud_Top'] = df[['Ichimoku_Senkou_A', 'Ichimoku_Senkou_B']].max(axis=1).shift(
                -self.displacement)
            df['Future_Cloud_Bottom'] = df[['Ichimoku_Senkou_A', 'Ichimoku_Senkou_B']].min(axis=1).shift(
                -self.displacement)

            # 未来云层颜色
            df['Future_Cloud_Green'] = (df['Ichimoku_Senkou_A'].shift(-self.displacement) >
                                        df['Ichimoku_Senkou_B'].shift(-self.displacement))

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        # 通过 self.parameters 访问参数
        tenkan_period = self.parameters.get('tenkan_period')
        kijun_period = self.parameters.get('kijun_period')
        senkou_span_b_period = self.parameters.get('senkou_span_b_period')
        displacement = self.parameters.get('displacement')

        periods = [tenkan_period, kijun_period, senkou_span_b_period]

        if not all(isinstance(p, int) and p > 0 for p in periods):
            logger.error("所有周期参数必须是正整数")
            return False

        if not (0 < displacement <= 100):
            logger.error("位移参数必须在1-100之间")
            return False

        # 验证周期关系
        if tenkan_period >= kijun_period:
            logger.warning("转换线周期通常小于基准线周期")

        if kijun_period >= senkou_span_b_period:
            logger.warning("基准线周期通常小于先行带B周期")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return ['high_price', 'low_price', 'close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'Ichimoku_Tenkan', 'Ichimoku_Kijun', 'Ichimoku_Senkou_A',
            'Ichimoku_Senkou_B', 'Ichimoku_Chikou', 'Tenkan_Kijun_Cross_Up',
            'Tenkan_Kijun_Cross_Down', 'Cloud_Top', 'Cloud_Bottom',
            'Price_Above_Cloud', 'Price_Below_Cloud', 'Price_In_Cloud',
            'Chikou_Above_Price', 'Chikou_Below_Price', 'Cloud_Thickness',
            'Cloud_Color_Green', 'Cloud_Color_Red'
        ]