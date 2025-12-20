# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/volatility\bollinger_bands.py
# File Name: bollinger_bands
# @ Author: mango-gh22
# @ Date：2025/12/20 22:54
"""
desc 
"""
# src/indicators/volatility/bollinger_bands.py
"""
布林带指标 - 趋势和波动率指标
用于衡量价格波动性和识别趋势反转
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class BollingerBands(BaseIndicator):
    """布林带指标"""

    def __init__(self, period: int = 20,
                 std_dev: float = 2.0,
                 price_column: str = 'close_price',
                 ma_type: str = 'sma'):
        """
        初始化布林带指标

        Args:
            period: 移动平均周期，默认20天
            std_dev: 标准差倍数，默认2.0
            price_column: 价格列名
            ma_type: 移动平均类型，'sma'或'ema'
        """
        super().__init__("bollinger_bands", IndicatorType.VOLATILITY)

        self.period = period
        self.std_dev = std_dev
        self.price_column = price_column
        self.ma_type = ma_type.lower()
        self.requires_adjusted_price = True
        self.min_data_points = period + 5
        self.description = f"布林带({period}, {std_dev}σ)"

        # 设置参数
        self.parameters = {
            'period': period,
            'std_dev': std_dev,
            'price_column': price_column,
            'ma_type': ma_type
        }

        logger.info(f"初始化布林带: period={period}, std_dev={std_dev}, ma_type={ma_type}")

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算布林带

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含布林带的DataFrame
        """
        logger.info(f"计算布林带({self.period}, {self.std_dev}σ)")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 检查数据是否足够
        if len(price_series) < self.period:
            logger.warning(f"数据不足，需要至少{self.period}条数据，当前只有{len(price_series)}条")
            self._add_empty_bands(result_df)
            return result_df

        # 计算中轨（移动平均线）
        if self.ma_type == 'sma':
            middle_band = price_series.rolling(window=self.period).mean()
        elif self.ma_type == 'ema':
            middle_band = price_series.ewm(span=self.period, adjust=False).mean()
        else:
            logger.warning(f"不支持的MA类型: {self.ma_type}，使用SMA")
            middle_band = price_series.rolling(window=self.period).mean()

        result_df['BB_MIDDLE'] = middle_band

        # 计算标准差
        std = price_series.rolling(window=self.period).std()

        # 计算上下轨
        result_df['BB_UPPER'] = middle_band + (std * self.std_dev)
        result_df['BB_LOWER'] = middle_band - (std * self.std_dev)

        # 计算布林带宽度和位置
        self._calculate_band_metrics(result_df)

        # 添加交易信号
        self._add_bollinger_signals(result_df, price_series)

        # 计算统计信息
        self._calculate_statistics(result_df, price_series)

        logger.info(
            f"布林带计算完成，带宽范围: [{result_df['BB_WIDTH'].min():.2f}%, {result_df['BB_WIDTH'].max():.2f}%]")

        return result_df

    def _add_empty_bands(self, df: pd.DataFrame):
        """添加空的布林带列"""
        df['BB_MIDDLE'] = np.nan
        df['BB_UPPER'] = np.nan
        df['BB_LOWER'] = np.nan
        df['BB_WIDTH'] = np.nan
        df['BB_POSITION'] = np.nan
        df['BB_SQUEEZE'] = False

    def _calculate_band_metrics(self, df: pd.DataFrame):
        """计算布林带宽度和位置指标"""
        if 'BB_UPPER' not in df.columns or 'BB_LOWER' not in df.columns:
            return

        # 布林带宽度（百分比）
        df['BB_WIDTH'] = ((df['BB_UPPER'] - df['BB_LOWER']) / df['BB_MIDDLE']) * 100

        # 布林带位置（价格在中轨的位置，0-100）
        df['BB_POSITION'] = self._calculate_band_position(df)

        # 布林带收缩（带宽缩小）
        df['BB_SQUEEZE'] = self._detect_band_squeeze(df['BB_WIDTH'])

        # 布林带扩张（带宽扩大）
        df['BB_EXPANSION'] = self._detect_band_expansion(df['BB_WIDTH'])

    def _calculate_band_position(self, df: pd.DataFrame) -> pd.Series:
        """计算价格在布林带中的位置"""
        position = pd.Series(np.nan, index=df.index)

        if self.price_column not in df.columns:
            return position

        price = df[self.price_column]

        # 当上下轨有效时计算位置
        valid_mask = df['BB_UPPER'].notna() & df['BB_LOWER'].notna() & price.notna()
        if not valid_mask.any():
            return position

        # 位置 = (价格 - 下轨) / (上轨 - 下轨) * 100
        numerator = price[valid_mask] - df.loc[valid_mask, 'BB_LOWER']
        denominator = df.loc[valid_mask, 'BB_UPPER'] - df.loc[valid_mask, 'BB_LOWER']

        # 防止除零
        zero_denom = denominator == 0
        position_values = numerator / denominator.replace(0, np.nan) * 100

        # 处理分母为零的情况
        position_values[zero_denom] = 50  # 设为中间位置

        position[valid_mask] = position_values
        return position

    def _detect_band_squeeze(self, width_series: pd.Series, lookback: int = 20) -> pd.Series:
        """检测布林带收缩（波动率降低）"""
        squeeze = pd.Series(False, index=width_series.index)

        if len(width_series) < lookback + 5:
            return squeeze

        # 计算带宽的百分位
        for i in range(lookback, len(width_series)):
            window = width_series.iloc[i - lookback:i]
            current_width = width_series.iloc[i]

            if pd.notna(current_width) and len(window.dropna()) >= 10:
                # 如果当前宽度处于历史低位（后10%）
                percentile = (window < current_width).sum() / len(window.dropna())
                squeeze.iloc[i] = percentile < 0.1  # 低于10%分位

        return squeeze

    def _detect_band_expansion(self, width_series: pd.Series, lookback: int = 20) -> pd.Series:
        """检测布林带扩张（波动率增加）"""
        expansion = pd.Series(False, index=width_series.index)

        if len(width_series) < lookback + 5:
            return expansion

        # 计算带宽变化率
        width_change = width_series.pct_change()

        for i in range(lookback, len(width_series)):
            if pd.notna(width_change.iloc[i]) and pd.notna(width_series.iloc[i]):
                # 大幅扩张：带宽增加超过30%
                if width_change.iloc[i] > 0.3:
                    expansion.iloc[i] = True

                # 或者带宽处于历史高位（前10%）
                window = width_series.iloc[i - lookback:i]
                if len(window.dropna()) >= 10:
                    percentile = (window < width_series.iloc[i]).sum() / len(window.dropna())
                    if percentile > 0.9:  # 高于90%分位
                        expansion.iloc[i] = True

        return expansion

    def _add_bollinger_signals(self, df: pd.DataFrame, price_series: pd.Series):
        """添加布林带交易信号"""
        if 'BB_UPPER' not in df.columns or 'BB_LOWER' not in df.columns:
            return

        # 价格触及上轨（超买）
        df['BB_TOUCH_UPPER'] = price_series >= df['BB_UPPER']

        # 价格触及下轨（超卖）
        df['BB_TOUCH_LOWER'] = price_series <= df['BB_LOWER']

        # 价格突破上轨
        df['BB_BREAKOUT_UPPER'] = (price_series > df['BB_UPPER']) & \
                                  (price_series.shift(1) <= df['BB_UPPER'].shift(1))

        # 价格突破下轨
        df['BB_BREAKOUT_LOWER'] = (price_series < df['BB_LOWER']) & \
                                  (price_series.shift(1) >= df['BB_LOWER'].shift(1))

        # 价格回到布林带内
        df['BB_RETURN_INSIDE'] = ((price_series <= df['BB_UPPER']) & \
                                  (price_series >= df['BB_LOWER'])) & \
                                 ((price_series.shift(1) > df['BB_UPPER'].shift(1)) | \
                                  (price_series.shift(1) < df['BB_LOWER'].shift(1)))

        # 布林带交易策略信号
        self._add_strategy_signals(df, price_series)

    def _add_strategy_signals(self, df: pd.DataFrame, price_series: pd.Series):
        """添加布林带策略信号"""
        # 1. 布林带收缩后的突破（经典策略）
        if 'BB_SQUEEZE' in df.columns:
            df['BB_SQUEEZE_BREAKOUT'] = df['BB_SQUEEZE'] & \
                                        (df['BB_BREAKOUT_UPPER'] | df['BB_BREAKOUT_LOWER'])

        # 2. 价格从下轨反弹（买入信号）
        df['BB_BOUNCE_FROM_LOWER'] = (price_series > df['BB_LOWER']) & \
                                     (price_series.shift(1) <= df['BB_LOWER'].shift(1)) & \
                                     (price_series > price_series.shift(1))

        # 3. 价格从上轨回落（卖出信号）
        df['BB_REJECT_FROM_UPPER'] = (price_series < df['BB_UPPER']) & \
                                     (price_series.shift(1) >= df['BB_UPPER'].shift(1)) & \
                                     (price_series < price_series.shift(1))

        # 4. 价格在中轨获得支撑/阻力
        df['BB_SUPPORT_AT_MIDDLE'] = (price_series > df['BB_MIDDLE']) & \
                                     (price_series.shift(1) <= df['BB_MIDDLE'].shift(1))

        df['BB_RESISTANCE_AT_MIDDLE'] = (price_series < df['BB_MIDDLE']) & \
                                        (price_series.shift(1) >= df['BB_MIDDLE'].shift(1))

    def _calculate_statistics(self, df: pd.DataFrame, price_series: pd.Series):
        """计算布林带统计信息"""
        if 'BB_POSITION' not in df.columns:
            return

        # 价格在布林带内的持续时间
        df['BB_TIME_IN_BAND'] = self._calculate_time_in_band(df, price_series)

        # 布林带有效性指标
        df['BB_EFFECTIVENESS'] = self._calculate_band_effectiveness(df, price_series)

    def _calculate_time_in_band(self, df: pd.DataFrame, price_series: pd.Series) -> pd.Series:
        """计算价格在布林带内的连续天数"""
        time_in_band = pd.Series(0, index=df.index)

        if 'BB_UPPER' not in df.columns or 'BB_LOWER' not in df.columns:
            return time_in_band

        # 判断价格是否在布林带内
        in_band = (price_series <= df['BB_UPPER']) & (price_series >= df['BB_LOWER'])

        # 计算连续天数
        counter = 0
        for i in range(len(df)):
            if in_band.iloc[i]:
                counter += 1
                time_in_band.iloc[i] = counter
            else:
                counter = 0
                time_in_band.iloc[i] = 0

        return time_in_band

    def _calculate_band_effectiveness(self, df: pd.DataFrame, price_series: pd.Series) -> pd.Series:
        """计算布林带有效性（价格是否被限制在带内）"""
        effectiveness = pd.Series(np.nan, index=df.index)

        if 'BB_UPPER' not in df.columns or 'BB_LOWER' not in df.columns:
            return effectiveness

        # 使用滚动窗口计算有效性
        window_size = min(50, len(df))

        for i in range(window_size, len(df)):
            window_start = max(0, i - window_size)
            window_prices = price_series.iloc[window_start:i]
            window_upper = df['BB_UPPER'].iloc[window_start:i]
            window_lower = df['BB_LOWER'].iloc[window_start:i]

            # 计算价格在布林带内的比例
            in_band_ratio = ((window_prices <= window_upper) & \
                             (window_prices >= window_lower)).mean()

            effectiveness.iloc[i] = in_band_ratio

        return effectiveness

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        if not isinstance(self.period, int) or self.period <= 0:
            logger.error(f"布林带周期必须是正整数: {self.period}")
            return False

        if not isinstance(self.std_dev, (int, float)) or self.std_dev <= 0:
            logger.error(f"标准差倍数必须是正数: {self.std_dev}")
            return False

        if self.ma_type not in ['sma', 'ema']:
            logger.error(f"不支持的移动平均类型: {self.ma_type}")
            return False

        if self.std_dev < 1.5:
            logger.warning(f"标准差倍数{self.std_dev}较小，布林带可能过窄")
        elif self.std_dev > 3:
            logger.warning(f"标准差倍数{self.std_dev}较大，布林带可能过宽")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        base_columns = [
            'BB_MIDDLE', 'BB_UPPER', 'BB_LOWER',
            'BB_WIDTH', 'BB_POSITION', 'BB_SQUEEZE', 'BB_EXPANSION'
        ]

        signal_columns = [
            'BB_TOUCH_UPPER', 'BB_TOUCH_LOWER',
            'BB_BREAKOUT_UPPER', 'BB_BREAKOUT_LOWER',
            'BB_RETURN_INSIDE', 'BB_SQUEEZE_BREAKOUT',
            'BB_BOUNCE_FROM_LOWER', 'BB_REJECT_FROM_UPPER',
            'BB_SUPPORT_AT_MIDDLE', 'BB_RESISTANCE_AT_MIDDLE'
        ]

        stat_columns = ['BB_TIME_IN_BAND', 'BB_EFFECTIVENESS']

        return base_columns + signal_columns + stat_columns

    def analyze_band_structure(self, df: pd.DataFrame) -> Dict:
        """
        分析布林带结构

        Args:
            df: 包含布林带的DataFrame

        Returns:
            布林带分析结果
        """
        analysis = {
            'current_band_width': None,
            'band_position': None,
            'volatility_state': 'unknown',
            'squeeze_detected': False,
            'band_effectiveness': None,
            'signals': []
        }

        if df.empty or 'BB_WIDTH' not in df.columns or 'BB_POSITION' not in df.columns:
            return analysis

        # 获取最新数据
        last_row = df.iloc[-1]

        if pd.notna(last_row['BB_WIDTH']):
            analysis['current_band_width'] = round(last_row['BB_WIDTH'], 2)

            # 判断波动率状态
            if last_row['BB_WIDTH'] < 5:
                analysis['volatility_state'] = 'low'
            elif last_row['BB_WIDTH'] > 15:
                analysis['volatility_state'] = 'high'
            else:
                analysis['volatility_state'] = 'normal'

        if pd.notna(last_row['BB_POSITION']):
            analysis['band_position'] = round(last_row['BB_POSITION'], 1)

            # 位置判断
            if last_row['BB_POSITION'] > 80:
                analysis['signals'].append('价格接近上轨')
            elif last_row['BB_POSITION'] < 20:
                analysis['signals'].append('价格接近下轨')
            elif 40 < last_row['BB_POSITION'] < 60:
                analysis['signals'].append('价格在中轨附近')

        # 检查收缩
        if 'BB_SQUEEZE' in df.columns and last_row['BB_SQUEEZE']:
            analysis['squeeze_detected'] = True
            analysis['signals'].append('布林带收缩，波动率降低')

        # 检查扩张
        if 'BB_EXPANSION' in df.columns and last_row['BB_EXPANSION']:
            analysis['signals'].append('布林带扩张，波动率增加')

        # 有效性
        if 'BB_EFFECTIVENESS' in df.columns and pd.notna(last_row['BB_EFFECTIVENESS']):
            analysis['band_effectiveness'] = round(last_row['BB_EFFECTIVENESS'] * 100, 1)
            if last_row['BB_EFFECTIVENESS'] > 0.9:
                analysis['signals'].append('布林带约束力强')
            elif last_row['BB_EFFECTIVENESS'] < 0.7:
                analysis['signals'].append('布林带约束力弱')

        return analysis