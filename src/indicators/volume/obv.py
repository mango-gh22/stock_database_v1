# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/volume\obv.py
# File Name: obv
# @ Author: mango-gh22
# @ Date：2025/12/20 22:55
"""
desc 
"""
# src/indicators/volume/obv.py
"""
OBV（能量潮）指标 - 成交量指标
通过成交量变化预测价格趋势
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class OBV(BaseIndicator):
    """OBV（能量潮）指标"""

    def __init__(self,
                 price_column: str = 'close_price',
                 volume_column: str = 'volume',
                 ma_periods: List[int] = None,
                 use_sign_correction: bool = True):
        """
        初始化OBV指标

        Args:
            price_column: 价格列名
            volume_column: 成交量列名
            ma_periods: OBV的移动平均周期列表
            use_sign_correction: 是否使用符号修正
        """
        super().__init__("obv", IndicatorType.VOLUME)

        self.price_column = price_column
        self.volume_column = volume_column
        self.ma_periods = ma_periods or [5, 10, 20, 30]
        self.use_sign_correction = use_sign_correction
        self.requires_adjusted_price = False  # OBV不需要复权价格
        self.min_data_points = 5
        self.description = "OBV能量潮指标"

        # 设置参数
        self.parameters = {
            'price_column': price_column,
            'volume_column': volume_column,
            'ma_periods': self.ma_periods,
            'use_sign_correction': use_sign_correction
        }

        logger.info(f"初始化OBV指标: ma_periods={self.ma_periods}")

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算OBV指标

        Args:
            df: 包含价格和成交量的DataFrame

        Returns:
            包含OBV指标的DataFrame
        """
        logger.info("计算OBV指标")

        # 准备数据
        df = self.prepare_data(df)

        # 检查必要列
        required_cols = [self.price_column, self.volume_column]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            raise ValueError(f"数据中缺少列: {missing_cols}")

        result_df = df.copy()
        price_series = df[self.price_column]
        volume_series = df[self.volume_column]

        # 计算价格变化
        price_change = price_series.diff()

        # 计算OBV
        obv = self._calculate_obv(price_change, volume_series)
        result_df['OBV'] = obv

        # 计算OBV移动平均
        self._calculate_obv_moving_averages(result_df)

        # 计算OBV变化率
        self._calculate_obv_rate_of_change(result_df)

        # 添加OBV信号
        self._add_obv_signals(result_df)

        # 计算OBV与价格的背离
        self._calculate_obv_divergence(result_df, price_series)

        # 计算OBV统计信息
        self._calculate_obv_statistics(result_df)

        logger.info(f"OBV计算完成，范围: [{obv.min():.0f}, {obv.max():.0f}]")

        return result_df

    def _calculate_obv(self, price_change: pd.Series, volume: pd.Series) -> pd.Series:
        """计算OBV值"""
        obv = pd.Series(0.0, index=price_change.index)

        # 初始化第一个值
        if len(price_change) > 0:
            obv.iloc[0] = volume.iloc[0] if pd.notna(price_change.iloc[0]) else 0

        # 计算累积OBV
        for i in range(1, len(price_change)):
            if pd.isna(price_change.iloc[i]) or pd.isna(volume.iloc[i]):
                obv.iloc[i] = obv.iloc[i - 1]
                continue

            # 符号修正版本
            if self.use_sign_correction:
                if price_change.iloc[i] > 0:
                    obv.iloc[i] = obv.iloc[i - 1] + volume.iloc[i]
                elif price_change.iloc[i] < 0:
                    obv.iloc[i] = obv.iloc[i - 1] - volume.iloc[i]
                else:
                    obv.iloc[i] = obv.iloc[i - 1]
            else:
                # 传统版本
                sign = 1 if price_change.iloc[i] >= 0 else -1
                obv.iloc[i] = obv.iloc[i - 1] + (volume.iloc[i] * sign)

        return obv

    def _calculate_obv_moving_averages(self, df: pd.DataFrame):
        """计算OBV移动平均线"""
        if 'OBV' not in df.columns:
            return

        for period in self.ma_periods:
            if period <= 0:
                continue

            ma_col = f'OBV_MA{period}'

            # 简单移动平均
            df[ma_col] = df['OBV'].rolling(window=period, min_periods=max(3, period // 2)).mean()

            # 添加移动平均信号
            signal_col = f'OBV_MA{period}_SIGNAL'
            df[signal_col] = df['OBV'] > df[ma_col]

    def _calculate_obv_rate_of_change(self, df: pd.DataFrame):
        """计算OBV变化率"""
        if 'OBV' not in df.columns:
            return

        # 日变化率
        df['OBV_ROC'] = df['OBV'].pct_change() * 100

        # N日变化率
        for period in [5, 10, 20]:
            roc_col = f'OBV_ROC_{period}'
            df[roc_col] = (df['OBV'] / df['OBV'].shift(period) - 1) * 100

        # OBV动量（变化值）
        df['OBV_MOMENTUM'] = df['OBV'].diff()

        # OBV加速度（动量的变化）
        df['OBV_ACCELERATION'] = df['OBV_MOMENTUM'].diff()

    def _add_obv_signals(self, df: pd.DataFrame):
        """添加OBV交易信号"""
        if 'OBV' not in df.columns:
            return

        # OBV突破信号
        df['OBV_BREAKOUT'] = self._detect_obv_breakouts(df['OBV'])

        # OBV趋势信号
        df['OBV_TREND'] = self._calculate_obv_trend(df)

        # OBV与价格同向/反向
        if self.price_column in df.columns:
            df['OBV_PRICE_CONFIRMATION'] = self._check_price_confirmation(df)

        # OBV极端值信号
        df['OBV_EXTREME'] = self._detect_obv_extremes(df['OBV'])

    def _detect_obv_breakouts(self, obv_series: pd.Series, lookback: int = 20) -> pd.Series:
        """检测OBV突破"""
        breakouts = pd.Series(False, index=obv_series.index)

        if len(obv_series) < lookback + 5:
            return breakouts

        # 计算OBV的高点和低点
        for i in range(lookback, len(obv_series)):
            if pd.isna(obv_series.iloc[i]):
                continue

            window = obv_series.iloc[i - lookback:i]
            window_valid = window.dropna()

            if len(window_valid) < lookback // 2:
                continue

            current_obv = obv_series.iloc[i]
            window_max = window_valid.max()
            window_min = window_valid.min()

            # 突破前期高点
            if current_obv > window_max:
                breakouts.iloc[i] = True

            # 跌破前期低点
            elif current_obv < window_min:
                breakouts.iloc[i] = True

        return breakouts

    def _calculate_obv_trend(self, df: pd.DataFrame) -> pd.Series:
        """计算OBV趋势"""
        trend = pd.Series('unknown', index=df.index)

        if 'OBV' not in df.columns:
            return trend

        obv_series = df['OBV']

        # 使用多个时间窗口判断趋势
        for i in range(len(obv_series)):
            if pd.isna(obv_series.iloc[i]):
                continue

            # 短期趋势（5日）
            short_trend = 'neutral'
            if i >= 5:
                short_change = obv_series.iloc[i] - obv_series.iloc[i - 5]
                if short_change > 0:
                    short_trend = 'up'
                elif short_change < 0:
                    short_trend = 'down'

            # 中期趋势（20日）
            medium_trend = 'neutral'
            if i >= 20:
                medium_change = obv_series.iloc[i] - obv_series.iloc[i - 20]
                if medium_change > 0:
                    medium_trend = 'up'
                elif medium_change < 0:
                    medium_trend = 'down'

            # 综合判断
            if short_trend == 'up' and medium_trend == 'up':
                trend.iloc[i] = 'strong_bullish'
            elif short_trend == 'up':
                trend.iloc[i] = 'bullish'
            elif short_trend == 'down' and medium_trend == 'down':
                trend.iloc[i] = 'strong_bearish'
            elif short_trend == 'down':
                trend.iloc[i] = 'bearish'
            else:
                trend.iloc[i] = 'neutral'

        return trend

    def _check_price_confirmation(self, df: pd.DataFrame) -> pd.Series:
        """检查OBV与价格是否同向"""
        confirmation = pd.Series('unknown', index=df.index)

        if 'OBV' not in df.columns or self.price_column not in df.columns:
            return confirmation

        price = df[self.price_column]
        obv = df['OBV']

        for i in range(1, len(df)):
            if pd.isna(price.iloc[i]) or pd.isna(obv.iloc[i]) or \
                    pd.isna(price.iloc[i - 1]) or pd.isna(obv.iloc[i - 1]):
                continue

            price_change = price.iloc[i] - price.iloc[i - 1]
            obv_change = obv.iloc[i] - obv.iloc[i - 1]

            if price_change > 0 and obv_change > 0:
                confirmation.iloc[i] = 'bullish_confirmation'
            elif price_change < 0 and obv_change < 0:
                confirmation.iloc[i] = 'bearish_confirmation'
            elif price_change > 0 and obv_change < 0:
                confirmation.iloc[i] = 'bearish_divergence'
            elif price_change < 0 and obv_change > 0:
                confirmation.iloc[i] = 'bullish_divergence'
            else:
                confirmation.iloc[i] = 'neutral'

        return confirmation

    def _detect_obv_extremes(self, obv_series: pd.Series, lookback: int = 50) -> pd.Series:
        """检测OBV极端值"""
        extremes = pd.Series('normal', index=obv_series.index)

        if len(obv_series) < lookback:
            return extremes

        # 计算滚动分位数
        for i in range(lookback, len(obv_series)):
            if pd.isna(obv_series.iloc[i]):
                continue

            window = obv_series.iloc[i - lookback:i]
            window_valid = window.dropna()

            if len(window_valid) < lookback // 2:
                continue

            current_obv = obv_series.iloc[i]

            # 计算分位数
            lower_bound = np.percentile(window_valid, 10)
            upper_bound = np.percentile(window_valid, 90)

            if current_obv < lower_bound:
                extremes.iloc[i] = 'extremely_low'
            elif current_obv > upper_bound:
                extremes.iloc[i] = 'extremely_high'

        return extremes

    def _calculate_obv_divergence(self, df: pd.DataFrame, price_series: pd.Series):
        """计算OBV与价格的背离"""
        if 'OBV' not in df.columns:
            return

        # 价格创新高但OBV未创新高（顶背离）
        df['OBV_TOP_DIVERGENCE'] = False

        # 价格创新低但OBV未创新低（底背离）
        df['OBV_BOTTOM_DIVERGENCE'] = False

        # 需要足够的数据进行背离检测
        if len(df) < 30:
            return

        # 简单背离检测（实际应用需要更复杂的逻辑）
        lookback = 20

        for i in range(lookback, len(df) - 1):
            price_window = price_series.iloc[i - lookback:i + 1]
            obv_window = df['OBV'].iloc[i - lookback:i + 1]

            # 检查价格高点
            if price_window.idxmax() == df.index[i]:
                # 价格创新高，检查OBV是否同步
                if obv_window.idxmax() != df.index[i]:
                    df.loc[df.index[i], 'OBV_TOP_DIVERGENCE'] = True

            # 检查价格低点
            if price_window.idxmin() == df.index[i]:
                # 价格创新低，检查OBV是否同步
                if obv_window.idxmin() != df.index[i]:
                    df.loc[df.index[i], 'OBV_BOTTOM_DIVERGENCE'] = True

    def _calculate_obv_statistics(self, df: pd.DataFrame):
        """计算OBV统计信息"""
        if 'OBV' not in df.columns:
            return

        # OBV波动率
        df['OBV_VOLATILITY'] = df['OBV'].rolling(window=20, min_periods=10).std()

        # OBV与成交量的相关性
        if self.volume_column in df.columns:
            df['OBV_VOLUME_CORR'] = df['OBV'].rolling(window=20, min_periods=10).corr(df[self.volume_column])

        # OBV累积天数
        df['OBV_CUMULATIVE_DAYS'] = self._calculate_cumulative_days(df['OBV'])

    def _calculate_cumulative_days(self, obv_series: pd.Series) -> pd.Series:
        """计算OBV连续上涨/下跌天数"""
        cumulative_days = pd.Series(0, index=obv_series.index)

        if len(obv_series) < 2:
            return cumulative_days

        counter = 0
        trend = None  # 'up', 'down', or None

        for i in range(1, len(obv_series)):
            if pd.isna(obv_series.iloc[i]) or pd.isna(obv_series.iloc[i - 1]):
                counter = 0
                trend = None
                cumulative_days.iloc[i] = 0
                continue

            current_change = obv_series.iloc[i] - obv_series.iloc[i - 1]

            if current_change > 0:
                if trend == 'up':
                    counter += 1
                else:
                    counter = 1
                    trend = 'up'
            elif current_change < 0:
                if trend == 'down':
                    counter += 1
                else:
                    counter = 1
                    trend = 'down'
            else:
                counter = 0
                trend = None

            cumulative_days.iloc[i] = counter

        return cumulative_days

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        if not isinstance(self.ma_periods, list) or len(self.ma_periods) == 0:
            logger.error(f"MA周期列表不能为空: {self.ma_periods}")
            return False

        if not all(isinstance(p, int) and p > 0 for p in self.ma_periods):
            logger.error(f"MA周期必须是正整数: {self.ma_periods}")
            return False

        if self.use_sign_correction not in [True, False]:
            logger.warning(f"use_sign_correction参数应为布尔值: {self.use_sign_correction}")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column, self.volume_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        base_columns = ['OBV', 'OBV_ROC', 'OBV_MOMENTUM', 'OBV_ACCELERATION']

        ma_columns = []
        for period in self.ma_periods:
            ma_columns.extend([f'OBV_MA{period}', f'OBV_MA{period}_SIGNAL'])

        roc_columns = [f'OBV_ROC_{p}' for p in [5, 10, 20]]

        signal_columns = [
            'OBV_BREAKOUT', 'OBV_TREND', 'OBV_PRICE_CONFIRMATION',
            'OBV_EXTREME', 'OBV_TOP_DIVERGENCE', 'OBV_BOTTOM_DIVERGENCE'
        ]

        stat_columns = ['OBV_VOLATILITY', 'OBV_CUMULATIVE_DAYS']

        if self.volume_column:
            stat_columns.append('OBV_VOLUME_CORR')

        return base_columns + ma_columns + roc_columns + signal_columns + stat_columns

    def analyze_obv_pattern(self, df: pd.DataFrame) -> Dict:
        """
        分析OBV模式

        Args:
            df: 包含OBV的DataFrame

        Returns:
            OBV分析结果
        """
        analysis = {
            'current_obv': None,
            'trend': 'unknown',
            'confirmation': 'unknown',
            'momentum': 0,
            'signals': [],
            'divergence_detected': False
        }

        if df.empty or 'OBV' not in df.columns:
            return analysis

        # 获取最新数据
        last_row = df.iloc[-1]

        if pd.notna(last_row['OBV']):
            analysis['current_obv'] = round(last_row['OBV'], 0)

        # 趋势判断
        if 'OBV_TREND' in df.columns and last_row['OBV_TREND'] != 'unknown':
            analysis['trend'] = last_row['OBV_TREND']

            if 'strong' in last_row['OBV_TREND']:
                analysis['signals'].append('OBV趋势强烈')

        # 价格确认
        if 'OBV_PRICE_CONFIRMATION' in df.columns and last_row['OBV_PRICE_CONFIRMATION'] != 'unknown':
            analysis['confirmation'] = last_row['OBV_PRICE_CONFIRMATION']

            if 'confirmation' in last_row['OBV_PRICE_CONFIRMATION']:
                analysis['signals'].append('OBV与价格同向，趋势确认')
            elif 'divergence' in last_row['OBV_PRICE_CONFIRMATION']:
                analysis['divergence_detected'] = True
                analysis['signals'].append('OBV与价格背离，可能反转')

        # 动量
        if 'OBV_MOMENTUM' in df.columns and pd.notna(last_row['OBV_MOMENTUM']):
            analysis['momentum'] = round(last_row['OBV_MOMENTUM'], 0)

            if abs(last_row['OBV_MOMENTUM']) > 0:
                direction = '增加' if last_row['OBV_MOMENTUM'] > 0 else '减少'
                analysis['signals'].append(f'OBV动量{direction}')

        # 极端值
        if 'OBV_EXTREME' in df.columns and last_row['OBV_EXTREME'] != 'normal':
            analysis['signals'].append(f'OBV处于{last_row["OBV_EXTREME"]}水平')

        # 背离
        if 'OBV_TOP_DIVERGENCE' in df.columns and last_row['OBV_TOP_DIVERGENCE']:
            analysis['divergence_detected'] = True
            analysis['signals'].append('顶背离信号')
        elif 'OBV_BOTTOM_DIVERGENCE' in df.columns and last_row['OBV_BOTTOM_DIVERGENCE']:
            analysis['divergence_detected'] = True
            analysis['signals'].append('底背离信号')

        return analysis


