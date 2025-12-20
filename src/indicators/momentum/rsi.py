# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/momentum\rsi.py
# File Name: rsi
# @ Author: mango-gh22
# @ Date：2025/12/20 22:42
"""
desc 
"""

# src/indicators/momentum/rsi.py
"""
RSI（相对强弱指数）指标
用于衡量价格变动的速度和幅度，识别超买超卖状态
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class RSI(BaseIndicator):
    """RSI（相对强弱指数）指标"""

    def __init__(self, period: int = 14,
                 overbought: float = 70.0,
                 oversold: float = 30.0,
                 price_column: str = 'close_price'):
        """
        初始化RSI指标

        Args:
            period: RSI计算周期，默认14天
            overbought: 超买阈值，默认70
            oversold: 超卖阈值，默认30
            price_column: 价格列名
        """
        super().__init__("rsi", IndicatorType.MOMENTUM)

        self.period = period
        self.overbought = overbought
        self.oversold = oversold
        self.price_column = price_column
        self.requires_adjusted_price = True
        self.min_data_points = period + 10  # 需要足够数据计算
        self.description = f"RSI({period})相对强弱指数"

        # 设置参数
        self.parameters = {
            'period': period,
            'overbought': overbought,
            'oversold': oversold,
            'price_column': price_column
        }

        logger.info(f"初始化RSI指标: period={period}, overbought={overbought}, oversold={oversold}")

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算RSI指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含RSI指标的DataFrame
        """
        logger.info(f"计算RSI({self.period})指标")

        # 准备数据
        df = self.prepare_data(df)

        if self.price_column not in df.columns:
            raise ValueError(f"数据中缺少价格列: {self.price_column}")

        result_df = df.copy()
        price_series = df[self.price_column]

        # 检查数据是否足够
        if len(price_series) < self.period:
            logger.warning(f"数据不足，需要至少{self.period}条数据，当前只有{len(price_series)}条")
            result_df['RSI'] = np.nan
            return result_df

        # 计算价格变化
        price_diff = price_series.diff()

        # 分离上涨和下跌
        gains = price_diff.where(price_diff > 0, 0)
        losses = -price_diff.where(price_diff < 0, 0)

        # 计算平均上涨和下跌（使用指数移动平均）
        avg_gain = gains.ewm(alpha=1 / self.period, adjust=False).mean()
        avg_loss = losses.ewm(alpha=1 / self.period, adjust=False).mean()

        # 计算相对强弱（RS）
        rs = avg_gain / (avg_loss + 1e-10)  # 防止除零

        # 计算RSI
        rsi = 100 - (100 / (1 + rs))
        result_df['RSI'] = rsi

        # 添加RSI信号
        self._add_rsi_signals(result_df)

        # 计算RSI的衍生指标
        self._add_rsi_derivatives(result_df)

        logger.info(f"RSI计算完成，范围: [{rsi.min():.2f}, {rsi.max():.2f}]")

        return result_df

    def _add_rsi_signals(self, df: pd.DataFrame):
        """添加RSI交易信号"""
        if 'RSI' not in df.columns:
            return

        # 超买信号
        df['RSI_OVERBOUGHT'] = df['RSI'] >= self.overbought

        # 超卖信号
        df['RSI_OVERSOLD'] = df['RSI'] <= self.oversold

        # RSI金叉：从超卖区间上穿超卖线
        df['RSI_GOLDEN_CROSS'] = (df['RSI'] > self.oversold) & \
                                 (df['RSI'].shift(1) <= self.oversold)

        # RSI死叉：从超买区间下穿超买线
        df['RSI_DEATH_CROSS'] = (df['RSI'] < self.overbought) & \
                                (df['RSI'].shift(1) >= self.overbought)

        # RSI背离检测（简化版）
        self._add_divergence_signals(df)

        # RSI趋势状态
        df['RSI_TREND'] = self._calculate_rsi_trend(df['RSI'])

    def _add_divergence_signals(self, df: pd.DataFrame, lookback: int = 20):
        """添加RSI背离信号（简化实现）"""
        if 'RSI' not in df.columns or len(df) < lookback * 2:
            return

        # 顶背离：价格创新高，RSI未创新高
        df['RSI_TOP_DIVERGENCE'] = False

        # 底背离：价格创新低，RSI未创新低
        df['RSI_BOTTOM_DIVERGENCE'] = False

        # 简单背离检测（实际应用需要更复杂的逻辑）
        if 'close_price' in df.columns:
            for i in range(lookback, len(df) - lookback):
                # 检查价格高点
                price_window = df['close_price'].iloc[i - lookback:i + 1]
                rsi_window = df['RSI'].iloc[i - lookback:i + 1]

                if price_window.idxmax() == df.index[i]:
                    # 价格创新高，检查RSI是否同步
                    if rsi_window.idxmax() != df.index[i]:
                        df.loc[df.index[i], 'RSI_TOP_DIVERGENCE'] = True

    def _calculate_rsi_trend(self, rsi_series: pd.Series) -> pd.Series:
        """计算RSI趋势状态"""
        trend = pd.Series('neutral', index=rsi_series.index)

        if len(rsi_series) < 3:
            return trend

        # 使用RSI的简单移动平均判断趋势
        rsi_ma = rsi_series.rolling(window=5, min_periods=3).mean()

        # 判断趋势
        trend = pd.Series('neutral', index=rsi_series.index)

        # RSI > 50且上升为上升趋势
        trend[(rsi_series > 50) & (rsi_series > rsi_series.shift(1))] = 'bullish'

        # RSI < 50且下降为下降趋势
        trend[(rsi_series < 50) & (rsi_series < rsi_series.shift(1))] = 'bearish'

        return trend

    def _add_rsi_derivatives(self, df: pd.DataFrame):
        """添加RSI衍生指标"""
        if 'RSI' not in df.columns:
            return

        # 1. RSI的移动平均（平滑RSI）
        df['RSI_MA5'] = df['RSI'].rolling(window=5, min_periods=3).mean()
        df['RSI_MA10'] = df['RSI'].rolling(window=10, min_periods=5).mean()

        # 2. RSI动量（变化率）
        df['RSI_MOMENTUM'] = df['RSI'].diff()

        # 3. RSI标准差（波动率）
        df['RSI_STD'] = df['RSI'].rolling(window=10, min_periods=5).std()

        # 4. RSI分位数（当前位置）
        for window in [20, 50, 100]:
            if len(df) >= window:
                col_name = f'RSI_PERCENTILE_{window}'
                df[col_name] = df['RSI'].rolling(window=window).apply(
                    lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min() + 1e-10) * 100,
                    raw=False
                )

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        if not isinstance(self.period, int) or self.period <= 0:
            logger.error(f"RSI周期必须是正整数: {self.period}")
            return False

        if not (0 < self.oversold < self.overbought < 100):
            logger.error(f"超买超卖阈值无效: oversold={self.oversold}, overbought={self.overbought}")
            logger.error("必须满足: 0 < oversold < overbought < 100")
            return False

        if self.oversold >= 50 or self.overbought <= 50:
            logger.warning(f"超买超卖阈值距离50过近，建议: oversold<30, overbought>70")

        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        base_columns = ['RSI', 'RSI_OVERBOUGHT', 'RSI_OVERSOLD',
                        'RSI_GOLDEN_CROSS', 'RSI_DEATH_CROSS',
                        'RSI_TREND']

        derivative_columns = ['RSI_MA5', 'RSI_MA10', 'RSI_MOMENTUM',
                              'RSI_STD', 'RSI_TOP_DIVERGENCE',
                              'RSI_BOTTOM_DIVERGENCE']

        # 添加分位数列
        percentile_columns = []
        for window in [20, 50, 100]:
            percentile_columns.append(f'RSI_PERCENTILE_{window}')

        return base_columns + derivative_columns + percentile_columns

    def analyze_rsi_pattern(self, rsi_values: List[float]) -> Dict:
        """
        分析RSI模式

        Args:
            rsi_values: RSI值列表

        Returns:
            模式分析结果
        """
        if not rsi_values:
            return {}

        current_rsi = rsi_values[-1]
        analysis = {
            'current_rsi': round(current_rsi, 2),
            'status': 'neutral',
            'signal': 'hold',
            'strength': 0,
            'recommendation': '暂无建议'
        }

        # 判断状态
        if current_rsi >= self.overbought:
            analysis['status'] = 'overbought'
            analysis['signal'] = 'sell' if current_rsi > 80 else 'caution'
            analysis['strength'] = min((current_rsi - self.overbought) / (100 - self.overbought), 1.0)
            analysis['recommendation'] = '超买状态，考虑减仓或观望'

        elif current_rsi <= self.oversold:
            analysis['status'] = 'oversold'
            analysis['signal'] = 'buy' if current_rsi < 20 else 'caution'
            analysis['strength'] = min((self.oversold - current_rsi) / self.oversold, 1.0)
            analysis['recommendation'] = '超卖状态，考虑逢低买入'

        # 判断趋势
        if len(rsi_values) >= 3:
            recent_trend = self._calculate_recent_trend(rsi_values[-5:])
            analysis['trend'] = recent_trend

            if recent_trend == 'rising' and analysis['status'] == 'oversold':
                analysis['signal'] = 'strong_buy'
                analysis['recommendation'] = '超卖区间出现上升趋势，强烈买入信号'
            elif recent_trend == 'falling' and analysis['status'] == 'overbought':
                analysis['signal'] = 'strong_sell'
                analysis['recommendation'] = '超买区间出现下降趋势，强烈卖出信号'

        return analysis

    def _calculate_recent_trend(self, values: List[float]) -> str:
        """计算近期趋势"""
        if len(values) < 3:
            return 'unknown'

        # 计算斜率
        x = np.arange(len(values))
        y = np.array(values)

        try:
            slope, _ = np.polyfit(x, y, 1)

            if slope > 0.1:
                return 'rising'
            elif slope < -0.1:
                return 'falling'
            else:
                return 'sideways'
        except:
            return 'unknown'


class RSIMultiPeriod:
    """多周期RSI分析器"""

    def __init__(self, periods: List[int] = None):
        """
        初始化多周期RSI

        Args:
            periods: RSI周期列表，默认[6, 12, 24]
        """
        self.periods = periods or [6, 12, 24]
        self.rsi_indicators = {
            period: RSI(period=period)
            for period in self.periods
        }

        logger.info(f"初始化多周期RSI分析器: periods={self.periods}")

    def calculate_multi_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算多周期RSI

        Args:
            df: 价格数据

        Returns:
            包含多周期RSI的DataFrame
        """
        result_df = df.copy()

        for period, indicator in self.rsi_indicators.items():
            try:
                rsi_df = indicator.calculate(df.copy())
                rsi_col = f'RSI_{period}'
                result_df[rsi_col] = rsi_df['RSI']

                logger.debug(f"计算RSI_{period}完成")

            except Exception as e:
                logger.error(f"计算RSI_{period}失败: {e}")
                result_df[f'RSI_{period}'] = np.nan

        # 添加多周期RSI综合分析
        self._add_multi_rsi_analysis(result_df)

        return result_df

    def _add_multi_rsi_analysis(self, df: pd.DataFrame):
        """添加多周期RSI综合分析"""
        if len(self.periods) < 2:
            return

        # 检查所有RSI列是否都存在
        rsi_cols = [f'RSI_{p}' for p in self.periods]
        existing_cols = [col for col in rsi_cols if col in df.columns]

        if len(existing_cols) < 2:
            return

        # 1. RSI多周期一致性
        df['RSI_CONSISTENCY'] = self._calculate_rsi_consistency(df, existing_cols)

        # 2. RSI多周期排列（判断趋势强度）
        df['RSI_ALIGNMENT'] = self._check_rsi_alignment(df, existing_cols)

        # 3. RSI多周期共振信号
        self._add_rsi_resonance_signals(df, existing_cols)

    def _calculate_rsi_consistency(self, df: pd.DataFrame, rsi_cols: List[str]) -> pd.Series:
        """计算RSI多周期一致性"""
        if not rsi_cols:
            return pd.Series(0, index=df.index)

        # 计算所有RSI的标准差（标准差越小，一致性越高）
        rsi_matrix = df[rsi_cols].values
        consistency = np.std(rsi_matrix, axis=1)

        # 转换为0-100的分数（100表示完全一致）
        consistency_score = 100 - (consistency / 50 * 100).clip(0, 100)

        return pd.Series(consistency_score, index=df.index)

    def _check_rsi_alignment(self, df: pd.DataFrame, rsi_cols: List[str]) -> pd.Series:
        """检查RSI多周期排列"""
        alignment = pd.Series('mixed', index=df.index)

        if len(rsi_cols) < 2:
            return alignment

        # 按周期排序（从短到长）
        sorted_cols = sorted(rsi_cols, key=lambda x: int(x.split('_')[1]))

        for i in range(len(df)):
            try:
                values = [df.loc[df.index[i], col] for col in sorted_cols if pd.notna(df.loc[df.index[i], col])]

                if len(values) < 2:
                    alignment.iloc[i] = 'unknown'
                    continue

                # 判断排列顺序
                if all(values[j] <= values[j + 1] for j in range(len(values) - 1)):
                    alignment.iloc[i] = 'ascending'  # 短周期<长周期，可能处于上升趋势
                elif all(values[j] >= values[j + 1] for j in range(len(values) - 1)):
                    alignment.iloc[i] = 'descending'  # 短周期>长周期，可能处于下降趋势
                else:
                    alignment.iloc[i] = 'mixed'

            except:
                alignment.iloc[i] = 'unknown'

        return alignment

    def _add_rsi_resonance_signals(self, df: pd.DataFrame, rsi_cols: List[str]):
        """添加RSI共振信号"""
        if len(rsi_cols) < 2:
            return

        # 多周期同时超买
        overbought_cols = [f'{col}_OVERBOUGHT' for col in rsi_cols]
        existing_overbought = [col for col in overbought_cols if col in df.columns]

        if len(existing_overbought) >= 2:
            df['MULTI_RSI_OVERBOUGHT'] = df[existing_overbought].all(axis=1)

        # 多周期同时超卖
        oversold_cols = [f'{col}_OVERSOLD' for col in rsi_cols]
        existing_oversold = [col for col in oversold_cols if col in df.columns]

        if len(existing_oversold) >= 2:
            df['MULTI_RSI_OVERSOLD'] = df[existing_oversold].all(axis=1)