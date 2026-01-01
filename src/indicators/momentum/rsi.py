# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators/momentum\rsi.py
# File Name: rsi
# @ Author: mango-gh22
# @ Date：2025/12/20 22:42
"""
File: src/indicators/momentum/rsi.py (修复)
Desc: RSI指标 - 修复参数验证问题
相对强弱指数 (RSI) 指标 - 修复版
修复与 BaseIndicator 的兼容性问题
"""

import pandas as pd
import numpy as np
from typing import List
from ..base_indicator import BaseIndicator, IndicatorType
import logging

logger = logging.getLogger(__name__)


class RSI(BaseIndicator):
    """RSI指标 - 修复版"""

    # 类属性
    name = "rsi"
    indicator_type = IndicatorType.MOMENTUM
    description = "相对强弱指数(RSI)"

    # 默认参数
    default_parameters = {
        'period': 14,
        'price_column': 'close_price',  # 使用 close_price 而不是 close
        'overbought': 70,
        'oversold': 30
    }

    def __init__(self, **parameters):
        """
        初始化RSI指标

        Args:
            period: RSI计算周期，默认14
            price_column: 价格列名，默认'close_price'
            overbought: 超买线，默认70
            oversold: 超卖线，默认30
        """
        # 合并默认参数和用户参数
        merged_params = {**self.default_parameters, **parameters}

        # 调用父类初始化
        super().__init__(**merged_params)

        # 设置实例属性
        self.period = int(self.parameters.get('period', 14))
        self.price_column = self.parameters.get('price_column', 'close_price')
        self.overbought = float(self.parameters.get('overbought', 70))
        self.oversold = float(self.parameters.get('oversold', 30))

        # 调整最小数据点要求
        self.min_data_points = max(self.period + 10, 20)

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算RSI指标

        Args:
            df: 包含价格数据的DataFrame

        Returns:
            包含RSI指标的DataFrame
        """
        logger.info(f"计算RSI指标，周期: {self.period}, 价格列: {self.price_column}")

        # 准备数据
        df_prepared = self.prepare_data(df)

        if df_prepared.empty:
            logger.warning("准备后的数据为空")
            return pd.DataFrame()

        # 检查价格列是否存在
        if self.price_column not in df_prepared.columns:
            # 尝试其他可能的列名
            possible_columns = ['close', 'Close', 'CLOSE', 'close_price', 'price']
            for col in possible_columns:
                if col in df_prepared.columns:
                    self.price_column = col
                    logger.info(f"使用替代价格列: {col}")
                    break
            else:
                logger.error(f"找不到价格列。可用列: {list(df_prepared.columns)}")
                return pd.DataFrame()

        # 获取价格序列
        try:
            prices = pd.to_numeric(df_prepared[self.price_column], errors='coerce')

            # 检查数据量是否足够
            if len(prices) < self.period + 5:
                logger.warning(f"数据不足，需要至少{self.period + 5}个数据点，当前只有{len(prices)}个")
                # 返回包含NaN的RSI序列
                rsi_series = pd.Series([np.nan] * len(prices), index=df_prepared.index)
                result_df = df_prepared.copy()
                result_df['RSI'] = rsi_series
                return result_df

            # 计算价格变化
            delta = prices.diff()

            # 分离上涨和下跌
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            # 计算平均增益和平均损失（使用简单移动平均）
            avg_gain = gain.rolling(window=self.period, min_periods=1).mean()
            avg_loss = loss.rolling(window=self.period, min_periods=1).mean()

            # 避免除零错误
            avg_loss = avg_loss.replace(0, np.nan)
            avg_loss_filled = avg_loss.ffill().bfill().replace(np.nan, 1e-10)

            # 计算相对强度
            rs = avg_gain / avg_loss_filled

            # 计算RSI
            rsi = 100 - (100 / (1 + rs))

            # 处理NaN值
            rsi = rsi.ffill().bfill().fillna(50)  # 用50填充剩余的NaN

            # 创建结果DataFrame
            result_df = df_prepared.copy()
            result_df['RSI'] = rsi

            # 添加信号（可选）
            self._add_rsi_signals(result_df)

            logger.info(f"RSI计算完成，有效值: {rsi.notna().sum()}/{len(rsi)}")
            return result_df

        except Exception as e:
            logger.error(f"计算RSI失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")

            # 返回包含NaN的结果，而不是空DataFrame
            result_df = df_prepared.copy()
            result_df['RSI'] = np.nan
            return result_df

    def _add_rsi_signals(self, df: pd.DataFrame):
        """添加RSI信号（简化版）"""
        if 'RSI' not in df.columns:
            return

        rsi = df['RSI']

        # 基本超买超卖信号
        df['RSI_Overbought'] = rsi >= self.overbought
        df['RSI_Oversold'] = rsi <= self.oversold

        # 金叉死叉信号
        df['RSI_Buy_Signal'] = (rsi > self.oversold) & (rsi.shift(1) <= self.oversold)
        df['RSI_Sell_Signal'] = (rsi < self.overbought) & (rsi.shift(1) >= self.overbought)

    def validate_parameters(self) -> bool:
        """验证参数有效性"""
        try:
            period = int(self.parameters.get('period', 14))
            oversold = float(self.parameters.get('oversold', 30))
            overbought = float(self.parameters.get('overbought', 70))

            if period <= 0:
                logger.error("RSI周期必须是正整数")
                return False

            if not (0 < oversold < overbought < 100):
                logger.error("超卖线必须在0-100之间且小于超买线")
                return False

            return True
        except (ValueError, TypeError) as e:
            logger.error(f"参数验证失败: {e}")
            return False

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列"""
        return [self.price_column]

    def get_output_columns(self) -> List[str]:
        """获取输出列名"""
        return [
            'RSI',
            'RSI_Overbought',
            'RSI_Oversold',
            'RSI_Buy_Signal',
            'RSI_Sell_Signal'
        ]