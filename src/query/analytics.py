# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\analytics.py
# File Name: analytics
# @ Author: m_mango
# @ Date：2025/12/6 16:28
"""
desc 数据分析模块
"""

"""
数据分析模块 - v0.4.0
功能：收益率分析、波动率分析、相关性分析等
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta


class StockAnalytics:
    """股票数据分析器"""

    @staticmethod
    def calculate_returns(data: pd.DataFrame,
                          period: int = 1,
                          return_type: str = 'simple') -> pd.DataFrame:
        """
        计算收益率

        Args:
            data: 包含'close'列的DataFrame
            period: 收益率计算周期
            return_type: 收益率类型 ('simple'或'log')

        Returns:
            DataFrame: 包含收益率的DataFrame
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame必须包含'close'列")

        result = data.copy()

        if return_type == 'simple':
            result[f'return_{period}d'] = data['close'].pct_change(periods=period)
        elif return_type == 'log':
            result[f'log_return_{period}d'] = np.log(data['close'] / data['close'].shift(period))
        else:
            raise ValueError("return_type必须是'simple'或'log'")

        return result

    @staticmethod
    def calculate_volatility(data: pd.DataFrame,
                             window: int = 20,
                             return_col: str = None) -> pd.DataFrame:
        """
        计算波动率

        Args:
            data: 包含收益率的DataFrame
            window: 滚动窗口大小
            return_col: 收益率列名

        Returns:
            DataFrame: 包含波动率的DataFrame
        """
        result = data.copy()

        if return_col is None:
            # 自动查找收益率列
            return_cols = [col for col in data.columns if 'return' in col.lower()]
            if not return_cols:
                # 如果没有收益率列，先计算日收益率
                result = StockAnalytics.calculate_returns(result, period=1)
                return_col = 'return_1d'
            else:
                return_col = return_cols[0]

        if return_col not in result.columns:
            raise ValueError(f"收益率列'{return_col}'不存在")

        # 计算滚动波动率（年化）
        if len(result) >= window:
            daily_volatility = result[return_col].rolling(window=window).std()
            # 年化波动率（假设252个交易日）
            annual_volatility = daily_volatility * np.sqrt(252)
            result[f'volatility_{window}d'] = annual_volatility
        else:
            result[f'volatility_{window}d'] = np.nan

        return result

    @staticmethod
    def calculate_sharpe_ratio(data: pd.DataFrame,
                               risk_free_rate: float = 0.03,
                               window: int = 20,
                               return_col: str = None) -> pd.DataFrame:
        """
        计算夏普比率

        Args:
            data: 包含收益率的DataFrame
            risk_free_rate: 无风险利率（年化）
            window: 滚动窗口大小
            return_col: 收益率列名

        Returns:
            DataFrame: 包含夏普比率的DataFrame
        """
        result = data.copy()

        if return_col is None:
            return_cols = [col for col in data.columns if 'return' in col.lower()]
            if not return_cols:
                result = StockAnalytics.calculate_returns(result, period=1)
                return_col = 'return_1d'
            else:
                return_col = return_cols[0]

        if return_col not in result.columns:
            raise ValueError(f"收益率列'{return_col}'不存在")

        # 计算夏普比率
        if len(result) >= window:
            # 日度无风险利率
            daily_rf = risk_free_rate / 252

            # 超额收益率
            excess_return = result[return_col] - daily_rf

            # 滚动超额收益均值和标准差
            rolling_mean = excess_return.rolling(window=window).mean()
            rolling_std = excess_return.rolling(window=window).std()

            # 年化夏普比率
            sharpe_ratio = (rolling_mean / rolling_std) * np.sqrt(252)
            result[f'sharpe_ratio_{window}d'] = sharpe_ratio
        else:
            result[f'sharpe_ratio_{window}d'] = np.nan

        return result

    @staticmethod
    def calculate_correlation(data_dict: Dict[str, pd.DataFrame],
                              return_col: str = 'return_1d',
                              start_date: str = None,
                              end_date: str = None) -> pd.DataFrame:
        """
        计算股票间的相关性

        Args:
            data_dict: 股票代码到DataFrame的映射
            return_col: 用于计算相关性的列
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame: 相关性矩阵
        """
        if len(data_dict) < 2:
            raise ValueError("至少需要两只股票数据来计算相关性")

        # 准备收益率数据
        returns_data = {}
        for symbol, df in data_dict.items():
            if return_col not in df.columns:
                # 如果没有收益率列，计算日收益率
                temp_df = StockAnalytics.calculate_returns(df, period=1)
                if 'return_1d' in temp_df.columns:
                    returns_data[symbol] = temp_df['return_1d']
            elif return_col in df.columns:
                returns_data[symbol] = df[return_col]

        if len(returns_data) < 2:
            raise ValueError("无法获取足够的收益率数据")

        # 创建DataFrame
        returns_df = pd.DataFrame(returns_data)

        # 日期筛选
        if start_date:
            returns_df = returns_df[returns_df.index >= start_date]
        if end_date:
            returns_df = returns_df[returns_df.index <= end_date]

        # 计算相关性矩阵
        correlation_matrix = returns_df.corr()

        return correlation_matrix

    @staticmethod
    def analyze_stock_performance(data: pd.DataFrame,
                                  risk_free_rate: float = 0.03) -> Dict:
        """
        分析股票综合表现

        Args:
            data: 包含'close'列的股票数据
            risk_free_rate: 无风险利率

        Returns:
            Dict: 性能指标字典
        """
        if data.empty:
            return {}

        # 计算收益率
        result = StockAnalytics.calculate_returns(data, period=1)

        # 基础统计
        stats = {
            'total_days': len(data),
            'start_date': data.index[0] if hasattr(data.index[0], 'strftime') else str(data.index[0]),
            'end_date': data.index[-1] if hasattr(data.index[-1], 'strftime') else str(data.index[-1]),
            'start_price': float(data['close'].iloc[0]),
            'end_price': float(data['close'].iloc[-1]),
            'price_change': float(data['close'].iloc[-1] - data['close'].iloc[0]),
            'price_change_pct': float((data['close'].iloc[-1] / data['close'].iloc[0] - 1) * 100)
        }

        # 收益率分析
        if 'return_1d' in result.columns:
            returns = result['return_1d'].dropna()
            if len(returns) > 0:
                stats.update({
                    'mean_daily_return': float(returns.mean()),
                    'std_daily_return': float(returns.std()),
                    'max_daily_return': float(returns.max()),
                    'min_daily_return': float(returns.min()),
                    'annualized_return': float((1 + returns.mean()) ** 252 - 1),
                    'annualized_volatility': float(returns.std() * np.sqrt(252)),
                    'sharpe_ratio': float((returns.mean() - risk_free_rate / 252) / returns.std() * np.sqrt(252))
                })

        # 成交量分析
        if 'volume' in data.columns:
            volume_stats = data['volume'].describe()
            stats.update({
                'mean_volume': float(volume_stats['mean']),
                'max_volume': float(volume_stats['max']),
                'min_volume': float(volume_stats['min'])
            })

        return stats