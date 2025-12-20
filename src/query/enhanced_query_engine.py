# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\enhanced_query_engine.py
# File Name: enhanced_query_engine
# @ Author: mango-gh22
# @ Date：2025/12/20 19:20
"""
desc 扩展查询引擎支持技术指标
"""

"""
增强版查询引擎 - 支持技术指标计算
"""
import pandas as pd
from typing import List, Dict, Optional
import logging

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))  # 添加项目根
from src.query.query_engine import QueryEngine
from src.indicators.indicator_manager import IndicatorManager

logger = logging.getLogger(__name__)


class EnhancedQueryEngine(QueryEngine):
    """增强版查询引擎 - 支持技术指标"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化增强查询引擎

        Args:
            config_path: 配置文件路径
        """
        super().__init__(config_path)
        self.indicator_manager = IndicatorManager(config_path)

    def query_with_indicators(self, symbol: str,
                              indicators: List[str],
                              start_date: str,
                              end_date: str,
                              use_cache: bool = True) -> pd.DataFrame:
        """
        查询数据并计算技术指标

        Args:
            symbol: 股票代码
            indicators: 指标名称列表
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存

        Returns:
            包含技术指标的DataFrame
        """
        logger.info(f"查询带指标的数据: {symbol}, 指标: {indicators}, 日期: {start_date} - {end_date}")

        # 验证指标
        available_indicators = self.indicator_manager.get_available_indicators()
        invalid_indicators = [ind for ind in indicators if ind not in available_indicators]

        if invalid_indicators:
            logger.warning(f"以下指标无效或未注册: {invalid_indicators}")
            indicators = [ind for ind in indicators if ind in available_indicators]

        if not indicators:
            logger.warning("没有有效的指标，返回原始数据")
            return self.query_daily_data(symbol, start_date, end_date)

        # 获取基础数据
        df = self.query_daily_data(symbol, start_date, end_date)

        if df.empty:
            logger.warning(f"股票 {symbol} 在指定日期范围内无数据")
            return df

        # 计算技术指标
        indicator_results = self.indicator_manager.calculate_for_symbol(
            symbol, indicators, start_date, end_date, use_cache
        )

        if not indicator_results:
            logger.warning("技术指标计算失败，返回原始数据")
            return df

        # 合并结果
        for indicator_name, indicator_df in indicator_results.items():
            # 只添加指标列，避免重复列
            indicator_cols = [col for col in indicator_df.columns
                              if col not in df.columns or col in ['trade_date', 'symbol']]

            if indicator_cols:
                df = pd.concat([df, indicator_df[indicator_cols]], axis=1)

        logger.info(f"成功合并 {len(indicator_results)} 个指标，最终列数: {len(df.columns)}")
        return df

    def get_available_indicators(self) -> Dict[str, Dict]:
        """
        获取所有可用指标信息

        Returns:
            指标信息字典
        """
        return self.indicator_manager.get_available_indicators()

    def validate_indicator_calculation(self, symbol: str,
                                       indicator_name: str,
                                       start_date: str,
                                       end_date: str) -> Dict:
        """
        验证指标计算可行性

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            验证结果字典
        """
        result = {
            'symbol': symbol,
            'indicator': indicator_name,
            'date_range': f"{start_date} - {end_date}",
            'is_valid': False,
            'message': '',
            'data_points': 0,
            'required_points': 0
        }

        # 获取可用指标
        available_indicators = self.get_available_indicators()
        if indicator_name not in available_indicators:
            result['message'] = f"指标 {indicator_name} 未注册"
            return result

        # 获取数据
        df = self.query_daily_data(symbol, start_date, end_date)
        if df.empty:
            result['message'] = f"股票 {symbol} 在指定日期范围内无数据"
            return result

        result['data_points'] = len(df)
        result['required_points'] = available_indicators[indicator_name]['min_data_points']

        # 检查数据是否足够
        if result['data_points'] < result['required_points']:
            result['message'] = f"数据不足: {result['data_points']} < {result['required_points']}"
            return result

        result['is_valid'] = True
        result['message'] = "可以计算该指标"

        return result