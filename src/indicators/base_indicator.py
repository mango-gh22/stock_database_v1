# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\base_indicator.py
# File Name: base_indicator
# @ Author: mango-gh22
# @ Date：2025/12/20 19:17
"""
desc 
"""

"""
技术指标抽象基类
为所有技术指标提供统一的接口和基础功能
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Optional, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class IndicatorType(Enum):
    """指标类型枚举"""
    TREND = "trend"  # 趋势指标
    MOMENTUM = "momentum"  # 动量指标
    VOLATILITY = "volatility"  # 波动率指标
    VOLUME = "volume"  # 成交量指标
    OTHER = "other"  # 其他指标


class BaseIndicator(ABC):
    """技术指标抽象基类"""

    def __init__(self, name: str, indicator_type: IndicatorType):
        """
        初始化指标

        Args:
            name: 指标名称
            indicator_type: 指标类型
        """
        self.name = name
        self.indicator_type = indicator_type
        self.parameters: Dict[str, Any] = {}
        self.requires_adjusted_price = True  # 默认需要复权价格
        self.min_data_points = 0  # 计算所需最小数据点数
        self.description = ""  # 指标描述

    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        Args:
            df: 包含股票数据的DataFrame，必须包含以下列：
                - close_price: 收盘价
                - high_price: 最高价
                - low_price: 最低价
                - volume: 成交量
                - trade_date: 交易日期（索引）

        Returns:
            包含计算结果的DataFrame
        """
        pass

    @abstractmethod
    def validate_parameters(self) -> bool:
        """
        验证参数有效性

        Returns:
            参数是否有效
        """
        pass

    # 修改BaseIndicator的prepare_data方法
    def prepare_data(self, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        # 导入DataPreparer
        from src.utils.data_preparer import DataPreparer

        # 使用DataPreparer处理数据
        df_processed = DataPreparer.prepare_stock_data(df, symbol)

        # 检查数据点数是否足够
        if len(df_processed) < self.min_data_points:
            logger.warning(f"数据点数不足: {len(df_processed)} < {self.min_data_points}")

        return df_processed

    def get_required_columns(self) -> List[str]:
        """
        获取计算所需的列

        Returns:
            必需的列名列表
        """
        return ['close_price', 'high_price', 'low_price', 'volume']

    def get_output_columns(self) -> List[str]:
        """
        获取输出列名

        Returns:
            输出列名列表
        """
        return [f"{self.name}_{col}" for col in self.parameters.keys()]