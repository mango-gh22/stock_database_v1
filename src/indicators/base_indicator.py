# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\base_indicator.py
# File Name: base_indicator
# @ Author: mango-gh22
# @ Date：2025/12/20 19:17
"""
desc
技术指标抽象基类
为所有技术指标提供统一的接口和基础功能
"""

"""
File: src/indicators/base_indicator.py (添加内容)
Desc: 基础指标类 - 增强版
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class IndicatorType(Enum):
    """指标类型枚举"""
    TREND = "trend"  # 趋势指标
    MOMENTUM = "momentum"  # 动量指标
    VOLATILITY = "volatility"  # 波动率指标
    VOLUME = "volume"  # 成交量指标


class BaseIndicator(ABC):
    """技术指标基类"""

    # 类属性：指标名称（子类必须覆盖）
    name: str = None
    indicator_type: IndicatorType = None
    description: str = ""

    def __init__(self, **parameters):
        """
        初始化指标

        Args:
            **parameters: 指标参数
        """
        if self.name is None:
            raise ValueError(f"指标类 {self.__class__.__name__} 必须定义 name 属性")
        if self.indicator_type is None:
            raise ValueError(f"指标类 {self.__class__.__name__} 必须定义 indicator_type 属性")

        self.parameters = parameters
        self.requires_adjusted_price = getattr(self, 'requires_adjusted_price', True)
        self.min_data_points = getattr(self, 'min_data_points', 20)

        # 验证参数
        if not self.validate_parameters():
            raise ValueError(f"指标 {self.name} 参数验证失败")

    @classmethod
    def get_default_parameters(cls) -> Dict:
        """获取默认参数"""
        return getattr(cls, 'default_parameters', {})

    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算指标（子类必须实现）"""
        pass

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备数据用于计算

        Args:
            df: 原始数据

        Returns:
            处理后的数据
        """
        if df.empty:
            return df

        df_processed = df.copy()

        # 检查并处理必需的列
        required_cols = self.get_required_columns()

        for col in required_cols:
            if col not in df_processed.columns:
                # 尝试使用默认列名
                if col == 'close_price':
                    alternatives = ['close_price', 'close', 'Close', 'CLOSE']
                    for alt in alternatives:
                        if alt in df_processed.columns:
                            logger.debug(f"使用替代列名 {alt} 代替 {col}")
                            df_processed.rename(columns={alt: col}, inplace=True)
                            break
                    else:
                        raise ValueError(f"缺少价格列。尝试的列名: {alternatives}")
                else:
                    raise ValueError(f"缺少必需的列: {col}")

            # 确保列是数值类型
            if df_processed[col].dtype == object:
                try:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
                    logger.debug(f"将列 {col} 转换为数值类型")
                except Exception as e:
                    logger.error(f"无法将列 {col} 转换为数值类型: {e}")
                    raise ValueError(f"无法将列 {col} 转换为数值类型: {e}")

            # 填充NaN值
            if df_processed[col].isnull().any():
                nan_count = df_processed[col].isnull().sum()
                logger.warning(f"列 {col} 中有 {nan_count} 个NaN值，进行填充")

                # 先尝试前向填充
                df_processed[col] = df_processed[col].ffill()

                # 然后后向填充
                df_processed[col] = df_processed[col].bfill()

                # 如果还有NaN（比如整个序列都是NaN），使用均值填充
                if df_processed[col].isnull().any():
                    df_processed[col] = df_processed[col].fillna(df_processed[col].mean())

                    # 如果均值也是NaN（比如所有值都是NaN），使用0
                    if df_processed[col].isnull().any():
                        df_processed[col] = df_processed[col].fillna(0)
                        logger.warning(f"列 {col} 使用0填充NaN值")

        # 确保数据点足够
        if len(df_processed) < self.min_data_points:
            logger.warning(f"数据点不足: {len(df_processed)} < {self.min_data_points}")

        return df_processed

    def validate_parameters(self) -> bool:
        """验证参数有效性（子类可以覆盖）"""
        return True

    def get_required_columns(self) -> List[str]:
        """获取计算所需的列（子类可以覆盖）"""
        return ['close_price']

    def get_output_columns(self) -> List[str]:
        """获取输出列名（子类可以覆盖）"""
        return []