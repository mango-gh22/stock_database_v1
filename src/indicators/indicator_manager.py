# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\indicator_manager.py
# @ Author: mango-gh22
# @ Date：2025/12/20 19:18

"""
File: src/indicators/indicator_manager.py (重构)
Desc: 技术指标管理器 - 重构版
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Type, Any
import hashlib
import json
import pickle
from datetime import datetime, timedelta
import logging

from src.indicators.base_indicator import BaseIndicator, IndicatorType
from src.query.query_engine import QueryEngine
from src.processors.adjustor import StockAdjustor

logger = logging.getLogger(__name__)


class IndicatorFactory:
    """指标工厂类"""

    @staticmethod
    def create_indicator(indicator_name: str, **parameters) -> BaseIndicator:
        """
        创建指标实例

        Args:
            indicator_name: 指标名称
            **parameters: 指标参数

        Returns:
            指标实例
        """
        # 导入所有指标类
        from .trend.moving_average import MovingAverage
        from .trend.macd import MACD
        from .trend.parabolic_sar import ParabolicSAR
        from .trend.ichimoku_cloud import IchimokuCloud

        from .momentum.rsi import RSI
        from .momentum.stochastic import Stochastic
        from .momentum.cci import CCI
        from .momentum.williams_r import WilliamsR

        from .volatility.bollinger_bands import BollingerBands

        from .volume.obv import OBV

        # 指标类映射
        indicator_classes = {
            'moving_average': MovingAverage,
            'macd': MACD,
            'parabolic_sar': ParabolicSAR,
            'ichimoku_cloud': IchimokuCloud,
            'rsi': RSI,
            'stochastic': Stochastic,
            'cci': CCI,
            'williams_r': WilliamsR,
            'bollinger_bands': BollingerBands,
            'obv': OBV,
        }

        if indicator_name not in indicator_classes:
            raise ValueError(f"未知的指标: {indicator_name}")

        indicator_class = indicator_classes[indicator_name]

        try:
            # 创建指标实例
            return indicator_class(**parameters)
        except Exception as e:
            logger.error(f"创建指标 {indicator_name} 失败: {e}")
            raise


class IndicatorCacheManager:
    """指标缓存管理器"""

    def __init__(self, cache_dir: str = "data/cache/indicators"):
        self.cache_dir = cache_dir
        self.memory_cache: Dict[str, pd.DataFrame] = {}
        self.cache_metadata: Dict[str, Dict] = {}

        # 确保缓存目录存在
        from pathlib import Path
        Path(cache_dir).mkdir(parents=True, exist_ok=True)

        # 缓存配置
        self.memory_ttl = 3600  # 内存缓存1小时

    def _get_cache_key(self, symbol: str, indicator_name: str,
                       parameters: Dict, start_date: str, end_date: str) -> str:
        """生成缓存键"""
        cache_str = f"{symbol}_{indicator_name}_{json.dumps(parameters, sort_keys=True)}_\
                     {start_date}_{end_date}"
        return hashlib.md5(cache_str.encode()).hexdigest()

    def get(self, symbol: str, indicator_name: str,
            parameters: Dict, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取缓存数据

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            parameters: 指标参数
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缓存的数据或None
        """
        cache_key = self._get_cache_key(symbol, indicator_name, parameters, start_date, end_date)

        # 检查内存缓存
        if cache_key in self.memory_cache:
            logger.debug(f"从内存缓存获取: {cache_key}")
            return self.memory_cache[cache_key]

        return None

    def set(self, symbol: str, indicator_name: str, parameters: Dict,
            start_date: str, end_date: str, data: pd.DataFrame):
        """
        设置缓存数据

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            parameters: 指标参数
            start_date: 开始日期
            end_date: 结束日期
            data: 要缓存的数据
        """
        cache_key = self._get_cache_key(symbol, indicator_name, parameters, start_date, end_date)

        # 设置内存缓存
        self.memory_cache[cache_key] = data

        # 管理内存缓存大小
        if len(self.memory_cache) > 1000:
            # 移除最早的缓存项
            keys = list(self.memory_cache.keys())
            for key in keys[:100]:
                self.memory_cache.pop(key, None)

        logger.debug(f"设置缓存: {cache_key}, 形状: {data.shape}")

    def clear(self, cache_type: str = 'all'):
        """
        清理缓存

        Args:
            cache_type: 缓存类型，'memory'、'disk' 或 'all'
        """
        if cache_type in ['memory', 'all']:
            self.memory_cache.clear()
            logger.info("清理内存缓存")


class IndicatorManager:
    """技术指标管理器 - 重构版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化指标管理器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.query_engine = QueryEngine(config_path)
        self.adjustor = StockAdjustor(config_path)
        self.cache_manager = IndicatorCacheManager()

        # 指标工厂
        self.indicator_factory = IndicatorFactory()

        # 可用指标信息（不存储实例，只存储信息）
        self._init_available_indicators()

    def _init_available_indicators(self):
        """初始化可用指标信息"""
        self.available_indicators = {
            'moving_average': {
                'class': None,  # 延迟加载
                'default_params': {'periods': [5, 10, 20, 30, 60], 'ma_type': 'sma'},
                'type': IndicatorType.TREND,
                'description': '移动平均线',
                'requires_adjusted_price': True,
                'min_data_points': 70,  # max(periods) + 10
            },
            'macd': {
                'class': None,
                'default_params': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
                'type': IndicatorType.TREND,
                'description': 'MACD指标',
                'requires_adjusted_price': True,
                'min_data_points': 45,
            },
            'rsi': {
                'class': None,
                'default_params': {'period': 14},
                'type': IndicatorType.MOMENTUM,
                'description': '相对强弱指数',
                'requires_adjusted_price': True,
                'min_data_points': 30,
            },
            'bollinger_bands': {
                'class': None,
                'default_params': {'period': 20, 'std_dev': 2},
                'type': IndicatorType.VOLATILITY,
                'description': '布林带',
                'requires_adjusted_price': True,
                'min_data_points': 30,
            },
            'obv': {
                'class': None,
                'default_params': {},
                'type': IndicatorType.VOLUME,
                'description': '能量潮指标',
                'requires_adjusted_price': False,
                'min_data_points': 2,
            },
            'parabolic_sar': {
                'class': None,
                'default_params': {'acceleration_factor': 0.02, 'acceleration_max': 0.2},
                'type': IndicatorType.TREND,
                'description': '抛物线指标',
                'requires_adjusted_price': False,
                'min_data_points': 20,
            },
            'ichimoku_cloud': {
                'class': None,
                'default_params': {'tenkan_period': 9, 'kijun_period': 26, 'senkou_span_b_period': 52},
                'type': IndicatorType.TREND,
                'description': '一目均衡表',
                'requires_adjusted_price': False,
                'min_data_points': 62,
            },
            'stochastic': {
                'class': None,
                'default_params': {'k_period': 14, 'd_period': 3, 'smoothing': 3},
                'type': IndicatorType.MOMENTUM,
                'description': '随机指标',
                'requires_adjusted_price': False,
                'min_data_points': 30,
            },
            'cci': {
                'class': None,
                'default_params': {'period': 20, 'constant': 0.015},
                'type': IndicatorType.MOMENTUM,
                'description': '商品通道指数',
                'requires_adjusted_price': False,
                'min_data_points': 30,
            },
            'williams_r': {
                'class': None,
                'default_params': {'period': 14},
                'type': IndicatorType.MOMENTUM,
                'description': '威廉指标',
                'requires_adjusted_price': False,
                'min_data_points': 24,
            },
        }

    def _preprocess_data_for_calculation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        增强版数据预处理

        Args:
            df: 原始数据

        Returns:
            处理后的数据
        """
        if df.empty:
            return df

        logger.debug(f"开始数据预处理，原始形状: {df.shape}")
        df_processed = df.copy()

        # 首先检查数据类型
        logger.debug("原始数据类型:")
        for col, dtype in df_processed.dtypes.items():
            logger.debug(f"  {col}: {dtype}")

        # 定义可能的数值列
        potential_numeric_cols = [
            'open_price', 'high_price', 'low_price', 'close_price',
            'volume', 'amount', 'pct_change', 'price_change',
            'pre_close', 'turnover_rate', 'amplitude', 'ma5', 'ma10', 'ma20',
            'open', 'high', 'low', 'close', 'volume', 'amount'
        ]

        # 实际存在的列
        existing_cols = [col for col in potential_numeric_cols if col in df_processed.columns]

        for col in existing_cols:
            try:
                # 检查列是否包含非数值数据
                if df_processed[col].dtype == object:
                    logger.debug(f"处理列 {col}，类型为 object")

                    # 显示前几个值以了解数据类型
                    sample_values = df_processed[col].head(3).tolist()
                    logger.debug(f"列 {col} 前3个值: {sample_values}")

                    # 尝试转换为数值
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

                    # 检查转换结果
                    nan_count = df_processed[col].isnull().sum()
                    if nan_count > 0:
                        logger.warning(f"列 {col} 转换后有 {nan_count} 个NaN值")

                # 处理NaN值
                if df_processed[col].isnull().any():
                    nan_count = df_processed[col].isnull().sum()
                    logger.debug(f"列 {col} 有 {nan_count} 个NaN值，开始填充")

                    # 先尝试前向填充
                    original_nan = df_processed[col].isnull().sum()
                    df_processed[col] = df_processed[col].ffill()
                    ffill_nan = df_processed[col].isnull().sum()
                    logger.debug(f"前向填充后，NaN从 {original_nan} 减少到 {ffill_nan}")

                    # 然后后向填充
                    df_processed[col] = df_processed[col].bfill()
                    bfill_nan = df_processed[col].isnull().sum()
                    logger.debug(f"后向填充后，NaN从 {ffill_nan} 减少到 {bfill_nan}")

                    # 如果还有NaN（比如开头或结尾），尝试其他方法
                    if df_processed[col].isnull().any():
                        # 尝试使用列均值
                        col_mean = df_processed[col].mean()
                        logger.debug(f"尝试使用均值 {col_mean} 填充")
                        df_processed[col] = df_processed[col].fillna(col_mean)

                        # 如果均值也是NaN，使用0
                        if df_processed[col].isnull().any():
                            logger.warning(f"列 {col} 使用0填充剩余的NaN值")
                            df_processed[col] = df_processed[col].fillna(0)

            except Exception as e:
                logger.error(f"处理列 {col} 时出错: {e}")
                # 尝试最后的手段：转换为字符串，然后提取数字
                try:
                    def extract_numeric(x):
                        if x is None:
                            return 0
                        # 如果是Decimal类型，转换为float
                        if hasattr(x, '__class__') and 'Decimal' in str(x.__class__):
                            return float(x)
                        # 如果是字符串，尝试提取数字
                        if isinstance(x, str):
                            import re
                            nums = re.findall(r'[-+]?\d*\.\d+|\d+', x)
                            return float(nums[0]) if nums else 0
                        # 其他情况尝试直接转换
                        return float(x) if x is not None else 0

                    df_processed[col] = df_processed[col].apply(extract_numeric)
                    logger.debug(f"列 {col} 使用自定义提取函数处理完成")
                except Exception as inner_e:
                    logger.error(f"列 {col} 自定义处理也失败: {inner_e}")

        # 确保所有数值列都是float类型
        numeric_cols = df_processed.select_dtypes(include=['int', 'float', 'int64', 'float64']).columns
        for col in numeric_cols:
            try:
                df_processed[col] = df_processed[col].astype(float)
            except:
                pass

        # 记录处理后的数据类型
        logger.debug("处理后数据类型:")
        for col, dtype in df_processed.dtypes.items():
            logger.debug(f"  {col}: {dtype}")

        # 记录最终状态
        total_nan = df_processed.isnull().sum().sum()
        if total_nan > 0:
            logger.warning(f"数据预处理后仍有 {total_nan} 个NaN值")
            # 显示包含NaN的列
            nan_columns = df_processed.columns[df_processed.isnull().any()].tolist()
            logger.warning(f"包含NaN的列: {nan_columns}")
        else:
            logger.debug("数据预处理完成，没有NaN值")

        logger.debug(f"数据预处理完成，最终形状: {df_processed.shape}")

        return df_processed



    def create_indicator(self, indicator_name: str, **parameters) -> BaseIndicator:
        """
        创建指标实例

        Args:
            indicator_name: 指标名称
            **parameters: 指标参数

        Returns:
            指标实例
        """
        return self.indicator_factory.create_indicator(indicator_name, **parameters)

    def calculate_for_symbol(self, symbol: str,
                             indicator_names: List[str],
                             start_date: str,
                             end_date: str,
                             indicator_params: Optional[Dict[str, Dict]] = None,
                             use_cache: bool = True) -> Dict[str, pd.DataFrame]:
        """
        为指定股票计算多个指标

        Args:
            symbol: 股票代码
            indicator_names: 指标名称列表
            start_date: 开始日期
            end_date: 结束日期
            indicator_params: 各指标参数，格式：{'indicator_name': {param1: value1, ...}}
            use_cache: 是否使用缓存

        Returns:
            计算结果字典 {指标名: DataFrame}
        """
        logger.info(f"计算指标: {symbol}, 指标: {indicator_names}, 日期: {start_date} - {end_date}")

        # 确保indicator_params是字典类型
        if indicator_params is None or not isinstance(indicator_params, dict):
            indicator_params = {}

        # 如果传递了布尔值（可能是误传的use_cache），重置为空字典
        if isinstance(indicator_params, bool):
            logger.warning("indicator_params参数接收到布尔值，已重置为空字典")
            indicator_params = {}

        # 获取原始数据
        try:

            # 在获取数据后立即预处理
            df = self.query_engine.query_daily_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                logger.warning(f"股票 {symbol} 在 {start_date} - {end_date} 期间无数据")
                return {}

            logger.info(f"获取到 {len(df)} 条数据")

            # 预处理数据：确保没有None值，转换Decimal为float
            df = self._preprocess_data_for_calculation(df)

            # 检查预处理后的数据
            logger.info(f"预处理后数据形状: {df.shape}")
            logger.info(f"预处理后数据类型:\n{df.dtypes}")

        except Exception as e:
            logger.error(f"获取或预处理数据失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return {}

        # 计算结果
        results = {}
        for indicator_name in indicator_names:
            if indicator_name not in self.available_indicators:
                logger.warning(f"指标 {indicator_name} 不可用")
                continue

            try:
                # 安全获取指标参数
                params = {}
                if isinstance(indicator_params, dict):
                    params = indicator_params.get(indicator_name, {})

                # 确保params是字典
                if not isinstance(params, dict):
                    logger.warning(f"指标 {indicator_name} 的参数不是字典类型，已重置为空字典")
                    params = {}

                # 检查缓存
                if use_cache:
                    cached_result = self.cache_manager.get(
                        symbol, indicator_name, params, start_date, end_date
                    )
                    if cached_result is not None:
                        results[indicator_name] = cached_result
                        logger.debug(f"使用缓存结果: {indicator_name}")
                        continue

                # 创建指标实例
                indicator = self.create_indicator(indicator_name, **params)

                # 计算指标（使用预处理后的数据副本）
                result_df = indicator.calculate(df.copy())

                # 缓存结果
                if use_cache:
                    self.cache_manager.set(
                        symbol, indicator_name, params, start_date, end_date, result_df
                    )

                results[indicator_name] = result_df
                logger.info(f"完成指标计算: {indicator_name}")

            except Exception as e:
                logger.error(f"计算指标 {indicator_name} 失败: {e}")
                # 添加更详细的错误信息
                import traceback
                logger.error(f"详细错误: {traceback.format_exc()}")

        return results

    def calculate_single(self, symbol: str,
                         indicator_name: str,
                         start_date: str,
                         end_date: str,
                         **parameters) -> Optional[pd.DataFrame]:
        """
        计算单个指标（简化接口）

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            start_date: 开始日期
            end_date: 结束日期
            **parameters: 指标参数

        Returns:
            计算结果DataFrame或None
        """
        results = self.calculate_for_symbol(
            symbol=symbol,
            indicator_names=[indicator_name],
            start_date=start_date,
            end_date=end_date,
            indicator_params={indicator_name: parameters},
            use_cache=True
        )

        return results.get(indicator_name)

    def get_available_indicators(self) -> Dict[str, Dict]:
        """
        获取所有可用指标信息

        Returns:
            指标信息字典
        """
        info = {}
        for name, indicator_info in self.available_indicators.items():
            info[name] = {
                'type': indicator_info['type'].value,
                'description': indicator_info['description'],
                'default_parameters': indicator_info['default_params'],
                'requires_adjusted_price': indicator_info['requires_adjusted_price'],
                'min_data_points': indicator_info['min_data_points']
            }
        return info

    def validate_data_sufficiency(self, symbol: str, indicator_names: List[str],
                                  start_date: str, end_date: str) -> Tuple[bool, str]:
        """
        验证数据是否足够计算指定指标

        Args:
            symbol: 股票代码
            indicator_names: 指标名称列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            (是否足够, 消息)
        """
        # 获取数据点数
        df = self.query_engine.query_daily_data(symbol, start_date, end_date)
        if df.empty:
            return False, f"股票 {symbol} 无数据"

        data_count = len(df)

        # 检查每个指标的最小数据要求
        for indicator_name in indicator_names:
            if indicator_name in self.available_indicators:
                min_points = self.available_indicators[indicator_name]['min_data_points']
                if data_count < min_points:
                    return False, f"指标 {indicator_name} 需要至少 {min_points} 条数据，当前只有 {data_count} 条"

        return True, f"数据足够，共 {data_count} 条数据"

    def calculate_indicator(self, indicator_name: str, price_values: np.ndarray, **kwargs) -> np.ndarray:
        """
        兼容性方法：直接计算指标（简化接口）

        注意：这是一个简化的兼容性方法，建议使用 calculate_single 或 calculate_for_symbol

        Args:
            indicator_name: 指标名称
            price_values: 价格序列
            **kwargs: 指标参数

        Returns:
            指标值数组
        """
        logger.warning("使用兼容性方法 calculate_indicator，建议使用 calculate_single 或 calculate_for_symbol")

        # 创建临时的DataFrame
        df = pd.DataFrame({
            'close_price': price_values,
            'close': price_values  # 添加别名
        })

        try:
            # 创建指标实例
            indicator = self.create_indicator(indicator_name, **kwargs)

            # 计算指标
            result_df = indicator.calculate(df)

            # 提取结果
            if result_df.empty:
                return np.array([])

            # 尝试不同的列名
            for col in ['RSI', 'rsi', 'value']:
                if col in result_df.columns:
                    return result_df[col].values

            # 如果找不到特定列，返回第一列
            return result_df.iloc[:, 0].values if len(result_df.columns) > 0 else np.array([])

        except Exception as e:
            logger.error(f"calculate_indicator 失败: {e}")
            return np.array([])




    def get_cache_stats(self) -> Dict:
        """获取缓存统计信息"""
        return {
            'memory_cache_items': len(self.cache_manager.memory_cache),
            'memory_ttl': self.cache_manager.memory_ttl
        }

    def clear_cache(self, cache_type: str = 'all'):
        """
        清理缓存

        Args:
            cache_type: 缓存类型，'memory'、'disk' 或 'all'
        """
        self.cache_manager.clear(cache_type)