# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\indicator_manager.py
# File Name: indicator_manager
# @ Author: mango-gh22
# @ Date：2025/12/20 19:18
"""
desc
技术指标管理器
管理所有指标的计算、注册和缓存
"""
import pandas as pd
from typing import Dict, List, Optional, Tuple
import hashlib
import json
import pickle
from datetime import datetime, timedelta
import logging

from src.indicators.base_indicator import BaseIndicator
from src.query.query_engine import QueryEngine
from src.processors.adjustor import StockAdjustor

logger = logging.getLogger(__name__)


class IndicatorCache:
    """指标计算缓存"""

    def __init__(self, cache_dir: str = "data/cache/indicators"):
        self.cache_dir = cache_dir
        self.cache: Dict[str, pd.DataFrame] = {}

    def get_cache_key(self, symbol: str, indicator_name: str,
                      parameters: Dict, start_date: str, end_date: str) -> str:
        """生成缓存键"""
        cache_str = f"{symbol}_{indicator_name}_{json.dumps(parameters, sort_keys=True)}_\
                     {start_date}_{end_date}"
        return hashlib.md5(cache_str.encode()).hexdigest()

    def get(self, cache_key: str) -> Optional[pd.DataFrame]:
        """从缓存获取数据"""
        if cache_key in self.cache:
            logger.debug(f"从内存缓存获取: {cache_key}")
            return self.cache[cache_key]
        return None

    def set(self, cache_key: str, data: pd.DataFrame, ttl: int = 3600):
        """设置缓存"""
        self.cache[cache_key] = data
        logger.debug(f"设置内存缓存: {cache_key}")


class IndicatorManager:
    """技术指标管理器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化指标管理器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.query_engine = QueryEngine(config_path)
        self.adjustor = StockAdjustor(config_path)
        self.indicators: Dict[str, BaseIndicator] = {}
        self.cache = IndicatorCache()

        # 注册内置指标
        self._register_builtin_indicators()

    def _register_builtin_indicators(self):
        """注册内置指标"""
        from .trend.moving_average import MovingAverage
        from .trend.macd import MACD
        from .momentum.rsi import RSI
        from .volatility.bollinger_bands import BollingerBands
        from .volume.obv import OBV

        # 注册趋势指标
        self.register_indicator(MovingAverage())
        self.register_indicator(MACD())

        # 注册动量指标
        self.register_indicator(RSI())

        # 注册波动率指标
        self.register_indicator(BollingerBands())

        # 注册成交量指标
        self.register_indicator(OBV())

        logger.info(f"已注册内置指标: {list(self.indicators.keys())}")

    def register_indicator(self, indicator: BaseIndicator):
        """
        注册技术指标

        Args:
            indicator: 技术指标实例
        """
        if indicator.name in self.indicators:
            logger.warning(f"指标 {indicator.name} 已存在，将被覆盖")

        self.indicators[indicator.name] = indicator
        logger.info(f"注册指标: {indicator.name} ({indicator.indicator_type.value})")

    def calculate_for_symbol(self, symbol: str,
                             indicator_names: List[str],
                             start_date: str,
                             end_date: str,
                             use_cache: bool = True) -> Dict[str, pd.DataFrame]:
        """
        为指定股票计算多个指标

        Args:
            symbol: 股票代码
            indicator_names: 指标名称列表
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存

        Returns:
            计算结果字典 {指标名: DataFrame}
        """
        logger.info(f"计算指标: {symbol}, 指标: {indicator_names}, 日期: {start_date} - {end_date}")

        # 获取原始数据
        try:
            df = self.query_engine.query_daily_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                logger.warning(f"股票 {symbol} 在 {start_date} - {end_date} 期间无数据")
                return {}

            logger.info(f"获取到 {len(df)} 条数据")

        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return {}

        # 计算结果
        results = {}
        for indicator_name in indicator_names:
            if indicator_name not in self.indicators:
                logger.warning(f"指标 {indicator_name} 未注册")
                continue

            try:
                indicator = self.indicators[indicator_name]

                # 检查缓存
                cache_key = None
                if use_cache:
                    cache_key = self.cache.get_cache_key(
                        symbol, indicator_name,
                        indicator.parameters, start_date, end_date
                    )
                    cached_result = self.cache.get(cache_key)
                    if cached_result is not None:
                        results[indicator_name] = cached_result
                        logger.debug(f"使用缓存结果: {indicator_name}")
                        continue

                # 计算指标
                result_df = indicator.calculate(df.copy())

                # 缓存结果
                if use_cache and cache_key:
                    self.cache.set(cache_key, result_df)

                results[indicator_name] = result_df
                logger.info(f"完成指标计算: {indicator_name}")

            except Exception as e:
                logger.error(f"计算指标 {indicator_name} 失败: {e}")

        return results

    def get_available_indicators(self) -> Dict[str, Dict]:
        """
        获取所有可用指标信息

        Returns:
            指标信息字典
        """
        info = {}
        for name, indicator in self.indicators.items():
            info[name] = {
                'type': indicator.indicator_type.value,
                'description': indicator.description,
                'parameters': indicator.parameters,
                'requires_adjusted_price': indicator.requires_adjusted_price,
                'min_data_points': indicator.min_data_points
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
            if indicator_name in self.indicators:
                indicator = self.indicators[indicator_name]
                if data_count < indicator.min_data_points:
                    return False, f"指标 {indicator_name} 需要至少 {indicator.min_data_points} 条数据，当前只有 {data_count} 条"

        return True, f"数据足够，共 {data_count} 条数据"