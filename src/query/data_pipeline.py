# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\data_pipeline.py
# File Name: data_pipeline
# @ Author: mango-gh22
# @ Date：2025/12/21 18:49
"""
desc 数据预处理管道，这是查询系统的基础
"""

"""
File: src/query/data_pipeline.py
Desc: 数据预处理管道 - 为指标计算准备数据
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DataQuality(Enum):
    """数据质量等级"""
    EXCELLENT = "excellent"  # 数据完整，质量高
    GOOD = "good"  # 数据基本完整，少量缺失
    FAIR = "fair"  # 数据有缺失，但可用
    POOR = "poor"  # 数据缺失严重
    INVALID = "invalid"  # 数据无效


@dataclass
class DataQualityReport:
    """数据质量报告"""
    symbol: str
    date_range: Tuple[str, str]
    total_rows: int
    missing_data: Dict[str, int]
    quality_score: float
    quality_level: DataQuality
    suggestions: List[str]
    processed_rows: int


class DataPreprocessor:
    """数据预处理器"""

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化数据预处理器

        Args:
            config: 配置字典
        """
        self.config = config or {}

        # 默认配置
        self.default_config = {
            'fill_method': 'ffill',  # 缺失值填充方法
            'outlier_method': 'iqr',  # 异常值处理方法
            'normalize': False,  # 是否标准化
            'remove_duplicates': True,  # 是否去重
            'adjust_prices': True,  # 是否复权价格
            'min_data_points': 20,  # 最小数据点数
            'quality_threshold': 0.8,  # 质量阈值
        }

        # 更新配置
        self.default_config.update(self.config)

        # 必需的价格字段
        self.required_price_columns = [
            'open_price', 'high_price', 'low_price', 'close_price'
        ]

        # 辅助字段
        self.auxiliary_columns = ['volume', 'amount', 'turnover_rate', 'pre_close']

    def preprocess(self, df: pd.DataFrame, symbol: str,
                   start_date: str, end_date: str) -> Tuple[pd.DataFrame, DataQualityReport]:
        """
        预处理数据

        Args:
            df: 原始数据DataFrame
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            (预处理后的DataFrame, 质量报告)
        """
        logger.info(f"预处理数据: {symbol}, {start_date} 到 {end_date}, 原始形状: {df.shape}")

        if df.empty:
            logger.warning("数据为空，返回空DataFrame")
            report = DataQualityReport(
                symbol=symbol,
                date_range=(start_date, end_date),
                total_rows=0,
                missing_data={},
                quality_score=0.0,
                quality_level=DataQuality.INVALID,
                suggestions=["数据为空"],
                processed_rows=0
            )
            return df, report

        # 复制数据避免修改原始数据
        processed_df = df.copy()

        # 1. 基础处理
        processed_df = self._basic_processing(processed_df, symbol)

        # 2. 数据质量检查
        quality_report = self._check_data_quality(processed_df, symbol, start_date, end_date)

        # 3. 如果质量太差，返回原始数据
        if quality_report.quality_level == DataQuality.INVALID:
            logger.warning(f"数据质量太差: {symbol}, 跳过进一步处理")
            return df, quality_report

        # 4. 缺失值处理
        processed_df = self._handle_missing_values(processed_df)

        # 5. 异常值处理
        processed_df = self._handle_outliers(processed_df)

        # 6. 数据增强
        processed_df = self._enhance_data(processed_df)

        # 7. 数据类型转换
        processed_df = self._convert_data_types(processed_df)

        # 更新处理后的行数
        quality_report.processed_rows = len(processed_df)

        logger.info(
            f"预处理完成: {symbol}, 处理后形状: {processed_df.shape}, 质量: {quality_report.quality_level.value}")

        return processed_df, quality_report

    def _basic_processing(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """基础处理"""
        result_df = df.copy()

        # 确保有日期索引
        if 'trade_date' in result_df.columns:
            result_df['trade_date'] = pd.to_datetime(result_df['trade_date'])
            result_df.set_index('trade_date', inplace=True)

        # 确保有symbol列
        if 'symbol' not in result_df.columns:
            result_df['symbol'] = symbol

        # 按日期排序
        result_df.sort_index(inplace=True)

        # 去除完全重复的行
        if self.default_config['remove_duplicates']:
            before = len(result_df)
            result_df = result_df[~result_df.index.duplicated(keep='first')]
            after = len(result_df)
            if before > after:
                logger.info(f"去重: {symbol}, 移除 {before - after} 个重复行")

        return result_df

    def _check_data_quality(self, df: pd.DataFrame, symbol: str,
                            start_date: str, end_date: str) -> DataQualityReport:
        """检查数据质量"""
        total_rows = len(df)

        if total_rows == 0:
            return DataQualityReport(
                symbol=symbol,
                date_range=(start_date, end_date),
                total_rows=0,
                missing_data={},
                quality_score=0.0,
                quality_level=DataQuality.INVALID,
                suggestions=["数据为空"],
                processed_rows=0
            )

        # 检查缺失值
        missing_data = {}
        for col in self.required_price_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    missing_data[col] = missing_count

        # 计算质量分数
        completeness_scores = []

        # 价格列完整度
        for col in self.required_price_columns:
            if col in df.columns:
                completeness = 1.0 - (df[col].isnull().sum() / total_rows)
                completeness_scores.append(completeness)
            else:
                completeness_scores.append(0.0)

        # 数据连续性评分
        if len(df) > 1:
            date_diff = df.index.to_series().diff().dropna()
            expected_freq = pd.Timedelta(days=1)  # 假设日频数据
            continuity_score = (date_diff == expected_freq).mean()
            completeness_scores.append(continuity_score)

        # 最终质量分数
        quality_score = np.mean(completeness_scores) if completeness_scores else 0.0

        # 确定质量等级
        if quality_score >= 0.95:
            quality_level = DataQuality.EXCELLENT
        elif quality_score >= 0.85:
            quality_level = DataQuality.GOOD
        elif quality_score >= 0.70:
            quality_level = DataQuality.FAIR
        elif quality_score >= 0.50:
            quality_level = DataQuality.POOR
        else:
            quality_level = DataQuality.INVALID

        # 生成建议
        suggestions = []

        if quality_score < 0.95:
            if len(df) < self.default_config['min_data_points']:
                suggestions.append(f"数据点数不足: {len(df)} < {self.default_config['min_data_points']}")

            for col, missing_count in missing_data.items():
                if missing_count > 0:
                    suggestions.append(f"列 {col} 有 {missing_count} 个缺失值")

        return DataQualityReport(
            symbol=symbol,
            date_range=(start_date, end_date),
            total_rows=total_rows,
            missing_data=missing_data,
            quality_score=quality_score,
            quality_level=quality_level,
            suggestions=suggestions,
            processed_rows=total_rows
        )

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理缺失值"""
        result_df = df.copy()

        fill_method = self.default_config['fill_method']

        # 对于价格数据，使用前向填充
        price_cols = [col for col in self.required_price_columns if col in result_df.columns]

        if price_cols:
            if fill_method == 'ffill':
                result_df[price_cols] = result_df[price_cols].ffill()
            elif fill_method == 'bfill':
                result_df[price_cols] = result_df[price_cols].bfill()
            elif fill_method == 'interpolate':
                result_df[price_cols] = result_df[price_cols].interpolate(method='linear')

        # 对于成交量等数据，用0填充
        volume_cols = ['volume', 'amount']
        for col in volume_cols:
            if col in result_df.columns:
                result_df[col] = result_df[col].fillna(0)

        return result_df

    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理异常值"""
        result_df = df.copy()

        outlier_method = self.default_config['outlier_method']

        # 只处理价格数据
        price_cols = [col for col in self.required_price_columns if col in result_df.columns]

        if not price_cols or outlier_method == 'none':
            return result_df

        for col in price_cols:
            if col in result_df.columns:
                series = result_df[col]

                if outlier_method == 'iqr':
                    # IQR方法检测异常值
                    Q1 = series.quantile(0.25)
                    Q3 = series.quantile(0.75)
                    IQR = Q3 - Q1

                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR

                    # 标记异常值但不修改（只记录日志）
                    outliers = (series < lower_bound) | (series > upper_bound)
                    if outliers.any():
                        logger.debug(f"检测到 {col} 有 {outliers.sum()} 个异常值")

                elif outlier_method == 'zscore':
                    # Z-score方法
                    mean = series.mean()
                    std = series.std()

                    if std > 0:
                        z_scores = (series - mean) / std
                        outliers = abs(z_scores) > 3
                        if outliers.any():
                            logger.debug(f"检测到 {col} 有 {outliers.sum()} 个异常值 (Z-score)")

        return result_df

    def _enhance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据增强 - 添加衍生特征"""
        result_df = df.copy()

        # 确保有必要的列
        if 'close_price' not in result_df.columns:
            return result_df

        close_series = result_df['close_price']

        # 1. 价格变化
        result_df['price_change'] = close_series.diff()
        result_df['price_pct_change'] = close_series.pct_change(fill_method=None) * 100

        # 2. 简单移动平均（短期）
        if len(result_df) >= 5:
            result_df['sma_5'] = close_series.rolling(window=5).mean()

        if len(result_df) >= 10:
            result_df['sma_10'] = close_series.rolling(window=10).mean()

        # 3. 价格范围
        if all(col in result_df.columns for col in ['high_price', 'low_price']):
            result_df['price_range'] = result_df['high_price'] - result_df['low_price']
            result_df['price_range_pct'] = result_df['price_range'] / result_df['low_price'].replace(0, 1) * 100

        # 4. 成交量相关特征
        if 'volume' in result_df.columns:
            result_df['volume_ma_5'] = result_df['volume'].rolling(window=5).mean()
            result_df['volume_ratio'] = result_df['volume'] / result_df['volume_ma_5'].replace(0, 1)

        # 5. 日期特征
        if isinstance(result_df.index, pd.DatetimeIndex):
            result_df['day_of_week'] = result_df.index.dayofweek
            result_df['month'] = result_df.index.month
            result_df['quarter'] = result_df.index.quarter

        return result_df

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据类型转换"""
        result_df = df.copy()

        # 价格列转换为float32
        price_cols = [col for col in self.required_price_columns if col in result_df.columns]
        for col in price_cols:
            if result_df[col].dtype != np.float32:
                result_df[col] = result_df[col].astype(np.float32)

        # 成交量列转换为int32
        volume_cols = ['volume', 'amount']
        for col in volume_cols:
            if col in result_df.columns and result_df[col].dtype != np.int32:
                result_df[col] = result_df[col].fillna(0).astype(np.int32)

        return result_df

    def prepare_for_indicator(self, df: pd.DataFrame, indicator_name: str,
                              indicator_params: Dict) -> pd.DataFrame:
        """
        为特定指标准备数据

        Args:
            df: 原始数据
            indicator_name: 指标名称
            indicator_params: 指标参数

        Returns:
            准备好的数据
        """
        logger.debug(f"为指标 {indicator_name} 准备数据")

        # 复制数据
        prepared_df = df.copy()

        # 根据指标类型进行特殊处理
        if indicator_name == 'macd':
            # MACD需要复权价格
            if self.default_config['adjust_prices']:
                prepared_df = self._adjust_prices(prepared_df)

        elif indicator_name == 'rsi':
            # RSI需要价格变化
            if 'close_price' in prepared_df.columns:
                prepared_df['price_change'] = prepared_df['close_price'].diff()

        elif indicator_name == 'bollinger_bands':
            # 布林带可能需要波动率特征
            if all(col in prepared_df.columns for col in ['high_price', 'low_price']):
                prepared_df['price_range'] = prepared_df['high_price'] - prepared_df['low_price']

        return prepared_df

    def _adjust_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """价格复权（简化版本）"""
        result_df = df.copy()

        # 这里应该调用复权模块，这里简化处理
        # 在实际系统中，这里会调用 StockAdjustor
        logger.debug("价格复权处理（简化）")

        return result_df

    def batch_preprocess(self, data_dict: Dict[str, pd.DataFrame],
                         start_date: str, end_date: str) -> Dict[str, Tuple[pd.DataFrame, DataQualityReport]]:
        """
        批量预处理数据

        Args:
            data_dict: {symbol: DataFrame} 字典
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            {symbol: (预处理后的DataFrame, 质量报告)} 字典
        """
        results = {}

        for symbol, df in data_dict.items():
            try:
                processed_df, quality_report = self.preprocess(
                    df, symbol, start_date, end_date
                )
                results[symbol] = (processed_df, quality_report)

                logger.info(f"批量预处理完成: {symbol}, 质量: {quality_report.quality_level.value}")

            except Exception as e:
                logger.error(f"预处理 {symbol} 失败: {e}")
                # 返回原始数据和质量报告
                report = DataQualityReport(
                    symbol=symbol,
                    date_range=(start_date, end_date),
                    total_rows=len(df),
                    missing_data={},
                    quality_score=0.0,
                    quality_level=DataQuality.INVALID,
                    suggestions=[f"预处理失败: {str(e)}"],
                    processed_rows=0
                )
                results[symbol] = (df, report)

        return results


class DataPipeline:
    """数据管道 - 集成预处理和增强功能"""

    def __init__(self, preprocessor_config: Optional[Dict] = None):
        """
        初始化数据管道

        Args:
            preprocessor_config: 预处理器配置
        """
        self.preprocessor = DataPreprocessor(preprocessor_config)
        self.cache: Dict[str, pd.DataFrame] = {}

    def process(self, df: pd.DataFrame, symbol: str,
                start_date: str, end_date: str,
                use_cache: bool = True) -> Tuple[pd.DataFrame, DataQualityReport]:
        """
        处理数据

        Args:
            df: 原始数据
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存

        Returns:
            (处理后的数据, 质量报告)
        """
        # 生成缓存键
        cache_key = f"{symbol}_{start_date}_{end_date}"

        # 检查缓存
        if use_cache and cache_key in self.cache:
            logger.debug(f"使用缓存数据: {cache_key}")
            cached_data = self.cache[cache_key]

            # 生成简化质量报告
            report = DataQualityReport(
                symbol=symbol,
                date_range=(start_date, end_date),
                total_rows=len(cached_data),
                missing_data={},
                quality_score=1.0,
                quality_level=DataQuality.EXCELLENT,
                suggestions=["使用缓存数据"],
                processed_rows=len(cached_data)
            )

            return cached_data, report

        # 预处理数据
        processed_df, quality_report = self.preprocessor.preprocess(
            df, symbol, start_date, end_date
        )

        # 缓存结果
        if use_cache and quality_report.quality_level != DataQuality.INVALID:
            self.cache[cache_key] = processed_df.copy()
            logger.debug(f"缓存数据: {cache_key}, 形状: {processed_df.shape}")

        return processed_df, quality_report

    def prepare_for_indicators(self, df: pd.DataFrame, symbol: str,
                               indicators: List[str],
                               indicator_params: Dict[str, Dict]) -> Dict[str, pd.DataFrame]:
        """
        为多个指标准备数据

        Args:
            df: 原始数据
            symbol: 股票代码
            indicators: 指标列表
            indicator_params: 各指标参数

        Returns:
            {指标名: 准备好的数据} 字典
        """
        results = {}

        # 确保有日期列
        if 'trade_date' not in df.columns and isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()

        # 基础预处理 - 获取日期范围
        if 'trade_date' in df.columns:
            try:
                # 将日期列转换为datetime类型
                if not pd.api.types.is_datetime64_any_dtype(df['trade_date']):
                    df['trade_date'] = pd.to_datetime(df['trade_date'])

                min_date = df['trade_date'].min()
                max_date = df['trade_date'].max()

                # 格式化为字符串
                min_date_str = min_date.strftime('%Y-%m-%d') if hasattr(min_date, 'strftime') else str(min_date)
                max_date_str = max_date.strftime('%Y-%m-%d') if hasattr(max_date, 'strftime') else str(max_date)

                processed_df, _ = self.preprocessor.preprocess(
                    df, symbol, min_date_str, max_date_str
                )
            except Exception as e:
                # 如果日期处理失败，使用默认值
                logger.warning(f"日期处理失败: {e}")
                processed_df, _ = self.preprocessor.preprocess(
                    df, symbol, '2020-01-01', '2024-12-31'
                )
        else:
            # 如果没有日期列，使用默认值
            processed_df, _ = self.preprocessor.preprocess(
                df, symbol, '2020-01-01', '2024-12-31'
            )

        # 为每个指标准备数据
        for indicator in indicators:
            if indicator not in indicator_params:
                indicator_params[indicator] = {}

            # 复制基础数据
            indicator_df = processed_df.copy()

            # 添加时间索引（如果还没有）
            if not isinstance(indicator_df.index, pd.DatetimeIndex):
                if 'trade_date' in indicator_df.columns:
                    indicator_df.set_index('trade_date', inplace=True)

            # 确保有必要的列
            required_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for col in required_cols:
                if col not in indicator_df.columns:
                    if col == 'volume' and 'volume' not in indicator_df.columns:
                        # 为volume创建默认值
                        indicator_df['volume'] = 1000
                    else:
                        # 为价格列创建默认值
                        indicator_df[col] = indicator_df.get('close_price', 100)

            results[indicator] = indicator_df

        return results

    def clear_cache(self):
        """清理缓存"""
        self.cache.clear()
        logger.info("清理数据管道缓存")

    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        return {
            'cache_size': len(self.cache),
            'cached_symbols': list(self.cache.keys()),
            'total_rows': sum(len(df) for df in self.cache.values())
        }