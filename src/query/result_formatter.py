# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\result_formatter.py
# File Name: result_formatter
# @ Author: mango-gh22
# @ Date：2025/12/21 18:50
"""
desc 
"""

"""
File: src/query/result_formatter.py
Desc: 结果格式化器 - 格式化查询结果
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
import json
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class ResultFormatter:
    """结果格式化器"""

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化结果格式化器

        Args:
            config: 配置字典
        """
        self.config = config or {}

        # 默认配置
        self.default_config = {
            'decimal_places': 4,  # 小数位数
            'date_format': '%Y-%m-%d',  # 日期格式
            'include_metadata': True,  # 是否包含元数据
            'compact_mode': False,  # 紧凑模式
            'max_rows': 1000,  # 最大行数
            'include_statistics': True,  # 是否包含统计信息
        }

        # 更新配置
        self.default_config.update(self.config)

    def format_dataframe(self, df: pd.DataFrame,
                         symbol: str = None,
                         indicators: List[str] = None,
                         metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        格式化DataFrame为API响应格式

        Args:
            df: 输入的DataFrame
            symbol: 股票代码
            indicators: 计算的指标列表
            metadata: 额外元数据

        Returns:
            格式化的字典
        """
        logger.info(f"格式化DataFrame，形状: {df.shape}")

        if df.empty:
            return self._format_empty_result(symbol, indicators)

        # 限制行数
        if len(df) > self.default_config['max_rows']:
            logger.warning(f"数据行数 {len(df)} 超过限制 {self.default_config['max_rows']}，进行截断")
            df = df.tail(self.default_config['max_rows'])

        # 格式化数据
        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
        }

        # 添加元数据
        if self.default_config['include_metadata']:
            result['metadata'] = self._create_metadata(df, symbol, indicators, metadata)

        # 添加数据
        if self.default_config['compact_mode']:
            result['data'] = self._format_compact(df)
        else:
            result['data'] = self._format_detailed(df)

        # 添加统计信息
        if self.default_config['include_statistics']:
            result['statistics'] = self._calculate_statistics(df)

        return result

    def _format_empty_result(self, symbol: Optional[str], indicators: Optional[List[str]]) -> Dict[str, Any]:
        """格式化空结果"""
        return {
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'error': 'No data available',
            'symbol': symbol,
            'indicators': indicators,
            'data': [],
            'metadata': {
                'row_count': 0,
                'column_count': 0,
                'date_range': None
            }
        }

    def _create_metadata(self, df: pd.DataFrame, symbol: Optional[str],
                         indicators: Optional[List[str]],
                         extra_metadata: Optional[Dict]) -> Dict[str, Any]:
        """创建元数据"""
        metadata = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': df.columns.tolist(),
            'date_range': None,
            'symbol': symbol,
            'indicators': indicators,
        }

        # 添加日期范围
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 0:
            metadata['date_range'] = {
                'start': df.index.min().strftime(self.default_config['date_format']),
                'end': df.index.max().strftime(self.default_config['date_format']),
                'days': (df.index.max() - df.index.min()).days
            }
        elif 'trade_date' in df.columns and len(df) > 0:
            dates = pd.to_datetime(df['trade_date'])
            metadata['date_range'] = {
                'start': dates.min().strftime(self.default_config['date_format']),
                'end': dates.max().strftime(self.default_config['date_format']),
                'days': (dates.max() - dates.min()).days
            }

        # 添加指标列信息
        if indicators:
            indicator_columns = {}
            for indicator in indicators:
                # 查找属于该指标的列
                indicator_cols = [col for col in df.columns if indicator in col.lower() or col.lower() in indicator]
                if indicator_cols:
                    indicator_columns[indicator] = indicator_cols

            if indicator_columns:
                metadata['indicator_columns'] = indicator_columns

        # 合并额外元数据
        if extra_metadata:
            metadata.update(extra_metadata)

        return metadata

    def _format_compact(self, df: pd.DataFrame) -> List[Dict]:
        """紧凑格式：只包含数据"""
        return df.to_dict(orient='records')

    def _format_detailed(self, df: pd.DataFrame) -> Dict[str, Any]:
        """详细格式：包含数据类型和格式信息"""
        result = {
            'records': df.to_dict(orient='records'),
            'schema': self._get_schema(df),
            'sample': self._get_sample_data(df, n=5)
        }
        return result

    def _get_schema(self, df: pd.DataFrame) -> List[Dict]:
        """获取数据模式"""
        schema = []

        for col in df.columns:
            col_info = {
                'name': col,
                'type': str(df[col].dtype),
                'non_null_count': df[col].count(),
                'null_count': df[col].isnull().sum(),
            }

            # 数值列的额外信息
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info['min'] = float(df[col].min()) if not df[col].isnull().all() else None
                col_info['max'] = float(df[col].max()) if not df[col].isnull().all() else None
                col_info['mean'] = float(df[col].mean()) if not df[col].isnull().all() else None

            # 日期列的额外信息
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_info['date_format'] = self.default_config['date_format']

            schema.append(col_info)

        return schema

    def _get_sample_data(self, df: pd.DataFrame, n: int = 5) -> Dict[str, List]:
        """获取样本数据（首尾各n/2行）"""
        if len(df) <= n:
            sample_df = df
        else:
            # 取前n/2行和后n/2行
            first_half = df.head(n // 2)
            last_half = df.tail(n - n // 2)
            sample_df = pd.concat([first_half, last_half])

        return sample_df.to_dict(orient='list')

    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算统计信息"""
        stats = {
            'basic': {},
            'numeric_columns': {}
        }

        # 基础统计
        stats['basic']['row_count'] = len(df)
        stats['basic']['column_count'] = len(df.columns)
        stats['basic']['memory_usage'] = df.memory_usage(deep=True).sum()

        # 数值列统计
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].notnull().any():
                col_stats = {
                    'count': int(df[col].count()),
                    'mean': float(df[col].mean()),
                    'std': float(df[col].std()),
                    'min': float(df[col].min()),
                    '25%': float(df[col].quantile(0.25)),
                    '50%': float(df[col].quantile(0.50)),
                    '75%': float(df[col].quantile(0.75)),
                    'max': float(df[col].max()),
                    'null_count': int(df[col].isnull().sum())
                }
                stats['numeric_columns'][col] = col_stats

        return stats

    def format_indicator_results(self, results: Dict[str, pd.DataFrame],
                                 symbol: str,
                                 start_date: str,
                                 end_date: str) -> Dict[str, Any]:
        """
        格式化多个指标的结果

        Args:
            results: {指标名: DataFrame} 字典
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            格式化的结果
        """
        logger.info(f"格式化指标结果: {symbol}, {len(results)} 个指标")

        if not results:
            return {
                'success': False,
                'timestamp': datetime.now().isoformat(),
                'error': 'No indicator results',
                'symbol': symbol,
                'data': {}
            }

        # 合并所有指标结果
        merged_data = self._merge_indicator_results(results)

        # 格式化合并后的数据
        formatted_result = self.format_dataframe(
            df=merged_data,
            symbol=symbol,
            indicators=list(results.keys()),
            metadata={
                'date_range': {'start': start_date, 'end': end_date},
                'indicators_calculated': list(results.keys()),
                'results_count': len(results)
            }
        )

        # 添加每个指标的单独统计
        if self.default_config['include_statistics']:
            indicator_stats = {}
            for indicator_name, indicator_df in results.items():
                if not indicator_df.empty:
                    indicator_stats[indicator_name] = {
                        'row_count': len(indicator_df),
                        'column_count': len(indicator_df.columns),
                        'indicator_columns': [col for col in indicator_df.columns
                                              if col not in merged_data.columns or
                                              col in ['trade_date', 'symbol']]
                    }

            formatted_result['indicator_statistics'] = indicator_stats

        return formatted_result

    def _merge_indicator_results(self, results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """合并多个指标结果"""
        if not results:
            return pd.DataFrame()

        # 获取第一个DataFrame作为基础
        base_df = None
        for df in results.values():
            if df is not None and not df.empty:
                base_df = df.copy()
                break

        if base_df is None:
            return pd.DataFrame()

        merged_df = base_df.copy()

        # 合并其他指标的结果
        for indicator_name, indicator_df in results.items():
            if indicator_df is not base_df and indicator_df is not None and not indicator_df.empty:
                # 只添加指标特有的列，避免重复
                indicator_cols = [col for col in indicator_df.columns
                                  if col not in merged_df.columns or
                                  col in ['trade_date', 'symbol']]

                if indicator_cols:
                    # 确保索引对齐
                    if isinstance(indicator_df.index, pd.DatetimeIndex):
                        temp_df = indicator_df[indicator_cols].copy()
                        merged_df = merged_df.merge(temp_df,
                                                    left_index=True,
                                                    right_index=True,
                                                    how='left')
                    else:
                        # 如果没有DatetimeIndex，假设有trade_date列
                        if 'trade_date' in indicator_df.columns and 'trade_date' in merged_df.columns:
                            temp_df = indicator_df[['trade_date'] + indicator_cols].copy()
                            merged_df = merged_df.merge(temp_df,
                                                        on='trade_date',
                                                        how='left')

        return merged_df

    def format_for_export(self, df: pd.DataFrame, format_type: str = 'csv') -> Union[str, bytes]:
        """
        格式化数据用于导出

        Args:
            df: 输入的DataFrame
            format_type: 导出格式 ('csv', 'json', 'excel')

        Returns:
            格式化的字符串或字节
        """
        logger.info(f"格式化导出数据，格式: {format_type}, 形状: {df.shape}")

        if format_type.lower() == 'csv':
            return df.to_csv(index=False)

        elif format_type.lower() == 'json':
            # 使用自定义JSON序列化器处理日期和NaN
            def default_serializer(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                elif isinstance(obj, (np.integer, np.int64)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64)):
                    return float(obj) if not np.isnan(obj) else None
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                raise TypeError(f"Type {type(obj)} not serializable")

            records = df.to_dict(orient='records')
            return json.dumps(records, default=default_serializer, ensure_ascii=False)

        elif format_type.lower() == 'excel':
            # 这里返回字节，实际使用时可能需要保存到文件
            import io
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
            return output.getvalue()

        else:
            raise ValueError(f"不支持的导出格式: {format_type}")

    def format_error(self, error: Exception, symbol: Optional[str] = None,
                     indicators: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        格式化错误信息

        Args:
            error: 异常对象
            symbol: 股票代码
            indicators: 指标列表

        Returns:
            格式化的错误响应
        """
        return {
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'error': {
                'type': error.__class__.__name__,
                'message': str(error),
                'symbol': symbol,
                'indicators': indicators
            },
            'data': None,
            'metadata': None
        }


class BatchResultFormatter:
    """批量结果格式化器"""

    def __init__(self, config: Optional[Dict] = None):
        self.formatter = ResultFormatter(config)

    def format_batch_results(self, batch_results: Dict[str, Dict[str, pd.DataFrame]],
                             symbols: List[str],
                             indicators: List[str],
                             start_date: str,
                             end_date: str) -> Dict[str, Any]:
        """
        格式化批量计算结果

        Args:
            batch_results: {symbol: {indicator: DataFrame}} 字典
            symbols: 股票代码列表
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            格式化的批量结果
        """
        logger.info(f"格式化批量结果: {len(symbols)} 只股票, {len(indicators)} 个指标")

        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_symbols': len(symbols),
                'total_indicators': len(indicators),
                'successful_symbols': 0,
                'failed_symbols': 0,
                'date_range': {'start': start_date, 'end': end_date}
            },
            'results': {}
        }

        for symbol in symbols:
            if symbol in batch_results and batch_results[symbol]:
                try:
                    # 格式化单个股票的结果
                    symbol_results = batch_results[symbol]
                    formatted = self.formatter.format_indicator_results(
                        symbol_results, symbol, start_date, end_date
                    )

                    if formatted.get('success', False):
                        result['results'][symbol] = formatted
                        result['summary']['successful_symbols'] += 1
                    else:
                        result['results'][symbol] = formatted
                        result['summary']['failed_symbols'] += 1

                except Exception as e:
                    logger.error(f"格式化 {symbol} 结果失败: {e}")
                    result['results'][symbol] = self.formatter.format_error(e, symbol, indicators)
                    result['summary']['failed_symbols'] += 1
            else:
                result['results'][symbol] = {
                    'success': False,
                    'error': f'No results for symbol {symbol}',
                    'symbol': symbol
                }
                result['summary']['failed_symbols'] += 1

        return result