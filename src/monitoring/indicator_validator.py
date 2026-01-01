# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/monitoring\indicator_validator.py
# File Name: indicator_validator
# @ Author: mango-gh22
# @ Date：2025/12/21 22:22
"""
desc 
"""

"""
指标验证器
验证技术指标计算的正确性，确保数据质量
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import logging
from scipy import stats

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """验证结果"""
    indicator_name: str
    timestamp: datetime
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, float]
    test_data_size: int


class IndicatorValidator:
    """指标验证器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tolerance = config.get('tolerance', 0.001)
        self.validation_history = []
        self.max_history = config.get('max_history', 100)

        # 预定义的有效范围
        self.valid_ranges = {
            'RSI': (0, 100),
            'MACD': (-np.inf, np.inf),
            'BB_upper': (-np.inf, np.inf),
            'BB_lower': (-np.inf, np.inf),
            'Stochastic_K': (0, 100),
            'Stochastic_D': (0, 100),
            'CCI': (-200, 200),
            'Williams_R': (-100, 0),
            'OBV': (-np.inf, np.inf)
        }

    def validate_indicator(self,
                           indicator_name: str,
                           indicator_data: pd.Series,
                           price_data: pd.DataFrame,
                           params: Dict[str, Any]) -> ValidationResult:
        """验证技术指标"""
        errors = []
        warnings = []
        metrics = {}

        logger.info(f"开始验证指标: {indicator_name}")

        # 1. 基本数据验证
        self._validate_basic(indicator_data, errors, warnings)

        # 2. 特定指标验证
        if indicator_name in self.valid_ranges:
            self._validate_range(indicator_name, indicator_data, errors, warnings)

        # 3. 与价格数据的一致性验证
        if not price_data.empty:
            self._validate_consistency(indicator_name, indicator_data, price_data, errors, warnings, metrics)

        # 4. 统计特性验证
        self._validate_statistics(indicator_name, indicator_data, metrics)

        # 5. 边界条件验证
        self._validate_boundary(indicator_data, errors, warnings)

        # 6. 与历史计算的比较（如果可用）
        self._compare_with_history(indicator_name, indicator_data, errors, warnings, metrics)

        # 创建验证结果
        result = ValidationResult(
            indicator_name=indicator_name,
            timestamp=datetime.now(),
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            metrics=metrics,
            test_data_size=len(indicator_data)
        )

        # 保存到历史记录
        self._save_to_history(result)

        # 记录结果
        if result.is_valid:
            logger.info(f"指标 {indicator_name} 验证通过")
        else:
            logger.warning(f"指标 {indicator_name} 验证失败: {errors}")

        return result

    def _validate_basic(self, data: pd.Series, errors: List[str], warnings: List[str]):
        """基本数据验证"""
        # 检查是否为NaN
        nan_count = data.isna().sum()
        if nan_count > 0:
            warnings.append(f"包含 {nan_count} 个NaN值")

        # 检查无穷大值
        if np.isinf(data).any():
            errors.append("包含无穷大值")

        # 检查数据长度
        if len(data) < 10:
            warnings.append("数据长度小于10，可能不足以保证计算准确性")

    def _validate_range(self, indicator_name: str, data: pd.Series, errors: List[str], warnings: List[str]):
        """验证数值范围"""
        valid_min, valid_max = self.valid_ranges[indicator_name]

        if indicator_name in ['RSI', 'Stochastic_K', 'Stochastic_D']:
            # 这些指标应该在0-100之间，但允许轻微超出
            out_of_range = data[(data < valid_min - 0.1) | (data > valid_max + 0.1)]
            if len(out_of_range) > 0:
                warnings.append(f"{len(out_of_range)} 个值超出理论范围 ({valid_min}-{valid_max})")
        else:
            # 对于其他指标，检查极值
            extreme_values = data[abs(data) > 1e10]
            if len(extreme_values) > 0:
                warnings.append(f"发现 {len(extreme_values)} 个极端值")

    def _validate_consistency(self,
                              indicator_name: str,
                              indicator_data: pd.Series,
                              price_data: pd.DataFrame,
                              errors: List[str],
                              warnings: List[str],
                              metrics: Dict[str, float]):
        """验证与价格数据的一致性"""
        try:
            # 确保索引对齐
            aligned_data = indicator_data.align(price_data['close'].iloc[-len(indicator_data):])[0]

            # 计算相关性
            if len(aligned_data) > 5:
                correlation = aligned_data.corr(price_data['close'].iloc[-len(aligned_data):])
                metrics['price_correlation'] = correlation

                # 根据指标类型检查相关性
                if indicator_name == 'RSI':
                    # RSI应该与价格有负相关性（超买超卖）
                    if correlation > 0.8:
                        warnings.append(f"RSI与价格正相关性过高: {correlation:.3f}")
                elif indicator_name in ['MACD', 'CCI']:
                    # 这些指标应该与价格有一定相关性
                    if abs(correlation) < 0.3:
                        warnings.append(f"与价格相关性较低: {correlation:.3f}")

        except Exception as e:
            warnings.append(f"一致性验证失败: {str(e)}")

    def _validate_statistics(self, indicator_name: str, data: pd.Series, metrics: Dict[str, float]):
        """验证统计特性"""
        clean_data = data.dropna()

        if len(clean_data) > 10:
            metrics['mean'] = float(clean_data.mean())
            metrics['std'] = float(clean_data.std())
            metrics['skew'] = float(stats.skew(clean_data))
            metrics['kurtosis'] = float(stats.kurtosis(clean_data))
            metrics['min'] = float(clean_data.min())
            metrics['max'] = float(clean_data.max())

            # 检查正态性（对于某些指标）
            if indicator_name in ['MACD', 'CCI']:
                _, p_value = stats.normaltest(clean_data)
                metrics['normality_p'] = float(p_value)

    def _validate_boundary(self, data: pd.Series, errors: List[str], warnings: List[str]):
        """边界条件验证"""
        # 检查数据突变
        diff = data.diff().dropna()
        if len(diff) > 0:
            max_diff = diff.abs().max()
            if max_diff > 100:  # 假设变化超过100为异常
                warnings.append(f"发现数据突变，最大变化: {max_diff:.2f}")

    def _compare_with_history(self,
                              indicator_name: str,
                              current_data: pd.Series,
                              errors: List[str],
                              warnings: List[str],
                              metrics: Dict[str, float]):
        """与历史计算结果比较"""
        # 查找相同指标的历史验证结果
        history_results = [
            r for r in self.validation_history
            if r.indicator_name == indicator_name and r.test_data_size == len(current_data)
        ]

        if history_results:
            # 取最近的5个结果
            recent_results = history_results[-5:]

            # 计算平均统计量
            avg_mean = np.mean([r.metrics.get('mean', 0) for r in recent_results if 'mean' in r.metrics])
            avg_std = np.mean([r.metrics.get('std', 0) for r in recent_results if 'std' in r.metrics])

            current_mean = current_data.mean()
            current_std = current_data.std()

            # 检查偏差
            mean_diff = abs(current_mean - avg_mean)
            std_diff = abs(current_std - avg_std)

            metrics['mean_diff'] = float(mean_diff)
            metrics['std_diff'] = float(std_diff)

            if mean_diff > 0.1 * avg_std:  # 均值偏差超过0.1个标准差
                warnings.append(f"均值与历史平均值偏差较大: {mean_diff:.3f}")

    def _save_to_history(self, result: ValidationResult):
        """保存验证结果到历史记录"""
        self.validation_history.append(result)
        if len(self.validation_history) > self.max_history:
            self.validation_history = self.validation_history[-self.max_history:]

    def validate_multiple_indicators(self,
                                     indicators: Dict[str, pd.Series],
                                     price_data: pd.DataFrame) -> Dict[str, ValidationResult]:
        """批量验证多个指标"""
        results = {}
        for name, data in indicators.items():
            results[name] = self.validate_indicator(name, data, price_data, {})
        return results

    def generate_validation_report(self) -> Dict[str, Any]:
        """生成验证报告"""
        if not self.validation_history:
            return {}

        # 统计验证结果
        total = len(self.validation_history)
        valid_count = sum(1 for r in self.validation_history if r.is_valid)
        error_count = sum(len(r.errors) for r in self.validation_history)
        warning_count = sum(len(r.warnings) for r in self.validation_history)

        # 按指标分组统计
        indicator_stats = {}
        for result in self.validation_history:
            name = result.indicator_name
            if name not in indicator_stats:
                indicator_stats[name] = {
                    'count': 0,
                    'valid_count': 0,
                    'total_errors': 0,
                    'total_warnings': 0
                }

            stats = indicator_stats[name]
            stats['count'] += 1
            if result.is_valid:
                stats['valid_count'] += 1
            stats['total_errors'] += len(result.errors)
            stats['total_warnings'] += len(result.warnings)

        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_validations': total,
                'valid_rate': valid_count / total if total > 0 else 0,
                'total_errors': error_count,
                'total_warnings': warning_count,
                'avg_errors_per_validation': error_count / total if total > 0 else 0
            },
            'indicator_stats': indicator_stats,
            'recent_validations': [
                {
                    'indicator': r.indicator_name,
                    'timestamp': r.timestamp.isoformat(),
                    'is_valid': r.is_valid,
                    'error_count': len(r.errors),
                    'warning_count': len(r.warnings)
                }
                for r in self.validation_history[-10:]  # 最近10条
            ]
        }

        return report