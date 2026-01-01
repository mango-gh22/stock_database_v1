# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\config_validator.py
# File Name: config_validator
# @ Author: mango-gh22
# @ Date：2025/12/22 0:52
"""
desc 修复版（关键部分）
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config/config_validator.py

from typing import Dict, Any, Union
import logging

logger = logging.getLogger(__name__)


class ConfigValidator:
    """配置验证器 - 修复版"""

    @staticmethod
    def validate_and_fix(config: Dict[str, Any]) -> Dict[str, Any]:
        """验证并修复配置 - 修复版"""
        if not config:
            logger.warning("接收到空配置，返回空字典")
            return {}

        # 深度复制配置
        import copy
        fixed_config = copy.deepcopy(config)

        # 递归修复配置
        ConfigValidator._fix_config_recursive(fixed_config)

        return fixed_config

    @staticmethod
    def _fix_config_recursive(config: Dict[str, Any]):
        """递归修复配置 - 增强容错"""
        if not isinstance(config, dict):
            logger.warning(f"配置不是字典类型: {type(config)}")
            return

        for key, value in list(config.items()):  # 使用list避免遍历时修改字典
            if isinstance(value, dict):
                # 如果是 parallel_computing 或 parallel 配置，特殊处理
                if key in ['parallel_computing', 'parallel', 'cache', 'caching', 'memory_management']:
                    ConfigValidator._fix_dict_config(key, value, config)
                else:
                    ConfigValidator._fix_config_recursive(value)
            else:
                # 修复特定键的类型
                try:
                    config[key] = ConfigValidator._fix_value_type(key, value)
                except Exception as e:
                    logger.error(f"修复配置项 {key}={value} 失败: {e}")
                    # 使用安全默认值
                    config[key] = ConfigValidator._get_safe_default(key)

    @staticmethod
    def _fix_dict_config(dict_key: str, dict_value: Dict[str, Any], parent_config: Dict[str, Any]):
        """修复字典类型的配置（如 parallel_computing）"""
        logger.debug(f"修复字典配置: {dict_key}")

        # 确保字典中的值都是标量类型
        for k, v in dict_value.items():
            if isinstance(v, dict):
                # 如果值还是字典，递归修复
                ConfigValidator._fix_dict_config(k, v, dict_value)
            else:
                # 修复标量值类型
                dict_value[k] = ConfigValidator._fix_value_type(k, v)

        # 特殊处理：如果 parent_config 期望的是标量，但 dict_value 是字典
        # 例如：max_workers: {'max_workers': 4} → max_workers: 4
        if dict_key in ['max_workers', 'max_size', 'ttl', 'timeout', 'interval']:
            if 'max_workers' in dict_value:
                parent_config[dict_key] = dict_value['max_workers']
            elif dict_key in dict_value:
                parent_config[dict_key] = dict_value[dict_key]
            else:
                # 提取第一个整数值
                int_values = [v for v in dict_value.values() if isinstance(v, (int, float))]
                if int_values:
                    parent_config[dict_key] = int_values[0]
                else:
                    parent_config[dict_key] = ConfigValidator._get_safe_default(dict_key)

    @staticmethod
    def _fix_value_type(key: str, value: Any) -> Any:
        """根据键名修复值类型 - 增强容错"""
        # 修复点：处理 None 值
        if value is None:
            return ConfigValidator._get_safe_default(key)

        # 数值类型的键
        numeric_keys = {
            'max_size', 'ttl', 'max_workers', 'timeout', 'batch_size',
            'interval', 'collect_interval', 'metrics_port', 'chunk_size',
            'history_size', 'max_history', 'buffer_size', 'flush_interval',
            'rotation_size', 'max_log_size', 'retention_days', 'max_retries',
            'retry_delay', 'cleanup_interval', 'max_connections', 'min_connections',
            'max_idle_time', 'port', 'requests_per_minute', 'burst_size',
            'core_size', 'max_size', 'queue_size', 'keep_alive_time',
            'max_concurrent', 'poll_interval', 'sample_interval'
        }

        # 布尔类型的键
        boolean_keys = {
            'enabled', 'compression', 'prefetch', 'auto_adjust', 'adaptive',
            'compress', 'priority_enabled', 'retry_on_failure', 'use_cache',
            'parallel_execution', 'result_caching', 'chunked_return',
            'fallback_strategy', 'multi_level', 'downcast_integers',
            'downcast_floats', 'use_category', 'aggressive_mode',
            'explain_queries', 'log_slow_queries', 'watch_changes',
            'compare_with_baseline', 'check_nan', 'check_inf',
            'check_range', 'check_consistency', 'log_queries',
            'log_results', 'log_performance', 'log_cache'
        }

        # 浮点类型的键
        float_keys = {
            'tolerance', 'target_load', 'cleanup_ratio', 'threshold_mb',
            'threshold', 'slow_query_threshold'
        }

        # 修复点：处理字典类型错误
        if isinstance(value, dict):
            logger.warning(f"配置项 {key} 的值是字典而非标量，尝试提取值")
            # 尝试提取字典中的整数值
            if key in value:
                return ConfigValidator._fix_value_type(key, value[key])
            else:
                int_values = [v for v in value.values() if isinstance(v, (int, float))]
                if int_values:
                    return int(int_values[0])
                else:
                    return ConfigValidator._get_safe_default(key)

        # 处理数值类型
        if key in numeric_keys:
            try:
                return int(value)
            except (ValueError, TypeError):
                logger.warning(f"无法将配置项 {key}={value} 转换为整数，使用默认值")
                return ConfigValidator._get_safe_default(key)

        # 处理布尔类型
        elif key in boolean_keys:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ('true', 'yes', '1', 'enabled', 'on')
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return bool(value)

        # 处理浮点类型
        elif key in float_keys:
            try:
                return float(value)
            except (ValueError, TypeError):
                logger.warning(f"无法将配置项 {key}={value} 转换为浮点数，使用默认值")
                return ConfigValidator._get_safe_default(key)

        return value

    @staticmethod
    def _get_safe_default(key: str) -> Any:
        """获取安全的默认值"""
        if key in {'max_workers'}:
            return 4
        elif key in {'batch_size'}:
            return 100
        elif key in {'max_size', 'max_connections'}:
            return 1000
        elif key in {'ttl'}:
            return 3600
        elif key in {'timeout', 'interval'}:
            return 10
        elif key in {'enabled', 'compression', 'prefetch'}:
            return True
        elif key in {'tolerance'}:
            return 0.001
        elif key in {'target_load'}:
            return 0.7
        return 0