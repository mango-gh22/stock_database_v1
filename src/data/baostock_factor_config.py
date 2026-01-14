# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_factor_config.py
# File Name: baostock_factor_config
# @ Author: mango-gh22
# @ Date：2026/1/3 8:51
"""
desc PB因子下载配置管理模块
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class FactorConfigLoader:
    """因子下载配置加载器"""

    def __init__(self, config_path: str = 'config/factor_config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        config_file = Path(self.config_path)

        if not config_file.exists():
            logger.warning(f"配置文件不存在: {config_file}，使用默认配置")
            return self._get_default_config()

        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 合并环境变量
            self._merge_env_vars(config)

            logger.info(f"加载因子配置成功: {config_file}")
            return config

        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'execution': {
                'thread_num': 1,
                'request_interval': 1.5,
                'max_retries': 3,
                'retry_delay_base': 3
            },
            'batch': {
                'batch_size': 50,
                'symbols_per_request': 100,
                'progress_report_interval': 10
            },
            'date_range': {
                'default_days_back': 365,
                'max_history_years': 5,
                'full_update_months': 12
            },
            'storage': {
                'table_name': 'stock_daily_data',
                'enable_incremental': True,
                'force_update': False,
                'batch_insert_size': 500
            },
            'performance': {
                'enable_cache': True,
                'cache_ttl': 3600,
                'cache_dir': 'data/cache/baostock/factors'
            },
            'monitoring': {
                'enable_detailed_log': True,
                'log_level': 'INFO',
                'save_report': True,
                'report_dir': 'data/reports/factors'
            },
            'baostock_fields': {
                'daily_fields': 'date,code,peTTM,pbMRQ,psTTM'
            }
        }

    def _merge_env_vars(self, config: Dict[str, Any]):
        """合并环境变量"""
        # 支持通过环境变量覆盖配置
        env_mappings = {
            'FACTOR_THREAD_NUM': ('execution', 'thread_num', int),
            'FACTOR_REQUEST_INTERVAL': ('execution', 'request_interval', float),
            'FACTOR_MAX_RETRIES': ('execution', 'max_retries', int),
        }

        for env_var, (section, key, type_func) in env_mappings.items():
            if env_var in os.environ:
                try:
                    value = type_func(os.environ[env_var])
                    if section in config and key in config[section]:
                        config[section][key] = value
                        logger.info(f"从环境变量覆盖配置: {section}.{key}={value}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"环境变量转换失败 {env_var}: {e}")

    def get(self, key_path: str, default: Any = None) -> Any:
        """获取配置值（支持点路径）"""
        keys = key_path.split('.')
        value = self.config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def update(self, key_path: str, value: Any):
        """更新配置值"""
        keys = key_path.split('.')
        config = self.config

        for i, key in enumerate(keys[:-1]):
            if key not in config:
                config[key] = {}
            config = config[key]

        config[keys[-1]] = value

    def save(self, output_path: Optional[str] = None):
        """保存配置到文件"""
        if output_path is None:
            output_path = self.config_path

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
            logger.info(f"配置已保存到: {output_path}")
        except Exception as e:
            logger.error(f"保存配置失败: {e}")

    def validate(self) -> bool:
        """验证配置有效性"""
        errors = []

        # 检查必要配置
        required_paths = [
            'execution.thread_num',
            'execution.request_interval',
            'baostock_fields.daily_fields'
        ]

        for path in required_paths:
            if self.get(path) is None:
                errors.append(f"缺少必要配置: {path}")

        # 验证单线程约束（P6阶段）
        thread_num = self.get('execution.thread_num')
        if thread_num != 1:
            logger.warning(f"P6阶段强制单线程，当前配置为{thread_num}，将自动调整为1")
            self.update('execution.thread_num', 1)

        if errors:
            for error in errors:
                logger.error(error)
            return False

        return True

    def get_baostock_fields(self) -> str:
        """获取Baostock字段字符串"""
        return self.get('baostock_fields.daily_fields', 'date,code,peTTM,pbMRQ,psTTM')

    def get_cache_dir(self) -> Path:
        """获取缓存目录"""
        cache_dir_str = self.get('performance.cache_dir', 'data/cache/baostock/factors')
        cache_dir = Path(cache_dir_str)
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir


# 单例配置实例
_config_loader = None


def get_config_loader(config_path: str = 'config/factor_config.yaml') -> FactorConfigLoader:
    """获取配置加载器单例"""
    global _config_loader
    if _config_loader is None:
        _config_loader = FactorConfigLoader(config_path)
    return _config_loader


def test_config_loader():
    """测试配置加载器"""
    print("🧪 测试因子配置加载器")
    print("=" * 50)

    # 创建临时配置文件
    test_config = """
execution:
  thread_num: 1
  request_interval: 1.5
  max_retries: 3

batch:
  batch_size: 50
  symbols_per_request: 100
  progress_report_interval: 10
    """

    # 测试默认配置
    loader = FactorConfigLoader()
    config = loader.config

    print("📋 默认配置:")
    print(f"  线程数: {config['execution']['thread_num']}")
    print(f"  请求间隔: {config['execution']['request_interval']}秒")
    print(f"  批量大小: {config['batch']['batch_size']}")
    print(f"  Baostock字段: {config['baostock_fields']['daily_fields']}")

    # 测试获取方法
    print("\n🔍 测试get方法:")
    print(f"  execution.thread_num: {loader.get('execution.thread_num')}")
    print(f"  batch.batch_size: {loader.get('batch.batch_size')}")
    print(f"  non.existing.key: {loader.get('non.existing.key', 'default_value')}")

    # 测试更新方法
    print("\n✏️ 测试update方法:")
    loader.update('execution.request_interval', 2.0)
    print(f"  更新后请求间隔: {loader.get('execution.request_interval')}")

    # 测试验证
    print("\n✅ 测试配置验证:")
    is_valid = loader.validate()
    print(f"  配置有效性: {is_valid}")

    print("\n🎉 配置加载器测试完成")


if __name__ == "__main__":
    test_config_loader()