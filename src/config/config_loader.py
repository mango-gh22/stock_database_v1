# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\config_loader.py
# File Name: config_loader
# @ File: config_loader.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 20:05
"""
desc tushare的token配套代码文件
配置加载器 - 统一管理所有配置文件的加载
"""

import os
import yaml
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# 设置日志
logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置加载器类"""

    @staticmethod
    def load_yaml_config(config_path: str) -> Dict[str, Any]:
        """
        加载YAML配置文件

        Args:
            config_path: 配置文件路径

        Returns:
            配置字典
        """
        try:
            config_path = Path(config_path)
            if not config_path.exists():
                logger.error(f"配置文件不存在: {config_path}")
                return {}

            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 替换环境变量
            content = ConfigLoader._replace_env_variables(content)

            # 加载YAML
            config = yaml.safe_load(content)
            logger.info(f"成功加载配置文件: {config_path}")
            return config or {}

        except Exception as e:
            logger.error(f"加载配置文件失败 {config_path}: {e}")
            return {}

    @staticmethod
    def load_json_config(config_path: str) -> Dict[str, Any]:
        """
        加载JSON配置文件

        Args:
            config_path: 配置文件路径

        Returns:
            配置字典
        """
        try:
            config_path = Path(config_path)
            if not config_path.exists():
                logger.error(f"配置文件不存在: {config_path}")
                return {}

            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 替换环境变量
            content = ConfigLoader._replace_env_variables(content)

            # 加载JSON
            config = json.loads(content)
            logger.info(f"成功加载配置文件: {config_path}")
            return config

        except Exception as e:
            logger.error(f"加载配置文件失败 {config_path}: {e}")
            return {}

    @staticmethod
    def _replace_env_variables(content: str) -> str:
        """
        替换内容中的环境变量占位符

        Args:
            content: 原始内容

        Returns:
            替换后的内容
        """
        # 确保.env已加载
        load_dotenv()

        # 替换所有 ${VAR_NAME} 格式的环境变量
        for env_key, env_value in os.environ.items():
            placeholder = f'${{{env_key}}}'
            if placeholder in content:
                content = content.replace(placeholder, str(env_value))

        return content

    @staticmethod
    def get_config_value(config_dict: Dict[str, Any], key_path: str, default: Any = None) -> Any:
        """
        使用点号路径获取配置值

        Args:
            config_dict: 配置字典
            key_path: 点号分隔的键路径 (如: "database.host")
            default: 默认值

        Returns:
            配置值
        """
        try:
            keys = key_path.split('.')
            value = config_dict

            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return default

            return value if value is not None else default

        except Exception as e:
            logger.debug(f"获取配置值失败 {key_path}: {e}")
            return default


# 保持原有的函数，用于向后兼容
def load_tushare_config() -> Dict[str, Any]:
    """
    加载Tushare配置（从YAML解析环境变量占位符）
    返回：dict格式的配置
    """
    config_path = Path(__file__).parent.parent.parent / 'config' / 'tushare_config.yaml'
    return ConfigLoader.load_yaml_config(str(config_path)).get('tushare', {})


def load_database_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    加载数据库配置

    Args:
        config_path: 配置文件路径，默认使用 config/database.yaml

    Returns:
        数据库配置字典
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / 'config' / 'database.yaml'

    config = ConfigLoader.load_yaml_config(str(config_path))

    # 支持两种格式：嵌套和扁平
    if 'database' in config:
        # 嵌套格式
        db_config = config['database']
    else:
        # 扁平格式
        db_config = config

    return db_config


# 添加更多专门的配置加载函数
def load_performance_config() -> Dict[str, Any]:
    """加载性能配置"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'performance.yaml'
    return ConfigLoader.load_yaml_config(str(config_path))


def load_pipeline_config() -> Dict[str, Any]:
    """加载数据管道配置"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'pipeline_config.yaml'
    return ConfigLoader.load_yaml_config(str(config_path))


def load_indicators_config() -> Dict[str, Any]:
    """加载技术指标配置"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'indicators.yaml'
    return ConfigLoader.load_yaml_config(str(config_path))


def load_query_config() -> Dict[str, Any]:
    """加载查询配置"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'query_config.yaml'
    return ConfigLoader.load_yaml_config(str(config_path))


# 使用示例
if __name__ == '__main__':
    # 测试各个配置加载函数
    print("测试配置加载器:")
    print("-" * 40)

    # 加载Tushare配置
    tushare_cfg = load_tushare_config()
    print(f"Tushare配置: {len(tushare_cfg)} 项")

    # 加载数据库配置
    db_cfg = load_database_config()
    print(f"数据库配置: {len(db_cfg)} 项")

    # 加载性能配置
    try:
        perf_cfg = load_performance_config()
        print(f"性能配置: {len(perf_cfg)} 项")
    except Exception as e:
        print(f"性能配置加载失败: {e} (文件可能不存在)")

    # 测试ConfigLoader类
    print("\n测试ConfigLoader类:")
    print("-" * 40)

    loader = ConfigLoader()

    # 测试获取配置值
    test_config = {
        'database': {
            'host': 'localhost',
            'port': 3306,
            'credentials': {
                'username': 'admin',
                'password': 'secret'
            }
        }
    }

    value1 = loader.get_config_value(test_config, 'database.host')
    value2 = loader.get_config_value(test_config, 'database.credentials.username')
    value3 = loader.get_config_value(test_config, 'database.nonexistent', 'default_value')

    print(f"database.host: {value1}")
    print(f"database.credentials.username: {value2}")
    print(f"database.nonexistent: {value3}")