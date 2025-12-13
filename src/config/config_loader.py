# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\config_loader.py
# File Name: config_loader
# @ File: config_loader.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 20:05
"""
desc tushare的token配套代码文件
"""

# stock_database_v1/src/config/config_loader.py
import os
import yaml
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union


def load_tushare_config():
    """
    加载Tushare配置（从YAML解析环境变量占位符）
    返回：dict格式的配置
    """
    # 1. 确保.env已加载
    load_dotenv()

    # 2. 构建配置文件路径
    config_path = Path(__file__).parent.parent.parent / 'config' / 'tushare_config.yaml'

    # 3. 读取并解析YAML
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 4. 替换所有环境变量占位符 ${VAR_NAME}
    for env_key, env_value in os.environ.items():
        placeholder = f'${{{env_key}}}'
        if placeholder in content:
            content = content.replace(placeholder, env_value)

    # 5. 返回Tushare配置部分
    config = yaml.safe_load(content)
    return config.get('tushare', {})


# 在 config_loader.py 中添加以下函数
def load_database_config(config_path: Optional[str] = None) -> Dict:
    """
    加载数据库配置

    Args:
        config_path: 配置文件路径，默认使用 config/database.yaml

    Returns:
        数据库配置字典
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / 'config' / 'database.yaml'

    config_path = Path(config_path)

    if not config_path.exists():
        logger.warning(f"数据库配置文件不存在: {config_path}")
        return _get_default_database_config()

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        # 支持两种格式：嵌套和扁平
        if 'database' in config_data:
            # 嵌套格式
            db_config = config_data['database']
        else:
            # 扁平格式
            db_config = config_data

        # 替换环境变量
        db_config = _replace_env_variables(db_config)

        logger.info(f"从配置文件加载数据库配置: {config_path}")
        return db_config

    except Exception as e:
        logger.error(f"加载数据库配置失败: {e}")
        return _get_default_database_config()


def _get_default_database_config() -> Dict:
    """获取默认数据库配置"""
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '',
        'database': 'stock_database',
        'charset': 'utf8mb4',
        'pool_size': 5,
        'pool_name': 'stock_pool',
        'autocommit': True
    }


def _replace_env_variables(config_dict: Dict) -> Dict:
    """替换配置中的环境变量占位符"""
    if not config_dict:
        return config_dict

    result = {}
    for key, value in config_dict.items():
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            result[key] = os.environ.get(env_var, '')
        elif isinstance(value, dict):
            result[key] = _replace_env_variables(value)
        else:
            result[key] = value

    return result

# 使用示例
if __name__ == '__main__':
    tushare_cfg = load_tushare_config()
    print(f"API端点: {tushare_cfg.get('endpoints', {})}")
    print(f"速率限制: {tushare_cfg.get('rate_limit')}")