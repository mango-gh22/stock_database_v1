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


# 使用示例
if __name__ == '__main__':
    tushare_cfg = load_tushare_config()
    print(f"API端点: {tushare_cfg.get('endpoints', {})}")
    print(f"速率限制: {tushare_cfg.get('rate_limit')}")