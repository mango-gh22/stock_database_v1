# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\token_loader.py
# @ Author: mango-gh22
# @ Date：2025/12/5 20:20

# 创建文件：stock_database_v1/src/config/token_loader.py

# 1. 增强 src/config/token_loader.py，添加惰性加载
# src/config/token_loader.py
import os
from dotenv import load_dotenv

# 单例token缓存
_tushare_token = None


def get_tushare_token():
    """获取Tushare Token（带惰性加载）"""
    global _tushare_token
    if _tushare_token is None:
        _tushare_token = _load_token()
    return _tushare_token


def _load_token():
    """实际加载token的逻辑"""
    # 尝试加载项目根目录的.env文件
    env_loaded = load_dotenv()

    token = os.getenv('TUSHARE_TOKEN')

    if not token:
        raise ValueError(
            "未找到TUSHARE_TOKEN。请确保：\n"
            "1. 在项目根目录的.env文件中设置 TUSHARE_TOKEN=你的token\n"
            "2. 或者在系统环境变量中设置 TUSHARE_TOKEN"
        )

    # 简单验证token格式
    if len(token) < 20:
        print(f"⚠️ 警告：Token长度异常 ({len(token)}字符)，可能配置错误")

    print(f"✅ Token加载成功 (来源: {'项目.env文件' if env_loaded else '系统环境变量'})")
    return token


# 保持向后兼容的别名
get_token = get_tushare_token
