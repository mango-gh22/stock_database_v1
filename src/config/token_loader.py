# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\token_loader.py
# File Name: token_loader
# @ File: token_loader.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 20:20
"""
desc 
"""

# 创建文件：stock_database_v1/src/config/token_loader.py
# src/config/token_loader.py
import os
from dotenv import load_dotenv


def get_token():
    """
    安全地获取Tushare Token
    优先级：.env文件 > 系统环境变量
    """
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