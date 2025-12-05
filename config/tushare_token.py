# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/config\tushare_token.py
# File Name: tushare_token
# @ File: tushare_token.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 19:45
"""
desc tushare专门的 Token 配置模块
"""

# config/tushare_token.py 或 src/config/token_loader.py
import os
from dotenv import load_dotenv


def get_tushare_token():
    """
    安全地获取Tushare Token。
    优先级：.env文件 > 系统环境变量 > 抛出异常。
    """
    # 1. 尝试从项目根目录的.env文件加载
    # 如果你的.env文件不在项目根目录，请修改下面的路径
    env_loaded = load_dotenv()

    # 2. 尝试读取变量
    token = os.getenv('TUSHARE_TOKEN')

    if not token:
        # 两种方式都没找到Token，抛出清晰的错误
        raise ValueError(
            "未找到Tushare Token。请确保：\n"
            "1. 在项目根目录的.env文件中设置 TUSHARE_TOKEN=你的token\n"
            "2. 或者在系统环境变量中设置 TUSHARE_TOKEN\n"
            "3. 请勿将token硬编码在代码中提交！"
        )

    # 3. 安全检查：简单验证token格式（示例）
    if len(token) < 20:
        print(f"警告：Token长度异常 ({len(token)}字符)，可能配置错误。")

    print(f"Tushare Token加载成功 (来源: {'项目.env文件' if env_loaded else '系统环境变量'})")
    return token


# 提供一个可以直接导入的token变量（惰性加载）
_tushare_token = None


def get_token():
    """获取token的单例函数"""
    global _tushare_token
    if _tushare_token is None:
        _tushare_token = get_tushare_token()
    return _tushare_token