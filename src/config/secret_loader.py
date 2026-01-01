# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\secret_loader.py
# File Name: secret_loader
# @ Author: mango-gh22
# @ Date：2025/12/7 12:04
"""
desc 安全配置加载器--统一从环境变量加载所有敏感信息
"""

# src/config/secret_loader.py
import os
from dotenv import load_dotenv

load_dotenv()  # 加载 .env 文件

def get_db_password():
    return os.getenv('DB_PASSWORD')  # 从环境变量读取

def get_tushare_token():
    return os.getenv('TUSHARE_TOKEN')