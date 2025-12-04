# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\logger.py
# File Name: logger
# @ File: logger.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/4 23:58
"""
desc 创建日志模块
"""
import logging
import os
from logging.handlers import RotatingFileHandler

def get_logger(module_name: str) -> logging.Logger:
    """
    获取指定模块的日志记录器
    Args:
        module_name: 模块名称，如 'main', 'database.connection'
    Returns:
        配置好的Logger对象
    """
    # 确保日志目录存在
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 创建日志记录器
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    # 避免重复添加处理器（防止多次调用时重复打印）
    if not logger.handlers:
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)

        # 文件处理器（按大小轮换）
        file_handler = RotatingFileHandler(
            filename=os.path.join(log_dir, 'app.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)

        # 添加处理器
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger