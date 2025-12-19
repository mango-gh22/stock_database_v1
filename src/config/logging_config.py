# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\logging_config.py
# File Name: logging_config
# @ File: logging_config.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 21:03
"""
desc 
"""

# src/config/logging_config.py
import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path


def setup_logging(
        log_dir: str = "logs",
        console_level: int = logging.INFO,
        file_level: int = logging.DEBUG,
        log_to_file: bool = True
) -> logging.Logger:
    """
    设置日志配置

    Args:
        log_dir: 日志目录
        console_level: 控制台日志级别
        file_level: 文件日志级别
        log_to_file: 是否记录到文件

    Returns:
        配置好的logger
    """
    # 创建日志目录
    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)

    # 获取根logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # 清除现有的handlers（避免重复）
    logger.handlers.clear()

    # 创建formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件handler（如果需要）
    if log_to_file:
        # 按日期分割日志文件
        log_date = datetime.now().strftime('%Y%m%d')
        log_file = Path(log_dir) / f"stock_database_{log_date}.log"

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# 测试代码
if __name__ == "__main__":
    logger = setup_logging()
    logger.info("日志系统初始化成功")
    logger.debug("调试信息")
    logger.warning("警告信息")
    logger.error("错误信息")