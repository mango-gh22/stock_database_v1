# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\main.py
# File Name: main
# @ File: main.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/4 23:36
"""
desc 项目入口文件
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目主入口文件 - P1阶段版本 (v0.1.0)
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.logger import get_logger


def main():
    """项目启动主函数"""
    logger = get_logger("main")
    logger.info("=" * 50)
    logger.info("项目启动...")
    logger.info("当前阶段：P1 - 基础环境与框架搭建")
    logger.info("-" * 50)

    # P1阶段：基础环境自检
    try:
        # 1. 检查关键目录是否存在
        required_dirs = ['config', 'src', 'src/utils', 'src/database', 'data', 'logs']
        for dir_name in required_dirs:
            if os.path.exists(dir_name):
                logger.info(f"✓ 目录检查通过: {dir_name}")
            else:
                logger.error(f"✗ 目录缺失: {dir_name}")
                return False

        # 2. 检查关键文件是否存在
        required_files = [
            'config/database.yaml',
            'config/symbols.yaml',
            'src/utils/logger.py',
            'src/utils/code_converter.py',
            'src/database/connection.py'
        ]
        for file_path in required_files:
            if os.path.exists(file_path):
                logger.info(f"✓ 文件检查通过: {file_path}")
            else:
                logger.error(f"✗ 文件缺失: {file_path}")
                return False

        # 3. 尝试读取配置
        import yaml
        with open('config/symbols.yaml', 'r', encoding='utf-8') as f:
            pool_config = yaml.safe_load(f)
            pool_name = pool_config['symbol_pools']['csi_a50']['name']
            symbol_count = len(pool_config['symbol_pools']['csi_a50']['symbols'])
            logger.info(f"✓ 配置文件加载成功，初始股票池: {pool_name} ({symbol_count}只)")

        # 4. 测试代码转换器（核心工具）
        from src.utils.code_converter import normalize_stock_code
        test_cases = [("600519.SH", "sh600519"), ("000001.sz", "sz000001")]
        all_pass = True
        for input_code, expected in test_cases:
            result = normalize_stock_code(input_code)
            if result == expected:
                logger.info(f"✓ 代码转换测试通过: {input_code} -> {result}")
            else:
                logger.error(f"✗ 代码转换测试失败: {input_code} -> {result} (期望: {expected})")
                all_pass = False

        if not all_pass:
            return False

        # 5. 尝试数据库连接（但P1阶段不强制要求，因为数据库可能还没创建）
        try:
            from src.database.connection import test_connection
            if test_connection():
                logger.info("✓ 数据库连接测试成功（可选）")
            else:
                logger.warning("⚠ 数据库连接失败，这在P1阶段是正常的，将在P2阶段创建数据库")
        except Exception as e:
            logger.warning(f"⚠ 数据库连接测试跳过: {e}")

        logger.info("-" * 50)
        logger.info("✅ P1阶段环境自检通过！")
        logger.info("📁 项目框架已就绪，可进入P2阶段（数据库设计与创建）。")
        logger.info("=" * 50)
        return True

    except FileNotFoundError as e:
        logger.error(f"❌ 关键文件缺失: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ 启动过程发生未知错误: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)