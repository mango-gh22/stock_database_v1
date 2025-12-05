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
项目主入口文件 - P1阶段版本 (v0.1.0)+P3阶段功能
"""

# main.py（简化版）
import sys
import os
import argparse

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)


def setup_basic_logging():
    """基本日志设置"""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger()


# 在 main.py 的 main() 函数中，修改 args.action 的处理
def main():
    logger = setup_basic_logging()
    parser = argparse.ArgumentParser(description='股票数据库系统')
    parser.add_argument('--phase', type=str, choices=['p1', 'p2', 'p3'],
                        default='p3', help='执行阶段')
    parser.add_argument('--action', type=str,
                        choices=['create_tables', 'import_a50', 'collect_daily',
                                 'collect_latest', 'schedule', 'validate'],
                        help='执行动作')

    args = parser.parse_args()
    logger.info(f"启动股票数据库系统 - 阶段 {args.phase}")
    logger.info(f"执行动作: {args.action}")

    if args.action == 'create_tables':
    # ... 原有的 create_tables 代码 ...

    elif args.action == 'import_a50':  # 添加这个处理
        logger.info("开始导入中证A50成分股...")
        try:
            from src.data.import_csi_a50 import CSI_A50_Importer
            importer = CSI_A50_Importer()
            if importer.run_full_import():
                logger.info("✅ 中证A50成分股导入完成")
            else:
                logger.error("❌ 中证A50成分股导入失败")
        except ImportError as e:
            logger.error(f"❌ 导入模块失败: {e}")
            logger.error("请确保 import_csi_a50.py 模块存在")

    # ... 其他 action 处理 ...

    logger.info("程序执行完成")


if __name__ == "__main__":
    main()