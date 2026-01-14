# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\run_factor_update.py
# File Name: run_factor_update
# @ Author: mango-gh22
# @ Date：2026/1/3 12:44
"""
desc
运行因子数据更新的主脚本
支持增量更新、全量更新、单只股票更新
命令行参数控制，灵活配置
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子数据统一更新脚本 v1.2.0
支持从数据库读取股票代码（默认）或从配置文件读取
"""

import sys
from pathlib import Path
import argparse

sys.path.append(str(Path(__file__).parent.parent))

from src.data.factor_batch_processor import FactorBatchProcessor
from src.utils.stock_pool_loader import load_symbols_from_db, load_a50_components
from src.config.logging_config import setup_logging

logger = setup_logging()


def update_batch(symbols=None, mode='incremental', test_mode=False, source='db'):
    """
    批量更新因子数据

    Args:
        symbols: 股票代码列表（可选）
        mode: 更新模式
        test_mode: 是否测试模式
        source: 代码来源 'db' 或 'config'
    """
    # 如果未指定symbols，根据source自动加载
    if symbols is None:
        if source == 'db':
            symbols = load_symbols_from_db()
            print(f"📊 从数据库加载 {len(symbols)} 只股票")
        else:
            symbols = load_a50_components()
            print(f"📊 从配置文件加载 {len(symbols)} 只股票")

    if not symbols:
        logger.error("未找到股票列表")
        return False

    if test_mode:
        symbols = symbols[:3]
        print(f"🧪 测试模式，处理前 {len(symbols)} 只股票")

    print(f"\n" + "=" * 70)
    print("📈 因子数据批量更新")
    print(f"模式: {mode}")
    print(f"股票数量: {len(symbols)}")
    print("=" * 70)

    processor = FactorBatchProcessor()
    processor.batch_size = 10

    def progress_callback(progress, current, total):
        print(f"📈 进度: {progress:.1f}% ({current}/{total})", end='\r')

    report = processor.process_symbol_list(
        symbols=symbols,
        mode=mode,
        progress_callback=progress_callback
    )

    # 输出报告
    summary = report['summary']
    print("\n" + "=" * 70)
    print("✅ 更新完成报告")
    print("=" * 70)
    print(f"总股票数: {summary['total_symbols']}")
    print(f"成功更新: {summary['successful']}")
    print(f"更新失败: {summary['failed']}")
    print(f"已跳过: {summary['skipped']}")
    print(f"总记录数: {summary['total_records']:,}")
    print(f"成功率: {summary['success_rate']:.1f}%")

    # 失败详情
    if summary['failed'] > 0:
        print(f"\n❌ 失败股票 ({summary['failed']} 只):")
        for symbol in report.get('failed_symbols', [])[:5]:
            print(f"  {symbol}")

    processor.cleanup()
    return summary['failed'] == 0


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='因子数据更新 - 默认从数据库读取股票列表',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 从数据库读取股票（默认）
  python run_factor_update.py

  # 从配置文件读取（用于新增股票）
  python run_factor_update.py --source config

  # 增量更新指定股票
  python run_factor_update.py --symbols sh600519 sz000001

  # 测试模式
  python run_factor_update.py --test
        """
    )

    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--symbols', nargs='+', help='股票代码')
    parser.add_argument('--source', choices=['db', 'config'], default='db',
                        help='代码来源: db(数据库,默认), config(配置文件)')
    parser.add_argument('--test', action='store_true', help='测试模式')

    args = parser.parse_args()

    success = update_batch(
        symbols=args.symbols,
        mode=args.mode,
        test_mode=args.test,
        source=args.source
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())