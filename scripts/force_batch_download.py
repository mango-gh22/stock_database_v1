# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\force_batch_download.py
# File Name: force_batch_download
# @ Author: mango-gh22
# @ Date：2026/1/6 19:09
"""
desc 
"""

# File Path: E:/MyFile/stock_database_v1/scripts/force_batch_download.py
"""
强制批量下载脚本 - 修复跳过问题
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.factor_batch_processor import FactorBatchProcessor
from src.config.logging_config import setup_logging

logger = setup_logging()


class ForceBatchDownloader:
    """强制批量下载器 - 确保数据下载"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        self.processor = FactorBatchProcessor(config_path)
        logger.info("强制批量下载器初始化完成")

    def force_download_symbols(self, symbols: List[str], mode: str = 'full'):
        """
        强制下载股票数据

        Args:
            symbols: 股票代码列表
            mode: 下载模式 ('full' 或 'incremental')
        """
        logger.info(f"🚀 开始强制下载 {len(symbols)} 只股票，模式: {mode}")

        # 清除缓存，强制重新下载
        self._clear_cache()

        # 设置强制下载参数
        force_mode = mode if mode == 'full' else 'incremental'

        # 处理股票列表
        report = self.processor.process_symbol_list(
            symbols=symbols,
            mode=force_mode
        )

        return report

    def _clear_cache(self):
        """清除所有缓存"""
        try:
            # 清除文件缓存
            cache_dirs = [
                'data/cache/baostock/factors',
                'data/cache/indicators',
                'data/cache/tushare'
            ]

            for cache_dir in cache_dirs:
                if os.path.exists(cache_dir):
                    import shutil
                    shutil.rmtree(cache_dir)
                    os.makedirs(cache_dir, exist_ok=True)
                    logger.info(f"清除缓存目录: {cache_dir}")

            # 清除内存缓存
            if hasattr(self.processor.storage, '_last_date_cache'):
                self.processor.storage._last_date_cache.clear()
                logger.info("清除内存缓存")

        except Exception as e:
            logger.warning(f"清除缓存失败: {e}")

    def cleanup(self):
        """清理资源"""
        self.processor.cleanup()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='强制批量下载因子数据')

    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        required=True,
        help='股票代码列表，如: 600519 000001 000858'
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'incremental'],
        default='full',
        help='下载模式: full(全量), incremental(增量)'
    )

    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='清除所有缓存'
    )

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("🔧 强制批量下载因子数据")
    print("=" * 60)

    try:
        # 初始化下载器
        downloader = ForceBatchDownloader()

        # 如果需要清除缓存
        if args.clear_cache:
            downloader._clear_cache()

        # 执行强制下载
        start_time = datetime.now()
        report = downloader.force_download_symbols(args.symbols, args.mode)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # 输出结果
        print("\n📊 强制下载完成报告")
        print("=" * 60)

        summary = report['summary']
        print(f"总股票数: {summary['total_symbols']}")
        print(f"成功: {summary['successful']}")
        print(f"失败: {summary['failed']}")
        print(f"跳过: {summary['skipped']}")
        print(f"总记录数: {summary['total_records']:,}")
        print(f"成功率: {summary['success_rate']}%")
        print(f"总耗时: {duration:.2f}秒")

        # 显示详细结果
        print("\n🔍 详细结果:")
        for result in report['detailed_results']:
            symbol = result.get('symbol', 'unknown')
            status = result.get('status', 'unknown')
            records = result.get('records_stored', 0)

            if status == 'success':
                print(f"  ✅ {symbol}: {records} 条记录")
            elif status == 'skipped':
                print(f"  ⚠️  {symbol}: 跳过 - {result.get('reason', '无数据')}")
            elif status == 'error':
                print(f"  ❌ {symbol}: 错误 - {result.get('error', '未知')}")

        # 清理
        downloader.cleanup()

        print("\n" + "=" * 60)
        print("🎉 强制批量下载完成")
        print("=" * 60)

        # 验证数据库中的数据
        print("\n📋 数据库验证:")
        verify_database_data(args.symbols)

        return 0

    except Exception as e:
        logger.error(f"强制批量下载失败: {e}", exc_info=True)
        print(f"\n❌ 错误: {e}")
        return 1


def verify_database_data(symbols: List[str]):
    """验证数据库中的数据"""
    try:
        from src.database.db_connector import DatabaseConnector

        db = DatabaseConnector()

        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                for symbol in symbols:
                    clean_symbol = symbol.replace('.', '')
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_count,
                            MIN(trade_date) as first_date,
                            MAX(trade_date) as last_date,
                            SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,
                            SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count
                        FROM stock_daily_data 
                        WHERE symbol = %s
                    """, (clean_symbol,))

                    result = cursor.fetchone()
                    if result:
                        total, first_date, last_date, pb_count, pe_count = result
                        print(f"  {symbol}: {total}条记录, {first_date} 到 {last_date}")
                        print(f"      有PB数据: {pb_count}条, 有PE数据: {pe_count}条")

    except Exception as e:
        print(f"数据库验证失败: {e}")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)