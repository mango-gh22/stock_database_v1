# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\download_a50_complete.py
# File Name: download_a50_complete
# @ Author: mango-gh22
# @ Date：2026/1/9 21:55
"""
desc A50数据统一下载脚本 v1.1.0
整合全量/增量/价格/因子下载
"""

import sys
import os
from datetime import datetime, timedelta
import logging
import random
import time
from pathlib import Path

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_daily_downloader import BaostockDailyDownloader
from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.data_storage import DataStorage
from src.data.factor_storage_manager import FactorStorageManager
from src.utils.stock_pool_loader import load_a50_components
from src.config.logging_config import setup_logging

logger = setup_logging()


def get_symbols(source):
    """统一股票代码获取"""
    if isinstance(source, list):
        return source

    if source == 'a50':
        return load_a50_components()
    elif source == 'csi300':
        # 预留扩展
        from src.data.symbol_manager import SymbolManager
        return SymbolManager().get_symbols('csi_300')[:50]

    # 配置文件
    config_file = Path('config/symbols.yaml')
    if config_file.exists():
        import yaml
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config.get('csi_a50', [])

    return []


def incremental_download(symbols=None):
    """智能增量下载（整合collect_a50_daily.py逻辑）"""
    print("\n" + "=" * 70)
    print("📈 智能增量下载模式")
    print("=" * 70)

    symbols = symbols or get_symbols('a50')
    if not symbols:
        logger.error("未找到股票列表")
        return False

    # 交易日历
    try:
        from src.utils.enhanced_trade_date_manager import EnhancedTradeDateManager
        trade_manager = EnhancedTradeDateManager()
        end_date = trade_manager.get_last_trade_date_str()
        print(f"📅 最后交易日: {end_date}")
    except:
        end_date = datetime.now().strftime('%Y%m%d')
        print(f"⚠️  使用系统日期: {end_date}")

    downloader = BaostockDailyDownloader()
    factor_downloader = BaostockPBFactorDownloader()
    storage = DataStorage()
    factor_storage = FactorStorageManager()

    success_count = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"\n[{i}/{len(symbols)}] {symbol}")

            # 1. 查询最后更新日期
            last_date = storage.get_last_update_date(symbol)
            if last_date:
                start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                if start_date > end_date:
                    print(f"  ⏭️  已最新，跳过")
                    continue
            else:
                start_date = "20200101"

            print(f"  📊 下载范围: {start_date} ~ {end_date}")

            # 2. 下载价格数据
            price_df = downloader.fetch_single_stock(symbol, start_date, end_date)
            if price_df is not None and not price_df.empty:
                price_affected, _ = storage.store_daily_data(price_df)
                print(f"  ✅ 价格: {price_affected}条")
            else:
                print(f"  ⚠️  无价格数据")

            # 3. 下载因子数据
            factor_df = factor_downloader.fetch_factor_data(symbol, start_date, end_date)
            if factor_df is not None and not factor_df.empty:
                factor_affected, _ = factor_storage.store_factor_data(factor_df)
                print(f"  ✅ 因子: {factor_affected}条")
            else:
                print(f"  ⚠️  无因子数据")

            success_count += 1

            # 请求间隔
            if i < len(symbols):
                time.sleep(random.uniform(3, 5))

        except Exception as e:
            logger.error(f"下载失败 {symbol}: {e}", exc_info=True)
            print(f"  ❌ 失败: {e}")

    print(f"\n✅ 完成: {success_count}/{len(symbols)} 只股票")
    return success_count > 0


def full_download(symbols=None):
    """全量下载（整合原download_a50_complete.py）"""
    print("\n" + "=" * 70)
    print("📊 全量下载模式")
    print("=" * 70)

    symbols = symbols or get_symbols('a50')
    if not symbols:
        logger.error("未找到股票列表")
        return False

    # 初始化下载器
    price_downloader = BaostockDailyDownloader()
    factor_downloader = BaostockPBFactorDownloader()
    storage = DataStorage()
    factor_storage = FactorStorageManager()

    total_price_records = 0
    total_factor_records = 0
    successful = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"\n[{i}/{len(symbols)}] {symbol}")

            # 下载价格数据
            price_df = price_downloader.fetch_single_stock(symbol, "20050101", datetime.now().strftime('%Y%m%d'))
            if price_df is not None and not price_df.empty:
                price_affected, _ = storage.store_daily_data(price_df)
                total_price_records += price_affected
                print(f"  ✅ 价格: {price_affected}条")

            # 下载因子数据
            factor_df = factor_downloader.fetch_factor_data(symbol, "20050101", datetime.now().strftime('%Y%m%d'))
            if factor_df is not None and not factor_df.empty:
                factor_affected, _ = factor_storage.store_factor_data(factor_df)
                total_factor_records += factor_affected
                print(f"  ✅ 因子: {factor_affected}条")

            successful += 1

            # 请求间隔
            if i < len(symbols):
                time.sleep(random.uniform(5, 7))

        except Exception as e:
            logger.error(f"下载失败 {symbol}: {e}", exc_info=True)
            print(f"  ❌ 失败: {e}")

    # 最终统计
    print("\n" + "=" * 70)
    print("📊 全量下载完成报告")
    print("=" * 70)
    print(f"总股票: {len(symbols)}")
    print(f"成功: {successful}")
    print(f"价格记录: {total_price_records:,}")
    print(f"因子记录: {total_factor_records:,}")

    return successful > 0


def download_batch(symbols, mode='incremental'):
    """批量下载（供其他脚本调用）"""
    if mode == 'incremental':
        return incremental_download(symbols)
    else:
        return full_download(symbols)


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='A50数据统一下载')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser.add_argument('--symbols', nargs='+', help='股票代码列表')
    parser.add_argument('--group', choices=['a50', 'csi300'], default='a50')

    args = parser.parse_args()

    # 获取股票列表
    symbols = args.symbols
    if not symbols:
        symbols = get_symbols(args.group)

    if not symbols:
        print("❌ 未找到股票列表")
        return 1

    print(f"📋 准备处理 {len(symbols)} 只股票")

    # 执行下载
    if args.mode == 'incremental':
        success = incremental_download(symbols)
    else:
        success = full_download(symbols)

    return 0 if success else 1


if __name__ == "__main__":
    import argparse

    sys.exit(main())

