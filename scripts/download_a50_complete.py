# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\download_a50_complete.py
# File Name: download_a50_complete
# @ Author: mango-gh22
# @ Date：2026/1/9 21:55
"""
desc 下载完整的A50成分股数据（包含价格和因子）
"""

import sys
import os
from datetime import datetime
import yaml
import logging
import time

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.baostock_daily_downloader import BaostockDailyDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.data.data_storage import DataStorage
from src.config.logging_config import setup_logging

logger = setup_logging()


def get_a50_symbols():
    """获取A50成分股列表"""
    config_file = 'config/symbols.yaml'
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    symbols = []
    for item in config.get('csi_a50', []):
        if isinstance(item, dict) and 'symbol' in item:
            symbols.append(item['symbol'])
        elif isinstance(item, str):
            symbols.append(item)

    # 限制为前50只
    return symbols[:50]


def download_a50_complete():
    """下载完整的A50数据"""
    print("\n" + "=" * 70)
    print("📊 下载完整的A50成分股数据")
    print("=" * 70)

    # 获取A50成分股
    symbols = get_a50_symbols()
    print(f"📋 A50成分股 ({len(symbols)} 只):")
    for i, symbol in enumerate(symbols, 1):
        print(f"  {i:2d}. {symbol}")

    # 初始化下载器（自动处理登录）
    price_downloader = BaostockDailyDownloader()
    factor_downloader = BaostockPBFactorDownloader()
    price_storage = DataStorage()
    factor_storage = FactorStorageManager()

    total_price_records = 0
    total_factor_records = 0
    successful = 0

    # 下载每只股票
    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"\n[{i}/{len(symbols)}] 处理 {symbol}")

            # 1. 下载价格数据
            print("  📈 下载价格数据...")
            # 统一使用 YYYYMMDD 格式
            start_date = '20050101'
            end_date = datetime.now().strftime('%Y%m%d')

            price_df = price_downloader.fetch_single_stock(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if price_df is not None and not price_df.empty:
                price_affected, _ = price_storage.store_daily_data(price_df)
                total_price_records += price_affected
                print(f"   价格数据: {len(price_df)}条 -> 存储{price_affected}条")
            else:
                print(f"   价格数据: 无数据")

            # 2. 下载因子数据
            print("  📊 下载因子数据...")
            factor_df = factor_downloader.fetch_factor_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if factor_df is not None and not factor_df.empty:
                factor_affected, _ = factor_storage.store_factor_data(factor_df)
                total_factor_records += factor_affected
                print(f"   因子数据: {len(factor_df)}条 -> 存储{factor_affected}条")

                # 显示因子统计
                if 'pb' in factor_df.columns:
                    pb_count = factor_df['pb'].notna().sum()
                    print(f"     有PB数据: {pb_count}条")
                if 'pe_ttm' in factor_df.columns:
                    pe_count = factor_df['pe_ttm'].notna().sum()
                    print(f"     有PE数据: {pe_count}条")
            else:
                print(f"   因子数据: 无数据")

            successful += 1

            # 请求间隔（避免API限制）
            if i < len(symbols):
                sleep_time = 5 + random.uniform(0, 2)  # 5-7秒随机间隔
                time.sleep(sleep_time)

        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            logger.error(f"处理 {symbol} 失败: {e}", exc_info=True)

    # 输出结果
    print("\n" + "=" * 70)
    print("📊 下载完成报告")
    print("=" * 70)

    print(f"总股票数: {len(symbols)}")
    print(f"成功处理: {successful}")
    print(f"失败: {len(symbols) - successful}")
    print(f"价格记录总数: {total_price_records:,}")
    print(f"因子记录总数: {total_factor_records:,}")

    # 数据库验证
    print("\n🔍 数据库验证:")
    try:
        from src.database.db_connector import DatabaseConnector
        db = DatabaseConnector()
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
                total = cursor.fetchone()[0]
                print(f"  总记录数: {total:,} 条")

                cursor.execute("SELECT COUNT(DISTINCT symbol) as symbols FROM stock_daily_data")
                symbol_count = cursor.fetchone()[0]
                print(f"  股票数量: {symbol_count} 只")

                cursor.execute("""
                    SELECT 
                        SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,
                        SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count,
                        SUM(CASE WHEN ps_ttm IS NOT NULL THEN 1 ELSE 0 END) as ps_count
                    FROM stock_daily_data
                """)
                pb_count, pe_count, ps_count = cursor.fetchone()
                print(f"  有PB数据: {pb_count or 0:,} 条")
                print(f"  有PE数据: {pe_count or 0:,} 条")
                print(f"  有PS数据: {ps_count or 0:,} 条")
    except Exception as e:
        print(f"  验证失败: {e}")
        logger.error("数据库验证失败", exc_info=True)

    print("\n" + "=" * 70)
    print("🎉 A50数据下载完成！")
    print("=" * 70)

    return successful > 0


if __name__ == "__main__":
    # 添加随机模块导入
    import random

    success = download_a50_complete()
    exit(0 if success else 1)

