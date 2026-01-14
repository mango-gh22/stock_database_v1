# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\download_csi300.py
# File Name: download_csi300
# @ Author: mango-gh22
# @ Date：2026/1/9 21:59
"""
desc 下载沪深300样本股数据
scripts / download_csi300.py
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.config.logging_config import setup_logging

logger = setup_logging()


def get_csi300_symbols():
    """获取沪深300样本股列表（简化版）"""
    # 实际应用中可以从文件或API获取，这里提供部分样本
    return [
               # 金融
               '601318.SH', '600036.SH', '000001.SZ', '601166.SH', '601328.SH',
               '601988.SH', '601998.SH', '600016.SH', '600000.SH', '601288.SH',
               # 消费
               '600519.SH', '000858.SZ', '000333.SZ', '002304.SZ', '600887.SH',
               '000651.SZ', '600690.SH', '600104.SH', '000568.SZ', '600809.SH',
               # 新能源
               '300750.SZ', '002594.SZ', '601012.SH', '600438.SH', '002129.SZ',
               # 医药
               '600276.SH', '000538.SZ', '600196.SH', '600085.SH', '000423.SZ',
               # 科技
               '002415.SZ', '000977.SZ', '603259.SH', '600570.SH', '002230.SZ',
               # 其他
               '600900.SH', '601088.SH', '601857.SH', '601898.SH', '601600.SH'
           ][:30]  # 限制为前30只用于测试


def download_csi300_sample():
    """下载沪深300样本数据"""
    symbols = get_csi300_symbols()
    print(f"下载沪深300样本股 ({len(symbols)} 只)")

    # 使用现有的批量处理器
    from src.data.factor_batch_processor import FactorBatchProcessor

    processor = FactorBatchProcessor()

    report = processor.process_symbol_list(
        symbols=symbols,
        mode='full',
        start_date='2024-01-01',  # 最近1年，加快速度
        end_date=datetime.now().strftime('%Y%m%d')
    )

    processor.cleanup()

    # 输出结果
    summary = report['summary']
    print(f"\n📊 下载结果:")
    print(f"  总股票数: {summary['total_symbols']}")
    print(f"  成功: {summary['successful']}")
    print(f"  失败: {summary['failed']}")
    print(f"  总记录数: {summary['total_records']:,}")

    return summary['successful'] > 0


if __name__ == "__main__":
    success = download_csi300_sample()
    exit(0 if success else 1)



