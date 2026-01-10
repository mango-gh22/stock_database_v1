# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\collect_a50_daily.py
# File Name: collect_a50_daily
# @ Author: mango-gh22
# @ Date：2025/12/13 12:42
"""
desc 从 symbols.yaml 读取50只成分股的代码
将股票列表和设定的日期范围传入 batch_process_stocks 方法

desc: 从中证A50成分股列表增量下载日线数据（仅下载缺失日期）
      使用交易日历智能确定数据范围，支持在任意日期（包括休市日）运行
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_daily_downloader import BaostockDailyDownloader
from src.utils.stock_pool_loader import load_a50_components
from src.data.data_storage import DataStorage

# 尝试导入 chinese_calendar，若无则降级为仅跳过周末
try:
    import chinese_calendar
    HAS_CHINESE_CALENDAR = True
except ImportError:
    HAS_CHINESE_CALENDAR = False
    logging.warning("未安装 chinese-calendar，节假日将按周末处理（建议: pip install chinesecalendar）")

    class SimpleCalendar:
        @staticmethod
        def is_workday(dt):
            return dt.weekday() < 5  # Mon-Fri
    chinese_calendar = SimpleCalendar()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_last_market_day(max_lookback: int = 7) -> str:
    """
    获取最近一个交易日（YYYYMMDD 格式，适配 Baostock）
    最多回溯 max_lookback 天（应对长假）
    """
    today = datetime.today()
    for i in range(max_lookback):
        check_day = today - timedelta(days=i)
        if chinese_calendar.is_workday(check_day):
            return check_day.strftime('%Y%m%d')
    # fallback: 返回今天（理论上不会触发）
    return today.strftime('%Y%m%d')


def main():
    logger.info("🚀 开始增量采集中证A50指数成分股日线数据")

    symbols = load_a50_components()
    logger.info(f"📋 加载 {len(symbols)} 只成分股: {symbols[:3]}...")

    downloader = BaostockDailyDownloader()
    storage = DataStorage()

    # ✅ 关键修复：end_date 是最近一个 *交易日*，不是今天！
    global_end_date = get_last_market_day(max_lookback=7)
    logger.info(f"📅 全局数据截止日（最近交易日）: {global_end_date}")

    success_count = 0
    for i, symbol in enumerate(symbols, 1):
        logger.info(f"[{i}/{len(symbols)}] 处理 {symbol}")

        # 1. 查询数据库中该股最新交易日（格式：'YYYY-MM-DD'）
        # latest_in_db = storage.get_latest_trade_date(symbol)
        last_date_str = storage.get_last_update_date(symbol)
        if last_date_str:
            # 转为整数格式 YYYYMMDD，与全局截止日 global_end_date 格式一致
            latest_in_db = int(last_date_str.replace('-', ''))
            logger.info(f"[{symbol}] 数据库最新交易日: {latest_in_db}")
        else:
            latest_in_db = None
            logger.info(f"[{symbol}] 数据库无历史数据，将全量采集")


        if latest_in_db:
            # 转为 datetime，加一天，再找下一个交易日
            last_dt = datetime.strptime(latest_in_db, '%Y-%m-%d')
            next_day = last_dt + timedelta(days=1)
            # 跳过非交易日
            while not chinese_calendar.is_workday(next_day):
                next_day += timedelta(days=1)
            start_date = next_day.strftime('%Y%m%d')
            logger.info(f"  → 增量模式: 从 {start_date} 开始下载")
        else:
            start_date = "20200101"
            logger.info(f"  → 首次下载: 从 {start_date} 开始")

        # 2. 比较：如果起始日 > 截止日，说明已最新
        if start_date > global_end_date:
            logger.info(f"  → 无需更新（{symbol} 已最新至 {global_end_date}）")
            continue

        # 3. 下载数据
        data_dict = downloader.download_batch([symbol], start_date, global_end_date)
        df = data_dict.get(symbol, pd.DataFrame())

        if df.empty:
            logger.warning(f"  → {symbol} 在 {start_date}-{global_end_date} 无返回数据（可能停牌或接口限制）")
            continue

        # 4. 存入数据库
        rows_affected, report = storage.store_daily_data(df)
        if report['status'] == 'success':
            success_count += 1
            logger.info(f"  → 成功写入 {rows_affected} 行")
        else:
            logger.error(f"  → 写入失败: {report.get('error')}")

    logger.info(f"✅ 增量采集完成！成功更新 {success_count}/{len(symbols)} 只股票")


if __name__ == "__main__":
    main()