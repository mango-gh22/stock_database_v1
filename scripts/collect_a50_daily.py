# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\collect_a50_daily.py
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
import time
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_daily_downloader import BaostockDailyDownloader
from src.data.data_storage import DataStorage
from src.utils.stock_pool_loader import load_a50_components

# 交易日历
try:
    import chinese_calendar

    HAS_CHINESE_CALENDAR = True
except ImportError:
    HAS_CHINESE_CALENDAR = False
    logging.warning("未安装 chinese-calendar，节假日将按周末处理")


    class SimpleCalendar:
        @staticmethod
        def is_workday(dt):
            return dt.weekday() < 5  # Mon-Fri


    chinese_calendar = SimpleCalendar()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_last_market_day(max_lookback: int = 7) -> str:
    """获取最近一个交易日（YYYYMMDD）"""
    today = datetime.today()
    for i in range(max_lookback):
        check_day = today - timedelta(days=i)
        if chinese_calendar.is_workday(check_day):
            return check_day.strftime('%Y%m%d')
    return today.strftime('%Y%m%d')


def incremental_download(symbols):
    """增量下载核心逻辑"""
    if not symbols:
        logger.error("股票列表为空")
        return False

    downloader = BaostockDailyDownloader()
    storage = DataStorage()

    # 获取最后交易日
    try:
        from src.utils.enhanced_trade_date_manager import EnhancedTradeDateManager
        trade_manager = EnhancedTradeDateManager()
        global_end_date = trade_manager.get_last_trade_date_str()
        logger.info(f"📅 全局截止日: {global_end_date}")
    except:
        global_end_date = get_last_market_day()
        logger.warning(f"⚠️  使用备用日期: {global_end_date}")

    success_count = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            logger.info(f"[{i}/{len(symbols)}] 处理 {symbol}")

            # 查询最后更新日期
            last_date_str = storage.get_last_update_date(symbol)

            if last_date_str:
                # 转为 datetime，加一天
                last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
                next_day = last_dt + timedelta(days=1)

                # 跳过非交易日
                while not chinese_calendar.is_workday(next_day):
                    next_day += timedelta(days=1)

                start_date = next_day.strftime('%Y%m%d')

                if start_date > global_end_date:
                    logger.info(f"  ⏭️  {symbol} 已最新，跳过")
                    continue
            else:
                start_date = "20200101"
                logger.info(f"  🔄 {symbol} 首次下载，从 {start_date} 开始")

            logger.info(f"  📊 下载范围: {start_date} ~ {global_end_date}")

            # 下载数据
            data_dict = downloader.download_batch([symbol], start_date, global_end_date)
            df = data_dict.get(symbol, pd.DataFrame())

            if df.empty:
                logger.warning(f"  ⚠️  {symbol} 无返回数据（可能停牌）")
                continue

            # 存储数据
            rows_affected, report = storage.store_daily_data(df)

            if report.get('status') == 'success':
                success_count += 1
                logger.info(f"  ✅ 成功写入 {rows_affected} 行")
            else:
                logger.error(f"  ❌ 写入失败: {report.get('error')}")

            # 请求间隔
            if i < len(symbols):
                time.sleep(2 + (i % 3))  # 2-4秒随机间隔

        except Exception as e:
            logger.error(f"  ❌ 处理 {symbol} 失败: {e}", exc_info=True)

    logger.info(f"✅ 增量采集完成！成功更新 {success_count}/{len(symbols)} 只股票")
    return success_count > 0


def main(symbols=None):
    """
    命令行入口
    Args:
        symbols: 股票代码列表（可选），如果为None则从配置文件加载
    """
    if symbols is None:
        symbols = load_a50_components()

    if not symbols:
        logger.error("未找到股票列表")
        return False

    logger.info(f"📋 加载 {len(symbols)} 只成分股: {symbols[:3]}...")

    # 执行增量下载
    return incremental_download(symbols)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)