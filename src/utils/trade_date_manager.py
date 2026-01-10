# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\trade_date_manager.py
# File Name: trade_date_manager
# @ Author: mango-gh22
# @ Date：2025/12/28 9:03
"""
desc 交易日范围管理器：基于数据库状态和中国交易日历，智能计算缺失数据区间
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any

# 可选：如果不想强依赖 chinese_calendar，可提供 fallback
try:
    import chinese_calendar
    HAS_CHINESE_CALENDAR = True
except ImportError:
    HAS_CHINESE_CALENDAR = False
    # 简易周末判断（不处理节假日）
    def is_workday(dt):
        return dt.weekday() < 5
    chinese_calendar = type('dummy', (), {'is_workday': is_workday})()

logger = logging.getLogger(__name__)


class TradeDateRangeManager:
    """
    管理股票数据缺失的交易日范围。
    与具体数据库实现解耦，只需提供查询最新日期的回调函数。
    """

    def __init__(self, get_latest_date_func: callable):
        """
        Args:
            get_latest_date_func: 函数，接收 symbol -> 返回 'YYYY-MM-DD' 或 None
        """
        if not callable(get_latest_date_func):
            raise ValueError("get_latest_date_func must be callable")
        self._get_latest_date = get_latest_date_func

    def get_missing_date_range(
        self,
        symbol: str,
        full_history_start: str = "2020-01-01",
        max_lookback_days: int = 7
    ) -> Optional[Tuple[str, str]]:
        """
        计算需要下载的数据日期范围 [start, end]（均为交易日）

        Args:
            symbol: 股票代码，如 'sh600000'
            full_history_start: 首次下载的起始日期
            max_lookback_days: 最多回溯多少天找最近交易日（应对长假）

        Returns:
            (start_date, end_date) 如 ('2025-12-26', '2025-12-30')
            None 表示已最新，无需下载
        """
        latest_in_db = self._get_latest_date(symbol)
        logger.debug(f"DB 中 {symbol} 最新日期: {latest_in_db}")

        # 确定起始日
        if latest_in_db is None:
            start = full_history_start
        else:
            start = self._next_trade_date(latest_in_db)

        # 确定截止日：最近一个交易日
        end = self._get_last_market_day(max_lookback_days)

        # 比较日期
        if self._date_to_timestamp(start) > self._date_to_timestamp(end):
            logger.info(f"{symbol} 数据已最新（截至 {end}）")
            return None

        return start, end

    def _next_trade_date(self, date_str: str) -> str:
        """获取下一个交易日（跳过周末/节假日）"""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        next_day = dt + timedelta(days=1)
        while not chinese_calendar.is_workday(next_day):
            next_day += timedelta(days=1)
        return next_day.strftime("%Y-%m-%d")

    def _get_last_market_day(self, max_lookback: int = 7) -> str:
        """获取最近一个交易日（最多回溯 max_lookback 天）"""
        today = datetime.today()
        for i in range(max_lookback):
            check_day = today - timedelta(days=i)
            if chinese_calendar.is_workday(check_day):
                return check_day.strftime("%Y-%m-%d")
        # fallback: 返回今天（理论上不会触发）
        return today.strftime("%Y-%m-%d")

    @staticmethod
    def _date_to_timestamp(date_str: str) -> float:
        """安全地将日期字符串转为时间戳用于比较"""
        return datetime.strptime(date_str, "%Y-%m-%d").timestamp()