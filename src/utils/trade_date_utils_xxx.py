# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\trade_date_utils.py
# File Name: trade_date_utils
# @ Author: mango-gh22
# @ Date：2026/1/3 13:52
"""
desc 
"""

# src/utils/trade_date_utils.py
"""
交易日工具函数 - 基于实际交易日，不是日历日期
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


class TradeDateManager:
    """交易日管理器"""

    def __init__(self):
        self._trade_dates_cache = None
        self._last_update = None

    def get_last_trade_date(self, date_str: str = None) -> str:
        """
        获取最后一个交易日

        Args:
            date_str: 参考日期 (YYYYMMDD)，如果为None则使用今天

        Returns:
            最后一个交易日的日期字符串 (YYYYMMDD)
        """
        try:
            if date_str:
                reference_date = datetime.strptime(date_str, '%Y%m%d')
            else:
                reference_date = datetime.now()

            # 简单实现：如果是周末，退回到周五
            while reference_date.weekday() >= 5:  # 5=周六, 6=周日
                reference_date -= timedelta(days=1)

            # 如果是节假日，继续向前推（简化版，实际应该使用交易日历）
            # 这里可以扩展为真正的交易日历

            return reference_date.strftime('%Y%m%d')

        except Exception as e:
            logger.error(f"获取最后交易日失败: {e}")
            # 默认返回昨天
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime('%Y%m%d')

    def get_trade_date_range(self, days_back: int = 30) -> tuple:
        """
        获取交易日范围

        Args:
            days_back: 回溯天数

        Returns:
            (start_date, end_date) 都是交易日
        """
        end_date = self.get_last_trade_date()

        # 计算开始日期（简单实现，实际应该跳过非交易日）
        end_datetime = datetime.strptime(end_date, '%Y%m%d')
        start_datetime = end_datetime - timedelta(days=days_back * 1.5)  # 考虑非交易日

        # 确保开始日期是交易日
        start_date = self.get_last_trade_date(start_datetime.strftime('%Y%m%d'))

        return start_date, end_date

    def get_previous_trade_date(self, date_str: str) -> str:
        """
        获取前一个交易日

        Args:
            date_str: 当前日期 (YYYYMMDD)

        Returns:
            前一个交易日的日期字符串
        """
        try:
            current_date = datetime.strptime(date_str, '%Y%m%d')

            # 向前推一天
            previous_date = current_date - timedelta(days=1)

            # 确保是交易日
            return self.get_last_trade_date(previous_date.strftime('%Y%m%d'))

        except Exception as e:
            logger.error(f"获取前一个交易日失败 {date_str}: {e}")
            return date_str

    def is_trade_date(self, date_str: str) -> bool:
        """
        判断是否为交易日

        Args:
            date_str: 日期 (YYYYMMDD)

        Returns:
            是否为交易日
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            # 基础判断：不是周末
            if date_obj.weekday() >= 5:
                return False

            # 这里可以添加节假日判断

            return True

        except:
            return False

    def get_valid_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取有效的交易日列表

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)

        Returns:
            交易日列表
        """
        try:
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')

            trade_dates = []
            current = start

            while current <= end:
                current_str = current.strftime('%Y%m%d')
                # if self.is_trade_date(current_str):
                if self.is_trade_day(current_str):
                    trade_dates.append(current_str)
                current += timedelta(days=1)

            return trade_dates

        except Exception as e:
            logger.error(f"获取交易日列表失败 {start_date}-{end_date}: {e}")
            return []


# 单例实例
_trade_date_manager = None


def get_trade_date_manager() -> TradeDateManager:
    """获取交易日管理器单例"""
    global _trade_date_manager
    if _trade_date_manager is None:
        _trade_date_manager = TradeDateManager()
    return _trade_date_manager


def test_trade_date_manager():
    """测试交易日管理器"""
    print("🧪 测试交易日管理器")
    print("=" * 50)

    manager = get_trade_date_manager()

    # 测试获取最后交易日
    today = datetime.now().strftime('%Y%m%d')
    last_trade_date = manager.get_last_trade_date()
    print(f"今天: {today}")
    print(f"最后交易日: {last_trade_date}")

    # 测试日期范围
    start_date, end_date = manager.get_trade_date_range(days_back=7)
    print(f"\n最近7个交易日范围: {start_date} - {end_date}")

    # 测试前一个交易日
    prev_date = manager.get_previous_trade_date(end_date)
    print(f"前一个交易日: {prev_date}")

    # 测试交易日判断
    test_dates = [
        '20241227',  # 周五
        '20241228',  # 周六
        '20241229',  # 周日
        '20241230',  # 周一
    ]

    print("\n交易日判断:")
    for date_str in test_dates:
        # is_trade = manager.is_trade_date(date_str)
        is_trade = manager.is_trade_day(date_str)
        print(f"  {date_str}: {'交易日' if is_trade else '非交易日'}")

    print("\n✅ 交易日管理器测试完成")