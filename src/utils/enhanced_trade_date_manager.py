# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\enhanced_trade_date_manager.py
# File Name: enhanced_trade_date_manager
# @ Author: mango-gh22
# @ Date：2026/1/3 14:30
"""
desc
增强版交易日管理器 - 基于chinese_calendar，处理节假日和调休
集成到现有因子数据下载流程
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Set
import pandas as pd

import sys
import os

# 将项目根目录加入 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# 使用您现有的chinese_calendar
try:
    import chinese_calendar as cn_calendar

    HAS_CHINESE_CALENDAR = True
except ImportError:
    HAS_CHINESE_CALENDAR = False
    logger = logging.getLogger(__name__)
    logger.warning("未安装chinese_calendar，使用简化版交易日判断")

# from src.utils.trade_date_manager import TradeDateRangeManager  # 您现有的类

logger = logging.getLogger(__name__)


class EnhancedTradeDateManager:
    """增强版交易日管理器"""

    def __init__(self):
        self._trade_dates_cache = {}  # 缓存交易日
        self._last_trade_date_cache = None
        self._cache_ttl = 3600  # 缓存1小时

    def is_trade_day(self, date_obj: datetime) -> bool:
        """
        判断是否为交易日（考虑节假日和调休）

        规则：
        1. 必须是工作日（周一至周五）
        2. 不能是法定节假日
        3. 调休的工作日算交易日
        """
        try:
            if HAS_CHINESE_CALENDAR:
                # 使用chinese_calendar判断是否为工作日（考虑调休）
                return cn_calendar.is_workday(date_obj)
            else:
                # 简化版：只判断周末
                return date_obj.weekday() < 5

        except Exception as e:
            logger.error(f"判断交易日失败 {date_obj}: {e}")
            # 默认使用简化判断
            return date_obj.weekday() < 5

    def get_last_trade_date(self, reference_date: Optional[datetime] = None) -> datetime:
        """
        获取最后一个交易日

        Args:
            reference_date: 参考日期，如果为None则使用今天

        Returns:
            最后一个交易日的datetime对象
        """
        if reference_date is None:
            reference_date = datetime.now()

        # 检查缓存
        cache_key = reference_date.strftime('%Y%m%d')
        if (cache_key in self._trade_dates_cache and
                (datetime.now() - self._trade_dates_cache[cache_key]['timestamp']).total_seconds() < self._cache_ttl):
            return self._trade_dates_cache[cache_key]['last_trade_date']

        # 向前查找交易日
        current_date = reference_date
        days_checked = 0
        max_days_back = 30  # 最多向前找30天（考虑长假）

        while days_checked < max_days_back:
            if self.is_trade_day(current_date):
                # 缓存结果
                self._trade_dates_cache[cache_key] = {
                    'last_trade_date': current_date,
                    'timestamp': datetime.now()
                }
                return current_date

            # 向前一天
            current_date = current_date - timedelta(days=1)
            days_checked += 1

        # 如果没找到，返回参考日期前30天
        fallback_date = reference_date - timedelta(days=30)
        logger.warning(f"未找到交易日，使用回退日期: {fallback_date}")
        return fallback_date

    # def adjust_to_trade_date(self, date_str: str) -> str:
    #     """将 'YYYYMMDD' 字符串调整为最近的交易日（返回字符串）"""
    #     try:
    #         date_obj = datetime.strptime(date_str, "%Y%m%d")
    #         while not self.is_trade_day(date_obj):
    #             date_obj -= timedelta(days=1)
    #         return date_obj.strftime("%Y%m%d")
    #     except Exception as e:
    #         logger.warning(f"adjust_to_trade_date failed for {date_str}: {e}")
    #         return date_str


    def get_last_trade_date_str(self, date_str: Optional[str] = None,
                                format_str: str = '%Y%m%d') -> str:
        """
        获取最后一个交易日的字符串

        Args:
            date_str: 参考日期字符串 (YYYYMMDD)
            format_str: 输出格式

        Returns:
            最后一个交易日的字符串
        """
        if date_str:
            reference_date = datetime.strptime(date_str, '%Y%m%d')
        else:
            reference_date = datetime.now()

        last_trade_date = self.get_last_trade_date(reference_date)
        return last_trade_date.strftime(format_str)

    def get_trade_date_range(self, days_back: int = 30,
                             end_date: Optional[datetime] = None) -> Tuple[datetime, datetime]:
        """
        获取交易日范围

        Args:
            days_back: 回溯交易日数量
            end_date: 结束日期，如果为None则使用最后一个交易日

        Returns:
            (start_date, end_date) 都是交易日
        """
        if end_date is None:
            end_date = self.get_last_trade_date()

        # 向前查找指定数量的交易日
        start_date = end_date
        trade_days_found = 0

        while trade_days_found < days_back:
            start_date = start_date - timedelta(days=1)
            if self.is_trade_day(start_date):
                trade_days_found += 1

        # 确保start_date是交易日
        while not self.is_trade_day(start_date):
            start_date = start_date - timedelta(days=1)

        return start_date, end_date

    def get_trade_date_range_str(self, days_back: int = 30,
                                 end_date_str: Optional[str] = None,
                                 format_str: str = '%Y%m%d') -> Tuple[str, str]:
        """
        获取交易日范围的字符串

        Args:
            days_back: 回溯交易日数量
            end_date_str: 结束日期字符串 (YYYYMMDD)
            format_str: 输出格式

        Returns:
            (start_date_str, end_date_str)
        """
        end_date = None
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y%m%d')

        start_date, end_date = self.get_trade_date_range(days_back, end_date)
        return start_date.strftime(format_str), end_date.strftime(format_str)

    def get_previous_trade_date(self, date_obj: datetime) -> datetime:
        """
        获取前一个交易日

        Args:
            date_obj: 当前日期

        Returns:
            前一个交易日
        """
        current_date = date_obj - timedelta(days=1)
        days_checked = 0
        max_days_back = 30

        while days_checked < max_days_back:
            if self.is_trade_day(current_date):
                return current_date

            current_date = current_date - timedelta(days=1)
            days_checked += 1

        # 没找到，返回30天前
        return date_obj - timedelta(days=30)

    def get_next_trade_date(self, date_obj: datetime) -> datetime:
        """
        获取下一个交易日

        Args:
            date_obj: 当前日期

        Returns:
            下一个交易日
        """
        current_date = date_obj + timedelta(days=1)
        days_checked = 0
        max_days_forward = 30

        while days_checked < max_days_forward:
            if self.is_trade_day(current_date):
                return current_date

            current_date = current_date + timedelta(days=1)
            days_checked += 1

        # 没找到，返回30天后
        return date_obj + timedelta(days=30)

    def get_trade_dates_between(self, start_date: datetime,
                                end_date: datetime) -> List[datetime]:
        """
        获取两个日期之间的所有交易日

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        trade_dates = []
        current_date = start_date

        while current_date <= end_date:
            if self.is_trade_day(current_date):
                trade_dates.append(current_date)
            current_date = current_date + timedelta(days=1)

        return trade_dates

    def get_trade_dates_between_str(self, start_date_str: str,
                                    end_date_str: str,
                                    format_str: str = '%Y%m%d') -> List[str]:
        """
        获取两个日期之间的所有交易日字符串

        Args:
            start_date_str: 开始日期字符串
            end_date_str: 结束日期字符串
            format_str: 输出格式

        Returns:
            交易日字符串列表
        """
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')

        trade_dates = self.get_trade_dates_between(start_date, end_date)
        return [date.strftime(format_str) for date in trade_dates]

    def validate_trade_date(self, date_str: str) -> Tuple[bool, str]:
        """
        验证日期是否为交易日

        Args:
            date_str: 日期字符串 (YYYYMMDD)

        Returns:
            (是否为交易日, 说明)
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            if self.is_trade_day(date_obj):
                return True, "交易日"
            else:
                # 判断原因
                if date_obj.weekday() >= 5:
                    return False, "周末"
                else:
                    return False, "节假日"

        except Exception as e:
            return False, f"日期格式错误: {e}"

    def get_trade_date_info(self, date_str: str) -> dict:
        """
        获取交易日详细信息

        Args:
            date_str: 日期字符串

        Returns:
            交易日信息字典
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            info = {
                'date': date_str,
                'weekday': date_obj.weekday(),  # 0=周一, 6=周日
                'weekday_name': ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][date_obj.weekday()],
                'is_trade_day': self.is_trade_day(date_obj),
                'is_weekend': date_obj.weekday() >= 5,
            }

            if HAS_CHINESE_CALENDAR:
                try:
                    # 获取节假日名称
                    holiday_name = cn_calendar.get_holiday_detail(date_obj)
                    if holiday_name:
                        info['holiday'] = holiday_name[1]
                        info['is_holiday'] = True
                    else:
                        info['is_holiday'] = False
                except:
                    info['is_holiday'] = False

            # 获取前一个和后一个交易日
            if info['is_trade_day']:
                info['previous_trade_date'] = self.get_previous_trade_date(date_obj).strftime('%Y%m%d')
                info['next_trade_date'] = self.get_next_trade_date(date_obj).strftime('%Y%m%d')

            return info

        except Exception as e:
            return {
                'date': date_str,
                'error': str(e),
                'is_trade_day': False
            }

    def adjust_to_trade_date(self, date_str: str, direction: str = 'backward') -> str:
        """
        将 'YYYYMMDD' 字符串调整为最近的交易日
        """
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            # 如果已经是交易日，直接返回
            if self.is_trade_day(date_obj):  # ✅ 传 datetime，不是字符串！
                return date_str

            if direction == 'forward':
                while True:
                    date_obj += timedelta(days=1)
                    if self.is_trade_day(date_obj):
                        return date_obj.strftime('%Y%m%d')

            elif direction == 'backward':
                while True:
                    date_obj -= timedelta(days=1)
                    if self.is_trade_day(date_obj):
                        return date_obj.strftime('%Y%m%d')

            else:  # nearest
                forward = date_obj
                backward = date_obj
                forward_days = backward_days = 0

                # 向前找
                while forward_days < 30:
                    forward += timedelta(days=1)
                    forward_days += 1
                    if self.is_trade_day(forward):
                        break

                # 向后找
                while backward_days < 30:
                    backward -= timedelta(days=1)
                    backward_days += 1
                    if self.is_trade_day(backward):
                        break

                return (forward if forward_days <= backward_days else backward).strftime('%Y%m%d')

        except Exception as e:
            logger.warning(f"adjust_to_trade_date failed for {date_str}: {e}")
            return date_str


# 单例实例
_enhanced_trade_date_manager = None


def get_enhanced_trade_date_manager() -> EnhancedTradeDateManager:
    """获取增强版交易日管理器单例"""
    global _enhanced_trade_date_manager
    if _enhanced_trade_date_manager is None:
        _enhanced_trade_date_manager = EnhancedTradeDateManager()
    return _enhanced_trade_date_manager


def integrate_with_existing_system():
    """
    与现有系统集成

    1. 替换原有的简单日期判断
    2. 提供向后兼容的接口
    """
    # 获取现有的TradeDateRangeManager的回调函数
    # 这里假设您有一个获取最后更新日期的回调函数
    pass


def test_enhanced_trade_date_manager():
    """测试增强版交易日管理器"""
    import sys

    print("🧪 测试增强版交易日管理器")
    print("=" * 50)

    manager = get_enhanced_trade_date_manager()

    # 测试今天是否是交易日
    today = datetime.now()
    is_today_trade = manager.is_trade_day(today)
    print(f"今天 ({today.strftime('%Y-%m-%d')}): {'交易日' if is_today_trade else '非交易日'}")

    # 测试特定日期（2026-01-02 周五但国休）
    test_date = datetime(2026, 1, 2)
    is_test_trade = manager.is_trade_day(test_date)
    print(f"2026-01-02 (周五但国休): {'交易日' if is_test_trade else '非交易日'}")

    # 测试获取最后一个交易日
    last_trade = manager.get_last_trade_date()
    print(f"\n最后一个交易日: {last_trade.strftime('%Y-%m-%d')}")

    # 测试交易日范围
    start_date, end_date = manager.get_trade_date_range(days_back=10)
    print(f"最近10个交易日范围: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")

    # 测试日期验证
    test_dates = [
        '20260102',  # 周五但国休
        '20260105',  # 周一（假设是交易日）
        '20260103',  # 周六
        '20260104',  # 周日
    ]

    print("\n日期验证测试:")
    for date_str in test_dates:
        is_valid, reason = manager.validate_trade_date(date_str)
        status = "✅" if is_valid else "❌"
        print(f"  {status} {date_str}: {reason}")

    # 测试交易日信息
    print("\n交易日详细信息:")
    for date_str in test_dates[:2]:
        info = manager.get_trade_date_info(date_str)
        print(f"\n  {date_str}:")
        for key, value in info.items():
            print(f"    {key}: {value}")

    print("\n✅ 增强版交易日管理器测试完成")
    return True


if __name__ == "__main__":
    success = test_enhanced_trade_date_manager()
    sys.exit(0 if success else 1)