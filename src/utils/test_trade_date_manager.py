# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\test_trade_date_manager.py
# File Name: test_trade_date_manager
# @ Author: mango-gh22
# @ Date：2025/12/28 9:04
"""
desc
单元测试：TradeDateRangeManager
使用 pytest 编写，支持 mock 数据库和日历
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from src.utils.trade_date_manager import TradeDateRangeManager


class MockChineseCalendar:
    @staticmethod
    def is_workday(dt):
        return dt.weekday() < 5  # 只处理周末


@pytest.fixture
def mock_chinese_calendar(monkeypatch):
    monkeypatch.setattr("src.utils.trade_date_manager.chinese_calendar", MockChineseCalendar())


# Helper: 用于 mock datetime.today 在 trade_date_manager 模块中的引用
def mock_today(target_date_str):
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    return patch('src.utils.trade_date_manager.datetime', Mock(wraps=datetime, today=Mock(return_value=target_dt)))


def test_fresh_symbol_needs_full_history(mock_chinese_calendar):
    """测试全新股票：应返回全量历史区间"""
    def mock_get_latest(symbol):
        return None

    with mock_today("2025-12-29"):  # 假设今天是周一
        manager = TradeDateRangeManager(mock_get_latest)
        result = manager.get_missing_date_range("sh000001", full_history_start="2020-01-01")

        assert result is not None
        start, end = result
        assert start == "2020-01-01"
        assert end == "2025-12-29"  # 周一，是交易日


def test_partial_data_needs_incremental(mock_chinese_calendar):
    """测试已有部分数据：应返回增量区间"""
    def mock_get_latest(symbol):
        return "2025-12-26"  # 最新到周五

    with mock_today("2025-12-29"):  # 今天是周一
        manager = TradeDateRangeManager(mock_get_latest)
        result = manager.get_missing_date_range("sh000001")

        assert result is not None
        start, end = result
        assert start == "2025-12-29"  # 下一个交易日
        assert end == "2025-12-29"    # 今天就是交易日


def test_already_up_to_date(mock_chinese_calendar):
    """测试数据已最新：应返回 None"""
    def mock_get_latest(symbol):
        return "2025-12-29"

    with mock_today("2025-12-29"):
        manager = TradeDateRangeManager(mock_get_latest)
        result = manager.get_missing_date_range("sh000001")
        assert result is None


def test_handles_weekend_correctly(mock_chinese_calendar):
    """确保在周末运行时能正确找到最近交易日"""
    def mock_get_latest(symbol):
        return "2025-12-26"  # 上周五

    # 今天是周六 2025-12-27
    with mock_today("2025-12-27"):
        manager = TradeDateRangeManager(mock_get_latest)
        result = manager.get_missing_date_range("sh000001")
        # 最近交易日是 26 号，数据库也是 26 号 → 已最新
        assert result is None

    # 再测周日
    with mock_today("2025-12-28"):
        manager = TradeDateRangeManager(mock_get_latest)
        result = manager.get_missing_date_range("sh000001")
        assert result is None