# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\adjustment_factor_date_calculator.py
# File Name: adjustment_factor_data_calculator
# @ Author: mango-gh22
# @ Date：2026/1/2 19:21
"""
desc
复权因子日期计算器
基于分红事件的多个关键日期（公告日、登记日、除权日）智能计算下载范围
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
import pandas as pd
from src.data.adjustment_factor_storage import AdjustmentFactorStorage
from src.utils.logger import get_logger
from src.utils.trade_date_manager import TradeDateRangeManager

logger = get_logger(__name__)


class AdjustmentFactorDateCalculator:
    """复权因子日期计算器（支持多日期类型）"""

    def __init__(self, storage: AdjustmentFactorStorage):
        self.storage = storage
        # 使用交易日历管理器
        self.trade_date_manager = TradeDateRangeManager(
            get_latest_date_func=self._get_latest_ex_date_from_db
        )

    def _get_latest_ex_date_from_db(self, symbol: str) -> Optional[str]:
        """
        从数据库获取最新除权除息日
        适配 TradeDateRangeManager 接口
        """
        try:
            df = self.storage.get_factors_by_symbol(symbol)
            if df.empty:
                return None
            # 返回最早日期（因为查询结果是DESC排序）
            latest_date = df['ex_date'].iloc[0]
            return latest_date.strftime('%Y-%m-%d') if isinstance(latest_date, pd.Timestamp) else str(latest_date)
        except Exception as e:
            logger.warning(f"获取 {symbol} 最新除权日失败: {e}")
            return None

    def calculate_download_range(
            self,
            symbol: str,
            mode: str = 'incremental',
            custom_params: Optional[Dict] = None
    ) -> Optional[Tuple[str, str]]:
        """
        计算分红数据下载日期范围

        Args:
            symbol: 股票代码
            mode: 模式 ('incremental', 'full', 'specific')
            custom_params: 自定义参数

        Returns:
            (start_date, end_date) 格式YYYYMMDD
            None 表示无需下载
        """
        if custom_params is None:
            custom_params = {}

        logger.info(f"计算复权因子下载范围: {symbol}, 模式: {mode}")

        if mode == 'incremental':
            return self._calculate_incremental_range(symbol, custom_params)
        elif mode == 'full':
            return self._calculate_full_range(symbol, custom_params)
        elif mode == 'specific':
            return self._calculate_specific_range(symbol, custom_params)
        else:
            raise ValueError(f"不支持的日期计算模式: {mode}")

    def _calculate_incremental_range(self, symbol: str, params: Dict) -> Optional[Tuple[str, str]]:
        """增量更新模式"""
        # 尝试获取数据库中最新除权日
        latest_ex_date = self._get_latest_ex_date_from_db(symbol)

        # 结束日期：最近一个交易日（考虑长假）
        max_lookback = params.get('max_lookback_days', 7)
        end_date = self._get_last_market_day(max_lookback)

        if latest_ex_date:
            # 从最后除权日的下一个交易日开始
            start_date = self._next_trade_date(latest_ex_date)
            logger.info(f"增量更新 {symbol}: 最新除权日 {latest_ex_date} -> 起始 {start_date}")
        else:
            # 无历史数据，使用默认回溯
            days_back = params.get('initial_lookback_days', 3650)
            start_date = self._get_date_before(end_date, days_back)
            logger.info(f"首次下载 {symbol}: 回溯 {days_back} 天 -> 起始 {start_date}")

        # 验证范围有效性
        if not self._validate_range(start_date, end_date):
            logger.warning(f"日期范围无效: {start_date} - {end_date}")
            return None

        return start_date, end_date

    def _calculate_full_range(self, symbol: str, params: Dict) -> Tuple[str, str]:
        """全量下载模式"""
        start_date = params.get('full_start_date', '2000-01-01')
        max_lookback = params.get('max_lookback_days', 7)
        end_date = self._get_last_market_day(max_lookback)
        logger.info(f"全量下载 {symbol}: {start_date} - {end_date}")
        return start_date, end_date

    def _calculate_specific_range(self, symbol: str, params: Dict) -> Tuple[str, str]:
        """指定日期范围模式"""
        if 'date_range' not in params:
            raise ValueError("specific模式必须提供 date_range 参数")

        date_range = params['date_range']
        start_date = date_range.get('start')
        end_date = date_range.get('end')

        if not start_date or not end_date:
            raise ValueError("date_range必须包含 start 和 end")

        logger.info(f"指定范围 {symbol}: {start_date} - {end_date}")
        return start_date, end_date

    def _next_trade_date(self, date_str: str) -> str:
        """获取下一个交易日"""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        next_day = dt + timedelta(days=1)

        # 使用chinese_calendar判断（如果可用）
        try:
            import chinese_calendar
            while not chinese_calendar.is_workday(next_day):
                next_day += timedelta(days=1)
        except ImportError:
            # 降级：仅跳过周末
            while next_day.weekday() >= 5:
                next_day += timedelta(days=1)

        return next_day.strftime("%Y%m%d")

    def _get_last_market_day(self, max_lookback: int = 7) -> str:
        """获取最近一个交易日"""
        today = datetime.today()
        for i in range(max_lookback):
            check_day = today - timedelta(days=i)
            try:
                import chinese_calendar
                if chinese_calendar.is_workday(check_day):
                    return check_day.strftime("%Y%m%d")
            except ImportError:
                if check_day.weekday() < 5:
                    return check_day.strftime("%Y%m%d")

        # Fallback
        return today.strftime("%Y%m%d")

    def _get_date_before(self, end_date: str, days: int) -> str:
        """获取N天前的日期"""
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        start_dt = end_dt - timedelta(days=days)
        return start_dt.strftime("%Y%m%d")

    def _validate_range(self, start_date: str, end_date: str) -> bool:
        """验证日期范围"""
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")

            if start_dt > end_dt:
                logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}")
                return False

            if start_dt > datetime.now():
                logger.error(f"开始日期 {start_date} 在未来")
                return False

            # 跨度限制（最多15年）
            max_span = timedelta(days=365 * 15)
            if (end_dt - start_dt) > max_span:
                logger.warning(f"日期范围超过15年，可能导致数据量过大")

            return True
        except Exception as e:
            logger.error(f"日期验证失败: {e}")
            return False

    def split_large_range(self, start_date: str, end_date: str,
                          max_days: int = 365) -> List[Tuple[str, str]]:
        """
        分割大的日期范围（用于未来多线程优化）

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            max_days: 每段最大天数

        Returns:
            日期段列表
        """
        ranges = []

        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        total_days = (end_dt - start_dt).days + 1

        if total_days <= max_days:
            return [(start_date, end_date)]

        # 计算分段
        num_chunks = (total_days + max_days - 1) // max_days

        for i in range(num_chunks):
            chunk_start = start_dt + timedelta(days=i * max_days)
            chunk_end = min(chunk_start + timedelta(days=max_days - 1), end_dt)

            ranges.append((
                chunk_start.strftime("%Y%m%d"),
                chunk_end.strftime("%Y%m%d")
            ))

        logger.info(f"日期范围分割为 {len(ranges)} 段")
        return ranges


# 与storage集成的便捷方法
def get_adjustment_factor_date_calculator(config_path: str = 'config/database.yaml') -> AdjustmentFactorDateCalculator:
    """工厂函数"""
    storage = AdjustmentFactorStorage(config_path)
    return AdjustmentFactorDateCalculator(storage)