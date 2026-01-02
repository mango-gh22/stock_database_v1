# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\date_calculator.py
# File Name: date_calculator
# @ Author: mango-gh22
# @ Date：2025/12/14 8:37
"""
desc 
"""

# src/data/date_calculator.py
"""
日期范围计算器 - 智能计算不同模式下的日期范围
"""

from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any, List
import pandas as pd
from src.utils.logger import get_logger
from src.data.adaptive_storage import AdaptiveDataStorage

logger = get_logger(__name__)


class DateRangeCalculator:
    """日期范围计算器"""

    def __init__(self, storage: AdaptiveDataStorage):
        self.storage = storage

    def calculate_range(self,
                        symbol: str,
                        mode: str,
                        custom_params: Optional[Dict] = None) -> Tuple[str, str]:
        """
        计算日期范围

        Args:
            symbol: 股票代码
            mode: 模式 ('incremental', 'batch_init', 'specific')
            custom_params: 自定义参数

        Returns:
            (start_date, end_date) 格式为 YYYYMMDD
        """
        if custom_params is None:
            custom_params = {}

        handlers = {
            'incremental': self._calculate_incremental_range,
            'batch_init': self._calculate_batch_range,
            'specific': self._calculate_specific_range,
        }

        if mode not in handlers:
            raise ValueError(f"不支持的模式: {mode}")

        return handlers[mode](symbol, custom_params)

    def _calculate_incremental_range(self, symbol: str, params: Dict) -> Tuple[str, str]:
        """计算增量更新日期范围"""
        # 默认回溯天数
        default_days_back = params.get('days_back', 30)

        # 获取数据库中最后日期
        last_date = self.storage.get_last_update_date(symbol)

        if last_date:
            # 从最后日期的下一天开始
            last_dt = datetime.strptime(last_date, '%Y%m%d')
            start_dt = last_dt + timedelta(days=1)
            start_date = start_dt.strftime('%Y%m%d')
            logger.info(f"增量更新 {symbol}: 从最后日期 {last_date} 的后一天开始")
        else:
            # 没有历史数据，使用默认回溯天数
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=default_days_back)
            start_date = start_dt.strftime('%Y%m%d')
            logger.info(f"增量更新 {symbol}: 无历史数据，使用默认{default_days_back}天回溯")

        # 结束日期为今天
        end_date = datetime.now().strftime('%Y%m%d')

        # 确保开始日期不大于结束日期
        if start_date > end_date:
            logger.warning(f"开始日期{start_date}大于结束日期{end_date}，使用结束日期前一天")
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            start_dt = end_dt - timedelta(days=1)
            start_date = start_dt.strftime('%Y%m%d')

        return start_date, end_date

    def _calculate_batch_range(self, symbol: str, params: Dict) -> Tuple[str, str]:
        """计算批量初始化日期范围"""
        # 默认起始日期
        default_start = params.get('start_date', '20200101')

        # 获取上市日期（如果数据库中有）
        list_date = self._get_list_date(symbol)

        if list_date:
            # 使用上市日期和今天
            start_date = max(list_date, default_start)
        else:
            # 使用默认起始日期
            start_date = default_start

        # 结束日期
        end_date = params.get('end_date', datetime.now().strftime('%Y%m%d'))

        logger.info(f"批量更新 {symbol}: 日期范围 {start_date} - {end_date}")
        return start_date, end_date

    def _calculate_specific_range(self, symbol: str, params: Dict) -> Tuple[str, str]:
        """计算指定更新日期范围"""
        # 支持单个日期范围或多个日期段
        if 'date_range' in params:
            # 单个日期范围
            date_range = params['date_range']
            if isinstance(date_range, dict) and 'start' in date_range and 'end' in date_range:
                start_date = date_range['start']
                end_date = date_range['end']
            else:
                raise ValueError(f"无效的日期范围格式: {date_range}")

        elif 'periods' in params and isinstance(params['periods'], list):
            # 多个日期段 - 使用第一个段
            periods = params['periods']
            if periods and isinstance(periods[0], dict):
                first_period = periods[0]
                start_date = first_period.get('start', '20200101')
                end_date = first_period.get('end', datetime.now().strftime('%Y%m%d'))
            else:
                raise ValueError(f"无效的日期段格式: {periods}")
        else:
            # 默认使用最近30天
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

        logger.info(f"指定更新 {symbol}: 日期范围 {start_date} - {end_date}")
        return start_date, end_date

    def _get_list_date(self, symbol: str) -> Optional[str]:
        """获取上市日期"""
        try:
            # 从stock_basic_info表获取
            with self.storage.db_connector.get_connection() as conn:
                cursor = conn.cursor()
                query = """
                    SELECT list_date FROM stock_basic_info 
                    WHERE symbol = %s OR ts_code = %s
                    LIMIT 1
                """
                cursor.execute(query, (symbol, symbol))
                result = cursor.fetchone()

                if result and result[0]:
                    # 转换为YYYYMMDD格式
                    if isinstance(result[0], str):
                        return result[0].replace('-', '')
                    else:
                        return result[0].strftime('%Y%m%d')

                return None

        except Exception as e:
            logger.warning(f"获取上市日期失败 {symbol}: {e}")
            return None

    def validate_date_range(self, start_date: str, end_date: str) -> bool:
        """验证日期范围有效性"""
        try:
            # 转换日期
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')

            # 检查基本合理性
            if start_dt > end_dt:
                logger.error(f"开始日期{start_date}不能大于结束日期{end_date}")
                return False

            if start_dt > datetime.now():
                logger.error(f"开始日期{start_date}不能大于今天")
                return False

            if start_dt < datetime(1990, 1, 1):
                logger.warning(f"开始日期{start_date}早于1990年，可能存在数据问题")

            # 检查跨度（最多允许10年）
            max_span = timedelta(days=3650)  # 约10年
            if (end_dt - start_dt) > max_span:
                logger.warning(f"日期范围跨度超过10年，建议分批次处理")

            return True

        except ValueError as e:
            logger.error(f"日期格式无效: {start_date} 或 {end_date}, 错误: {e}")
            return False
        except Exception as e:
            logger.error(f"验证日期范围失败: {e}")
            return False

    def split_large_range(self, start_date: str, end_date: str,
                          max_days: int = 365) -> List[Tuple[str, str]]:
        """
        分割大的日期范围

        Args:
            start_date: 开始日期
            end_date: 结束日期
            max_days: 最大天数

        Returns:
            日期段列表
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')

            total_days = (end_dt - start_dt).days + 1
            if total_days <= max_days:
                return [(start_date, end_date)]

            # 计算需要分割成多少段
            num_chunks = (total_days + max_days - 1) // max_days

            ranges = []
            for i in range(num_chunks):
                chunk_start = start_dt + timedelta(days=i * max_days)
                chunk_end = min(chunk_start + timedelta(days=max_days - 1), end_dt)

                ranges.append((
                    chunk_start.strftime('%Y%m%d'),
                    chunk_end.strftime('%Y%m%d')
                ))

            logger.info(f"日期范围分割为 {len(ranges)} 段")
            return ranges

        except Exception as e:
            logger.error(f"分割日期范围失败: {e}")
            return [(start_date, end_date)]