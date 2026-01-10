# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\data_collector.py
# File Name: data_collector
# @ Author: mango-gh22
# @ Date：2025/12/5 18:44
"""
desc 数据采集器抽象基类
定义统一接口，支持多数据源（Baostock/Tushare/AKShare等）
目前基于baostock，稳定可靠，低效（受平台单线程数据下载所限）
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import yaml
import logging
import time
import re

logger = logging.getLogger(__name__)


class BaseDataCollector(ABC):
    """
    数据采集器抽象基类
    所有具体采集器（如 BaostockCollector）必须继承并实现抽象方法
    """

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化配置

        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.rate_limit_count = 0
        self.rate_limit_time = None
        self._request_stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'start_time': time.time()
        }

    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"⚠️ 配置文件加载失败 {config_path}: {e}")
            return {}

    # ==================== 抽象方法（子类必须实现）====================

    @abstractmethod
    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取日线行情数据
        Args:
            symbol: 股票代码（标准化格式，如 sh600519）
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        Returns:
            DataFrame 包含标准字段（见文档）
        """
        pass

    @abstractmethod
    def fetch_minute_data(self, symbol: str, trade_date: str, freq: str = '5min') -> Optional[pd.DataFrame]:
        """
        获取分钟级行情数据
        Args:
            symbol: 股票代码
            trade_date: 交易日 (YYYYMMDD)
            freq: 频率 ('1min', '5min', '15min', '30min', '60min')
        Returns:
            DataFrame
        """
        pass

    @abstractmethod
    def fetch_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取股票基本信息
        Returns:
            字典，包含 name, list_date, market_type 等
        """
        pass

    @abstractmethod
    def fetch_stock_list(self, market: str = "A股") -> pd.DataFrame:
        """
        获取股票列表
        Args:
            market: 市场类型（"上证", "深证", "科创板", "创业板", "北交所", "A股"）
        Returns:
            DataFrame 列：symbol, stock_code, name, list_date, market_type, exchange
        """
        pass

    # ==================== 可选/默认方法（子类可重写）====================

    def batch_download_daily_data(
            self,
            symbols: List[str],
            start_date: str,
            end_date: str,
            max_workers: int = 1
    ) -> Dict[str, pd.DataFrame]:
        """
        批量下载日线数据（默认单线程实现，子类可优化为并发）
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = {}
        logger.info(f"🔄 开始批量下载 {len(symbols)} 只股票的日线数据")
        logger.info(f"📅 时间范围: {start_date} 至 {end_date}")

        if max_workers > 1 and len(symbols) > 1:
            # 多线程模式
            logger.info(f"⚡ 使用多线程模式，线程数: {max_workers}")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_symbol = {
                    executor.submit(self.fetch_daily_data, symbol, start_date, end_date): symbol
                    for symbol in symbols
                }

                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            results[symbol] = df
                            self._request_stats['successful'] += 1
                        else:
                            logger.warning(f"⚠️  {symbol}: 获取数据为空")
                            self._request_stats['failed'] += 1
                    except Exception as e:
                        logger.error(f"❌  {symbol}: 下载失败 - {e}")
                        self._request_stats['failed'] += 1
        else:
            # 单线程模式
            logger.info("🐌 使用单线程模式")
            for symbol in symbols:
                try:
                    self.enforce_rate_limit()  # 应用速率限制
                    df = self.fetch_daily_data(symbol, start_date, end_date)
                    if df is not None and not df.empty:
                        results[symbol] = df
                        self._request_stats['successful'] += 1
                    else:
                        logger.warning(f"⚠️  {symbol}: 获取数据为空")
                        self._request_stats['failed'] += 1
                except Exception as e:
                    logger.error(f"❌  {symbol}: 下载失败 - {e}")
                    self._request_stats['failed'] += 1

                self._request_stats['total_requests'] += 1

        logger.info(f"✅ 批量下载完成，成功: {len(results)}/{len(symbols)}")
        return results

    def get_download_stats(self) -> Dict[str, Any]:
        """
        获取下载统计信息
        """
        total = self._request_stats['total_requests']
        successful = self._request_stats['successful']
        failed = self._request_stats['failed']

        success_rate = successful / total if total > 0 else 0.0

        return {
            'total_requests': total,
            'successful': successful,
            'failed': failed,
            'success_rate': success_rate,
            'duration_seconds': time.time() - self._request_stats['start_time']
        }

    def logout(self):
        """
        退出登录/释放资源（可选）
        """
        logger.info("👋 数据采集器退出")
        pass

    # ==================== 工具方法 ====================

    def _format_date_to_standard(self, date_series) -> pd.Series:
        """
        将日期列统一转为 YYYYMMDD 格式（字符串）
        子类可复用
        """
        return pd.to_datetime(date_series).dt.strftime('%Y%m%d')

    def enforce_rate_limit(self, requests_per_minute: int = 500):
        """
        执行速率限制，防止API调用过于频繁

        Args:
            requests_per_minute: 每分钟最大请求数
        """
        current_time = time.time()

        if self.rate_limit_time is None:
            self.rate_limit_time = current_time
            self.rate_limit_count = 1
            return

        # 如果超过1分钟，重置计数器
        if current_time - self.rate_limit_time > 60:
            self.rate_limit_time = current_time
            self.rate_limit_count = 1
            return

        # 检查是否超过限制
        if self.rate_limit_count >= requests_per_minute:
            sleep_time = 60 - (current_time - self.rate_limit_time)
            if sleep_time > 0:
                logger.info(f"⚠️ 达到速率限制，等待 {sleep_time:.1f} 秒")
                time.sleep(sleep_time)
                self.rate_limit_time = time.time()
                self.rate_limit_count = 1
        else:
            self.rate_limit_count += 1

    def convert_to_dataframe(self, data: List[Dict], columns: List[str]) -> pd.DataFrame:
        """
        将数据列表转换为DataFrame

        Args:
            data: 数据字典列表
            columns: 需要保留的列名列表

        Returns:
            DataFrame
        """
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if set(columns).issubset(set(df.columns)):
            return df[columns]
        return df

    def validate_date_format(self, date_str: str) -> bool:
        """
        验证日期格式是否为YYYYMMDD

        Args:
            date_str: 日期字符串

        Returns:
            bool: 是否有效
        """
        pattern = r'^\d{8}$'
        if not re.match(pattern, date_str):
            return False

        try:
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            # 基本验证
            if not (1900 <= year <= 2100):
                return False
            if not (1 <= month <= 12):
                return False
            if not (1 <= day <= 31):
                return False

            # 更精确的日期验证
            from datetime import datetime
            datetime.strptime(date_str, '%Y%m%d')
            return True
        except:
            return False

    def standardize_symbol(self, symbol: str) -> str:
        """
        标准化股票代码

        Args:
            symbol: 股票代码（如 600519, sh600519, 000001.SZ等）

        Returns:
            str: 标准化格式（如 sh600519）
        """
        # 移除空格和换行符
        symbol = symbol.strip().upper()

        # 如果已经是标准格式，直接返回
        if symbol.startswith(('SH', 'SZ', 'BJ')):
            return symbol.lower()

        # 处理带后缀的格式
        if symbol.endswith(('.SH', '.SZ', '.BJ')):
            prefix = symbol[-3:].lower().replace('.', '')
            code = symbol[:-3]
            return f"{prefix}{code}"

        # 根据代码判断市场
        if symbol.startswith(('6', '9', '5')):
            return f"sh{symbol}"
        elif symbol.startswith(('0', '3', '2')):
            return f"sz{symbol}"
        elif symbol.startswith(('4', '8')):
            return f"bj{symbol}"
        else:
            logger.warning(f"⚠️ 无法识别股票代码格式: {symbol}")
            return symbol


# 在 data_collector.py 末尾添加
def get_data_collector(collector_type: str = 'baostock', config_path: str = 'config/database.yaml'):
    """
    获取数据采集器实例（工厂函数）

    Args:
        collector_type: 采集器类型 ('baostock', 'tushare', 'akshare')
        config_path: 配置文件路径

    Returns:
        数据采集器实例
    """
    if collector_type.lower() == 'baostock':
        from src.data.baostock_collector import BaostockCollector
        return BaostockCollector(config_path)
    elif collector_type.lower() == 'tushare':
        from src.data.tushare_collector import TushareDataCollector
        return TushareDataCollector(config_path)
        # raise NotImplementedError("Tushare采集器暂未实现")
    elif collector_type.lower() == 'akshare':
        from src.data.akshare_collector import AKShareCollector
        return AKShareCollector(config_path)
        # raise NotImplementedError("AKShare采集器暂未实现")
    else:
        raise ValueError(f"不支持的数据采集器类型: {collector_type}")


# 修改之前的 DataCollector 别名，使其指向工厂函数或基类
# 保持兼容性，但不直接实例化
DataCollector = BaseDataCollector  # 仍作为基类引用
