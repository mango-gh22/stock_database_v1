# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_adjustment_factor_downloader.py
# File Name: baostock_adjustment_factor_downloader
# @ Author: mango-gh22
# @ Date：2026/1/2 18:44
"""
desc Baostock复权因子单线程下载器 - P6阶段最终版
使用 query_adjust_factor 接口（非 query_dividend_data）
"""

import baostock as bs
import pandas as pd
import numpy as np
import time
import random
import logging
from datetime import datetime
from typing import List, Optional, Dict, Tuple
import threading
from pathlib import Path

from src.utils.code_converter import normalize_stock_code
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaostockAdjustmentFactorDownloader:
    """Baostock复权因子单线程下载器 - 生产稳定版"""

    def __init__(self, config_path: str = 'config/adjustment_factor_config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.min_request_interval = 1.5
        self.last_request_time = None
        self._login_baostock()

        self.cache_dir = Path('data/cache/baostock/adjustment_factors')
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.download_stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'last_download': None
        }

        self._download_lock = threading.Lock()

    def _load_config(self) -> Dict:
        """加载配置"""
        try:
            from src.config.config_loader import ConfigLoader
            loader = ConfigLoader()
            return loader.load_yaml_config(self.config_path)
        except Exception as e:
            logger.warning(f"加载复权因子配置失败 {self.config_path}: {e}，使用默认配置")
            return {
                'baostock': {
                    'fields': [
                        'code', 'divOperateDate', 'foreAdjustFactor',
                        'backAdjustFactor', 'adjustFactor'
                    ],
                    'max_retries': 3,
                    'retry_delay_base': 3,
                    'min_request_interval': 1.5,
                    'enable_cache': True
                }
            }

    def _login_baostock(self):
        """强制干净登录"""
        try:
            bs.logout()
        except:
            pass
        self.lg = bs.login()
        if self.lg.error_code != '0':
            logger.error(f"❌ Baostock登录失败: {self.lg.error_msg}")
            raise ConnectionError("Baostock login failed for adjustment factor downloader")
        logger.info("✅ Baostock复权因子下载器登录成功")

    def _ensure_logged_in(self):
        """确保登录状态"""
        if not self.lg or self.lg.error_code != '0':
            self._login_baostock()

    def _convert_to_bs_code(self, symbol: str) -> str:
        """转换为Baostock格式"""
        normalized_code = normalize_stock_code(symbol)
        market = normalized_code[:2]
        code_num = normalized_code[2:]
        return f"{market}.{code_num}"

    def _is_valid_stock(self, bs_code: str) -> bool:
        """验证股票代码"""
        if not bs_code or '.' not in bs_code:
            return False

        market, code = bs_code.split('.')

        if market == 'sh':
            return code.startswith(('6', '9')) and not code.startswith(('000', '950', '951'))
        elif market == 'sz':
            return code.startswith(('00', '30')) and not code.startswith('399')
        elif market == 'bj':
            return code.startswith(('43', '83', '87', '88'))
        return False

    def _enforce_rate_limit(self):
        """强制执行请求速率限制"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_request_interval:
                sleep_time = self.min_request_interval - elapsed + random.uniform(0, 0.5)
                time.sleep(sleep_time)
        self.last_request_time = time.time()

    def fetch_adjustment_factor_data(self, symbol: str, start_date: str = None,
                                     end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取复权因子数据（使用正确的Baostock接口）

        Args:
            symbol: 股票代码（标准化格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            复权因子DataFrame
        """
        with self._download_lock:
            return self._fetch_adjustment_data_sync(symbol, start_date, end_date)

    def _fetch_adjustment_data_sync(self, symbol: str, start_date: str = None,
                                    end_date: str = None) -> Optional[pd.DataFrame]:
        """同步获取复权因子数据（内部实现）"""
        self._ensure_logged_in()

        bs_code = self._convert_to_bs_code(symbol)
        if not self._is_valid_stock(bs_code):
            logger.warning(f"⚠️ 跳过非股票代码: {bs_code}")
            return pd.DataFrame()

        # 日期范围处理
        if not start_date:
            start_date = "2016-01-01"
        else:
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"

        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        else:
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        logger.info(f"📥 获取复权因子: {bs_code} [{start_date} - {end_date}]")

        max_retries = self.config.get('baostock', {}).get('max_retries', 3)
        retry_delay_base = self.config.get('baostock', {}).get('retry_delay_base', 3)

        for attempt in range(max_retries):
            try:
                self._enforce_rate_limit()

                if attempt > 0:
                    self._login_baostock()

                # ✅ 使用正确的接口：query_adjust_factor
                rs = bs.query_adjust_factor(
                    code=bs_code,
                    start_date=start_date,
                    end_date=end_date
                )

                if rs.error_code != '0':
                    raise RuntimeError(f"Baostock error: {rs.error_msg}")

                if not rs:
                    logger.warning(f"⚠️ 查询返回None: {bs_code}")
                    return pd.DataFrame()

                # 安全读取数据
                data_list = []
                row_count = 0
                max_rows = 10000

                while rs.next():
                    try:
                        row_data = rs.get_row_data()
                        if row_data:
                            data_list.append(row_data)
                            row_count += 1
                            if row_count >= max_rows:
                                logger.warning(f"达到最大行数限制 {max_rows}")
                                break
                    except Exception as row_e:
                        error_msg = str(row_e).lower()
                        if any(kw in error_msg for kw in ['utf-8', 'codec', 'decompress', 'invalid']):
                            logger.warning(f"跳过损坏行 {row_count}: {row_e}")
                            continue
                        else:
                            raise

                if not data_list:
                    logger.warning(f"⚠️ 未获取到复权因子数据: {bs_code}")
                    return pd.DataFrame()

                # 转换为DataFrame
                df = pd.DataFrame(data_list, columns=rs.fields)

                # 添加标准化代码
                df['symbol'] = normalize_stock_code(symbol)

                # 处理日期字段
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

                # 转换因子为数值类型
                factor_cols = ['foreAdjustFactor', 'backAdjustFactor', 'adjustFactor']
                for col in factor_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # 重命名列以匹配数据库（支持多种列名变体）
                column_mapping = {
                    'code': 'bs_code',
                    'foreAdjustFactor': 'forward_factor',
                    'backAdjustFactor': 'backward_factor',
                    'adjustFactor': 'total_factor'
                }

                # 除权日期列可能有不同名称
                if 'divOperateDate' in df.columns:
                    column_mapping['divOperateDate'] = 'ex_date'
                elif 'dividOperateDate' in df.columns:
                    column_mapping['dividOperateDate'] = 'ex_date'
                else:
                    logger.error(f"无法找到除权日期列，可用列: {list(df.columns)}")
                    return pd.DataFrame()

                df = df.rename(columns=column_mapping)

                # 强制转换ex_date为日期类型
                if 'ex_date' in df.columns:
                    df['ex_date'] = pd.to_datetime(df['ex_date'], errors='coerce')
                    if df['ex_date'].isna().all():
                        logger.error("ex_date列全为NaT，请检查数据源")
                        return pd.DataFrame()




                # 添加其他必需字段（默认值）
                if 'cash_div' not in df.columns:
                    df['cash_div'] = 0.0
                if 'shares_div' not in df.columns:
                    df['shares_div'] = 0.0
                if 'allotment_ratio' not in df.columns:
                    df['allotment_ratio'] = 0.0
                if 'allotment_price' not in df.columns:
                    df['allotment_price'] = 0.0
                if 'split_ratio' not in df.columns:
                    df['split_ratio'] = 1.0

                # 更新统计
                self.download_stats['successful'] += 1
                self.download_stats['last_download'] = datetime.now()

                logger.info(f"✅ 获取复权因子成功: {bs_code}, {len(df)} 条记录")
                return df

            except Exception as e:
                err_str = str(e).lower()
                wait_sec = retry_delay_base + attempt * 2 + random.uniform(0, 1)

                if any(kw in err_str for kw in ['utf', 'codec', 'decompress', 'invalid']):
                    wait_sec *= 2

                logger.warning(
                    f"⚠️ 尝试 {attempt + 1}/{max_retries} 失败 ({type(e).__name__}): {str(e)[:80]} → 等待 {wait_sec:.1f}s")
                time.sleep(wait_sec)

        logger.error(f"❌ {symbol} 所有重试失败")
        self.download_stats['failed'] += 1
        return pd.DataFrame()

    def download_batch(self, symbols: List[str], start_date: str = None,
                       end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        单线程批量下载复权因子数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            字典 {symbol: DataFrame}
        """
        results = {}
        total = len(symbols)

        logger.info(f"🚀 开始单线程批量下载复权因子: {total} 只股票")

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"[{i}/{total}] 处理 {symbol}")

            df = self.fetch_adjustment_factor_data(symbol, start_date, end_date)
            if df is not None and not df.empty:
                results[symbol] = df
            else:
                logger.warning(f"⚠️ {symbol} 无有效复权因子数据")

            if i < total:
                sleep_time = self.min_request_interval + random.uniform(0, 1.0)
                time.sleep(sleep_time)

        logger.info(f"📊 完成: 成功 {len(results)} / {total} 只股票")
        return results

    def get_download_stats(self) -> Dict:
        """获取下载统计"""
        stats = self.download_stats.copy()
        total = stats['successful'] + stats['failed']
        stats['total_requests'] = total
        stats['success_rate'] = (stats['successful'] / total * 100) if total > 0 else 0
        return stats

    def logout(self):
        """退出登录"""
        if self.lg:
            bs.logout()
            logger.info("🔒 Baostock复权因子下载器已退出登录")
            self.lg = None

    def __del__(self):
        try:
            self.logout()
        except:
            pass


def test_adjustment_factor_downloader():
    """测试下载器"""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

    print("🧪 测试Baostock复权因子下载器")
    print("=" * 60)

    downloader = BaostockAdjustmentFactorDownloader()

    if not downloader.lg:
        print("❌ Baostock登录失败")
        return

    test_symbols = ['sh603993', 'sz000002', 'sh600900']

    for symbol in test_symbols:
        print(f"\n📊 测试股票: {symbol}")

        df = downloader.fetch_adjustment_factor_data(symbol)

        if not df.empty:
            print(f"  ✅ 下载成功: {len(df)} 条")
            print(f"  📅 日期范围: {df['ex_date'].min()} - {df['ex_date'].max()}")
            print(f"  📊 前复权因子范围: {df['forward_factor'].min():.6f} - {df['forward_factor'].max():.6f}")
            print(df.head())
        else:
            print(f"  ⚠️ 无数据")

    downloader.logout()
    print("\n✅ 复权因子下载器测试完成")


if __name__ == "__main__":
    test_adjustment_factor_downloader()