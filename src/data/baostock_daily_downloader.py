# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_daily_downloader.py
# File Name: baostock_daily_downloader
# @ Author: mango-gh22
# @ Date：2025/12/27 16:46
"""
desc Baostock 日线数据下载器 - 生产稳定版
融合 E:\MyFile\ice\src\core\data_loader_daily.py 的稳健策略 + 本项目架构规范
"""

import baostock as bs
import pandas as pd
import time
import random
import logging
from typing import List, Optional, Dict
from datetime import datetime

# from src.data.data_collector import BaseDataCollector
from src.utils.code_converter import normalize_stock_code

logger = logging.getLogger(__name__)


# class BaostockDailyDownloader(BaseDataCollector):
class BaostockDailyDownloader:
    """专用于日线采集的稳定 Baostock 下载器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        # 如果你需要读取 database.yaml 中的某些参数（如存储路径），可以保留
        # 否则可简化为无参构造
        self.config_path = config_path
        self._login_baostock()

    def _login_baostock(self):
        """强制干净登录"""
        try:
            bs.logout()
        except:
            pass
        self.lg = bs.login()
        if self.lg.error_code != '0':
            logger.error(f"❌ Baostock 登录失败: {self.lg.error_msg}")
            raise ConnectionError("Baostock login failed")

    def _convert_to_bs_code(self, symbol: str) -> str:
        """标准化为 sh.600000 格式"""
        norm = normalize_stock_code(symbol)
        market = norm[:2]
        code = norm[2:]
        return f"{market}.{code}"

    def _is_valid_stock(self, bs_code: str) -> bool:
        """排除指数（与你原有逻辑一致）"""
        market, num = bs_code.split('.')
        if market == 'sh':
            return num.startswith(('6', '9')) and not num.startswith(('000', '95'))
        elif market == 'sz':
            return num.startswith(('00', '30')) and not num.startswith('399')
        return False

    def fetch_single_stock(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        max_retries: int = 3
    ) -> Optional[pd.DataFrame]:
        """
        稳定下载单只股票日线（核心方法）
        """
        bs_code = self._convert_to_bs_code(symbol)
        if not self._is_valid_stock(bs_code):
            logger.warning(f"⚠️ 跳过非股票代码: {bs_code}")
            return None

        # 格式化日期为 YYYY-MM-DD
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        for attempt in range(max_retries):
            # 每次尝试都重新登录（关键！）
            self._login_baostock()

            try:
                logger.debug(f"📥 请求 {bs_code} [{start_fmt} ~ {end_fmt}] (尝试 {attempt+1})")
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields="date,code,open,high,low,close,preclose,volume,amount,pctChg,turn,tradestatus",
                    start_date=start_fmt,
                    end_date=end_fmt,
                    frequency="d",
                    adjustflag="3"
                )

                if rs.error_code != '0':
                    raise RuntimeError(f"Baostock error: {rs.error_msg}")

                # 安全读取数据（防解压崩溃）
                data_list = []
                while rs.next():
                    try:
                        data_list.append(rs.get_row_data())
                    except Exception as row_e:
                        logger.warning(f"⚠️ 跳过损坏行: {row_e}")
                        continue

                if not data_list:
                    logger.warning(f"⚠️ {symbol} 无返回数据")
                    return None

                df = pd.DataFrame(data_list, columns=rs.fields)

                # 列重命名（匹配本项目）
                df.rename(columns={
                    'date': 'trade_date',
                    'code': 'bs_code',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                    'close': 'close_price',
                    'preclose': 'pre_close_price',
                    'volume': 'volume',
                    'amount': 'amount',
                    'pctChg': 'pct_change',
                    'turn': 'turnover_rate',
                    'tradestatus': 'trade_status'
                }, inplace=True)

                df['symbol'] = normalize_stock_code(symbol)

                # 日期转为 YYYYMMDD（本项目标准）
                df['trade_date'] = df['trade_date'].str.replace('-', '')

                # 数值列转换
                num_cols = ['open_price', 'high_price', 'low_price', 'close_price',
                           'pre_close_price', 'volume', 'amount', 'pct_change', 'turnover_rate']
                for col in num_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                logger.info(f"✅ {symbol}: 获取 {len(df)} 条记录")
                return df

            except Exception as e:
                err_str = str(e).lower()
                wait_sec = 3 + attempt * 2 + random.uniform(0, 1)
                # 对编码/解压错误加倍等待
                if any(kw in err_str for kw in ['utf', 'codec', 'decompress', 'invalid']):
                    wait_sec *= 2

                logger.warning(f"⚠️ 尝试 {attempt+1} 失败 ({type(e).__name__}): {str(e)[:80]} → 等待 {wait_sec:.1f}s")
                time.sleep(wait_sec)

        logger.error(f"❌ {symbol} 所有重试失败")
        return None

    def download_batch(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """
        单线程批量下载（确保稳定）
        """
        results = {}
        total = len(symbols)
        logger.info(f"🚀 开始单线程下载 {total} 只股票: {start_date} ~ {end_date}")

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"[{i}/{total}] 处理 {symbol}")
            df = self.fetch_single_stock(symbol, start_date, end_date)
            if df is not None and not df.empty:
                results[symbol] = df
            else:
                logger.warning(f"⚠️ {symbol} 无有效数据")

            # 关键节流：每只股票间隔 1.5~2.5 秒
            if i < total:
                sleep_time = 1.5 + random.uniform(0, 1.0)
                time.sleep(sleep_time)

        logger.info(f"📊 完成: 成功 {len(results)} / {total} 只股票")
        return results

    def __del__(self):
        try:
            bs.logout()
        except:
            pass