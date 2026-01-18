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
import numpy as np
import time
import random
import logging
from typing import List, Optional, Dict
from datetime import datetime

from src.utils.code_converter import normalize_stock_code

logger = logging.getLogger(__name__)


class BaostockDailyDownloader:
    """专用于日线采集的稳定 Baostock 下载器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
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
        """排除指数"""
        market, num = bs_code.split('.')
        if market == 'sh':
            return num.startswith(('6', '9')) and not num.startswith(('000', '95'))
        elif market == 'sz':
            return num.startswith(('00', '30')) and not num.startswith('399')
        return False

    def _convert_date_format(self, date_str: str) -> str:
        """
        转换日期格式为 YYYYMMDD (20250101)
        支持多种输入格式：YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD
        """
        if not date_str:
            return date_str

        try:
            # 清理分隔符
            date_str = str(date_str).strip()

            # 如果已经是8位数字，直接返回
            if date_str.isdigit() and len(date_str) == 8:
                return date_str

            # 尝试解析常见格式
            from datetime import datetime
            formats_to_try = [
                '%Y-%m-%d',  # 2025-12-01
                '%Y/%m/%d',  # 2025/12/01
                '%Y.%m.%d',  # 2025.12.01
                '%Y%m%d',  # 20251201
            ]

            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y%m%d')
                except:
                    continue

            # 如果都失败，尝试简单清理
            cleaned = date_str.replace('-', '').replace('/', '').replace('.', '').replace(' ', '')
            if len(cleaned) >= 8:
                return cleaned[:8]

            raise ValueError(f"无法解析日期格式: {date_str}")

        except Exception as e:
            logger.warning(f"日期格式转换失败 {date_str}: {e}")
            return date_str

    def fetch_single_stock(
            self,
            symbol: str,
            start_date: str,
            end_date: str,
            max_retries: int = 3
    ) -> Optional[pd.DataFrame]:
        """
        稳定下载单只股票日线数据（核心方法）- v0.7.0 修复版

        Args:
            symbol: 股票代码，支持多种格式 (600519, sh600519, 600519.SH)
            start_date: 开始日期，格式 YYYYMMDD (20250101)
            end_date: 结束日期，格式 YYYYMMDD (20250101)
            max_retries: 最大重试次数

        Returns:
            DataFrame 或 None
        """
        # 标准化日期格式
        start_date_fmt = self._convert_date_format(start_date)
        end_date_fmt = self._convert_date_format(end_date)

        bs_code = self._convert_to_bs_code(symbol)
        if not self._is_valid_stock(bs_code):
            logger.warning(f"⚠️ 跳过非股票代码: {bs_code}")
            return None

        # 转换为 Baostock 需要的格式
        start_fmt = f"{start_date_fmt[:4]}-{start_date_fmt[4:6]}-{start_date_fmt[6:8]}"
        end_fmt = f"{end_date_fmt[:4]}-{end_date_fmt[4:6]}-{end_date_fmt[6:8]}"

        for attempt in range(max_retries):
            # 每次尝试都重新登录（关键！）
            self._login_baostock()

            try:
                logger.debug(f"📥 请求 {bs_code} [{start_fmt} ~ {end_fmt}] (尝试 {attempt + 1})")

                # ✅ 明确指定所有字段（含因子字段）
                fields = "date,code,open,high,low,close,preclose,volume,amount,pctChg,turn,tradestatus,peTTM,pbMRQ,psTTM,pcfNcfTTM"

                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=fields,
                    start_date=start_fmt,
                    end_date=end_fmt,
                    frequency="d",
                    adjustflag="3"
                )

                if rs.error_code != '0':
                    raise RuntimeError(f"Baostock error: {rs.error_msg}")

                # 安全读取数据
                data_list = []
                while rs.next():
                    try:
                        data_list.append(rs.get_row_data())
                    except Exception as row_e:
                        logger.warning(f"⚠️ 跳过损坏行: {row_e}")
                        continue

                # ✅ 强化空数据判断
                if not data_list or len(data_list) == 0:
                    logger.warning(f"⚠️ {symbol} 无返回数据（数据长度为0）")
                    return None

                # ✅ 创建 DataFrame 并验证完整性
                df = pd.DataFrame(data_list, columns=rs.fields)

                if df.empty or len(df.columns) == 0:
                    logger.warning(f"⚠️ {symbol} DataFrame为空或无效")
                    return None

                # ✅ 关键验证：确保必需的 'date' 列存在
                if 'date' not in df.columns:
                    logger.error(f"❌ Baostock返回数据缺少 'date' 列，可用列: {list(df.columns)}")
                    logger.error(f"❌ 请求字段: {fields}")
                    return None

                # ✅ 列重命名（统一定义）
                rename_dict = {
                    'date': 'trade_date',
                    'code': 'bs_code',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                    'close': 'close_price',
                    'preclose': 'pre_close_price',
                    'volume': 'volume',
                    'amount': 'amount',
                    'pctChg': 'change_percent',
                    'turn': 'turnover_rate_f',
                    'tradestatus': 'trade_status',
                    # ✅ 新增因子字段映射（确保与 Baostock 字段一致）
                    'peTTM': 'pe_ttm',
                    'pbMRQ': 'pb',
                    'psTTM': 'ps_ttm',
                    'pcfNcfTTM': 'pcf_ttm',
                }

                # 只重命名存在的列，避免 KeyError
                actual_rename = {k: v for k, v in rename_dict.items() if k in df.columns}
                df = df.rename(columns=actual_rename)
                logger.debug(f"✅ 列重命名完成: {len(actual_rename)} 个字段被映射")

                # ✅ 强制验证：重命名后检查必需列
                required_cols = ['trade_date', 'close_price']
                for col in required_cols:
                    if col not in df.columns:
                        logger.error(f"❌ 必需列 '{col}' 不存在，当前列: {list(df.columns)}")
                        return None

                # ✅ 生成标准化股票代码（兼容多种情况）
                if 'bs_code' in df.columns:
                    df['symbol'] = df['bs_code'].apply(lambda x: normalize_stock_code(str(x)))
                    logger.debug(f"从 bs_code 生成标准化 symbol")
                elif 'code' in df.columns:
                    df['symbol'] = df['code'].apply(lambda x: normalize_stock_code(str(x)))
                    logger.debug(f"从 code 生成标准化 symbol")
                else:
                    df['symbol'] = normalize_stock_code(symbol)
                    logger.debug(f"使用参数 symbol: {symbol}")

                # ✅ 转换日期格式（YYYYMMDD -> YYYY-MM-DD）
                if 'trade_date' in df.columns:
                    # 确保是字符串
                    df['trade_date'] = df['trade_date'].astype(str)
                    # 移除分隔符
                    df['trade_date'] = df['trade_date'].str.replace('-', '')
                    # 转换为标准格式
                    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d', errors='coerce')
                    df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')

                # ✅ 数值列转换（包含因子字段）
                numeric_cols = [
                    'open_price', 'high_price', 'low_price', 'close_price',
                    'pre_close_price', 'volume', 'amount', 'change_percent',
                    'turnover_rate_f', 'pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm'
                ]

                for col in numeric_cols:
                    if col in df.columns:
                        # 先清理可能存在的 "--" 或空字符串
                        df[col] = df[col].replace(['--', '', 'None'], np.nan)
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        logger.debug(f"{col} 转换后 NaN 数量: {df[col].isna().sum()}")

                        # ✅ 因子字段特殊处理：如果全为NaN，尝试从其他字段获取
                        if col in ['pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm'] and df[col].isna().all():
                            # 检查是否有同义字段
                            synonym_map = {
                                'pe_ttm': ['peTTM', 'pe'],
                                'pb': ['pbMRQ', 'pb'],
                                'ps_ttm': ['psTTM', 'ps'],
                                'pcf_ttm': ['pcfNcfTTM', 'pcf']
                            }

                            for syn in synonym_map.get(col, []):
                                if syn in df.columns:
                                    df[col] = pd.to_numeric(df[syn], errors='coerce')
                                    logger.info(f"从同义字段 {syn} 恢复 {col}: {df[col].notna().sum()} 条")
                                    break


                # ✅ 删除无效行（symbol 或 trade_date 为空）
                before_filter = len(df)
                df = df.dropna(subset=['symbol', 'trade_date'], how='any')
                after_filter = len(df)
                if before_filter > after_filter:
                    logger.debug(f"过滤掉 {before_filter - after_filter} 条无效行")

                logger.info(f"✅ {symbol}: 获取 {len(df)} 条记录")
                return df

            except Exception as e:
                err_str = str(e).lower()
                wait_sec = 3 + attempt * 2 + random.uniform(0, 1)

                # 对特定错误类型增加等待时间
                if any(kw in err_str for kw in ['utf', 'codec', 'decompress', 'invalid', 'timeout']):
                    wait_sec *= 2

                logger.warning(
                    f"⚠️ 尝试 {attempt + 1}/{max_retries} 失败 ({type(e).__name__}): {str(e)[:100]} → 等待 {wait_sec:.1f}s")

                if attempt < max_retries - 1:
                    time.sleep(wait_sec)
                else:
                    logger.error(f"❌ {symbol} 所有重试均失败")
                    return None

        logger.error(f"❌ {symbol} 超出最大重试次数")
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

        # 标准化日期格式
        start_date_fmt = self._convert_date_format(start_date)
        end_date_fmt = self._convert_date_format(end_date)

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"[{i}/{total}] 处理 {symbol}")
            df = self.fetch_single_stock(symbol, start_date_fmt, end_date_fmt)

            if df is not None and not df.empty:
                results[symbol] = df
            else:
                logger.warning(f"⚠️ {symbol} 无有效数据")

            # 请求间隔
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