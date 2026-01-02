# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_collector.py
# File Name: baostock_collector
# @ Author: mango-gh22
# @ Date：2025/12/7 22:48
"""
desc
Baostock数据采集器 - 修复完善版
解决日期格式和股票代码过滤问题
"""

import baostock as bs
import pandas as pd
import numpy as np
import random
from typing import Dict, List, Optional, Any, Tuple
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import concurrent.futures

from src.data.data_collector import BaseDataCollector
from src.utils.code_converter import normalize_stock_code

logger = logging.getLogger(__name__)


class BaostockCollector(BaseDataCollector):
    """Baostock数据采集器 - 修复完善版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        super().__init__(config_path)
        self.lg = None  # Baostock登录对象
        self._login_baostock()

        # 在 BaostockCollector.__init__ 中添加
        self.min_request_interval = 0.5  # 每次请求最小间隔2秒-->0.5
        self.last_request_time = None


        # 缓存目录
        self.cache_dir = Path('data/cache/baostock')
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 下载统计
        self.download_stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'last_download': None
        }

    def _login_baostock(self):
        """登录Baostock"""
        try:
            # 先尝试退出（清理旧连接）
            try:
                bs.logout()
            except:
                pass

            self.lg = bs.login()
            if self.lg.error_code == '0':
                logger.info("✅ Baostock登录成功")
            else:
                logger.error(f"❌ Baostock登录失败: {self.lg.error_msg}")
                self.lg = None
        except Exception as e:
            logger.error(f"❌ Baostock登录异常: {e}")
            self.lg = None

    def _ensure_logged_in(self) -> bool:
        """确保已登录"""
        if self.lg is None:
            self._login_baostock()
        return self.lg is not None

    def _format_date_for_baostock(self, date_str: str) -> str:
        """
        格式化日期为Baostock需要的格式: YYYY-MM-DD

        支持输入格式:
        - YYYYMMDD
        - YYYY-MM-DD
        - YYYY/MM/DD
        """
        try:
            if not date_str or len(date_str) < 8:
                return date_str

            # 移除所有分隔符
            clean_date = date_str.replace('-', '').replace('/', '').replace('.', '')

            if len(clean_date) == 8 and clean_date.isdigit():
                # 格式化为 YYYY-MM-DD
                return f"{clean_date[0:4]}-{clean_date[4:6]}-{clean_date[6:8]}"
            else:
                logger.warning(f"⚠️ 日期格式异常: {date_str}")
                return date_str
        except Exception as e:
            logger.error(f"❌ 日期格式化失败 {date_str}: {e}")
            return date_str

    def _convert_to_bs_code(self, normalized_code: str) -> str:
        """
        将标准化代码转换为Baostock格式

        规则:
        - 股票: sh.600000, sz.000001
        - 指数: sh.000001, sz.399001

        Args:
            normalized_code: 标准化代码，如 sh600519, sz000001

        Returns:
            Baostock格式代码，如 sh.600519, sz.000001
        """
        try:
            if not normalized_code:
                return ""

            # 确保已经是标准化格式
            if not (normalized_code.startswith(('sh', 'sz', 'bj')) and len(normalized_code) >= 8):
                normalized_code = normalize_stock_code(normalized_code)

            if normalized_code.startswith('sh'):
                number_part = normalized_code[2:]
                return f"sh.{number_part}"
            elif normalized_code.startswith('sz'):
                number_part = normalized_code[2:]
                return f"sz.{number_part}"
            elif normalized_code.startswith('bj'):
                number_part = normalized_code[2:]
                return f"bj.{number_part}"
            else:
                return normalized_code
        except Exception as e:
            logger.error(f"❌ 代码转换失败 {normalized_code}: {e}")
            return normalized_code

    def _is_stock_code(self, bs_code: str) -> bool:
        """
        判断是否为股票代码（排除指数）

        规则:
        - 上证股票: sh.6xxxxx, sh.688xxx (科创板)
        - 上证指数: sh.000xxx, sh.950xxx (基金指数)
        - 深证股票: sz.00xxxx, sz.30xxxx (创业板)
        - 深证指数: sz.399xxx
        """
        try:
            if not bs_code or '.' not in bs_code:
                return False

            market, code = bs_code.split('.')

            if market == 'sh':
                # 上证股票以 6 或 9 开头（排除 000, 950, 951 等指数代码）
                return code.startswith(('6', '9')) and not code.startswith(('000', '950', '951'))
            elif market == 'sz':
                # 深证股票以 00, 30 开头（排除 399 指数代码）
                return code.startswith(('00', '30')) and not code.startswith('399')
            elif market == 'bj':
                # 北交所股票以 43, 83, 87, 88 开头
                return code.startswith(('43', '83', '87', '88'))
            else:
                return False
        except:
            return False

    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取日线数据 - 修复版

        Args:
            symbol: 股票代码，支持多种格式
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            日线数据DataFrame
        """

        # 在 fetch_daily_data 方法开头添加
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_request_interval:
                time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()


        if not self._ensure_logged_in():
            logger.error("❌ Baostock未登录，无法获取数据")
            return None

        try:
            # 标准化股票代码
            normalized_code = normalize_stock_code(symbol)
            bs_code = self._convert_to_bs_code(normalized_code)

            # 检查是否为股票代码
            if not self._is_stock_code(bs_code):
                logger.warning(f"⚠️ {bs_code} 可能是指数代码，跳过")
                return pd.DataFrame()

            # 格式化日期
            formatted_start = self._format_date_for_baostock(start_date)
            formatted_end = self._format_date_for_baostock(end_date)

            logger.info(f"📥 获取日线数据: {bs_code} [{formatted_start} - {formatted_end}]")

            # 设置重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # 查询日线数据
                    rs = bs.query_history_k_data_plus(
                        code=bs_code,
                        fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg",
                        start_date=formatted_start,
                        end_date=formatted_end,
                        frequency="d",
                        adjustflag="3"  # 复权类型：3=不复权；1后复权，2前复权，后可通过复权因子则需
                    )

                    if rs is None:
                        logger.warning(f"⚠️ 查询返回None (尝试{attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        else:
                            return pd.DataFrame()

                    if rs.error_code != '0':
                        logger.warning(f"⚠️ Baostock查询失败 (尝试{attempt + 1}/{max_retries}): {rs.error_msg}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        else:
                            return pd.DataFrame()

                    # 获取数据
                    # data_list = []
                    # while (rs.error_code == '0') & rs.next():
                    #     data_list.append(rs.get_row_data())

                    # 为：
                    data_list = self._safe_fetch_with_retry(rs)

                    if not data_list:
                        logger.warning(f"⚠️ 未获取到数据: {bs_code}")
                        return pd.DataFrame()

                    # 转换为DataFrame
                    df = pd.DataFrame(data_list, columns=rs.fields)

                    # 重命名列以匹配数据库
                    column_mapping = {
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
                        'adjustflag': 'adjust_flag',
                        'tradestatus': 'trade_status'
                    }

                    df = df.rename(columns=column_mapping)
                    df['symbol'] = normalized_code

                    # 转换数据类型
                    numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price',
                                    'pre_close_price', 'volume', 'amount', 'pct_change',
                                    'turnover_rate']

                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                    # 计算涨跌额
                    if 'close_price' in df.columns and 'pre_close_price' in df.columns:
                        df['change_amount'] = df['close_price'] - df['pre_close_price']

                    # 计算振幅
                    if 'high_price' in df.columns and 'low_price' in df.columns and 'pre_close_price' in df.columns:
                        df['amplitude'] = ((df['high_price'] - df['low_price']) / df['pre_close_price']) * 100

                    # 转换日期格式为 YYYYMMDD----问题，格式？
                    if 'trade_date' in df.columns:
                        # df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
                        df['trade_date'] = self._format_date_to_standard(df['trade_date'])

                    # 记录成功
                    self.download_stats['successful'] += 1
                    self.download_stats['last_download'] = datetime.now()

                    logger.info(f"✅ 获取日线数据成功: {bs_code}, {len(df)} 条记录")
                    return df

                except Exception as e:
                    logger.warning(f"⚠️ 获取数据异常 (尝试{attempt + 1}/{max_retries}): {str(e)[:100]}")
                    if attempt < max_retries - 1:
                        time.sleep(3)
                    else:
                        logger.error(f"❌ 所有重试均失败: {symbol}")
                        self.download_stats['failed'] += 1
                        return pd.DataFrame()

            self.download_stats['total_requests'] += 1
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ 获取日线数据失败 {symbol}: {e}")
            self.download_stats['failed'] += 1
            return pd.DataFrame()


    def fetch_minute_data(self, symbol: str, trade_date: str, freq: str = '5') -> Optional[pd.DataFrame]:
        """获取分钟线数据"""
        if not self._ensure_logged_in():
            return None

        try:
            normalized_code = normalize_stock_code(symbol)
            bs_code = self._convert_to_bs_code(normalized_code)

            # 格式化日期
            formatted_date = self._format_date_for_baostock(trade_date)

            logger.info(f"📥 获取分钟数据: {bs_code} {formatted_date} {freq}分钟")

            # Baostock分钟数据频率：5=5分钟，15=15分钟，30=30分钟，60=60分钟
            freq_map = {
                '1min': '1',
                '5min': '5',
                '15min': '15',
                '30min': '30',
                '60min': '60'
            }

            baostock_freq = freq_map.get(freq, '5')

            rs = bs.query_history_k_data_plus(
                code=bs_code,
                fields="time,code,open,high,low,close,volume,amount",
                start_date=formatted_date,
                end_date=formatted_date,
                frequency=baostock_freq,
                adjustflag="3"
            )

            if rs is None or rs.error_code != '0':
                logger.warning(f"⚠️ 获取分钟数据失败: {rs.error_msg if rs else '返回None'}")
                return pd.DataFrame()

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"⚠️ 未获取到分钟数据: {bs_code} {trade_date}")
                return pd.DataFrame()

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 重命名列
            df = df.rename(columns={
                'time': 'trade_time',
                'code': 'bs_code',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'volume',
                'amount': 'amount'
            })

            df['symbol'] = normalized_code
            df['trade_date'] = trade_date
            df['freq'] = freq

            # 转换数据类型
            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'amount']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 转换时间格式
            if 'trade_time' in df.columns:
                df['trade_time'] = pd.to_datetime(df['trade_time'])

            logger.info(f"✅ 获取分钟数据成功: {bs_code}, {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"❌ 获取分钟数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def fetch_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        if not self._ensure_logged_in():
            return None

        try:
            normalized_code = normalize_stock_code(symbol)
            bs_code = self._convert_to_bs_code(normalized_code)

            logger.info(f"📥 获取股票基本信息: {bs_code}")

            # 查询股票信息
            rs = bs.query_stock_basic(code=bs_code)

            if rs is None or rs.error_code != '0':
                logger.warning(f"⚠️ 获取股票信息失败: {rs.error_msg if rs else '返回None'}")
                return {}

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"⚠️ 未获取到股票信息: {bs_code}")
                return {}

            # 获取字段名
            fields = rs.fields
            row_data = data_list[0]

            # 创建信息字典
            info = {}
            for i, field in enumerate(fields):
                if i < len(row_data):
                    info[field] = row_data[i]

            # 标准化字段名
            info['normalized_code'] = normalized_code
            info['symbol'] = normalized_code[2:] if len(normalized_code) > 2 else normalized_code

            # 映射字段
            field_mapping = {
                'code_name': 'name',
                'ipoDate': 'list_date',
                'outDate': 'delist_date',
                'type': 'market_type'
            }

            for old_key, new_key in field_mapping.items():
                if old_key in info:
                    info[new_key] = info.pop(old_key)

            logger.info(f"✅ 获取股票信息成功: {info.get('name', 'Unknown')}")
            return info

        except Exception as e:
            logger.error(f"❌ 获取股票基本信息失败 {symbol}: {e}")
            return {}

    def fetch_stock_list(self, market: str = "A股") -> pd.DataFrame:
        """获取股票列表（排除指数）"""
        if not self._ensure_logged_in():
            logger.error("❌ Baostock未登录")
            return pd.DataFrame()

        try:
            logger.info(f"📋 获取{market}股票列表")

            # 获取所有证券（包括股票和指数）
            rs = bs.query_stock_basic()
            if rs is None or rs.error_code != '0':
                logger.error(f"❌ 获取股票列表失败: {rs.error_msg if rs else '返回None'}")
                return pd.DataFrame()

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning("⚠️ 未获取到证券列表")
                return pd.DataFrame()

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 重命名列
            df = df.rename(columns={
                'code': 'bs_code',
                'code_name': 'name',
                'ipoDate': 'list_date',
                'outDate': 'delist_date',
                'type': 'market_type',
                'status': 'list_status'
            })

            # 过滤：只保留正常上市的
            df = df[df['list_status'] == '1']  # 1表示上市

            # 排除指数，只保留股票
            df = df[df['bs_code'].apply(self._is_stock_code)]

            # 根据市场过滤
            if market == "上证":
                df = df[df['bs_code'].str.startswith('sh.')]
            elif market == "深证":
                df = df[df['bs_code'].str.startswith('sz.')]
            elif market == "科创板":
                df = df[df['bs_code'].str.startswith('sh.')]
                df = df[df['bs_code'].str[3:].str.startswith('688')]
            elif market == "创业板":
                df = df[df['bs_code'].str.startswith('sz.')]
                df = df[df['bs_code'].str[3:].str.startswith('300')]
            elif market == "北交所":
                df = df[df['bs_code'].str.startswith('bj.')]

            # 添加标准化代码
            df['symbol'] = df['bs_code'].apply(lambda x: x.replace('.', ''))

            # 添加交易所信息
            df['exchange'] = df['symbol'].apply(lambda x: x[:2].upper() if len(x) >= 2 else '')

            # 添加股票代码（纯数字部分）
            df['stock_code'] = df['symbol'].apply(lambda x: x[2:] if len(x) > 2 else x)

            # 按股票代码排序
            df = df.sort_values('stock_code')

            logger.info(f"✅ 获取股票列表成功: {len(df)} 只股票")
            return df[['symbol', 'stock_code', 'name', 'list_date', 'market_type', 'exchange']]

        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()


    # 在 BaostockCollector 类中添加新方法
    def _safe_fetch_with_retry(self, rs, max_rows=10000) -> list:
        """安全获取数据，防止解压/解码错误"""
        data_list = []
        row_count = 0

        while rs.error_code == '0' and rs.next():
            try:
                # 尝试获取单行数据
                row_data = rs.get_row_data()
                if row_data:  # 确保数据不为空
                    data_list.append(row_data)
                    row_count += 1

                    # 限制单次获取数量，防止内存溢出
                    if row_count >= max_rows:
                        self.logger.warning(f"达到最大行数限制 {max_rows}，提前终止")
                        break
            except Exception as e:
                # 捕获解压/解码错误，跳过损坏行
                error_msg = str(e).lower()
                if any(kw in error_msg for kw in ['utf-8', 'codec', 'decompress', 'invalid']):
                    self.logger.warning(f"跳过损坏行 {row_count}: {e}")
                    continue  # 跳过这一行，继续下一行
                else:
                    # 其他错误重新抛出
                    raise

        return data_list



    def batch_download_daily_data(self, symbols: List[str], start_date: str,
                                  end_date: str, max_workers: int = 3) -> Dict[str, pd.DataFrame]:
        """
        批量下载日线数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            max_workers: 最大线程数

        Returns:
            字典 {symbol: DataFrame}
        """
        results = {}
        failed_symbols = []

        logger.info(f"🚀 开始批量下载日线数据: {len(symbols)} 只股票，{max_workers} 线程")

        def download_single(symbol: str) -> Tuple[str, Optional[pd.DataFrame]]:
            """下载单只股票数据"""
            try:
                df = self.fetch_daily_data(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    return symbol, df
                else:
                    return symbol, None
            except Exception as e:
                logger.error(f"❌ 下载失败 {symbol}: {e}")
                return symbol, None

        # 使用线程池并发下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(download_single, symbol): symbol
                for symbol in symbols
            }

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result_symbol, df = future.result()
                    if df is not None:
                        results[result_symbol] = df
                        logger.info(f"✅ 下载完成: {result_symbol} ({len(df)} 条记录)")
                    else:
                        failed_symbols.append(symbol)
                        logger.warning(f"⚠️ 下载失败: {symbol}")
                except Exception as e:
                    failed_symbols.append(symbol)
                    logger.error(f"❌ 下载异常 {symbol}: {e}")

        # 统计信息
        total_downloaded = sum(len(df) for df in results.values())

        logger.info(f"📊 批量下载完成: 成功 {len(results)} 只股票, 失败 {len(failed_symbols)} 只")
        logger.info(f"📊 总记录数: {total_downloaded} 条")

        if failed_symbols:
            logger.warning(f"⚠️ 失败的股票: {failed_symbols[:10]}")  # 只显示前10个

        return results

    def get_download_stats(self) -> Dict[str, Any]:
        """获取下载统计信息"""
        stats = self.download_stats.copy()
        if stats['total_requests'] == 0:
            stats['total_requests'] = stats['successful'] + stats['failed']
        stats['success_rate'] = (stats['successful'] / stats['total_requests'] * 100) if stats[
                                                                                             'total_requests'] > 0 else 0
        return stats

    def logout(self):
        """退出登录"""
        if self.lg:
            bs.logout()
            logger.info("🔒 Baostock已退出登录")
            self.lg = None

    def __del__(self):
        """析构函数，确保退出登录"""
        try:
            self.logout()
        except:
            pass


def test_baostock_collector():
    """测试Baostock采集器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试Baostock数据采集器")
    print("=" * 60)

    collector = BaostockCollector()

    try:
        if not collector.lg:
            print("❌ Baostock登录失败，请检查网络连接")
            return

        print("✅ Baostock登录成功")

        # 1. 测试股票代码判断
        print("\n🔍 1. 测试股票代码判断")
        test_codes = [
            'sh.600519',  # 贵州茅台（股票）
            'sh.000001',  # 上证指数（指数）
            'sz.000001',  # 平安银行（股票）
            'sz.399001',  # 深证成指（指数）
            'sh.688981',  # 中芯国际（科创板股票）
            'sz.300750',  # 宁德时代（创业板股票）
        ]

        for code in test_codes:
            is_stock = collector._is_stock_code(code)
            print(f"   {code:15} -> {'股票' if is_stock else '指数'}")

        # 2. 测试获取真实的股票列表
        print("\n📋 2. 测试获取真实股票列表")

        markets = ["上证", "深证", "科创板", "创业板"]
        for market in markets:
            stock_list = collector.fetch_stock_list(market)
            if not stock_list.empty:
                print(f"   {market}: {len(stock_list)} 只股票")
                if len(stock_list) > 0:
                    sample = stock_list.head(3)
                    for _, row in sample.iterrows():
                        print(f"     {row['symbol']} - {row['name']}")
            else:
                print(f"   {market}: 无数据")

        # 3. 测试下载真实的股票数据
        print("\n📈 3. 测试下载真实的股票数据")

        # 测试一些知名股票
        test_stocks = ['600519', '000001', '000858', '300750', '688981']

        # 设置日期范围（最近7天）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

        print(f"   日期范围: {start_date} - {end_date}")

        for stock in test_stocks:
            print(f"\n   📊 测试股票: {stock}")

            # 标准化代码
            normalized = normalize_stock_code(stock)
            bs_format = collector._convert_to_bs_code(normalized)

            print(f"     标准化: {normalized}")
            print(f"     Baostock格式: {bs_format}")

            # 检查是否为股票
            if not collector._is_stock_code(bs_format):
                print(f"     ⚠️  可能是指数，跳过")
                continue

            # 下载数据
            df = collector.fetch_daily_data(stock, start_date, end_date)

            if df is not None and not df.empty:
                print(f"     ✅ 下载成功: {len(df)} 条记录")

                # 显示最新数据
                latest = df.iloc[0]
                print(f"     最新数据:")
                print(f"       日期: {latest['trade_date']}")
                print(f"       收盘价: {latest.get('close_price', 'N/A'):.2f}")
                print(f"       涨跌幅: {latest.get('pct_change', 0):+.2f}%")
                print(f"       成交量: {latest.get('volume', 0):,.0f}")
            else:
                print(f"     ❌ 下载失败或无数据")

        # 4. 显示统计信息
        print("\n📈 4. 下载统计信息")
        stats = collector.get_download_stats()
        print(f"   总请求: {stats['total_requests']}")
        print(f"   成功: {stats['successful']}")
        print(f"   失败: {stats['failed']}")
        print(f"   成功率: {stats['success_rate']:.1f}%")

        print("\n✅ Baostock采集器测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        collector.logout()


if __name__ == "__main__":
    test_baostock_collector()