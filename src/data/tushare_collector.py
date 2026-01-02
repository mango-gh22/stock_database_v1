# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\tushare_collector.py
# File Name: tushare_collector
# @ Author: mango-gh22
# @ Date：2025/12/7 22:40
"""
desc 
"""

# src/data/tushare_collector.py
"""
Tushare数据采集器 - 完整实现
集成secret_loader，支持完整的数据采集功能
"""

import pandas as pd
import tushare as ts
from typing import Dict, List, Optional, Any
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.data.data_collector import BaseDataCollector
from src.config.secret_loader import get_tushare_token
from src.utils.code_converter import normalize_stock_code

logger = logging.getLogger(__name__)


class TushareDataCollector(BaseDataCollector):
    """Tushare数据采集器 - 完整实现"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        super().__init__(config_path)
        self.pro = self._init_tushare()
        self.cache_dir = Path('data/cache/tushare')
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # API限制配置
        self.daily_limit = 5000
        self.minute_limit = 500
        self.request_count = 0
        self.reset_time = time.time()

    def _init_tushare(self):
        """初始化Tushare Pro API"""
        try:
            token = get_tushare_token()
            if not token:
                logger.warning("未找到Tushare token，相关功能将受限")
                return None

            ts.set_token(token)
            pro = ts.pro_api()

            # 测试API连接
            try:
                test_result = pro.query('trade_cal', exchange='SSE', start_date='20240101', end_date='20240105')
                logger.info(f"Tushare API连接成功，token有效期: {len(test_result) > 0}")
            except Exception as e:
                logger.warning(f"Tushare API测试失败，可能token无效: {e}")
                return None

            logger.info("Tushare Pro API初始化成功")
            return pro

        except Exception as e:
            logger.error(f"初始化Tushare失败: {e}")
            return None

    def _enforce_rate_limit(self):
        """执行API速率限制"""
        current_time = time.time()

        # 每分钟重置计数器
        if current_time - self.reset_time > 60:
            self.request_count = 0
            self.reset_time = current_time

        # 检查是否超过限制
        if self.request_count >= self.minute_limit:
            sleep_time = 60 - (current_time - self.reset_time) + 1
            logger.warning(f"达到API速率限制，等待 {sleep_time:.1f} 秒")
            time.sleep(sleep_time)
            self.request_count = 0
            self.reset_time = time.time()

        self.request_count += 1

    def _convert_to_ts_code(self, normalized_code: str) -> str:
        """将标准化代码转换为Tushare格式"""
        try:
            if normalized_code.startswith('sh'):
                return f"{normalized_code[2:]}.SH"
            elif normalized_code.startswith('sz'):
                return f"{normalized_code[2:]}.SZ"
            elif normalized_code.startswith('bj'):
                return f"{normalized_code[2:]}.BJ"
            else:
                return normalized_code
        except:
            return normalized_code

    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取日线数据"""
        if not self.pro:
            logger.error("Tushare未初始化，无法获取数据")
            return None

        try:
            # 执行速率限制
            self._enforce_rate_limit()

            # 标准化股票代码
            normalized_code = normalize_stock_code(symbol)
            ts_code = self._convert_to_ts_code(normalized_code)

            logger.info(f"获取日线数据: {ts_code} [{start_date} - {end_date}]")

            # 获取日线数据
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                logger.warning(f"未获取到日线数据: {ts_code}")
                return pd.DataFrame()

            # 重命名列以匹配数据库
            column_mapping = {
                'ts_code': 'ts_code',
                'trade_date': 'trade_date',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'pre_close': 'pre_close_price',
                'change': 'change_amount',
                'pct_chg': 'pct_change',
                'vol': 'volume',
                'amount': 'amount'
            }

            df = df.rename(columns=column_mapping)
            df['symbol'] = normalized_code

            # 添加额外字段
            df['volume_lot'] = df['volume'] / 100  # 转换为手
            df['amplitude'] = ((df['high_price'] - df['low_price']) / df['pre_close_price']) * 100

            logger.info(f"获取日线数据成功: {ts_code}, {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"获取日线数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def fetch_minute_data(self, symbol: str, trade_date: str, freq: str = '1min') -> Optional[pd.DataFrame]:
        """获取分钟线数据"""
        if not self.pro:
            logger.error("Tushare未初始化")
            return None

        try:
            self._enforce_rate_limit()

            normalized_code = normalize_stock_code(symbol)
            ts_code = self._convert_to_ts_code(normalized_code)

            logger.info(f"获取分钟数据: {ts_code} {trade_date} {freq}")

            df = self.pro.stk_mins(
                ts_code=ts_code,
                freq=freq,
                start_date=trade_date,
                end_date=trade_date
            )

            if df is None or df.empty:
                logger.warning(f"未获取到分钟数据: {ts_code} {trade_date}")
                return pd.DataFrame()

            # 重命名列
            df = df.rename(columns={
                'ts_code': 'ts_code',
                'trade_time': 'trade_time',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'vol': 'volume',
                'amount': 'amount'
            })

            df['symbol'] = normalized_code
            df['trade_date'] = trade_date
            df['freq'] = freq

            # 转换时间格式
            if 'trade_time' in df.columns:
                df['trade_time'] = pd.to_datetime(df['trade_time'])

            logger.info(f"获取分钟数据成功: {ts_code}, {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"获取分钟数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def fetch_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        if not self.pro:
            return None

        try:
            self._enforce_rate_limit()

            normalized_code = normalize_stock_code(symbol)
            ts_code = self._convert_to_ts_code(normalized_code)

            logger.info(f"获取股票基本信息: {ts_code}")

            df = self.pro.stock_basic(
                ts_code=ts_code,
                fields='ts_code,symbol,name,area,industry,market,list_date,fullname,enname,cnspell,exchange,curr_type,list_status,is_hs'
            )

            if df is None or df.empty:
                logger.warning(f"未获取到股票信息: {ts_code}")
                return {}

            info = df.iloc[0].to_dict()
            info['normalized_code'] = normalized_code

            logger.info(f"获取股票信息成功: {info.get('name', 'Unknown')}")
            return info

        except Exception as e:
            logger.error(f"获取股票基本信息失败 {symbol}: {e}")
            return {}

    def fetch_stock_list(self, market: str = "A股") -> pd.DataFrame:
        """获取股票列表"""
        if not self.pro:
            logger.error("Tushare未初始化")
            return pd.DataFrame()

        try:
            self._enforce_rate_limit()

            logger.info(f"获取{market}股票列表")

            exchange_map = {
                "A股": "",
                "上海主板": "SSE",
                "深圳主板": "SZSE",
                "科创板": "SSE",
                "创业板": "SZSE",
                "北京交易所": "BSE"
            }

            exchange = exchange_map.get(market, "")

            df = self.pro.stock_basic(
                exchange=exchange,
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )

            if df is None or df.empty:
                logger.warning(f"未获取到{market}股票列表")
                return pd.DataFrame()

            # 标准化代码
            df['normalized_code'] = df['ts_code'].apply(lambda x: normalize_stock_code(x))

            logger.info(f"获取股票列表成功: {len(df)} 只股票")
            return df

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def fetch_index_data(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指数数据"""
        if not self.pro:
            return pd.DataFrame()

        try:
            self._enforce_rate_limit()

            # 标准化指数代码
            if index_code == "000001":
                ts_code = "000001.SH"  # 上证指数
            elif index_code == "399001":
                ts_code = "399001.SZ"  # 深证成指
            elif index_code == "399006":
                ts_code = "399006.SZ"  # 创业板指
            else:
                ts_code = index_code

            logger.info(f"获取指数数据: {ts_code} [{start_date} - {end_date}]")

            df = self.pro.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                logger.warning(f"未获取到指数数据: {ts_code}")
                return pd.DataFrame()

            df = df.rename(columns={
                'ts_code': 'ts_code',
                'trade_date': 'trade_date',
                'open': 'open_point',
                'high': 'high_point',
                'low': 'low_point',
                'close': 'close_point',
                'pre_close': 'pre_close_point',
                'change': 'change_point',
                'pct_chg': 'pct_change',
                'vol': 'volume',
                'amount': 'amount'
            })

            df['normalized_code'] = normalize_stock_code(ts_code)

            logger.info(f"获取指数数据成功: {ts_code}, {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"获取指数数据失败 {index_code}: {e}")
            return pd.DataFrame()


def test_tushare_collector():
    """测试Tushare采集器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试Tushare数据采集器")
    print("=" * 50)

    try:
        collector = TushareDataCollector()

        if not collector.pro:
            print("❌ Tushare未初始化，跳过测试")
            return

        # 测试股票列表
        print("\n📋 1. 测试获取股票列表")
        stock_list = collector.fetch_stock_list("A股")
        if not stock_list.empty:
            print(f"   获取到 {len(stock_list)} 只股票")
            print("   前5只股票:")
            for i, (_, row) in enumerate(stock_list.head().iterrows()):
                print(f"     {i + 1}. {row['symbol']} - {row['name']}")

            # 测试获取单只股票信息
            print("\n📈 2. 测试获取单只股票数据")
            test_symbol = stock_list.iloc[0]['ts_code']
            normalized = normalize_stock_code(test_symbol)

            # 基本信息
            basic_info = collector.fetch_basic_info(normalized)
            if basic_info:
                print(f"   股票基本信息: {basic_info.get('name')} ({basic_info.get('industry')})")

            # 日线数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

            daily_data = collector.fetch_daily_data(normalized, start_date, end_date)
            if not daily_data.empty:
                print(f"   日线数据: {len(daily_data)} 条记录")
                print(f"   日期范围: {daily_data['trade_date'].min()} 到 {daily_data['trade_date'].max()}")
                print(f"   数据列: {list(daily_data.columns)}")

            # 测试指数数据
            print("\n📊 3. 测试获取指数数据")
            index_data = collector.fetch_index_data("000001", start_date, end_date)
            if not index_data.empty:
                print(f"   上证指数数据: {len(index_data)} 条记录")
                print(f"   最新点位: {index_data.iloc[0]['close_point']}")

        print("\n✅ Tushare采集器测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_tushare_collector()