# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\tushare_collector.py
# File Name: tushare_collector
# @ File: tushare_collector.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 18:45
"""
desc  Tushare数据采集器
# config/tushare_token.py （实际） 或 src/config/token_loader.py(没有）
"""

# src/data/tushare_collector.py
import os
import tushare as ts
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
import time

from src.data.data_collector import BaseDataCollector

logger = logging.getLogger(__name__)


class TushareCollector(BaseDataCollector):
    """Tushare数据采集器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        super().__init__(config_path)

        # 方法1：直接使用已有的token_loader（推荐）
        try:
            from src.config.token_loader import get_token
            self.token = get_token()
            logger.info("✅ Token通过token_loader模块加载")
        except ImportError:
            # 方法2：如果token_loader不存在，直接读环境变量
            from dotenv import load_dotenv
            load_dotenv()  # 加载.env文件
            self.token = os.getenv('TUSHARE_TOKEN')
            if not self.token:
                raise ValueError(
                    "未找到TUSHARE_TOKEN。请确保：\n"
                    "1. 在项目根目录的.env文件中设置 TUSHARE_TOKEN=你的token\n"
                    "2. 或者在系统环境变量中设置 TUSHARE_TOKEN"
                )
            logger.info("✅ Token通过环境变量加载")

        # 初始化Tushare Pro API
        try:
            self.pro = ts.pro_api(self.token)
            # 测试API连接
            test_result = self.pro.query('trade_cal', exchange='SSE', start_date='20240101', end_date='20240102')
            if test_result is not None:
                logger.info(f"✅ Tushare API连接成功，Token有效")
            else:
                logger.warning("⚠️ Tushare API连接测试返回空，但客户端已创建")
        except Exception as e:
            logger.error(f"❌ Tushare API初始化失败: {e}")
            raise

        # 设置API参数
        self.rate_limit = 500  # 默认值
        self.request_count = 0
        self.last_request_time = time.time()

        # 检查是否有config_loader模块（可选）
        try:
            from src.config.config_loader import load_tushare_config
            cfg = load_tushare_config()
            self.endpoints = cfg.get('endpoints', {})
            self.rate_limit = cfg.get('rate_limit', 500)
            logger.info(f"✅ 已加载Tushare配置，速率限制: {self.rate_limit}/分钟")
        except ImportError:
            # 如果config_loader不存在，使用默认配置
            self.endpoints = {
                'daily': 'daily',
                'basic_info': 'stock_basic',
                'trade_cal': 'trade_cal'
            }
            logger.info("ℹ️ 使用默认Tushare配置")

    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取日线数据"""
        try:
            # 执行速率限制检查
            self.enforce_rate_limit(self.rate_limit)

            logger.debug(f"获取日线数据: {symbol} [{start_date} - {end_date}]")

            # 调用Tushare API
            df = self.pro.daily(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                logger.warning(f"未获取到 {symbol} 的日线数据")
                return None

            # 重命名列以匹配数据库表结构
            column_mapping = {
                'ts_code': 'symbol',
                'trade_date': 'trade_date',
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'pre_close': 'pre_close_price',
                'change': 'change_amount',
                'pct_chg': 'pct_change',
                'vol': 'volume',
                'amount': 'amount'
            }

            # 重命名列
            df = df.rename(columns=column_mapping)

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])

            # 计算换手率等指标（如果需要）
            # 这里可以添加额外的数据处理逻辑

            logger.info(f"✅ 成功获取 {symbol} 日线数据 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"❌ 获取 {symbol} 日线数据失败: {e}")
            return None

    def fetch_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        try:
            self.enforce_rate_limit(self.rate_limit)

            logger.debug(f"获取股票基本信息: {symbol}")

            df = self.pro.stock_basic(
                ts_code=symbol,
                fields='ts_code,name,area,industry,market,list_date,fullname,enname,cnspell,exchange,curr_type,list_status,is_hs'
            )

            if df is None or df.empty:
                logger.warning(f"未获取到 {symbol} 的基本信息")
                return None

            info_dict = df.iloc[0].to_dict()
            logger.info(f"✅ 成功获取 {symbol} 基本信息")
            return info_dict

        except Exception as e:
            logger.error(f"❌ 获取 {symbol} 基本信息失败: {e}")
            return None

    def fetch_trade_calendar(self, start_date: str, end_date: str, exchange: str = 'SSE') -> Optional[pd.DataFrame]:
        """获取交易日历"""
        try:
            self.enforce_rate_limit(self.rate_limit)

            logger.debug(f"获取交易日历: {exchange} [{start_date} - {end_date}]")

            df = self.pro.trade_cal(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                logger.warning("未获取到交易日历数据")
                return None

            logger.info(f"✅ 成功获取交易日历 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"❌ 获取交易日历失败: {e}")
            return None

    def test_api_connection(self) -> bool:
        """测试API连接是否正常"""
        try:
            # 简单查询测试
            df = self.pro.query('trade_cal', exchange='SSE', start_date='20240101', end_date='20240105')
            if df is not None:
                logger.info(f"✅ API连接测试成功，返回{len(df)}条数据")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ API连接测试失败: {e}")
            return False


# 使用示例
if __name__ == "__main__":
    import sys

    sys.path.append('.')  # 确保可以找到src模块

    # 测试Token加载
    try:
        collector = TushareCollector()
        print("✅ TushareCollector初始化成功")

        # 测试API连接
        if collector.test_api_connection():
            print("✅ API连接正常")

            # 测试获取一只股票的基本信息
            test_symbol = "000001.SZ"
            info = collector.fetch_basic_info(test_symbol)
            if info:
                print(f"✅ 成功获取 {test_symbol} 基本信息")
                print(f"   股票名称: {info.get('name')}")
                print(f"   上市日期: {info.get('list_date')}")
                print(f"   所属行业: {info.get('industry')}")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()