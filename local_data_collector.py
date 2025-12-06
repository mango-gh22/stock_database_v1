# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\local_data_collector.py
# File Name: local_data_collector
# @ Author: mango-gh22
# @ Date：2025/12/6 17:40
"""
desc 创建临时的本地数据采集器
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地数据采集器（避免网络问题）
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import get_session
from src.utils.logger import get_logger
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

logger = get_logger(__name__)


class LocalDataCollector:
    """本地数据采集器（生成模拟数据）"""

    def __init__(self):
        self.session = get_session()
        self.logger = get_logger(__name__)

    def generate_daily_data(self, symbol, name, days=30):
        """生成日线数据"""
        data = []
        base_price = random.uniform(50, 200)

        current_date = datetime.now().date()

        for i in range(days):
            trade_date = current_date - timedelta(days=i)

            # 模拟价格波动
            change_percent = random.uniform(-0.05, 0.05)
            close = base_price * (1 + change_percent)
            open_price = close * random.uniform(0.98, 1.02)
            high = max(open_price, close) * random.uniform(1.0, 1.03)
            low = min(open_price, close) * random.uniform(0.97, 1.0)

            volume = random.randint(1000000, 50000000)
            amount = volume * close
            change = close - base_price
            pct_change = change_percent * 100

            data.append({
                'trade_date': trade_date,
                'symbol': symbol,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume,
                'amount': round(amount, 2),
                'change': round(change, 2),
                'pct_change': round(pct_change, 2)
            })

            base_price = close

        return data

    def collect_all_stocks(self):
        """采集所有股票的数据"""
        try:
            # 获取所有股票
            from sqlalchemy import text
            query = text("SELECT symbol, name FROM stock_basic WHERE is_active = TRUE")
            result = self.session.execute(query)
            stocks = result.fetchall()

            if not stocks:
                self.logger.warning("没有找到活跃的股票")
                return False

            self.logger.info(f"找到 {len(stocks)} 只活跃股票")

            total_records = 0
            for symbol, name in stocks:
                try:
                    # 生成数据
                    daily_data = self.generate_daily_data(symbol, name, days=20)

                    # 插入数据库
                    for record in daily_data:
                        insert_sql = text("""
                        INSERT INTO daily_data 
                        (trade_date, symbol, open, high, low, close, volume, amount, change, pct_change)
                        VALUES (:trade_date, :symbol, :open, :high, :low, :close, :volume, :amount, :change, :pct_change)
                        ON DUPLICATE KEY UPDATE 
                        open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
                        volume=VALUES(volume), amount=VALUES(amount), change=VALUES(change), pct_change=VALUES(pct_change)
                        """)
                        self.session.execute(insert_sql, record)

                    total_records += len(daily_data)
                    self.logger.info(f"✅ 为 {symbol} 生成 {len(daily_data)} 条数据")

                except Exception as e:
                    self.logger.error(f"❌ 为 {symbol} 生成数据失败: {e}")

            self.session.commit()
            self.logger.info(f"✅ 总共生成 {total_records} 条日线数据")
            return True

        except Exception as e:
            self.logger.error(f"❌ 采集数据失败: {e}")
            self.session.rollback()
            return False
        finally:
            self.session.close()


def main():
    """主函数"""
    print("📡 本地数据采集器")
    print("=" * 50)

    collector = LocalDataCollector()

    print("\n1️⃣ 检查股票数据...")
    from sqlalchemy import text
    session = get_session()
    try:
        result = session.execute(text("SELECT COUNT(*) FROM stock_basic"))
        stock_count = result.scalar()
        print(f"  股票数量: {stock_count}")

        result = session.execute(text("SELECT COUNT(*) FROM daily_data"))
        daily_count = result.scalar()
        print(f"  日线数据: {daily_count}")

    finally:
        session.close()

    if stock_count == 0:
        print("\n❌ 没有股票数据，请先导入股票基本信息")
        return

    choice = input(f"\n是否生成 {stock_count} 只股票的模拟数据？(y/n): ")
    if choice.lower() == 'y':
        print("\n2️⃣ 开始生成模拟数据...")
        success = collector.collect_all_stocks()

        if success:
            print("\n✅ 数据生成完成!")
        else:
            print("\n❌ 数据生成失败")
    else:
        print("\n⏸️  已取消")


if __name__ == "__main__":
    main()