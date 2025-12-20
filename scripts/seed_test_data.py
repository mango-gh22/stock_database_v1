# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\seed_test_data.py
# File Name: seed_test_data
# @ Author: mango-gh22
# @ Date：2025/12/21 0:39
"""
desc 
"""
# 创建数据补充脚本：scripts/seed_test_data.py
"""
补充测试数据 - 为P6阶段准备基础数据
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.query.query_engine import QueryEngine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_stock_data(symbol: str, days: int = 100):
    """
    创建测试用的股票日线数据

    Args:
        symbol: 股票代码
        days: 生成多少天的数据
    """
    # 生成日期序列（最近days天）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    # 生成模拟价格数据（随机漫步）
    np.random.seed(hash(symbol) % 10000)  # 用股票代码作为随机种子

    base_price = 100.0
    prices = []
    current_price = base_price

    for i in range(len(dates)):
        # 随机波动
        change = np.random.normal(0, 0.02)  # 平均波动2%
        current_price = current_price * (1 + change)

        # 生成OHLC数据
        open_price = current_price * (1 + np.random.normal(0, 0.01))
        high_price = max(open_price, current_price) * (1 + abs(np.random.normal(0, 0.01)))
        low_price = min(open_price, current_price) * (1 - abs(np.random.normal(0, 0.01)))
        close_price = current_price

        prices.append({
            'trade_date': dates[i].strftime('%Y-%m-%d'),
            'symbol': symbol,
            'open_price': round(open_price, 4),
            'high_price': round(high_price, 4),
            'low_price': round(low_price, 4),
            'close_price': round(close_price, 4),
            'volume': int(np.random.normal(1000000, 200000)),
            'amount': round(np.random.normal(50000000, 10000000), 3),
            'change_percent': round(change * 100, 4),
            'pre_close_price': round(current_price / (1 + change), 4) if i > 0 else round(open_price, 4),
            'turnover_rate': round(np.random.uniform(0.5, 5.0), 4),
            'amplitude': round(abs(high_price - low_price) / close_price * 100, 4),
            'ma5': round(close_price * (1 + np.random.normal(0, 0.005)), 4),
            'ma10': round(close_price * (1 + np.random.normal(0, 0.008)), 4),
            'ma20': round(close_price * (1 + np.random.normal(0, 0.01)), 4)
        })

    return pd.DataFrame(prices)


def seed_essential_test_data():
    """补充必要的测试数据"""
    logger.info("开始补充测试数据...")

    # 选择一些重要的测试股票
    test_symbols = [
        'sh600519',  # 贵州茅台
        'sz000001',  # 平安银行
        'sh600036',  # 招商银行
        'sz000858',  # 五粮液
        'sh601318',  # 中国平安
        'sh600276',  # 恒瑞医药
        'sz002415',  # 海康威视
        'sh600900',  # 长江电力
        'sh601166',  # 兴业银行
        'sz000002'  # 万科A
    ]

    all_data = []

    for symbol in test_symbols:
        logger.info(f"生成 {symbol} 的测试数据...")
        df = create_test_stock_data(symbol, days=60)  # 生成60天数据
        all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # 保存到CSV文件（暂时不写入数据库，避免影响现有数据）
        csv_path = "data/test_stock_data.csv"
        combined_df.to_csv(csv_path, index=False)
        logger.info(f"测试数据已保存到: {csv_path}")
        logger.info(f"共生成 {len(combined_df)} 条记录，{len(test_symbols)} 只股票")

        # 显示数据示例
        print("\n📊 数据示例：")
        print(combined_df[['trade_date', 'symbol', 'close_price', 'volume']].head(10))

        return True
    else:
        logger.error("未能生成测试数据")
        return False


if __name__ == "__main__":
    seed_essential_test_data()