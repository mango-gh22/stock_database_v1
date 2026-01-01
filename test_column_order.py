# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_column_order.py
# File Name: test_column_order
# @ Author: mango-gh22
# @ Date：2025/12/28 17:07
"""
desc 
"""
# test_column_order.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage


def test_column_order():
    """测试字段顺序"""
    storage = DataStorage()

    # 获取数据库表的字段顺序
    columns = storage._get_table_column_order('stock_daily_data')

    print("数据库表字段顺序:")
    for i, col in enumerate(columns[:20], 1):
        print(f"{i:2}. {col}")

    # 检查关键字段位置
    symbol_index = columns.index('symbol') if 'symbol' in columns else -1
    trade_date_index = columns.index('trade_date') if 'trade_date' in columns else -1

    print(f"\n关键字段位置: symbol={symbol_index}, trade_date={trade_date_index}")

    if symbol_index == 0 and trade_date_index == 1:
        print("✅ 字段顺序正确")
    else:
        print("⚠️  字段顺序可能需要调整")


if __name__ == "__main__":
    test_column_order()