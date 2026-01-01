# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_field_fix.py
# File Name: test_field_fix
# @ Author: mango-gh22
# @ Date：2025/12/28 16:37
"""
desc 
"""
# test_field_fix.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage
import pandas as pd


def test_field_fix():
    print("🧪 测试字段修复")
    print("=" * 50)

    storage = DataStorage()

    # 模拟 Baostock 返回的数据
    test_data = pd.DataFrame({
        'code': ['sh.600519', 'sh.600519'],
        'date': ['2025-12-27', '2025-12-28'],
        'open': [100.0, 101.0],
        'high': [105.0, 106.0],
        'low': [99.0, 100.0],
        'close': [102.0, 103.0],
        'preclose': [100.0, 102.0],
        'volume': [1000000, 1200000],
        'amount': [102000000, 123600000],
        'pctChg': [2.0, 0.98],
        'turn': [1.5, 1.8],
        'adjustflag': ['3', '3'],
        'tradestatus': ['1', '1']
    })

    print("原始数据字段:", list(test_data.columns))

    # 测试预处理
    processed = storage._preprocess_data(test_data, 'stock_daily_data')

    print("\n处理后的字段:", list(processed.columns))

    # 检查是否有不应该存在的字段
    invalid_fields = ['bs_code', 'code', 'date', 'open', 'close', 'pctChg']
    found_invalid = [field for field in invalid_fields if field in processed.columns]

    if found_invalid:
        print(f"❌ 错误：不应该存在的字段: {found_invalid}")
    else:
        print("✅ 字段清理正确")

    # 检查是否有必需的字段
    required_fields = ['symbol', 'trade_date', 'open_price', 'close_price', 'change_percent']
    missing_fields = [field for field in required_fields if field not in processed.columns]

    if missing_fields:
        print(f"❌ 缺失必需字段: {missing_fields}")
    else:
        print("✅ 必需字段完整")

    return len(found_invalid) == 0 and len(missing_fields) == 0


if __name__ == "__main__":
    success = test_field_fix()
    exit(0 if success else 1)