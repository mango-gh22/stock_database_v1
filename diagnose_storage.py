# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_storage.py
# File Name: diagnose_storage
# @ Author: mango-gh22
# @ Date：2025/12/28 14:09
"""
desc 
"""

# diagnose_storage.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage
import pandas as pd


def diagnose_store_daily_data():
    """诊断 store_daily_data 的实际返回值"""
    print("🔍 诊断 store_daily_data 返回值格式")
    print("=" * 50)

    storage = DataStorage()

    # 创建测试数据
    test_data = pd.DataFrame({
        'symbol': ['sh600519'],
        'trade_date': ['2025-12-28'],
        'open_price': [100.0],
        'high_price': [105.0],
        'low_price': [99.0],
        'close_price': [102.0],
        'volume': [1000000],
        'amount': [102000000]
    })

    try:
        # 调用并检查返回值
        result = storage.store_daily_data(test_data)

        print(f"返回值: {result}")
        print(f"返回值类型: {type(result)}")

        if isinstance(result, tuple):
            print(f"元组长度: {len(result)}")
            for i, item in enumerate(result):
                print(f"  元素{i}: {item} (类型: {type(item)})")

        # 测试 log_data_update 是否能处理
        print("\n🧪 测试 log_data_update 能否处理这个返回值:")
        try:
            storage.log_data_update(
                data_type='diagnose',
                symbol='TEST001',
                start_date='20251221',
                end_date='20251228',
                rows_affected=result,  # 使用实际的返回值
                status='test',
                execution_time=0.1
            )
            print("✅ log_data_update 可以处理这个返回值")
        except Exception as e:
            print(f"❌ log_data_update 处理失败: {e}")

    except Exception as e:
        print(f"❌ 诊断失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    diagnose_store_daily_data()