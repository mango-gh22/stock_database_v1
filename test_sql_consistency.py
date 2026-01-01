# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_sql_consistency.py
# File Name: test_sql_consistency
# @ Author: mango-gh22
# @ Date：2025/12/28 16:46
"""
desc 
"""
# test_sql_consistency.py
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data.data_storage import DataStorage
import pandas as pd


def test_sql_consistency():
    """测试SQL构建和记录准备的一致性"""
    print("🧪 测试SQL一致性")
    print("=" * 50)

    storage = DataStorage()

    # 创建测试数据
    test_data = pd.DataFrame({
        'symbol': ['sh600519', 'sh600519'],
        'trade_date': ['2025-12-27', '2025-12-28'],
        'open_price': [100.0, 101.0],
        'close_price': [102.0, 103.0],
        'volume': [1000000, 1200000],
        'amount': [102000000, 123600000]
    })

    try:
        # 测试SQL构建
        print("1. 测试SQL构建...")
        insert_sql, update_sql, valid_columns = storage._build_dynamic_sql(test_data, 'stock_daily_data')

        print(f"   SQL字段数: {len(valid_columns)}")
        print(f"   字段列表: {valid_columns}")

        # 测试记录准备
        print("\n2. 测试记录准备...")
        records = storage._prepare_records(test_data, valid_columns)

        print(f"   记录数: {len(records)}")
        print(f"   每条记录字段数: {len(records[0]) if records else 0}")

        # 检查一致性
        if len(valid_columns) == len(records[0]):
            print("✅ SQL字段和记录字段一致")
        else:
            print(f"❌ 不一致: SQL字段={len(valid_columns)}, 记录字段={len(records[0])}")

        # 测试完整存储
        print("\n3. 测试完整存储...")
        result = storage.store_daily_data(test_data)

        print(f"   存储结果: {result[0]} 行")
        print(f"   状态: {result[1].get('status')}")

        if result[0] > 0:
            print("✅ 存储成功")
        else:
            print(f"⚠️  存储失败: {result[1].get('reason', '未知原因')}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_sql_consistency()
    exit(0 if success else 1)