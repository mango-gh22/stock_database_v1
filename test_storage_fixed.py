# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_storage_fixed.py
# File Name: test_storage_fixed
# @ Author: mango-gh22
# @ Date：2025/12/31 22:10
"""
desc 
"""

# test_storage_fixed.py
import sys

sys.path.insert(0, r"E:\MyFile\stock_database_v1")

print("🔧 测试修复后的 DataStorage 类")
print("=" * 50)

from src.data.data_storage import DataStorage

try:
    # 1. 测试初始化
    print("1. 初始化 DataStorage...")
    storage = DataStorage()
    print("✅ DataStorage 初始化成功")

    # 2. 测试 logger 是否存在
    print("2. 检查 logger 属性...")
    if hasattr(storage, 'logger'):
        print(f"✅ logger 存在: {storage.logger}")
    else:
        print("❌ logger 不存在")

    # 3. 测试 get_last_update_date 方法
    print("3. 测试 get_last_update_date 方法...")
    last_date = storage.get_last_update_date('sh600000')
    print(f"✅ 方法调用成功，最后日期: {last_date}")

    # 4. 测试简化版 log_data_update
    print("4. 测试 log_data_update 方法...")

    # 测试1: DataScheduler 格式
    print("  测试格式1 (DataScheduler):")
    result1 = storage.log_data_update('daily', 'sh600000', 5, 'success')
    print(f"    结果: {result1}")

    # 测试2: DataPipeline 格式
    print("  测试格式2 (DataPipeline):")
    result2 = storage.log_data_update(
        data_type='daily',
        symbol='sh600000',
        start_date='20251201',
        end_date='20251228',
        rows_affected=10,
        status='success',
        error_message=None,
        execution_time=1.5
    )
    print(f"    结果: {result2}")

    print("\n🎉 所有基础测试通过！")

except Exception as e:
    print(f"\n❌ 测试失败: {e}")
    import traceback

    traceback.print_exc()