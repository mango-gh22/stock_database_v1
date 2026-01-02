# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\diagnose_storage.py
# File Name: diagnose_storage
# @ Author: mango-gh22
# @ Date：2025/12/10 21:08
"""
desc 
"""

# src/data/diagnose_storage.py
"""
诊断存储模块问题
"""

import sys
import os
from pathlib import Path


def diagnose_storage():
    print("🔍 诊断存储模块问题")
    print("=" * 60)

    # 1. 检查导入路径
    print("📂 Python搜索路径:")
    for i, path in enumerate(sys.path[:10]):  # 只显示前10个
        print(f"  {i:2d}: {path}")

    # 2. 查找 data_storage.py 文件
    print("\n🔎 查找 data_storage.py 文件:")
    storage_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file == 'data_storage.py':
                full_path = os.path.join(root, file)
                storage_files.append(full_path)
                print(f"  📄 {full_path}")

                # 检查文件内容
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        has_store = 'def store_daily_data' in content
                        has_save = 'def save_daily_data' in content
                        print(f"      store_daily_data: {'✅' if has_store else '❌'}")
                        print(f"      save_daily_data: {'✅' if has_save else '❌'}")
                except Exception as e:
                    print(f"      读取失败: {e}")

    # 3. 检查当前导入的是哪个模块
    print("\n📦 当前导入的模块:")
    try:
        from src.data import data_storage
        module_file = data_storage.__file__
        print(f"  📍 模块文件: {module_file}")

        # 检查模块中的方法
        import inspect
        methods = [name for name, obj in inspect.getmembers(data_storage.DataStorage)
                   if inspect.isfunction(obj) or inspect.ismethod(obj)]

        print(f"  🔧 DataStorage方法:")
        for method in sorted(methods):
            print(f"    - {method}")

        # 特别检查关键方法
        storage_instance = data_storage.DataStorage()
        print(f"  🔍 实例方法检查:")
        print(f"    store_daily_data: {'✅' if hasattr(storage_instance, 'store_daily_data') else '❌'}")
        print(f"    save_daily_data: {'✅' if hasattr(storage_instance, 'save_daily_data') else '❌'}")

        # 检查方法签名
        if hasattr(storage_instance, 'store_daily_data'):
            sig = inspect.signature(storage_instance.store_daily_data)
            print(f"    store_daily_data签名: {sig}")

    except Exception as e:
        print(f"  ❌ 导入失败: {e}")

    # 4. 测试存储功能
    print("\n🧪 测试存储功能:")
    try:
        import pandas as pd
        test_df = pd.DataFrame({
            'symbol': ['test001'],
            'trade_date': ['20241210']
        })

        from src.data.data_storage import DataStorage
        storage = DataStorage()

        # 尝试调用 store_daily_data
        if hasattr(storage, 'store_daily_data'):
            print("  🔧 调用 store_daily_data...")
            try:
                result = storage.store_daily_data(test_df)
                print(f"  ✅ 调用成功，返回类型: {type(result)}")
                print(f"     返回值: {result}")
            except Exception as e:
                print(f"  ❌ 调用失败: {e}")

        # 尝试调用 save_daily_data
        if hasattr(storage, 'save_daily_data'):
            print("  🔧 调用 save_daily_data...")
            try:
                result = storage.save_daily_data(test_df)
                print(f"  ✅ 调用成功，返回类型: {type(result)}")
                print(f"     返回值: {result}")
            except Exception as e:
                print(f"  ❌ 调用失败: {e}")

    except Exception as e:
        print(f"  ❌ 测试失败: {e}")

    print("\n" + "=" * 60)
    print("诊断完成")


if __name__ == "__main__":
    diagnose_storage()