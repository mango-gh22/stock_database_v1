# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\pipeline_fix.py
# File Name: pipeline_fix
# @ Author: mango-gh22
# @ Date：2025/12/10 20:57
"""
desc 管道修复 - 解决存储兼容性问题
"""

import sys
import importlib


def fix_storage_compatibility():
    """修复存储兼容性"""
    print("🔧 修复存储兼容性...")

    # 重新加载 data_storage 模块
    if 'src.data.data_storage' in sys.modules:
        importlib.reload(sys.modules['src.data.data_storage'])

    # 导入并检查
    from src.data.data_storage import DataStorage

    # 测试实例
    storage = DataStorage()

    # 检查方法
    methods = {
        'store_daily_data': hasattr(storage, 'store_daily_data'),
        'save_daily_data': hasattr(storage, 'save_daily_data')
    }

    print(f"📋 存储方法检查: {methods}")

    # 如果缺少 store_daily_data，添加兼容方法
    if not methods['store_daily_data'] and methods['save_daily_data']:
        print("➕ 添加兼容方法 store_daily_data")

        original_save = storage.save_daily_data

        def store_daily_data_compat(df):
            result = original_save(df)
            if isinstance(result, bool):
                return (1 if result else 0, {'status': 'compat'})
            elif isinstance(result, int):
                return (result, {'status': 'compat'})
            return (0, {'status': 'error'})

        storage.store_daily_data = store_daily_data_compat
        print("✅ 兼容方法添加成功")

    return storage


if __name__ == "__main__":
    storage = fix_storage_compatibility()
    print("✅ 存储兼容性修复完成")