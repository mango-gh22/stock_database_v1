# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\v060_verify_storage.py
# File Name: v060_verify_storage
# @ Author: mango-gh22
# @ Date：2026/1/1 21:20
"""
desc v0.6.0 存储层修复验证脚本（修正版）
清除污染 + 验证追踪
"""

import sys
from typing import Dict, List, Optional, Tuple, Any

sys.path.append('.')


def main():
    print("🚀 v0.6.0 存储层修复验证")
    print("=" * 60)

    # 1. 验证污染已清除
    print("\n1️⃣ 验证模块隔离...")
    from src.data.data_storage import DataStorage as OriginalDS
    from src.data.adaptive_storage import AdaptiveDataStorage

    assert id(OriginalDS) != id(AdaptiveDataStorage), "❌ 污染未清除！"
    print("   ✅ 模块已隔离")

    # 2. 验证调度器
    print("\n2️⃣ 验证 DataScheduler...")
    from src.data.data_scheduler import DataScheduler

    scheduler = DataScheduler()
    assert type(scheduler.storage).__name__ == 'AdaptiveDataStorage', "❌ 调度器未使用AdaptiveDataStorage"
    print("   ✅ 调度器使用AdaptiveDataStorage")

    # 3. 验证管道
    print("\n3️⃣ 验证 IntegratedDataPipeline...")
    from src.data.integrated_pipeline import IntegratedDataPipeline

    pipeline = IntegratedDataPipeline()
    assert hasattr(pipeline, 'tracer'), "❌ 管道缺少追踪器"
    print("   ✅ 管道集成StorageTracer")

    # 4. 测试日志接口兼容性（修正版）
    print("\n4️⃣ 测试日志接口兼容性...")

    # ✅ 正确方式：使用 (args, kwargs) 结构
    test_cases = [
        (('daily', 'sh600519', 5, 'success'), {}),  # 位置参数模式
        (('daily', 'sz000001', '20240101', '20240131', 10, 'success', None, 1.5), {}),  # 扩展位置参数
        (('daily', 'sz000858'), {'rows_affected': 8, 'status': 'partial', 'execution_time': 0.8}),  # 关键字参数模式
    ]

    for args, kwargs in test_cases:
        try:
            result = pipeline.storage.log_data_update(*args, **kwargs)
            print(f"   ✅ {args[1]}: 成功={result['success']}, 行数={result.get('rows_logged', 0)}")
        except Exception as e:
            print(f"   ❌ {args[1]}: {e}")
            import traceback
            traceback.print_exc()
            return False

    print("\n" + "=" * 60)
    print("🎉 v0.6.0 修复验证通过！")
    print("\n下一步：运行实际存储测试")
    print("python -c \"from src.data.data_scheduler import DataScheduler; s=DataScheduler(); s.run_demo_collection()\"")

    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n💥 验证失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)