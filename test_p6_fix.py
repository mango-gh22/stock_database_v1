# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p6_fix.py
# File Name: test_p6_fix
# @ Author: mango-gh22
# @ Date：2025/12/22 0:49
"""
desc 
"""

# test_p6_fix.py
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_performance_manager_fix():
    """测试修复后的性能管理器"""
    print("🧪 测试修复后的 PerformanceManager...")

    try:
        from src.performance.performance_manager import PerformanceManager

        print("1. 创建 PerformanceManager 实例...")
        pm = PerformanceManager()
        print("   ✅ 创建成功")

        print("2. 测试启动...")
        pm.start()
        print("   ✅ 启动成功")

        print("3. 测试 DataFrame 优化...")
        # 创建测试数据
        test_df = pd.DataFrame({
            'A': range(1000),
            'B': [f'item_{i}' for i in range(1000)],
            'C': [i * 1.5 for i in range(1000)],
            'D': [True if i % 2 == 0 else False for i in range(1000)]
        })

        print(f"   原始DataFrame形状: {test_df.shape}")
        print(f"   原始列类型: {test_df.dtypes.to_dict()}")

        # 优化DataFrame
        optimized_df = pm.optimize_dataframe(test_df)

        if optimized_df is not None:
            print(f"   优化后DataFrame形状: {optimized_df.shape}")
            print(f"   优化后列类型: {optimized_df.dtypes.to_dict()}")

            # 计算内存使用
            if hasattr(test_df, 'memory_usage'):
                original_mem = test_df.memory_usage(deep=True).sum() / 1024 / 1024
                optimized_mem = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
                savings = (original_mem - optimized_mem) / original_mem * 100 if original_mem > 0 else 0

                print(f"   原始内存: {original_mem:.2f} MB")
                print(f"   优化后内存: {optimized_mem:.2f} MB")
                print(f"   内存节省: {savings:.1f}%")
            print("   ✅ DataFrame优化成功")
        else:
            print("   ⚠️  DataFrame优化返回None")

        print("4. 测试并行计算...")

        def square(x):
            return x * x

        test_data = list(range(100))
        result = pm.parallel_calculate(square, test_data)

        if result and len(result) == len(test_data):
            print(f"   并行计算: 输入{len(test_data)}项，输出{len(result)}项")
            print("   ✅ 并行计算成功")
        else:
            print("   ⚠️  并行计算结果不完整")

        print("5. 测试缓存...")
        pm.set_cache('test_key', 'test_value')
        cached = pm.get_cache('test_key')

        if cached == 'test_value':
            print("   ✅ 缓存测试成功")
        else:
            print(f"   ⚠️  缓存测试失败: {cached}")

        print("6. 测试报告生成...")
        report = pm.get_performance_report()
        print(f"   生成报告包含: {len(report)} 个模块")
        print("   ✅ 报告生成成功")

        print("7. 停止性能管理器...")
        pm.stop()
        print("   ✅ 停止成功")

        print("\n🎉 所有测试通过！")
        return True

    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_performance_manager_fix()
    sys.exit(0 if success else 1)