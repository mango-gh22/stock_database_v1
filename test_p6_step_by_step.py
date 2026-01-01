# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p6_step_by_step.py
# File Name: test_p6_step_by_step
# @ Author: mango-gh22
# @ Date：2025/12/26 19:50
"""
desc 
"""
# test_p6_step_by_step.py
"""
P6功能逐步测试
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_step_1_config():
    """步骤1：测试配置加载"""
    print("步骤1: 测试配置加载")
    print("-" * 40)

    try:
        from src.config.config_loader import ConfigLoader
        config = ConfigLoader.load_yaml_config('config/performance.yaml')

        if config:
            print(f"✅ 配置加载成功，共 {len(config)} 个主配置项")

            # 检查关键配置
            check_keys = ['monitoring', 'indicators', 'query']
            for key in check_keys:
                if key in config:
                    print(f"   - {key}: 存在")
                else:
                    print(f"   - {key}: 缺失")

            return True
        else:
            print("❌ 配置加载为空")
            return False

    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return False


def test_step_2_memory_manager():
    """步骤2：测试内存管理器"""
    print("\n步骤2: 测试内存管理器")
    print("-" * 40)

    try:
        from src.performance.memory_manager_fixed import MemoryManagerFixed
        import pandas as pd
        import numpy as np

        manager = MemoryManagerFixed({})

        # 创建测试数据
        df = pd.DataFrame({
            'int_col': list(range(1000)),
            'float_col': np.random.randn(1000),
            'text_col': [f'item_{i}' for i in range(1000)]
        })

        original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        print(f"原始内存: {original_memory:.2f} MB")

        optimized = manager.optimize_dataframe(df)

        if optimized is not None:
            optimized_memory = optimized.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"优化后内存: {optimized_memory:.2f} MB")

            if original_memory > 0:
                reduction = (original_memory - optimized_memory) / original_memory * 100
                print(f"内存减少: {reduction:.1f}%")

            print("✅ 内存管理器测试通过")
            return True
        else:
            print("❌ 优化返回 None")
            return False

    except Exception as e:
        print(f"❌ 内存管理器测试失败: {e}")
        return False


def test_step_3_parallel_calculator():
    """步骤3：测试并行计算器"""
    print("\n步骤3: 测试并行计算器")
    print("-" * 40)

    try:
        from src.performance.parallel_calculator_fixed import ParallelCalculatorFixed
        import time

        calculator = ParallelCalculatorFixed({'max_workers': 2})

        def process_item(x):
            return x * x

        data = list(range(10))

        start_time = time.time()
        results = calculator.calculate(process_item, data)
        elapsed_time = time.time() - start_time

        print(f"处理 {len(data)} 个项目")
        print(f"耗时: {elapsed_time:.3f}秒")

        # 验证结果
        expected = [x * x for x in data]
        if results == expected:
            print(f"✅ 结果验证正确: {results[:3]}...")
            return True
        else:
            print(f"❌ 结果不匹配")
            return False

    except Exception as e:
        print(f"❌ 并行计算器测试失败: {e}")
        return False


def test_step_4_cache_manager():
    """步骤4：测试缓存管理器"""
    print("\n步骤4: 测试缓存管理器")
    print("-" * 40)

    try:
        from src.performance.cache_strategy_fixed import CacheManagerFixed

        manager = CacheManagerFixed({'max_size': 10})

        # 测试设置缓存
        set_result = manager.set('test_key', 'test_value')
        print(f"设置缓存: {'成功' if set_result else '失败'}")

        # 测试获取缓存
        cached = manager.get('test_key')
        print(f"获取缓存: {cached}")

        if cached == 'test_value':
            print("✅ 缓存功能正常")
            return True
        else:
            print("❌ 缓存不匹配")
            return False

    except Exception as e:
        print(f"❌ 缓存管理器测试失败: {e}")
        return False


def test_step_5_integration():
    """步骤5：测试简单集成"""
    print("\n步骤5: 测试简单集成")
    print("-" * 40)

    try:
        # 手动集成各个组件
        from src.performance.memory_manager_fixed import MemoryManagerFixed
        from src.performance.cache_strategy_fixed import CacheManagerFixed

        memory_manager = MemoryManagerFixed({})
        cache_manager = CacheManagerFixed({'max_size': 100})

        # 创建一些测试数据
        import pandas as pd
        data = pd.DataFrame({'values': range(100)})

        # 测试内存优化
        optimized = memory_manager.optimize_dataframe(data)
        print(f"内存优化: {'成功' if optimized is not None else '失败'}")

        # 测试缓存
        cache_manager.set('test_data',
                          {'original': len(data), 'optimized': len(optimized) if optimized is not None else 0})
        cached = cache_manager.get('test_data')
        print(f"缓存测试: {'成功' if cached else '失败'}")

        if optimized is not None and cached:
            print("✅ 集成测试通过")
            return True
        else:
            print("❌ 集成测试失败")
            return False

    except Exception as e:
        print(f"❌ 集成测试失败: {e}")
        return False


def main():
    """主测试函数"""
    print("🧪 P6功能逐步测试")
    print("=" * 50)

    results = []

    # 执行各个步骤
    results.append(test_step_1_config())
    results.append(test_step_2_memory_manager())
    results.append(test_step_3_parallel_calculator())
    results.append(test_step_4_cache_manager())
    results.append(test_step_5_integration())

    # 统计结果
    print("\n" + "=" * 50)
    print("测试结果统计:")
    print(f"总测试数: {len(results)}")
    print(f"通过数: {sum(results)}")
    print(f"失败数: {len(results) - sum(results)}")

    if all(results):
        print("\n🎉 所有测试通过!")
        return True
    else:
        print("\n⚠️ 部分测试失败")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)