# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p6_complete.py
# File Name: test_p6_complete
# @ Author: mango-gh22
# @ Date：2025/12/22 0:57
"""
desc 
"""
# test_p6_complete.py
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_complete_p6():
    """完整的P6测试"""
    print("🧪 完整P6功能测试")
    print("=" * 50)

    # 先测试配置
    print("1. 测试配置加载...")
    try:
        from src.config.config_loader import ConfigLoader
        config = ConfigLoader.load_yaml_config('config/performance.yaml')

        if config:
            print(f"   ✅ 配置加载成功")
            print(
                f"      indicators.cache.max_size: {config.get('indicators', {}).get('cache', {}).get('max_size', 'N/A')}")
            print(
                f"      indicators.parallel.max_workers: {config.get('indicators', {}).get('parallel', {}).get('max_workers', 'N/A')}")
        else:
            print("   ⚠️  配置加载为空")
    except Exception as e:
        print(f"   ❌ 配置加载失败: {e}")

    print("\n2. 测试性能管理器...")
    try:
        from src.performance.performance_manager_fixed import PerformanceManagerFixed
        pm = PerformanceManagerFixed()
        print("   ✅ 性能管理器创建成功")

        print("\n3. 启动性能管理器...")
        pm.start()
        print("   ✅ 启动成功")

        print("\n4. 测试DataFrame优化...")
        # 创建测试数据
        df = pd.DataFrame({
            'id': range(1000),
            'name': [f'stock_{i}' for i in range(1000)],
            'price': [100 + i * 0.5 for i in range(1000)],
            'volume': [1000 + i * 10 for i in range(1000)],
            'active': [True if i % 2 == 0 else False for i in range(1000)]
        })

        print(f"   原始DataFrame:")
        print(f"     - 形状: {df.shape}")
        print(f"     - 列类型: {dict(df.dtypes)}")

        if hasattr(df, 'memory_usage'):
            original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"     - 内存: {original_memory:.2f} MB")

        # 优化
        optimized_df = pm.optimize_dataframe(df)

        if optimized_df is not None:
            print(f"\n   优化后DataFrame:")
            print(f"     - 形状: {optimized_df.shape}")
            print(f"     - 列类型: {dict(optimized_df.dtypes)}")

            if hasattr(optimized_df, 'memory_usage'):
                optimized_memory = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
                print(f"     - 内存: {optimized_memory:.2f} MB")

                if original_memory > 0:
                    savings = (original_memory - optimized_memory) / original_memory * 100
                    print(f"     - 节省: {savings:.1f}%")

            print("   ✅ DataFrame优化成功")
        else:
            print("   ❌ DataFrame优化返回None")

        print("\n5. 测试并行计算...")

        def process_stock(i):
            return {
                'id': i,
                'squared': i * i,
                'sqrt': i ** 0.5
            }

        stock_ids = list(range(100))
        results = pm.parallel_calculate(process_stock, stock_ids)

        if results and len(results) == len(stock_ids):
            print(f"   并行计算完成: {len(results)} 个结果")
            print(f"   示例结果: {results[0]}")
            print("   ✅ 并行计算成功")
        else:
            print(f"   ⚠️  并行计算异常: 期望{len(stock_ids)}结果，实际{len(results) if results else 0}")

        print("\n6. 测试缓存...")
        test_data = {
            'stock_001': {'price': 100.5, 'volume': 10000},
            'stock_002': {'price': 200.3, 'volume': 20000}
        }

        # 设置缓存
        set_result = pm.set_cache('stock_data', test_data, ttl=60)
        print(f"   设置缓存: {'成功' if set_result else '失败'}")

        # 获取缓存
        cached_data = pm.get_cache('stock_data')
        if cached_data:
            print(f"   获取缓存: 成功 ({len(cached_data)} 条数据)")
            print("   ✅ 缓存测试成功")
        else:
            print("   获取缓存: 失败")

        # 获取不存在的缓存
        missing_data = pm.get_cache('non_existent')
        print(f"   获取不存在缓存: {'None' if missing_data is None else '有值'}")

        print("\n7. 测试性能报告...")
        report = pm.get_performance_report()
        if report:
            print(f"   性能报告生成: {len(report)} 项")
            cache_stats = report.get('cache', {})
            if cache_stats:
                print(f"   缓存命中率: {cache_stats.get('hit_rate', 0):.1%}")
            print("   ✅ 报告生成成功")
        else:
            print("   ⚠️  报告生成失败")

        print("\n8. 停止性能管理器...")
        pm.stop()
        print("   ✅ 停止成功")

        print("\n" + "=" * 50)
        print("🎉 P6功能测试完成！")
        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_complete_p6()
    sys.exit(0 if success else 1)