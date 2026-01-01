# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\benchmark_p6.py
# File Name: benchmark_p6
# @ Author: mango-gh22
# @ Date：2025/12/21 23:16
"""
desc 
"""

# File: benchmark_p6.py
# !/usr/bin/env python3
"""
P6阶段三性能基准测试
"""

import sys
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def benchmark_dataframe_optimization():
    """基准测试：数据框优化"""
    print("📊 数据框优化基准测试")
    print("-" * 40)

    try:
        from src.performance.performance_manager import PerformanceManager

        pm = PerformanceManager()

        # 创建不同大小的测试数据框
        sizes = [1000, 10000, 50000, 100000]

        results = []
        for size in sizes:
            print(f"\n测试数据框大小: {size:,} 行")

            # 创建测试数据
            df = pd.DataFrame({
                'int_col': np.random.randint(0, 100, size),
                'float_col': np.random.randn(size),
                'str_col': ['test_' + str(i) for i in range(size)],
                'date_col': pd.date_range('2023-01-01', periods=size),
                'bool_col': np.random.choice([True, False], size)
            })

            # 原始内存
            start_time = time.time()
            original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
            original_time = time.time() - start_time

            # 优化后内存
            start_time = time.time()
            optimized_df = pm.optimize_dataframe(df)
            optimized_memory = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
            optimized_time = time.time() - start_time

            # 计算节省
            memory_saved = original_memory - optimized_memory
            memory_saved_percent = (memory_saved / original_memory * 100) if original_memory > 0 else 0

            print(f"  原始内存: {original_memory:.2f} MB ({original_time:.3f}s)")
            print(f"  优化内存: {optimized_memory:.2f} MB ({optimized_time:.3f}s)")
            print(f"  节省内存: {memory_saved:.2f} MB ({memory_saved_percent:.1f}%)")

            results.append({
                'size': size,
                'original_memory_mb': original_memory,
                'optimized_memory_mb': optimized_memory,
                'memory_saved_mb': memory_saved,
                'memory_saved_percent': memory_saved_percent,
                'original_time_s': original_time,
                'optimized_time_s': optimized_time
            })

        pm.stop()

        # 输出总结
        print("\n" + "=" * 40)
        print("数据框优化基准测试总结:")
        for r in results:
            print(f"  大小 {r['size']:,}: 节省 {r['memory_saved_percent']:.1f}% 内存")

        return results
    except Exception as e:
        print(f"数据框优化测试失败: {e}")
        return None


def benchmark_parallel_calculation():
    """基准测试：并行计算"""
    print("\n\n⚡ 并行计算基准测试")
    print("-" * 40)

    try:
        from src.performance.performance_manager import PerformanceManager

        pm = PerformanceManager()

        # 测试函数
        def complex_calculation(chunk):
            time.sleep(0.001)  # 模拟计算负载
            return [x * x * x for x in chunk]

        # 不同数据大小和工作线程数
        data_sizes = [1000, 5000, 10000]
        worker_counts = [1, 2, 4, 8]

        results = []
        for size in data_sizes:
            print(f"\n数据大小: {size:,}")
            data = list(range(size))

            for workers in worker_counts:
                # 配置并行计算器
                pm.parallel_calculator.config['max_workers'] = workers

                # 并行计算
                start_time = time.time()
                parallel_results = pm.parallel_calculate(
                    complex_calculation, data, batch_size=100
                )
                parallel_time = time.time() - start_time

                # 串行计算
                start_time = time.time()
                serial_results = complex_calculation(data)
                serial_time = time.time() - start_time

                # 验证结果
                assert parallel_results == serial_results, "结果不一致"

                # 计算加速比
                speedup = serial_time / parallel_time if parallel_time > 0 else 0
                efficiency = speedup / workers if workers > 0 else 0

                print(f"  工作线程 {workers}: 串行={serial_time:.3f}s, 并行={parallel_time:.3f}s, "
                      f"加速={speedup:.2f}x, 效率={efficiency:.1%}")

                results.append({
                    'data_size': size,
                    'workers': workers,
                    'serial_time_s': serial_time,
                    'parallel_time_s': parallel_time,
                    'speedup': speedup,
                    'efficiency': efficiency
                })

        pm.stop()

        # 输出总结
        print("\n" + "=" * 40)
        print("并行计算基准测试总结:")
        for size in data_sizes:
            size_results = [r for r in results if r['data_size'] == size]
            if size_results:
                best = max(size_results, key=lambda x: x['speedup'])
                print(f"  数据大小 {size:,}: 最佳加速 {best['speedup']:.2f}x (使用 {best['workers']} 线程)")

        return results
    except Exception as e:
        print(f"并行计算测试失败: {e}")
        return None


def benchmark_cache_performance():
    """基准测试：缓存性能"""
    print("\n\n💾 缓存性能基准测试")
    print("-" * 40)

    try:
        from src.performance.performance_manager import PerformanceManager

        pm = PerformanceManager()

        # 测试缓存命中率
        operations = 1000
        cache_hits = 0
        cache_misses = 0

        start_time = time.time()

        for i in range(operations):
            key = f"test_key_{i % 100}"  # 只有100个不同的key，增加缓存命中机会

            # 尝试获取缓存
            value = pm.get_cache(key)

            if value is None:
                # 缓存未命中，模拟计算并设置缓存
                cache_misses += 1
                value = {"data": i * i, "timestamp": datetime.now().isoformat()}
                pm.set_cache(key, value, ttl=60)
            else:
                # 缓存命中
                cache_hits += 1

        total_time = time.time() - start_time

        # 计算命中率
        hit_rate = cache_hits / operations if operations > 0 else 0
        avg_time_per_op = total_time / operations * 1000  # 毫秒

        print(f"  总操作数: {operations}")
        print(f"  缓存命中: {cache_hits}")
        print(f"  缓存未命中: {cache_misses}")
        print(f"  命中率: {hit_rate:.1%}")
        print(f"  总时间: {total_time:.3f}s")
        print(f"  平均操作时间: {avg_time_per_op:.3f}ms")

        # 获取缓存统计
        cache_stats = pm.cache_manager.get_cache_stats()
        print(f"  缓存统计: {cache_stats}")

        pm.stop()

        return {
            'operations': operations,
            'hits': cache_hits,
            'misses': cache_misses,
            'hit_rate': hit_rate,
            'total_time_s': total_time,
            'avg_time_ms': avg_time_per_op
        }
    except Exception as e:
        print(f"缓存性能测试失败: {e}")
        return None


def benchmark_monitoring_overhead():
    """基准测试：监控开销"""
    print("\n\n👁️ 监控开销基准测试")
    print("-" * 40)

    try:
        from src.monitoring.performance_monitor import PerformanceMonitor

        # 无监控的基准测试
        def test_function():
            total = 0
            for i in range(100000):
                total += i * i
            return total

        print("无监控测试...")
        start_time = time.time()
        for _ in range(100):
            test_function()
        no_monitor_time = time.time() - start_time

        print(f"  无监控时间: {no_monitor_time:.3f}s")

        # 有监控的基准测试
        monitor = PerformanceMonitor({
            'enabled': True,
            'interval': 1,
            'history_size': 100
        })

        monitor.start()

        print("有监控测试...")
        start_time = time.time()
        for _ in range(100):
            test_function()
        with_monitor_time = time.time() - start_time

        monitor.stop()

        print(f"  有监控时间: {with_monitor_time:.3f}s")

        # 计算开销
        overhead = with_monitor_time - no_monitor_time
        overhead_percent = (overhead / no_monitor_time * 100) if no_monitor_time > 0 else 0

        print(f"  监控开销: {overhead:.3f}s ({overhead_percent:.2f}%)")

        return {
            'no_monitor_time_s': no_monitor_time,
            'with_monitor_time_s': with_monitor_time,
            'overhead_s': overhead,
            'overhead_percent': overhead_percent
        }
    except Exception as e:
        print(f"监控开销测试失败: {e}")
        return None


def generate_benchmark_report(results_dict):
    """生成基准测试报告"""
    print("\n" + "=" * 60)
    print("P6阶段三 - 性能基准测试报告")
    print("=" * 60)
    print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if results_dict.get('dataframe'):
        print("\n📊 数据框优化性能:")
        for r in results_dict['dataframe']:
            print(f"  大小 {r['size']:,}: 节省 {r['memory_saved_percent']:.1f}% 内存")

    if results_dict.get('parallel'):
        print("\n⚡ 并行计算性能:")
        # 找出最佳配置
        best_speedup = max((r for r in results_dict['parallel']),
                           key=lambda x: x['speedup'], default=None)
        if best_speedup:
            print(f"  最佳配置: {best_speedup['workers']}线程, "
                  f"加速 {best_speedup['speedup']:.2f}x")

    if results_dict.get('cache'):
        print("\n💾 缓存性能:")
        cache_results = results_dict['cache']
        print(f"  命中率: {cache_results['hit_rate']:.1%}")
        print(f"  平均操作时间: {cache_results['avg_time_ms']:.3f}ms")

    if results_dict.get('monitoring'):
        print("\n👁️ 监控开销:")
        monitor_results = results_dict['monitoring']
        print(f"  开销: {monitor_results['overhead_percent']:.2f}%")

    print("\n" + "=" * 60)
    print("✅ 基准测试完成")


def main():
    """主函数"""
    print("🚀 P6阶段三 - 性能基准测试")
    print("=" * 60)

    results = {}

    # 运行各个基准测试
    try:
        results['dataframe'] = benchmark_dataframe_optimization()
    except Exception as e:
        print(f"数据框优化测试异常: {e}")

    try:
        results['parallel'] = benchmark_parallel_calculation()
    except Exception as e:
        print(f"并行计算测试异常: {e}")

    try:
        results['cache'] = benchmark_cache_performance()
    except Exception as e:
        print(f"缓存性能测试异常: {e}")

    try:
        results['monitoring'] = benchmark_monitoring_overhead()
    except Exception as e:
        print(f"监控开销测试异常: {e}")

    # 生成报告
    generate_benchmark_report(results)

    # 保存结果到文件
    output_file = "reports/performance_benchmark.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    import json
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n📁 详细结果已保存到: {output_file}")

    return True


if __name__ == "__main__":
    main()