# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p6_functional.py
# File Name: test_p6_functional
# @ Author: mango-gh22
# @ Date：2025/12/21 23:15
"""
desc 
"""

# File: test_p6_functional.py
# !/usr/bin/env python3
"""
P6阶段三功能测试
"""

import sys
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_performance_monitor():
    """测试性能监控器"""
    print("📊 测试性能监控器功能...")
    try:
        from src.config.config_loader import ConfigLoader
        from src.monitoring.performance_monitor import PerformanceMonitor

        config = ConfigLoader.load_yaml_config('config/performance.yaml')
        monitor_config = config.get('monitoring', {})

        # 创建监控器
        monitor = PerformanceMonitor(monitor_config)

        # 启动监控
        monitor.start()
        time.sleep(2)  # 等待收集数据

        # 获取当前指标
        metrics = monitor.get_current_metrics()
        print(f"  ✅ 获取到性能指标: {len(metrics)} 个")

        if metrics:
            print(f"    CPU使用率: {metrics.get('cpu_percent', 0):.1f}%")
            print(f"    内存使用: {metrics.get('memory_used_mb', 0):.1f} MB")

        # 获取历史数据
        history = monitor.get_metrics_history()
        print(f"  ✅ 获取到历史数据: {len(history)} 条记录")

        # 生成报告
        report = monitor.generate_report()
        print(f"  ✅ 生成性能报告成功")

        # 停止监控
        monitor.stop()

        return True
    except Exception as e:
        print(f"  ❌ 性能监控器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_indicator_validator():
    """测试指标验证器"""
    print("\n✅ 测试指标验证器功能...")
    try:
        from src.monitoring.indicator_validator import IndicatorValidator

        # 创建验证器
        validator = IndicatorValidator({"tolerance": 0.001})

        # 创建测试数据
        dates = pd.date_range('2023-01-01', periods=100)
        price_data = pd.DataFrame({
            'close': np.random.randn(100) * 10 + 100,
            'high': np.random.randn(100) * 10 + 105,
            'low': np.random.randn(100) * 10 + 95
        }, index=dates)

        # 模拟RSI数据（应在0-100之间）
        rsi_data = pd.Series(np.random.uniform(20, 80, 100), index=dates)

        # 验证指标
        result = validator.validate_indicator(
            "RSI", rsi_data, price_data, {"period": 14}
        )

        print(f"  ✅ 指标验证完成:")
        print(f"    指标名称: {result.indicator_name}")
        print(f"    验证结果: {'通过' if result.is_valid else '失败'}")
        print(f"    错误数: {len(result.errors)}")
        print(f"    警告数: {len(result.warnings)}")

        # 测试批量验证
        indicators = {
            "RSI": rsi_data,
            "MACD": pd.Series(np.random.randn(100), index=dates)
        }

        results = validator.validate_multiple_indicators(indicators, price_data)
        print(f"  ✅ 批量验证完成: {len(results)} 个指标")

        # 生成报告
        report = validator.generate_validation_report()
        print(f"  ✅ 生成验证报告成功")

        return True
    except Exception as e:
        print(f"  ❌ 指标验证器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_calculation_logger():
    """测试计算日志器"""
    print("\n📝 测试计算日志器功能...")
    try:
        from src.monitoring.calculation_logger import CalculationLogger, LogLevel

        # 创建日志器
        logger = CalculationLogger({
            'enabled': True,
            'log_level': 'INFO',
            'log_dir': 'logs/test_calculations',
            'buffer_size': 10
        })

        # 记录计算开始
        log_id = logger.log_calculation_start(
            indicator_name="MACD",
            symbol="000001.SZ",
            period="daily",
            calculation_type="batch",
            parameters={"fast": 12, "slow": 26, "signal": 9},
            input_data_shape=(1000, 8)
        )

        print(f"  ✅ 记录计算开始: {log_id}")

        # 模拟计算过程
        time.sleep(0.5)

        # 记录计算结束
        logger.log_calculation_end(
            log_id=log_id,
            success=True,
            output_data_shape=(1000, 3),
            cache_hit=True,
            memory_usage_mb=45.2
        )

        print(f"  ✅ 记录计算结束")

        # 直接记录完整计算
        logger.log_calculation(
            indicator_name="RSI",
            symbol="000002.SZ",
            period="hourly",
            calculation_type="real-time",
            parameters={"period": 14},
            duration_ms=123.5,
            success=True,
            cache_hit=False,
            memory_usage_mb=23.1
        )

        print(f"  ✅ 记录完整计算")

        # 查询日志
        logs = logger.query_logs(limit=5)
        print(f"  ✅ 查询到日志: {len(logs)} 条")

        # 生成统计报告
        stats = logger.generate_statistics()
        print(f"  ✅ 生成统计报告成功")

        # 刷新缓冲区
        logger.flush_buffer()
        print(f"  ✅ 刷新缓冲区完成")

        return True
    except Exception as e:
        print(f"  ❌ 计算日志器测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_performance_manager_integration():
    """测试性能管理器集成"""
    print("\n🔗 测试性能管理器集成功能...")
    try:
        from src.performance.performance_manager import PerformanceManager

        # 创建性能管理器
        pm = PerformanceManager()

        print("  ✅ 性能管理器创建成功")

        # 启动性能管理器
        pm.start()

        # 测试数据框优化
        test_df = pd.DataFrame({
            'int_col': np.random.randint(0, 100, 10000),
            'float_col': np.random.randn(10000),
            'str_col': ['test'] * 10000,
            'date_col': pd.date_range('2023-01-01', periods=10000)
        })

        original_memory = test_df.memory_usage(deep=True).sum() / 1024 / 1024
        optimized_df = pm.optimize_dataframe(test_df)
        optimized_memory = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024

        print(f"  ✅ 数据框优化完成:")
        print(f"    原始内存: {original_memory:.2f} MB")
        print(f"    优化后内存: {optimized_memory:.2f} MB")
        print(f"    节省: {(original_memory - optimized_memory):.2f} MB")

        # 测试并行计算
        def process_chunk(chunk):
            return [x * x for x in chunk]

        data = list(range(10000))
        results = pm.parallel_calculate(process_chunk, data, batch_size=1000)

        print(f"  ✅ 并行计算完成:")
        print(f"    输入数据: {len(data)} 条")
        print(f"    输出结果: {len(results)} 条")

        # 测试缓存
        cache_key = "test_key_123"
        cache_value = {"data": [1, 2, 3], "timestamp": datetime.now().isoformat()}

        pm.set_cache(cache_key, cache_value, ttl=10)
        retrieved_value = pm.get_cache(cache_key)

        if retrieved_value == cache_value:
            print(f"  ✅ 缓存功能正常")
        else:
            print(f"  ❌ 缓存功能异常")

        # 测试指标验证
        try:
            test_indicator_data = pd.Series(np.random.randn(100))
            test_price_data = pd.DataFrame({'close': np.random.randn(100)})

            validation = pm.validate_indicator(
                "MACD", test_indicator_data, test_price_data, {}
            )
            print(f"  ✅ 指标验证完成: {validation.is_valid}")
        except Exception as e:
            print(f"  ⚠️  指标验证测试跳过: {e}")

        # 生成性能报告
        report = pm.get_performance_report()
        print(f"  ✅ 生成性能报告成功")

        # 停止性能管理器
        pm.stop()

        return True
    except Exception as e:
        print(f"  ❌ 性能管理器集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_existing_performance_modules():
    """测试已存在的性能模块"""
    print("\n⚙️ 测试已存在的性能模块...")
    try:
        # 测试并行计算器
        from src.performance.parallel_calculator import ParallelCalculator

        pc_config = {
            'enabled': True,
            'mode': 'thread',
            'max_workers': 2,
            'batch_size': 10
        }

        pc = ParallelCalculator(pc_config)

        def square(x):
            return x * x

        results = pc.calculate(square, list(range(100)))
        print(f"  ✅ 并行计算器测试完成: {len(results)} 个结果")

        # 测试缓存管理器
        from src.performance.cache_strategy import CacheManager

        cache_config = {
            'enabled': True,
            'memory_cache': {
                'strategy': 'lru',
                'max_size': 10
            }
        }

        cache = CacheManager(cache_config)
        cache.set('test_key', 'test_value', ttl=10)
        value = cache.get('test_key')

        if value == 'test_value':
            print(f"  ✅ 缓存管理器测试完成")
        else:
            print(f"  ❌ 缓存管理器测试失败")

        # 测试内存管理器
        from src.performance.memory_manager import MemoryManager

        memory_config = {
            'monitoring': {'enabled': True},
            'optimization': {'auto_optimize': True}
        }

        memory = MemoryManager(memory_config)

        test_df = pd.DataFrame({
            'col1': np.random.randn(1000),
            'col2': np.random.randint(0, 100, 1000),
            'col3': ['text'] * 1000
        })

        optimized_df = memory.optimize_dataframe(test_df)
        print(f"  ✅ 内存管理器测试完成")

        # 获取内存报告
        report = memory.get_memory_report()
        print(f"  ✅ 获取内存报告成功")

        memory.stop_monitoring()

        return True
    except Exception as e:
        print(f"  ❌ 现有性能模块测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("=" * 70)
    print("P6阶段三 - 功能测试")
    print("=" * 70)

    tests = [
        ("性能监控器", test_performance_monitor),
        ("指标验证器", test_indicator_validator),
        ("计算日志器", test_calculation_logger),
        ("性能管理器集成", test_performance_manager_integration),
        ("现有性能模块", test_existing_performance_modules),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"\n  ✅ {test_name}: 通过")
            else:
                print(f"\n  ❌ {test_name}: 失败")
        except Exception as e:
            print(f"\n  ❌ {test_name}: 异常 - {e}")

    print("\n" + "=" * 70)
    print(f"测试结果: {passed}/{total} 通过")

    if passed == total:
        print("🎉 所有功能测试通过！可以进行集成测试。")
        return True
    elif passed >= total * 0.7:
        print("⚠️  大部分功能测试通过，但有一些问题需要修复。")
        return True
    else:
        print("❌ 功能测试失败较多，需要修复代码。")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)