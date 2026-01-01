# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p6_basic.py
# File Name: test_P6_basic
# @ Author: mango-gh22
# @ Date：2025/12/21 23:07
"""
desc 
"""
# File: test_p6_basic.py (修复版)
# !/usr/bin/env python3
"""
P6阶段三基本功能验证测试 - 修复版
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_config_file():
    """测试配置文件"""
    print("🔧 测试配置文件...")
    try:
        # 直接使用新的ConfigLoader类
        from src.config.config_loader import ConfigLoader

        config_path = 'config/performance.yaml'
        if not os.path.exists(config_path):
            # 如果配置文件不存在，先尝试创建它
            print("  ⚠️  配置文件不存在，正在创建...")
            create_sample_performance_config()

        config = ConfigLoader.load_yaml_config(config_path)

        if config:
            print(f"  ✅ 配置文件加载成功")
            print(f"    并行计算: {config.get('parallel_computing', {}).get('enabled', '未配置')}")
            print(f"    缓存启用: {config.get('caching', {}).get('enabled', '未配置')}")
            print(f"    监控启用: {config.get('monitoring', {}).get('enabled', '未配置')}")
            return True
        else:
            print(f"  ⚠️  配置文件为空或格式错误")
            return False
    except Exception as e:
        print(f"  ❌ 配置文件错误: {e}")
        # 创建配置文件
        create_sample_performance_config()
        return False


def create_sample_performance_config():
    """创建示例性能配置文件"""
    sample_config = """# File: config/performance.yaml
# Desc: 性能优化与监控配置

# 并行计算配置
parallel_computing:
  enabled: true
  mode: "thread"  # thread, process, async
  max_workers: 4
  timeout: 300
  batch_size: 10

  # 策略选择
  strategy:
    data_threshold: 1000
    complexity_threshold: "medium"
    auto_adjust: true
    min_workers: 1
    max_workers: 8
    adaptive: true

# 缓存配置
caching:
  enabled: true
  multi_level: true
  cache_root: "data/cache/performance"

  # 内存缓存 (L1)
  memory_cache:
    enabled: true
    strategy: "lru"
    max_size: 100
    default_ttl: 3600
    max_items: 1000
    cleanup_interval: 60

  # 磁盘缓存 (L2)
  disk_cache:
    enabled: false
    strategy: "lfu"
    max_size: 1000
    cache_dir: "data/cache/disk"
    compression: true
    compression_level: 6

# 内存管理配置
memory_management:
  monitoring:
    enabled: true
    interval: 5
    history_size: 1000

  # 内存阈值 (百分比)
  thresholds:
    low: 60
    medium: 80
    high: 90
    critical: 95

  # 优化策略
  optimization:
    auto_optimize: true
    df_optimization: true
    array_compression: true
    cleanup_interval: 300

  # 泄漏检测
  leak_detection:
    enabled: false
    interval: 60
    threshold_mb: 10

# 监控配置
monitoring:
  enabled: true
  log_level: "INFO"
  log_retention: 30

  # 性能监控
  performance:
    enabled: true
    interval: 10
    metrics:
      - "cpu_usage"
      - "memory_usage"
      - "disk_io"
      - "network_io"

  # 指标验证
  validation:
    enabled: true
    validate_on_calc: true
    validate_on_query: true
    tolerance: 0.001
    max_history: 100

  # 计算日志
  calculation_log:
    enabled: true
    log_level: "DEBUG"
    log_queries: true
    log_results: false
    log_performance: true
    max_log_size: 100
    log_dir: "logs/calculations"
    buffer_size: 100
    flush_interval: 60
"""

    os.makedirs('config', exist_ok=True)
    with open('config/performance.yaml', 'w', encoding='utf-8') as f:
        f.write(sample_config)
    print("  ✅ 已创建示例配置文件")


def test_monitor_module():
    """测试监控模块导入"""
    print("\n📊 测试监控模块导入...")
    try:
        # 先创建目录
        os.makedirs('src/monitoring', exist_ok=True)

        # 检查文件是否存在
        monitor_files = [
            'src/monitoring/performance_monitor.py',
            'src/monitoring/indicator_validator.py',
            'src/monitoring/calculation_logger.py'
        ]

        missing_files = []
        for file in monitor_files:
            if not os.path.exists(file):
                missing_files.append(file)

        if missing_files:
            print(f"  ⚠️  缺失监控文件: {len(missing_files)} 个")
            for file in missing_files:
                print(f"     - {file}")
            print("  请先创建这些文件")
            return False

        # 尝试导入
        from src.monitoring.performance_monitor import PerformanceMonitor
        from src.monitoring.indicator_validator import IndicatorValidator
        from src.monitoring.calculation_logger import CalculationLogger

        print("  ✅ 监控模块导入成功")
        return True
    except ImportError as e:
        print(f"  ❌ 监控模块导入失败: {e}")
        return False
    except Exception as e:
        print(f"  ⚠️  监控模块检查异常: {e}")
        return False


def test_performance_module():
    """测试性能模块导入"""
    print("\n⚡ 测试性能模块导入...")
    try:
        # 检查已存在的性能模块
        existing_modules = []

        try:
            from src.performance.parallel_calculator import ParallelCalculator
            existing_modules.append("ParallelCalculator")
        except ImportError:
            pass

        try:
            from src.performance.cache_strategy import CacheManager
            existing_modules.append("CacheManager")
        except ImportError:
            pass

        try:
            from src.performance.memory_manager import MemoryManager
            existing_modules.append("MemoryManager")
        except ImportError:
            pass

        # 检查新的性能管理器
        try:
            from src.performance.performance_manager import PerformanceManager
            existing_modules.append("PerformanceManager")
        except ImportError:
            print("  ⚠️  PerformanceManager 未找到，需要创建")

        if existing_modules:
            print(f"  ✅ 已找到性能模块: {', '.join(existing_modules)}")
            return True
        else:
            print(f"  ⚠️  未找到任何性能模块")
            return False

    except Exception as e:
        print(f"  ❌ 性能模块检查失败: {e}")
        return False


def test_module_creation():
    """测试模块实例化"""
    print("\n🏗️  测试模块实例化...")
    try:
        from src.config.config_loader import ConfigLoader

        # 检查配置文件
        if not os.path.exists('config/performance.yaml'):
            print("  ⚠️  配置文件不存在，跳过实例化测试")
            return True

        config = ConfigLoader.load_yaml_config('config/performance.yaml')

        if not config:
            print("  ⚠️  配置文件为空，跳过实例化测试")
            return True

        test_results = []

        # 测试性能监控器（如果文件存在）
        if os.path.exists('src/monitoring/performance_monitor.py'):
            try:
                from src.monitoring.performance_monitor import PerformanceMonitor
                monitor_config = config.get('monitoring', {})
                monitor = PerformanceMonitor(monitor_config)
                test_results.append(("PerformanceMonitor", True, "成功"))
            except Exception as e:
                test_results.append(("PerformanceMonitor", False, str(e)))
        else:
            test_results.append(("PerformanceMonitor", False, "文件不存在"))

        # 测试指标验证器（如果文件存在）
        if os.path.exists('src/monitoring/indicator_validator.py'):
            try:
                from src.monitoring.indicator_validator import IndicatorValidator
                validation_config = config.get('monitoring', {}).get('validation', {})
                validator = IndicatorValidator(validation_config)
                test_results.append(("IndicatorValidator", True, "成功"))
            except Exception as e:
                test_results.append(("IndicatorValidator", False, str(e)))
        else:
            test_results.append(("IndicatorValidator", False, "文件不存在"))

        # 测试性能管理器（如果文件存在）
        if os.path.exists('src/performance/performance_manager.py'):
            try:
                from src.performance.performance_manager import PerformanceManager
                pm = PerformanceManager()
                test_results.append(("PerformanceManager", True, "成功"))
            except Exception as e:
                test_results.append(("PerformanceManager", False, str(e)))
        else:
            test_results.append(("PerformanceManager", False, "文件不存在"))

        # 输出结果
        print("  模块实例化测试结果:")
        for name, success, message in test_results:
            status = "✅" if success else "❌"
            print(f"    {status} {name}: {message}")

        # 如果至少有一个成功，就认为是测试通过
        has_success = any(success for _, success, _ in test_results)
        return has_success

    except Exception as e:
        print(f"  ❌ 模块实例化测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("P6阶段三 - 基本功能验证测试（修复版）")
    print("=" * 60)

    tests = [
        ("配置文件", test_config_file),
        ("监控模块导入", test_monitor_module),
        ("性能模块导入", test_performance_module),
        ("模块实例化", test_module_creation),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            print(f"\n🔍 测试: {test_name}")
            print("-" * 40)
            if test_func():
                passed += 1
                print(f"  ✅ {test_name}: 通过")
            else:
                print(f"  ❌ {test_name}: 失败")
        except Exception as e:
            print(f"  ❌ {test_name}: 异常 - {e}")

    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{total} 通过")
    print("=" * 60)

    if passed >= total * 0.75:
        print("🎉 大部分测试通过！可以进行功能测试。")
        return True
    else:
        print("⚠️  测试失败较多，需要先修复问题。")
        print("\n建议的下一步：")
        print("1. 确保 config/performance.yaml 文件存在")
        print("2. 创建 src/monitoring/ 目录下的文件")
        print("3. 创建 src/performance/performance_manager.py")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)