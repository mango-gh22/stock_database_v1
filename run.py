# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\run.py
# File Name: run
# @ Author: mango-gh22
# @ Date：2025/12/7 20:57
"""
desc 
"""

# # run.py - 放置在 E:\MyFile\stock_database_v1\
# import sys
# import os
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
#
# from src.data_processing.base_processor import main
# main()
# ------------------

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据库系统 - P6阶段三：性能优化与监控 (v0.6.0)
主入口文件
"""

from typing import Dict, Any, Optional
# 防御性导入检查
try:
    from typing import Dict, Any, Optional
    print("✅ typing 模块导入成功")
except ImportError as e:
    print(f"❌ typing 模块导入失败: {e}")
    from collections.abc import Dict  # 降级方案

import sys
import os
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path
import pandas as pd


# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)


def setup_logging(log_level=logging.INFO):
    """设置日志"""
    # 创建日志目录
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # 设置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format, date_format)
    console_handler.setFormatter(console_formatter)

    # 文件日志
    log_file = log_dir / f"stock_database_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format, date_format)
    file_handler.setFormatter(file_formatter)

    # 配置根日志器
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[console_handler, file_handler]
    )

    return logging.getLogger(__name__)


def validate_data():
    """验证数据 - P4核心功能"""
    logger = logging.getLogger(__name__)
    logger.info("🔍 数据验证报告")
    print("=" * 50)
    print("🔍 数据验证报告")
    print("=" * 50)

    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        try:
            # 获取统计信息
            stats = engine.get_data_statistics()

            if not stats:
                logger.error("无法获取数据统计")
                print("❌ 无法获取数据统计")
                return

            logger.info(f"数据统计: {stats}")
            print(f"\n📊 股票基本信息:")
            print(f"  总股票数: {stats.get('total_stocks', 0)}")
            print(f"  行业数量: {stats.get('industry_count', 0)}")

            print(f"\n📅 日线数据:")
            print(f"  总记录数: {stats.get('total_daily_records', 0)}")
            print(f"  最早日期: {stats.get('earliest_date', 'N/A')}")
            print(f"  最新日期: {stats.get('latest_date', 'N/A')}")
            print(f"  有数据的股票: {stats.get('stocks_with_data', 0)}")

            if stats.get('stock_list'):
                print(f"\n📋 股票列表 ({len(stats['stock_list'])} 只):")
                for i, symbol in enumerate(stats['stock_list'][:10], 1):
                    name = stats['stock_details'].get(symbol, '')
                    print(f"  {i:2}. {symbol} {name}")
                if len(stats['stock_list']) > 10:
                    print(f"  ... 还有 {len(stats['stock_list']) - 10} 只股票")

            logger.info("数据验证完成")
            print("\n✅ 数据验证完成")

        finally:
            engine.close()

    except Exception as e:
        logger.error(f"数据验证失败: {e}", exc_info=True)
        print(f"❌ 数据验证失败: {e}")


def p4_test():
    """P4测试 - 测试查询引擎"""
    logger = logging.getLogger(__name__)
    logger.info("P4查询引擎测试")
    print("🧪 P4查询引擎测试")
    print("=" * 50)

    try:
        from src.query.query_engine import test_query_engine
        test_query_engine()
        logger.info("P4测试完成")
        print("\n✅ P4测试完成")
    except Exception as e:
        logger.error(f"P4测试失败: {e}", exc_info=True)
        print(f"❌ P4测试失败: {e}")


def p4_demo():
    """P4演示 - 展示所有功能"""
    logger = logging.getLogger(__name__)
    logger.info("P4阶段功能演示")
    print("🚀 P4阶段功能演示")
    print("=" * 50)

    try:
        from src.query.query_engine import QueryEngine
        import pandas as pd

        engine = QueryEngine()

        print("\n1. 📊 数据统计演示")
        stats = engine.get_data_statistics()
        print(f"   数据库中有 {stats.get('total_stocks', 0)} 只股票")
        print(f"   和 {stats.get('total_daily_records', 0)} 条日线记录")

        if stats.get('stock_list'):
            print("\n2. 📈 数据查询演示")
            test_symbol = stats['stock_list'][0]
            print(f"   查询股票: {test_symbol}")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = str(row['trade_date'])[:10]
                    print(f"     {date_str}: {row['close']:.2f}")

            print("\n3. 💾 数据导出演示")
            os.makedirs('data/exports', exist_ok=True)
            export_file = engine.export_to_csv(filename='p4_demo_export.csv')
            print(f"   导出到: {export_file}")

        engine.close()
        logger.info("P4演示完成")
        print("\n🎉 P4演示完成!")

    except Exception as e:
        logger.error(f"演示失败: {e}", exc_info=True)
        print(f"❌ 演示失败: {e}")


# run.py - 完整修复版

# ... 保持所有导入和函数不变，只修改 p6_performance_test 函数 ...

# run.py - 完整修复版（仅修改 p6_performance_test 函数）

def p6_performance_test():
    """P6阶段三性能测试 - 修复版"""
    logger = logging.getLogger(__name__)
    logger.info("P6性能测试")
    print("🚀 P6阶段三性能测试")
    print("=" * 50)

    try:
        # 检查性能模块是否可用 - 显式导入fixed版本
        try:
            from src.performance.performance_manager import PerformanceManager
            from src.monitoring.performance_monitor import PerformanceMonitor
            from src.monitoring.indicator_validator import IndicatorValidator
            from src.monitoring.calculation_logger import CalculationLogger
            logger.info("✅ 性能模块导入成功")
        except ImportError as e:
            logger.error(f"性能模块导入失败: {e}")
            print("❌ 性能模块未找到，请先创建P6阶段三的文件")
            return

        print("\n1. 📊 初始化性能管理器...")
        # 使用修复版管理器
        performance_manager = PerformanceManager()

        with performance_manager:
            print("2. 🔧 测试性能优化功能...")

            # 测试数据框优化
            import pandas as pd
            import numpy as np

            # 创建测试数据
            test_data = pd.DataFrame({
                'symbol': ['000001.SZ'] * 1000,
                'date': pd.date_range('2023-01-01', periods=1000),
                'open': np.random.randn(1000) * 10 + 100,
                'high': np.random.randn(1000) * 10 + 105,
                'low': np.random.randn(1000) * 10 + 95,
                'close': np.random.randn(1000) * 10 + 100,
                'volume': np.random.randint(1000000, 10000000, 1000),
                'amount': np.random.randn(1000) * 1000000 + 5000000
            })

            original_memory = test_data.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"   原始数据框内存: {original_memory:.2f} MB")

            # 关键修复：添加返回值检查
            optimized_data = performance_manager.optimize_dataframe(test_data)

            # ===== 修复点1：确保 optimized_data 不为 None =====
            if optimized_data is None:
                logger.warning("optimize_dataframe 返回 None，使用原始数据")
                optimized_data = test_data  # 回退到原始数据

            # 确保是 DataFrame 类型
            if not isinstance(optimized_data, pd.DataFrame):
                logger.warning(f"optimize_dataframe 返回类型 {type(optimized_data)}，使用原始数据")
                optimized_data = test_data

            # 现在可以安全调用 memory_usage
            try:
                optimized_memory = optimized_data.memory_usage(deep=True).sum() / 1024 / 1024
                print(f"   优化后数据框内存: {optimized_memory:.2f} MB")
                print(
                    f"   内存减少: {original_memory - optimized_memory:.2f} MB ({((original_memory - optimized_memory) / original_memory * 100):.1f}%)")
            except Exception as e:
                logger.error(f"计算优化后内存失败: {e}")
                optimized_memory = original_memory
                print(f"   优化后内存计算失败，使用原始值: {optimized_memory:.2f} MB")

            # ... 保持其余测试代码不变 ...

            print("\n🎉 P6性能测试完成!")
            logger.info("P6性能测试完成")

    except Exception as e:
        logger.error(f"P6性能测试失败: {e}", exc_info=True)
        print(f"❌ P6性能测试失败: {e}")
        # 添加更详细的错误信息
        import traceback
        print("详细错误信息:")
        traceback.print_exc()


def p6_monitoring_demo():
    """P6监控演示"""
    logger = logging.getLogger(__name__)
    logger.info("P6监控演示")
    print("👁️ P6监控系统演示")
    print("=" * 50)

    try:
        from src.monitoring.performance_monitor import PerformanceMonitor
        from src.monitoring.indicator_validator import IndicatorValidator
        from src.monitoring.calculation_logger import CalculationLogger

        # 加载配置
        from src.config.config_loader import ConfigLoader
        config_path = Path(__file__).parent / 'config' / 'performance.yaml'
        config = ConfigLoader.load_yaml_config(str(config_path))

        print("\n1. 📊 初始化监控组件...")

        # 性能监控器
        monitor_config = config.get('monitoring', {})
        performance_monitor = PerformanceMonitor(monitor_config)
        performance_monitor.start()

        print("2. 🔄 收集性能数据...")
        time.sleep(2)  # 等待收集一些数据

        current_metrics = performance_monitor.get_current_metrics()
        if current_metrics:
            print(f"   CPU使用率: {current_metrics.get('cpu_percent', 0):.1f}%")
            print(
                f"   内存使用: {current_metrics.get('memory_used_mb', 0):.1f} MB ({current_metrics.get('memory_percent', 0):.1f}%)")
            print(f"   活动线程: {current_metrics.get('active_threads', 0)}")

        print("\n3. ✅ 演示指标验证...")
        indicator_validator = IndicatorValidator(monitor_config.get('validation', {}))

        import pandas as pd
        import numpy as np

        # 创建测试数据
        test_price_data = pd.DataFrame({
            'close': np.random.randn(100) * 10 + 100,
            'high': np.random.randn(100) * 10 + 105,
            'low': np.random.randn(100) * 10 + 95,
            'volume': np.random.randint(1000000, 10000000, 100)
        }, index=pd.date_range('2023-01-01', periods=100))

        # 模拟一个RSI指标
        test_rsi_data = pd.Series(np.random.uniform(0, 100, 100),
                                  index=pd.date_range('2023-01-01', periods=100))

        validation_result = indicator_validator.validate_indicator(
            "RSI", test_rsi_data, test_price_data, {"period": 14}
        )

        print(f"   指标: {validation_result.indicator_name}")
        print(f"   验证结果: {'✅ 通过' if validation_result.is_valid else '❌ 失败'}")
        print(f"   测试数据大小: {validation_result.test_data_size}")

        if validation_result.errors:
            print(f"   错误: {validation_result.errors}")
        if validation_result.warnings:
            print(f"   警告: {validation_result.warnings[:2]}")  # 只显示前两个警告

        print("\n4. 📝 演示计算日志...")
        log_config = monitor_config.get('calculation_log', {})
        calculation_logger = CalculationLogger(log_config)

        # 记录一些计算日志
        calculation_logger.log_calculation(
            indicator_name="MACD",
            symbol="000001.SZ",
            period="daily",
            calculation_type="real-time",
            parameters={"fast": 12, "slow": 26, "signal": 9},
            duration_ms=125.5,
            success=True,
            cache_hit=True,
            memory_usage_mb=45.2
        )

        calculation_logger.log_calculation(
            indicator_name="Bollinger Bands",
            symbol="000002.SZ",
            period="hourly",
            calculation_type="batch",
            parameters={"period": 20, "std": 2},
            duration_ms=234.7,
            success=False,
            error_message="数据不足",
            memory_usage_mb=67.8
        )

        print("   ✅ 计算日志记录完成")

        # 生成统计报告
        print("\n5. 📈 生成监控报告...")
        stats = calculation_logger.generate_statistics()
        if stats:
            summary = stats.get('summary', {})
            print(f"   总计算次数: {summary.get('total_calculations', 0)}")
            print(f"   成功率: {summary.get('success_rate', 0) * 100:.1f}%")
            print(f"   平均耗时: {summary.get('avg_duration_ms', 0):.1f} ms")

        # 停止监控
        performance_monitor.stop()

        print("\n🎉 P6监控演示完成!")
        logger.info("P6监控演示完成")

    except Exception as e:
        logger.error(f"P6监控演示失败: {e}", exc_info=True)
        print(f"❌ P6监控演示失败: {e}")


# File: run.py 中的 p6_full_integration 函数修复
def p6_full_integration():
    """P6完整集成演示 - 修复版"""
    logger = logging.getLogger(__name__)
    logger.info("P6完整集成演示")
    print("🚀 P6阶段三完整集成演示")
    print("=" * 50)

    try:
        from src.performance.performance_manager import PerformanceManager
        from src.query.query_engine import QueryEngine
        from src.indicators.indicator_manager import IndicatorManager
        import pandas as pd
        import numpy as np
        import time

        print("\n1. 🔧 初始化所有系统组件...")

        # 初始化性能管理器
        performance_manager = PerformanceManager()
        performance_manager.start()

        # 初始化查询引擎
        query_engine = QueryEngine()

        # 初始化指标管理器
        indicator_manager = IndicatorManager()

        with performance_manager:
            print("2. 📊 执行带性能监控的查询...")

            # 获取股票列表
            stats = query_engine.get_data_statistics()

            if not stats or not stats.get('stock_list'):
                print("⚠️  没有找到股票数据")
                return

            # 使用数据库中实际存在的日期
            start_date = stats.get('earliest_date', '2024-01-01')
            test_symbols = stats['stock_list'][:5]  # 前5只股票

            print(f"测试股票: {test_symbols}")
            print(f"日期范围: {start_date} 至今")

            print("\n3. ⚡ 并行计算指标...")

            all_results = {}

            # 修复：为每只股票计算RSI
            for symbol in test_symbols:
                print(f"   处理 {symbol}...")

                try:
                    # 查询数据 - 使用实际日期
                    import datetime as dt
                    data = query_engine.query_daily_data(
                        symbol=symbol,
                        start_date=start_date,  # 使用实际数据起始日期
                        limit=100
                    )

                    # 调试信息
                    print(f"     🔍 数据类型: {type(data)}, 记录数: {len(data) if data is not None else 0}")

                    # 修复：确保返回的是 DataFrame
                    if isinstance(data, list):
                        if len(data) == 0:
                            print(f"     ⚠️  {symbol} 无数据，跳过")
                            continue
                        # 如果是列表，转换为 DataFrame
                        import pandas as pd
                        data = pd.DataFrame(data)
                    elif data is None or (hasattr(data, 'empty') and data.empty):
                        print(f"     ⚠️  {symbol} 无数据，跳过")
                        continue

                    if not data.empty:
                        print(f"     ✅ 查询到 {len(data)} 条记录")

                        # ✅ 修复：使用正确的计算方法
                        # 记录计算开始
                        log_id = performance_manager.calculation_logger.log_calculation_start(
                            indicator_name="rsi",  # 注意：使用小写的 'rsi'
                            symbol=symbol,
                            period="daily",
                            calculation_type="parallel_batch",
                            parameters={"period": 14},
                            input_data_shape=data.shape
                        )

                        start_time = time.time()

                        try:
                            # ✅ 修复：使用正确的方法名 calculate_single
                            # 注意：使用小写的 'rsi' 作为指标名称
                            rsi_result = indicator_manager.calculate_single(
                                symbol=symbol,
                                indicator_name="rsi",  # 注意：使用小写的 'rsi'
                                start_date=start_date,
                                end_date=datetime.now().strftime('%Y-%m-%d'),
                                period=14
                            )

                            duration_ms = (time.time() - start_time) * 1000

                            # ✅ 修复：使用正确的参数调用 log_calculation_end
                            # calculation_logger.log_calculation_end 应该这样调用：
                            performance_manager.calculation_logger.log_calculation_end(
                                log_id=log_id,
                                success=rsi_result is not None and not rsi_result.empty,
                                output_data_shape=rsi_result.shape if rsi_result is not None else None,
                                error_message=None if (rsi_result is not None and not rsi_result.empty) else "计算失败",
                                cache_hit=False,
                                cache_key=None,
                                performance_metrics={},
                                memory_usage_mb=data.memory_usage(
                                    deep=True).sum() / 1024 / 1024 if data is not None else 0,
                                tags=["rsi_calculation"],
                                duration_ms=duration_ms  # 注意：这里传递 duration_ms
                            )

                            if rsi_result is not None and not rsi_result.empty:
                                # 提取RSI值
                                if 'RSI' in rsi_result.columns:
                                    rsi_values = rsi_result['RSI'].values
                                else:
                                    rsi_values = rsi_result.iloc[:, 0].values if len(
                                        rsi_result.columns) > 0 else np.array([])

                                all_results[symbol] = {
                                    'rsi': rsi_values,
                                    'data_count': len(data),
                                    'result_shape': rsi_result.shape
                                }

                                print(f"     ✅ {symbol} RSI计算完成 ({len(rsi_values)}个值)")
                            else:
                                print(f"     ❌ {symbol} RSI计算结果为空")

                        except Exception as e:
                            duration_ms = (time.time() - start_time) * 1000
                            # 记录失败的计算
                            performance_manager.calculation_logger.log_calculation_end(
                                log_id=log_id,
                                success=False,
                                error_message=str(e),
                                duration_ms=duration_ms
                            )
                            print(f"     ❌ {symbol} 计算失败: {e}")
                    else:
                        print(f"     ⚠️  {symbol} 无数据")

                except Exception as e:
                    print(f"     ❌ {symbol} 处理异常: {e}")
                    import traceback
                    traceback.print_exc()

            print(f"\n4. 📈 生成综合性能报告...")
            print(f"   共处理 {len(all_results)} 只股票的指标计算")

            if all_results:
                # 显示前几个结果
                for symbol, result in list(all_results.items())[:3]:
                    rsi_values = result['rsi']
                    if len(rsi_values) > 0:
                        print(
                            f"   {symbol}: {len(rsi_values)} 个RSI值，最后5个: {rsi_values[-5:] if len(rsi_values) >= 5 else rsi_values}")
                    else:
                        print(f"   {symbol}: RSI值为空")

                print("\n✅ 所有计算完成")
            else:
                print("\n⚠️  没有成功处理任何股票")

        # 清理资源
        query_engine.close()
        performance_manager.stop()

    except Exception as e:
        logger.error(f"P6完整集成演示失败: {e}", exc_info=True)
        print(f"❌ P6完整集成演示失败: {e}")
        import traceback
        traceback.print_exc()


def collect_data():
    """数据采集"""
    logger = logging.getLogger(__name__)
    logger.info("数据采集")
    print("📥 数据采集")
    print("=" * 50)

    try:
        # 导入所需模块
        from src.data.data_pipeline import DataPipeline
        # from src.data.data_collector import DataCollector
        from src.data.data_collector import get_data_collector
        from src.data.data_storage import DataStorage
        from src.performance.performance_manager import PerformanceManager

        print("初始化数据采集管道...")

        # 初始化 collector 和 storage（根据你的项目结构）
        # collector = DataCollector()
        collector = get_data_collector('baostock')
        storage = DataStorage()

        # 使用性能管理器优化采集过程
        with PerformanceManager() as pm:
            pipeline = DataPipeline(collector=collector, storage=storage)

            print("开始数据采集...")
            result = pipeline.run_incremental_update()

            if result.get('success'):
                print(f"✅ 数据采集成功")
                print(f"   新增记录: {result.get('new_records', 0)}")
                print(f"   更新记录: {result.get('updated_records', 0)}")
                print(f"   耗时: {result.get('duration', 0):.1f} 秒")

                # 记录性能指标
                report = pm.get_performance_report()
                cpu_avg = report.get('performance', {}).get('cpu', {}).get('avg', 0)
                print(f"   CPU平均使用率: {cpu_avg:.1f}%")
            else:
                print(f"❌ 数据采集失败: {result.get('error', '未知错误')}")

    except Exception as e:
        logger.error(f"数据采集失败: {e}", exc_info=True)
        print(f"❌ 数据采集失败: {e}")


def query_data(symbol=None, start_date=None, end_date=None, limit=10):
    """数据查询"""
    logger = logging.getLogger(__name__)

    try:
        from src.query.query_engine import QueryEngine
        from src.performance.performance_manager import PerformanceManager

        with PerformanceManager() as pm:
            engine = QueryEngine()

            if symbol:
                logger.info(f"查询股票 {symbol}")
                print(f"📈 查询股票 {symbol}")
                print("=" * 50)

                data = engine.query_daily_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit
                )

                if not data.empty:
                    print(f"查询到 {len(data)} 条记录:")
                    print(data[['trade_date', 'symbol', 'close', 'volume', 'amount']].to_string())

                    # 记录查询性能
                    pm.log_calculation(
                        indicator_name="Data Query",
                        symbol=symbol,
                        period="daily",
                        calculation_type="query",
                        parameters={
                            "start_date": start_date,
                            "end_date": end_date,
                            "limit": limit
                        },
                        duration_ms=0,  # 实际应该计时
                        success=True,
                        input_data_shape=(len(data), len(data.columns))
                    )
                else:
                    print(f"未找到 {symbol} 的数据")
            else:
                print("查询所有股票统计...")
                stats = engine.get_data_statistics()

                if stats:
                    print(f"\n📊 数据库统计:")
                    print(f"   总股票数: {stats.get('total_stocks', 0)}")
                    print(f"   日线记录: {stats.get('total_daily_records', 0)}")
                    print(f"   最早日期: {stats.get('earliest_date', 'N/A')}")
                    print(f"   最新日期: {stats.get('latest_date', 'N/A')}")

                    if stats.get('stock_list'):
                        print(f"\n前10只股票:")
                        for i, sym in enumerate(stats['stock_list'][:10], 1):
                            name = stats['stock_details'].get(sym, '')
                            print(f"  {i:2}. {sym} {name}")

            engine.close()

    except Exception as e:
        logger.error(f"数据查询失败: {e}", exc_info=True)
        print(f"❌ 数据查询失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='股票数据库系统 v0.6.0 - P6阶段三：性能优化与监控',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python run.py --action validate          # 验证数据
  python run.py --action query --symbol 000001.SZ --limit 5  # 查询数据
  python run.py --action p6_test          # P6性能测试
  python run.py --action p6_monitor       # P6监控演示
  python run.py --action p6_integration   # P6完整集成演示
  python run.py --action collect          # 采集数据

性能优化命令:
  python run.py --action benchmark        # 运行基准测试
  python run.py --action monitor          # 实时监控
  python run.py --action report           # 生成性能报告
        """
    )

    # 主动作参数
    parser.add_argument('--action', default='validate',
                        choices=['validate', 'p4_test', 'p4_demo', 'collect', 'query',
                                 'p6_test', 'p6_monitor', 'p6_integration',
                                 'benchmark', 'monitor', 'report'],
                        help='执行动作')

    # 查询参数
    parser.add_argument('--symbol', help='股票代码')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=10, help='查询限制')

    # 性能参数
    parser.add_argument('--log_level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='日志级别')
    parser.add_argument('--config', help='配置文件路径')

    args = parser.parse_args()

    # 设置日志
    log_level = getattr(logging, args.log_level)
    logger = setup_logging(log_level)

    logger.info(f"启动股票数据库系统 v0.6.0")
    logger.info(f"执行动作: {args.action}")

    print(f"\n📊 股票数据库系统 v0.6.0")
    print(f"🔄 执行动作: {args.action}")
    print("=" * 50)

    try:
        # 根据action执行相应的函数
        if args.action == "validate":
            validate_data()

        elif args.action == "p4_test":
            p4_test()

        elif args.action == "p4_demo":
            p4_demo()

        elif args.action == "collect":
            collect_data()

        elif args.action == "query":
            query_data(args.symbol, args.start_date, args.end_date, args.limit)

        elif args.action == "p6_test":
            p6_performance_test()

        elif args.action == "p6_monitor":
            p6_monitoring_demo()

        elif args.action == "p6_integration":
            p6_full_integration()

        elif args.action == "benchmark":
            print("🔧 运行基准测试...")
            # 这里可以调用专门的基准测试函数
            p6_performance_test()

        elif args.action == "monitor":
            print("👁️ 启动实时监控...")
            from src.monitoring.performance_monitor import PerformanceMonitor
            from src.config.config_loader import ConfigLoader

            config = ConfigLoader.load_yaml_config('config/performance.yaml')
            monitor = PerformanceMonitor(config.get('monitoring', {}))
            monitor.start()

            print("监控已启动，按 Ctrl+C 停止...")
            try:
                while True:
                    metrics = monitor.get_current_metrics()
                    if metrics:
                        print(f"\rCPU: {metrics.get('cpu_percent', 0):5.1f}% | "
                              f"内存: {metrics.get('memory_percent', 0):5.1f}% | "
                              f"线程: {metrics.get('active_threads', 0):3d}",
                              end='', flush=True)
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n\n停止监控...")
                monitor.stop()
                print("监控已停止")

        elif args.action == "report":
            print("📈 生成性能报告...")
            try:
                import pandas as pd  # ✅ 延迟导入
                from src.performance.performance_manager import PerformanceManager
                # from src.performance.performance_manager_fixed import PerformanceManagerFixed

                pm = PerformanceManager()
                # pm = PerformanceManagerFixed()
                report = pm.get_performance_report()

                # # ✅ 临时：打印报告结构
                # print("报告结构:", type(report))
                # print("报告键:", report.keys() if report else "None")


                if report:
                    print("\n性能报告摘要:")
                    print("-" * 40)

                    if report.get('performance'):
                        perf = report['performance']
                        print(f"性能监控:")
                        print(f"  时长: {perf.get('duration_seconds', 0):.0f}秒")
                        print(
                            f"  CPU: {perf.get('cpu', {}).get('avg', 0):.1f}% (最大{perf.get('cpu', {}).get('max', 0):.1f}%)")
                        print(f"  内存: {perf.get('memory', {}).get('avg', 0):.1f}%")

                    if report.get('cache'):
                        cache = report['cache']
                        print(f"缓存统计:")
                        print(f"  命中率: {cache.get('hit_rate', 0) * 100:.1f}%")
                        print(f"  总项目: {cache.get('total_items', 0)}")

                    print(f"\n详细报告已生成")
                else:
                    print("无法生成性能报告")

                pm.stop()

            except Exception as e:
                print(f"生成报告失败: {e}")

        else:
            print(f"⚠️ 未知动作: {args.action}")
            print("可用动作: validate, p4_test, p4_demo, collect, query, "
                  "p6_test, p6_monitor, p6_integration, benchmark, monitor, report")

    except KeyboardInterrupt:
        logger.info("用户中断程序")
        print("\n\n👋 程序被用户中断")

    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        print(f"\n❌ 执行失败: {e}")

    finally:
        logger.info("程序执行完成")
        print("\n" + "=" * 50)
        print("✅ 程序执行完成")
        print(f"📅 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
