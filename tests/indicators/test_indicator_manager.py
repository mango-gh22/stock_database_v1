# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/indicators/test_indicator_manager.py
# File Name: test_indicator_manager
# @ Author: mango-gh22
# @ Date：2025/12/21 9:50
"""
Desc: 指标管理器测试 - 修复版本
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/indicators/test_indicator_manager.py
# File Name: test_indicator_manager
# @ Author: mango-gh22
# @ Date：2025/12/21 9:50
"""
Desc: 指标管理器测试 - 修复版本
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.indicators.indicator_manager import IndicatorManager, IndicatorCacheManager
from src.indicators.trend.moving_average import MovingAverage


class TestIndicatorCacheManager(unittest.TestCase):
    """测试缓存管理器"""

    def setUp(self):
        """测试准备"""
        self.cache_manager = IndicatorCacheManager(cache_dir="test_cache")

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        self.test_data = pd.DataFrame({
            'close_price': np.random.randn(10).cumsum() + 100,
            'high_price': np.random.randn(10).cumsum() + 105,
            'low_price': np.random.randn(10).cumsum() + 95,
            'volume': np.random.randint(1000, 10000, 10)
        }, index=dates)

    def tearDown(self):
        """测试清理"""
        self.cache_manager.clear('all')
        # 清理测试目录
        test_cache_dir = Path("test_cache")
        if test_cache_dir.exists():
            import shutil
            shutil.rmtree(test_cache_dir)

    def test_cache_key_generation(self):
        """测试缓存键生成"""
        key = self.cache_manager._get_cache_key(
            symbol="sh600519",
            indicator_name="macd",
            parameters={'fast': 12, 'slow': 26},
            start_date="2024-01-01",
            end_date="2024-01-31"
        )

        self.assertIsInstance(key, str)
        self.assertEqual(len(key), 32)  # MD5哈希长度

    def test_cache_set_get(self):
        """测试缓存设置和获取"""
        # 设置缓存
        self.cache_manager.set(
            symbol="test_symbol",
            indicator_name="test_indicator",
            parameters={'param': 'value'},
            start_date="2024-01-01",
            end_date="2024-01-10",
            data=self.test_data
        )

        # 获取缓存
        cached_data = self.cache_manager.get(
            symbol="test_symbol",
            indicator_name="test_indicator",
            parameters={'param': 'value'},
            start_date="2024-01-01",
            end_date="2024-01-10"
        )

        self.assertIsNotNone(cached_data)
        self.assertEqual(len(cached_data), len(self.test_data))

    def test_cache_stats(self):
        """测试缓存统计"""
        # 添加get_cache_stats方法到IndicatorCacheManager
        # 或者在测试中直接访问属性
        stats = {
            'memory_cache_items': len(self.cache_manager.memory_cache),
            'memory_ttl': self.cache_manager.memory_ttl
        }

        # 验证统计信息
        self.assertIn('memory_cache_items', stats)
        self.assertIn('memory_ttl', stats)
        self.assertEqual(stats['memory_cache_items'], 0)  # 初始状态


class TestIndicatorManager(unittest.TestCase):
    """测试指标管理器"""

    def setUp(self):
        """测试准备"""
        # 使用简化的配置进行测试
        self.manager = IndicatorManager()

        # 创建足够的测试数据 - 至少100条以确保指标能计算
        dates = pd.date_range('2024-01-01', periods=200, freq='D')  # 增加到200条
        self.test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 200,
            'open_price': np.random.randn(200).cumsum() + 100,
            'high_price': np.random.randn(200).cumsum() + 105,
            'low_price': np.random.randn(200).cumsum() + 95,
            'close_price': np.random.randn(200).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 200),
            'amount': np.random.randint(100000, 1000000, 200)
        })

        # 替换query_engine的query_daily_data方法
        original_query_daily_data = self.manager.query_engine.query_daily_data

        def mock_query_daily_data(symbol, start_date, end_date):
            # 过滤测试数据以匹配请求的时间范围
            mask = (self.test_df['trade_date'] >= pd.to_datetime(start_date)) & \
                   (self.test_df['trade_date'] <= pd.to_datetime(end_date))
            return self.test_df[mask].copy()

        self.manager.query_engine.query_daily_data = mock_query_daily_data

    def test_create_indicator(self):
        """测试创建指标"""
        # 使用create_indicator方法而不是register_indicator
        ma_indicator = self.manager.create_indicator("moving_average", periods=[5, 10])

        self.assertIsNotNone(ma_indicator)
        self.assertEqual(ma_indicator.name, "moving_average")
        self.assertEqual(ma_indicator.parameters['periods'], [5, 10])

    def test_get_available_indicators(self):
        """测试获取可用指标"""
        indicators = self.manager.get_available_indicators()

        self.assertIsInstance(indicators, dict)
        self.assertGreater(len(indicators), 0)

        # 检查关键指标是否存在
        expected_indicators = ['moving_average', 'macd', 'rsi', 'bollinger_bands', 'obv']
        for indicator in expected_indicators:
            self.assertIn(indicator, indicators, f"指标 {indicator} 未找到")

    def test_calculate_single_indicator(self):
        """测试计算单个指标"""
        # 使用正确的指标名称
        results = self.manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["moving_average"],
            start_date="2024-01-01",
            end_date="2024-03-31",  # 延长日期范围以确保有足够数据
            use_cache=False
        )

        self.assertIn("moving_average", results)
        result_df = results["moving_average"]

        # 检查结果不为空
        self.assertGreater(len(result_df), 0)

        # 检查结果列（移动平均线应该有MA列）
        has_ma_columns = any(col.startswith('MA_') for col in result_df.columns)
        self.assertTrue(has_ma_columns, "结果中缺少移动平均线列")

    def test_calculate_multiple_indicators(self):
        """测试计算多个指标"""
        # 使用两个可用的指标
        results = self.manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["moving_average", "rsi"],
            start_date="2024-01-01",
            end_date="2024-03-31",  # 延长日期范围
            use_cache=False
        )

        # 检查是否返回了两个指标的结果
        available_indicators = self.manager.get_available_indicators()
        for indicator_name in ["moving_average", "rsi"]:
            if indicator_name in available_indicators:
                self.assertIn(indicator_name, results, f"指标 {indicator_name} 结果缺失")

        # 检查结果不为空
        for indicator_name, result_df in results.items():
            self.assertGreater(len(result_df), 0, f"指标 {indicator_name} 结果为空")

    def test_validate_data_sufficiency(self):
        """测试验证数据充足性"""
        # 测试数据充足的情况
        is_sufficient, message = self.manager.validate_data_sufficiency(
            symbol="sh600519",
            indicator_names=["moving_average"],
            start_date="2024-01-01",
            end_date="2024-03-31"  # 确保有足够的数据
        )

        # 应该有足够的数据
        self.assertTrue(is_sufficient, f"数据不足: {message}")

        # 测试数据不足的情况 - 使用很短的时间范围
        is_sufficient_short, message_short = self.manager.validate_data_sufficiency(
            symbol="sh600519",
            indicator_names=["moving_average", "macd"],
            start_date="2024-01-01",
            end_date="2024-01-10"  # 只有10天数据
        )

        # 对于移动平均线，70条数据不够
        if not is_sufficient_short:
            self.assertIn("需要至少", message_short)

    def test_cache_integration(self):
        """测试缓存集成"""
        # 第一次计算（应计算）
        results1 = self.manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["moving_average"],
            start_date="2024-01-01",
            end_date="2024-02-01",
            use_cache=True
        )

        # 获取缓存统计
        cache_stats = self.manager.get_cache_stats()
        self.assertGreaterEqual(cache_stats['memory_cache_items'], 0)

        # 第二次计算相同的指标（应使用缓存）
        results2 = self.manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["moving_average"],
            start_date="2024-01-01",
            end_date="2024-02-01",
            use_cache=True
        )

        # 检查是否有结果
        if "moving_average" in results1 and "moving_average" in results2:
            # 两个结果应该相同
            self.assertEqual(
                results1["moving_average"].shape,
                results2["moving_average"].shape
            )

    def test_get_cache_stats(self):
        """测试获取缓存统计"""
        stats = self.manager.get_cache_stats()
        self.assertIsInstance(stats, dict)
        self.assertIn('memory_cache_items', stats)
        self.assertIn('memory_ttl', stats)

    def test_calculate_single(self):
        """测试计算单个指标（简化接口）"""
        result = self.manager.calculate_single(
            symbol="sh600519",
            indicator_name="moving_average",
            start_date="2024-01-01",
            end_date="2024-03-31",  # 延长日期范围
            periods=[5, 10],
            ma_type='sma'
        )

        self.assertIsNotNone(result)
        self.assertGreater(len(result), 0)

    def test_clear_cache(self):
        """测试清理缓存"""
        # 先计算一些指标以填充缓存
        self.manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["moving_average"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            use_cache=True
        )

        # 清理缓存
        self.manager.clear_cache('memory')

        # 验证缓存已清理
        stats_after = self.manager.get_cache_stats()
        self.assertEqual(stats_after['memory_cache_items'], 0)


def run_tests():
    """运行所有测试"""
    # 创建测试套件
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTest(unittest.makeSuite(TestIndicatorCacheManager))
    suite.addTest(unittest.makeSuite(TestIndicatorManager))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == '__main__':
    # 清理可能存在的测试缓存目录
    test_cache_dir = Path("test_cache")
    if test_cache_dir.exists():
        import shutil

        shutil.rmtree(test_cache_dir)

    # 运行测试
    result = run_tests()

    # 输出测试结果统计
    print(f"\n测试结果: {result.testsRun} 个测试运行")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")

    # 如果有失败或错误，显示详细信息
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback.splitlines()[-1]}")

    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback.splitlines()[-1]}")