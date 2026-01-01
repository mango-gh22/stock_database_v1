# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/query\test_enhanced_query.py
# File Name: test_enhanced_query
# @ Author: mango-gh22
# @ Date：2025/12/21 19:44
"""
Desc: 增强查询引擎测试
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/query\test_enhanced_query.py
# File Name: test_enhanced_query
# @ Author: mango-gh22
# @ Date：2025/12/21 19:44
"""
Desc: 增强查询引擎测试
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import tempfile
import json

# 添加项目根目录
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.query.data_pipeline import DataPipeline, DataPreprocessor, DataQuality, DataQualityReport
from src.query.result_formatter import ResultFormatter, BatchResultFormatter
from src.query.enhanced_query_engine import EnhancedQueryEngine
from unittest.mock import MagicMock, patch, Mock


class TestDataPreprocessor(unittest.TestCase):
    """测试数据预处理器"""

    def setUp(self):
        """测试准备"""
        self.preprocessor = DataPreprocessor()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=50, freq='D')
        self.test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 50,
            'open_price': np.random.randn(50).cumsum() + 100,
            'high_price': np.random.randn(50).cumsum() + 105,
            'low_price': np.random.randn(50).cumsum() + 95,
            'close_price': np.random.randn(50).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 50),
            'amount': np.random.randint(100000, 1000000, 50)
        })

        # 添加一些缺失值
        self.test_df.loc[10:15, 'close_price'] = np.nan
        self.test_df.loc[20, 'volume'] = np.nan

    def test_basic_processing(self):
        """测试基础处理"""
        processed_df = self.preprocessor._basic_processing(self.test_df.copy(), 'sh600519')

        # 检查是否设置了索引
        self.assertIsInstance(processed_df.index, pd.DatetimeIndex)

        # 检查是否添加了symbol列
        self.assertIn('symbol', processed_df.columns)

        # 检查排序
        self.assertTrue(processed_df.index.is_monotonic_increasing)

    def test_check_data_quality(self):
        """测试数据质量检查"""
        report = self.preprocessor._check_data_quality(
            self.test_df, 'sh600519', '2024-01-01', '2024-02-01'
        )

        # 检查报告结构
        self.assertIsInstance(report, DataQualityReport)
        self.assertEqual(report.symbol, 'sh600519')
        self.assertEqual(report.total_rows, 50)

        # 检查质量分数
        self.assertGreaterEqual(report.quality_score, 0)
        self.assertLessEqual(report.quality_score, 1)

        # 检查质量等级
        self.assertIsInstance(report.quality_level, DataQuality)

        # 检查缺失数据统计
        self.assertIn('close_price', report.missing_data)

    def test_handle_missing_values(self):
        """测试缺失值处理"""
        # 复制数据并添加更多缺失值
        df_with_nan = self.test_df.copy()
        df_with_nan.loc[5:10, 'open_price'] = np.nan

        processed_df = self.preprocessor._handle_missing_values(df_with_nan)

        # 检查缺失值是否被填充
        self.assertFalse(processed_df['close_price'].isnull().any())
        self.assertFalse(processed_df['open_price'].isnull().any())

        # 检查成交量缺失值是否用0填充
        if 'volume' in processed_df.columns:
            self.assertEqual(processed_df['volume'].isnull().sum(), 0)

    def test_enhance_data(self):
        """测试数据增强"""
        enhanced_df = self.preprocessor._enhance_data(self.test_df.copy())

        # 检查是否添加了衍生特征
        expected_cols = ['price_change', 'price_pct_change', 'sma_5', 'sma_10']
        for col in expected_cols:
            if col not in ['sma_5', 'sma_10'] or len(self.test_df) >= 10:
                self.assertIn(col, enhanced_df.columns)

        # 检查价格范围
        if all(col in self.test_df.columns for col in ['high_price', 'low_price']):
            self.assertIn('price_range', enhanced_df.columns)

        # 检查日期特征
        if isinstance(enhanced_df.index, pd.DatetimeIndex):
            self.assertIn('day_of_week', enhanced_df.columns)
            self.assertIn('month', enhanced_df.columns)

    def test_batch_preprocess(self):
        """测试批量预处理"""
        data_dict = {
            'sh600519': self.test_df.copy(),
            'sz000001': self.test_df.copy()
        }

        results = self.preprocessor.batch_preprocess(
            data_dict, '2024-01-01', '2024-02-01'
        )

        # 检查结果数量
        self.assertEqual(len(results), 2)

        # 检查每个结果
        for symbol, (df, report) in results.items():
            self.assertIsInstance(df, pd.DataFrame)
            self.assertIsInstance(report, DataQualityReport)
            self.assertEqual(report.symbol, symbol)


class TestDataPipeline(unittest.TestCase):
    """测试数据管道"""

    def setUp(self):
        """测试准备"""
        self.pipeline = DataPipeline()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        self.test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 30,
            'open_price': np.random.randn(30).cumsum() + 100,
            'high_price': np.random.randn(30).cumsum() + 105,
            'low_price': np.random.randn(30).cumsum() + 95,
            'close_price': np.random.randn(30).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 30)
        })

    def test_process(self):
        """测试处理流程"""
        processed_df, report = self.pipeline.process(
            self.test_df, 'sh600519', '2024-01-01', '2024-01-31'
        )

        # 检查返回类型
        self.assertIsInstance(processed_df, pd.DataFrame)
        self.assertIsInstance(report, DataQualityReport)

        # 检查处理后的数据形状
        self.assertGreaterEqual(len(processed_df.columns), len(self.test_df.columns))

        # 检查是否添加了索引
        self.assertIsInstance(processed_df.index, pd.DatetimeIndex)

    def test_cache_functionality(self):
        """测试缓存功能"""
        # 第一次处理（应该计算）
        processed_df1, report1 = self.pipeline.process(
            self.test_df, 'sh600519', '2024-01-01', '2024-01-31',
            use_cache=True
        )

        # 获取缓存统计
        stats1 = self.pipeline.get_cache_stats()
        self.assertGreater(stats1['cache_size'], 0)

        # 第二次处理相同数据（应该使用缓存）
        processed_df2, report2 = self.pipeline.process(
            self.test_df, 'sh600519', '2024-01-01', '2024-01-31',
            use_cache=True
        )

        # 检查两次处理的数据形状相同
        self.assertEqual(processed_df1.shape, processed_df2.shape)

        # 清理缓存
        self.pipeline.clear_cache()
        stats2 = self.pipeline.get_cache_stats()
        self.assertEqual(stats2['cache_size'], 0)

    def test_prepare_for_indicators(self):
        """测试为指标准备数据"""
        indicators = ['moving_average', 'rsi', 'macd']
        indicator_params = {
            'moving_average': {'periods': [5, 10]},
            'rsi': {'period': 14},
            'macd': {'fast_period': 12, 'slow_period': 26}
        }

        results = self.pipeline.prepare_for_indicators(
            self.test_df, 'sh600519', indicators, indicator_params
        )

        # 检查结果
        self.assertEqual(len(results), len(indicators))

        for indicator in indicators:
            self.assertIn(indicator, results)
            self.assertIsInstance(results[indicator], pd.DataFrame)

            # 检查数据是否包含必要列
            df = results[indicator]
            self.assertIn('close_price', df.columns)


class TestResultFormatter(unittest.TestCase):
    """测试结果格式化器"""

    def setUp(self):
        """测试准备"""
        self.formatter = ResultFormatter()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        self.test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 20,
            'close_price': np.random.randn(20).cumsum() + 100,
            'MA_5': np.random.randn(20).cumsum() + 100,
            'MA_10': np.random.randn(20).cumsum() + 101,
            'RSI': np.random.uniform(0, 100, 20)
        })

        # 设置日期索引
        self.test_df.set_index('trade_date', inplace=True)

    def test_format_dataframe(self):
        """测试DataFrame格式化"""
        result = self.formatter.format_dataframe(
            self.test_df, 'sh600519', ['moving_average', 'rsi']
        )

        # 检查基本结构
        self.assertIn('success', result)
        self.assertIn('timestamp', result)
        self.assertIn('metadata', result)
        self.assertIn('data', result)

        # 检查元数据
        metadata = result['metadata']
        self.assertEqual(metadata['row_count'], len(self.test_df))
        self.assertEqual(metadata['symbol'], 'sh600519')
        self.assertEqual(metadata['indicators'], ['moving_average', 'rsi'])

        # 检查数据
        if isinstance(result['data'], dict) and 'records' in result['data']:
            records = result['data']['records']
            self.assertEqual(len(records), len(self.test_df))
        elif isinstance(result['data'], list):
            self.assertEqual(len(result['data']), len(self.test_df))

    def test_format_empty_result(self):
        """测试空结果格式化"""
        empty_df = pd.DataFrame()
        result = self.formatter.format_dataframe(empty_df)

        self.assertFalse(result['success'])
        self.assertEqual(result['metadata']['row_count'], 0)

    def test_format_indicator_results(self):
        """测试指标结果格式化"""
        # 创建多个指标结果
        indicator_results = {
            'moving_average': self.test_df[['MA_5', 'MA_10']].copy(),
            'rsi': self.test_df[['RSI']].copy()
        }

        result = self.formatter.format_indicator_results(
            indicator_results, 'sh600519', '2024-01-01', '2024-01-20'
        )

        # 检查结果
        self.assertTrue(result['success'])
        self.assertIn('indicator_statistics', result)

        stats = result['indicator_statistics']
        self.assertIn('moving_average', stats)
        self.assertIn('rsi', stats)

        # 检查合并的数据
        if isinstance(result['data'], dict) and 'records' in result['data']:
            # 应该包含所有指标列
            first_record = result['data']['records'][0]
            self.assertIn('MA_5', first_record)
            self.assertIn('MA_10', first_record)
            self.assertIn('RSI', first_record)

    def test_format_for_export(self):
        """测试导出格式化"""
        # CSV格式
        csv_result = self.formatter.format_for_export(self.test_df, 'csv')
        self.assertIsInstance(csv_result, str)
        self.assertGreater(len(csv_result), 0)

        # JSON格式
        json_result = self.formatter.format_for_export(self.test_df, 'json')
        self.assertIsInstance(json_result, str)

        # 验证JSON可解析
        parsed_json = json.loads(json_result)
        self.assertIsInstance(parsed_json, list)
        self.assertEqual(len(parsed_json), len(self.test_df))

        # Excel格式
        excel_result = self.formatter.format_for_export(self.test_df, 'excel')
        self.assertIsInstance(excel_result, bytes)
        self.assertGreater(len(excel_result), 0)

    def test_format_error(self):
        """测试错误格式化"""
        error = ValueError("测试错误")
        result = self.formatter.format_error(error, 'sh600519', ['rsi'])

        self.assertFalse(result['success'])
        self.assertIn('error', result)
        self.assertEqual(result['error']['type'], 'ValueError')
        self.assertEqual(result['error']['message'], '测试错误')


class TestBatchResultFormatter(unittest.TestCase):
    """测试批量结果格式化器"""

    def setUp(self):
        """测试准备"""
        self.batch_formatter = BatchResultFormatter()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=10, freq='D')

        self.batch_results = {
            'sh600519': {
                'moving_average': pd.DataFrame({
                    'trade_date': dates,
                    'MA_5': np.random.randn(10).cumsum() + 100,
                    'MA_10': np.random.randn(10).cumsum() + 101
                }),
                'rsi': pd.DataFrame({
                    'trade_date': dates,
                    'RSI': np.random.uniform(0, 100, 10)
                })
            },
            'sz000001': {
                'moving_average': pd.DataFrame({
                    'trade_date': dates,
                    'MA_5': np.random.randn(10).cumsum() + 50,
                    'MA_10': np.random.randn(10).cumsum() + 51
                })
            }
        }

    def test_format_batch_results(self):
        """测试批量结果格式化"""
        symbols = ['sh600519', 'sz000001']
        indicators = ['moving_average', 'rsi']

        result = self.batch_formatter.format_batch_results(
            self.batch_results, symbols, indicators,
            '2024-01-01', '2024-01-10'
        )

        # 检查总结信息
        self.assertIn('summary', result)
        summary = result['summary']
        self.assertEqual(summary['total_symbols'], 2)
        self.assertEqual(summary['total_indicators'], 2)

        # 检查每个股票的结果
        self.assertIn('results', result)
        self.assertIn('sh600519', result['results'])
        self.assertIn('sz000001', result['results'])

        # 检查成功计数
        self.assertEqual(summary['successful_symbols'] + summary['failed_symbols'], 2)


# 修改 TestEnhancedQueryEngine 类
class TestEnhancedQueryEngine(unittest.TestCase):
    """测试增强查询引擎"""

    def setUp(self):
        """测试准备"""
        # 创建模拟的查询引擎
        self.mock_engine = Mock()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=50, freq='D')
        self.test_data = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 50,
            'open_price': np.random.randn(50).cumsum() + 100,
            'high_price': np.random.randn(50).cumsum() + 105,
            'low_price': np.random.randn(50).cumsum() + 95,
            'close_price': np.random.randn(50).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 50),
            'amount': np.random.randint(100000, 1000000, 50)
        })

        # 创建增强查询引擎，注入模拟的查询引擎
        self.engine = EnhancedQueryEngine(query_engine=self.mock_engine)

    def test_query_with_indicators(self):
        """测试带指标查询"""
        # 设置模拟返回值
        self.mock_engine.query_daily_data.return_value = self.test_data

        # 模拟指标管理器
        with patch.object(self.engine.indicator_manager, 'calculate_for_symbol') as mock_calculate:
            # 设置指标计算结果
            indicator_results = {
                'moving_average': pd.DataFrame({
                    'MA_5': np.random.randn(50).cumsum() + 100,
                    'MA_10': np.random.randn(50).cumsum() + 101
                }, index=self.test_data.index),
                'rsi': pd.DataFrame({
                    'RSI': np.random.uniform(0, 100, 50)
                }, index=self.test_data.index)
            }
            mock_calculate.return_value = indicator_results

            # 执行查询
            result = self.engine.query_with_indicators(
                symbol='sh600519',
                indicators=['moving_average', 'rsi'],
                start_date='2024-01-01',
                end_date='2024-02-01',
                use_cache=False
            )

            # 检查结果
            self.assertIsInstance(result, pd.DataFrame)
            self.assertGreater(len(result.columns), len(self.test_data.columns))

            # 检查是否包含指标列
            expected_cols = ['MA_5', 'MA_10', 'RSI']
            for col in expected_cols:
                self.assertIn(col, result.columns)

            # 验证方法调用
            mock_calculate.assert_called_once()
            self.mock_engine.query_daily_data.assert_called_once()

    def test_query_with_indicators_no_data(self):
        """测试无数据情况"""
        # 模拟无数据返回
        empty_df = pd.DataFrame()
        self.mock_engine.query_daily_data.return_value = empty_df

        # 模拟指标管理器（确保它不会被调用）
        with patch.object(self.engine.indicator_manager, 'calculate_for_symbol') as mock_calculate:
            mock_calculate.return_value = {}

            result = self.engine.query_with_indicators(
                symbol='sh600519',
                indicators=['moving_average'],
                start_date='2024-01-01',
                end_date='2024-02-01'
            )

            # 检查返回空DataFrame
            self.assertTrue(result.empty)

            # 验证query_daily_data被调用
            self.mock_engine.query_daily_data.assert_called_once()

            # 指标管理器不应该被调用，因为数据为空
            mock_calculate.assert_not_called()

    def test_query_with_indicators_invalid_indicators(self):
        """测试无效指标"""
        # 设置模拟返回值
        self.mock_engine.query_daily_data.return_value = self.test_data

        # 模拟指标管理器
        with patch.object(self.engine.indicator_manager, 'get_available_indicators') as mock_available:
            mock_available.return_value = {
                'moving_average': {'type': 'trend', 'description': '移动平均线'},
                'rsi': {'type': 'momentum', 'description': 'RSI指标'}
            }

            with patch.object(self.engine.indicator_manager, 'calculate_for_symbol') as mock_calculate:
                # 只计算有效指标
                mock_calculate.return_value = {
                    'moving_average': pd.DataFrame({
                        'MA_5': np.random.randn(50).cumsum() + 100
                    }, index=self.test_data.index)
                }

                # 包含无效指标的查询
                result = self.engine.query_with_indicators(
                    symbol='sh600519',
                    indicators=['moving_average', 'invalid_indicator'],
                    start_date='2024-01-01',
                    end_date='2024-02-01'
                )

                # 应该只计算有效指标
                self.assertIsInstance(result, pd.DataFrame)
                self.assertIn('MA_5', result.columns)

                # 验证方法调用
                self.mock_engine.query_daily_data.assert_called_once()
                mock_available.assert_called_once()

    def test_get_available_indicators(self):
        """测试获取可用指标"""
        # 模拟指标管理器
        with patch.object(self.engine.indicator_manager, 'get_available_indicators') as mock_available:
            mock_available.return_value = {
                'moving_average': {
                    'type': 'trend',
                    'description': '移动平均线',
                    'parameters': {'periods': [5, 10, 20]}
                },
                'rsi': {
                    'type': 'momentum',
                    'description': 'RSI指标',
                    'parameters': {'period': 14}
                }
            }

            indicators = self.engine.get_available_indicators()

            # 检查返回结果
            self.assertIsInstance(indicators, dict)
            self.assertIn('moving_average', indicators)
            self.assertIn('rsi', indicators)

            # 验证方法调用
            mock_available.assert_called_once()

    def test_validate_indicator_calculation(self):
        """测试验证指标计算"""
        # 设置模拟返回值
        self.mock_engine.query_daily_data.return_value = self.test_data

        # 模拟指标管理器
        with patch.object(self.engine.indicator_manager, 'get_available_indicators') as mock_available:
            mock_available.return_value = {
                'macd': {
                    'type': 'trend',
                    'description': 'MACD指标',
                    'min_data_points': 45
                }
            }

            validation = self.engine.validate_indicator_calculation(
                symbol='sh600519',
                indicator_name='macd',
                start_date='2024-01-01',
                end_date='2024-02-01'
            )

            # 检查验证结果
            self.assertIsInstance(validation, dict)
            self.assertIn('symbol', validation)
            self.assertIn('indicator', validation)
            self.assertIn('is_valid', validation)
            self.assertIn('data_points', validation)
            self.assertIn('required_points', validation)

            # 验证方法调用
            self.mock_engine.query_daily_data.assert_called_once()
            mock_available.assert_called_once()


class TestIntegration(unittest.TestCase):
    """集成测试"""

    def test_full_pipeline_integration(self):
        """测试完整管道集成"""
        # 创建数据管道
        pipeline = DataPipeline()

        # 创建测试数据
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 100,
            'open_price': np.random.randn(100).cumsum() + 100,
            'high_price': np.random.randn(100).cumsum() + 105,
            'low_price': np.random.randn(100).cumsum() + 95,
            'close_price': np.random.randn(100).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 100)
        })

        # 1. 数据预处理
        processed_df, quality_report = pipeline.process(
            test_df, 'sh600519', '2024-01-01', '2024-04-01'
        )

        # 检查预处理结果
        self.assertIsInstance(processed_df, pd.DataFrame)
        self.assertIsInstance(quality_report, DataQualityReport)
        self.assertGreater(len(processed_df.columns), len(test_df.columns))

        # 2. 结果格式化
        formatter = ResultFormatter()
        formatted_result = formatter.format_dataframe(
            processed_df, 'sh600519', ['enhanced_features']
        )

        # 检查格式化结果
        self.assertIn('success', formatted_result)
        self.assertIn('metadata', formatted_result)
        self.assertIn('data', formatted_result)

        # 3. 导出测试
        csv_export = formatter.format_for_export(processed_df, 'csv')
        self.assertIsInstance(csv_export, str)
        self.assertGreater(len(csv_export), 0)

        # 4. 验证数据可以重新加载
        import io
        reloaded_df = pd.read_csv(io.StringIO(csv_export))
        self.assertEqual(len(reloaded_df), len(processed_df))


def run_all_tests():
    """运行所有测试"""
    # 创建测试套件
    suite = unittest.TestSuite()

    # 添加测试类
    test_classes = [
        TestDataPreprocessor,
        TestDataPipeline,
        TestResultFormatter,
        TestBatchResultFormatter,
        TestEnhancedQueryEngine,
        TestIntegration
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == '__main__':
    print("=" * 70)
    print("运行增强查询引擎测试")
    print("=" * 70)

    result = run_all_tests()

    # 输出统计
    print("\n" + "=" * 70)
    print(f"测试结果: {result.testsRun} 个测试运行")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print("=" * 70)

    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"\n{test}:")
            print(f"错误: {traceback.split('AssertionError:')[-1].strip()}")

    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"\n{test}:")
            print(f"错误: {traceback.split('Error:')[-1].strip()}")

    # 返回退出码
    exit_code = 0 if result.wasSuccessful() else 1
    exit(exit_code)