# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_complete_system.py
# File Name: test_complete_system
# @ Author: mango-gh22
# @ Date：2025/12/21 15:08
"""
desc 
"""
"""
File: scripts/test_complete_system.py
Desc: 测试完整的指标系统
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_complete_indicator_system():
    """测试完整的指标系统"""
    print("测试完整的指标系统...")
    print("=" * 60)

    # 创建测试数据
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    test_df = pd.DataFrame({
        'trade_date': dates,
        'symbol': ['sh600519'] * 100,
        'open_price': np.random.randn(100).cumsum() + 100,
        'high_price': np.random.randn(100).cumsum() + 105,
        'low_price': np.random.randn(100).cumsum() + 95,
        'close_price': np.random.randn(100).cumsum() + 100,
        'volume': np.random.randint(1000, 10000, 100),
        'amount': np.random.randint(100000, 1000000, 100)
    })

    try:
        # 测试1: 指标管理器基本功能
        print("\n1. 测试指标管理器基本功能")
        from src.indicators.indicator_manager import IndicatorManager
        from unittest.mock import MagicMock, patch

        with patch('src.indicators.indicator_manager.QueryEngine') as MockQueryEngine, \
                patch('src.indicators.indicator_manager.StockAdjustor') as MockAdjustor:

            mock_engine = MagicMock()
            mock_engine.query_daily_data.return_value = test_df
            MockQueryEngine.return_value = mock_engine

            mock_adjustor = MagicMock()
            MockAdjustor.return_value = mock_adjustor

            manager = IndicatorManager()

            # 获取可用指标
            available = manager.get_available_indicators()
            print(f"✓ 可用指标: {len(available)} 个")
            print(f"  指标列表: {list(available.keys())}")

            # 测试2: 单指标计算（不同参数）
            print("\n2. 测试单指标计算（不同参数）")

            # 测试移动平均线 - 默认参数
            result1 = manager.calculate_single(
                symbol="sh600519",
                indicator_name="moving_average",
                start_date="2024-01-01",
                end_date="2024-04-01"
            )
            ma_cols1 = [col for col in result1.columns if 'MA_' in col or 'EMA_' in col]
            print(f"  ✓ 默认MA: {len(ma_cols1)} 个均线列")

            # 测试移动平均线 - 自定义参数
            result2 = manager.calculate_single(
                symbol="sh600519",
                indicator_name="moving_average",
                start_date="2024-01-01",
                end_date="2024-04-01",
                periods=[5, 20, 60],
                ma_type='ema'
            )
            ma_cols2 = [col for col in result2.columns if 'MA_' in col or 'EMA_' in col]
            print(f"  ✓ 自定义EMA: {len(ma_cols2)} 个均线列")

            # 测试3: 多指标批量计算
            print("\n3. 测试多指标批量计算")
            results = manager.calculate_for_symbol(
                symbol="sh600519",
                indicator_names=["moving_average", "rsi", "macd", "bollinger_bands"],
                start_date="2024-01-01",
                end_date="2024-04-01",
                indicator_params={
                    "moving_average": {"periods": [10, 20], "ma_type": "sma"},
                    "rsi": {"period": 7},  # 短期RSI
                    "macd": {"fast_period": 8, "slow_period": 17},  # 快速MACD
                    "bollinger_bands": {"period": 10, "std_dev": 1.5}  # 窄布林带
                }
            )

            print(f"  ✓ 成功计算 {len(results)} 个指标:")
            for name, df in results.items():
                indicator_cols = [col for col in df.columns if col not in test_df.columns]
                print(f"    - {name}: {len(indicator_cols)} 个指标列")

            # 测试4: 缓存功能
            print("\n4. 测试缓存功能")

            # 第一次计算
            manager.clear_cache()
            cache_stats1 = manager.get_cache_stats()
            print(f"  ✓ 初始缓存: {cache_stats1['memory_cache_items']} 项")

            # 计算并缓存
            manager.calculate_single(
                symbol="sh600519",
                indicator_name="rsi",
                start_date="2024-01-01",
                end_date="2024-01-31",
                period=14
            )

            cache_stats2 = manager.get_cache_stats()
            print(f"  ✓ 计算后缓存: {cache_stats2['memory_cache_items']} 项")

            # 清理缓存
            manager.clear_cache()
            cache_stats3 = manager.get_cache_stats()
            print(f"  ✓ 清理后缓存: {cache_stats3['memory_cache_items']} 项")

            # 测试5: 新指标验证
            print("\n5. 测试新指标")
            new_indicators = ['parabolic_sar', 'ichimoku_cloud', 'stochastic', 'cci', 'williams_r']

            for indicator in new_indicators:
                if indicator in available:
                    try:
                        result = manager.calculate_single(
                            symbol="sh600519",
                            indicator_name=indicator,
                            start_date="2024-01-01",
                            end_date="2024-04-01"
                        )
                        print(f"  ✓ {indicator}: 计算成功，{len(result.columns)} 列")
                    except Exception as e:
                        print(f"  ✗ {indicator}: 计算失败 - {str(e)[:50]}...")
                else:
                    print(f"  ✗ {indicator}: 未注册")

            # 测试6: 数据充足性验证
            print("\n6. 测试数据充足性验证")
            is_sufficient, message = manager.validate_data_sufficiency(
                symbol="sh600519",
                indicator_names=["moving_average", "macd", "ichimoku_cloud"],
                start_date="2024-01-01",
                end_date="2024-04-01"
            )
            print(f"  ✓ 数据验证: {message}")

            return True

    except Exception as e:
        print(f"\n✗ 系统测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_enhanced_query_engine():
    """测试增强查询引擎"""
    print("\n" + "=" * 60)
    print("测试增强查询引擎...")
    print("=" * 60)

    try:
        from src.query.enhanced_query_engine import EnhancedQueryEngine
        from unittest.mock import MagicMock, patch

        # 创建测试数据 - 确保使用float类型
        dates = pd.date_range('2024-01-01', periods=50, freq='D')
        test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 50,
            'open_price': list(range(100, 150)),  # 简单递增序列
            'high_price': list(range(105, 155)),
            'low_price': list(range(95, 145)),
            'close_price': list(range(102, 152)),
            'volume': list(range(1000, 1050)),
            'amount': list(range(100000, 100050))
        })

        # 确保所有都是数值类型
        for col in ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'amount']:
            test_df[col] = test_df[col].astype(float)

        print(f"✓ 创建测试数据，数据形状: {test_df.shape}")
        print(f"  数据类型:")
        for col, dtype in test_df.dtypes.items():
            print(f"    {col}: {dtype}")

        with patch('src.query.enhanced_query_engine.QueryEngine') as MockBaseEngine:
            mock_engine = MagicMock()
            mock_engine.query_daily_data.return_value = test_df
            MockBaseEngine.return_value = mock_engine

            engine = EnhancedQueryEngine()

            # 测试带指标查询
            print("\n测试带指标查询...")
            result = engine.query_with_indicators(
                symbol="sh600519",
                indicators=["rsi"],  # 先只测试RSI
                start_date="2024-01-01",
                end_date="2024-02-20",
                use_cache=False
            )

            print(f"✓ 增强查询完成")
            print(f"  原始列数: {len(test_df.columns)}")
            print(f"  结果列数: {len(result.columns)}")
            print(f"  新增指标列: {len(result.columns) - len(test_df.columns)}")

            # 检查RSI列
            rsi_cols = [col for col in result.columns if 'RSI' in col]
            print(f"  RSI相关列: {len(rsi_cols)} 个")

            if rsi_cols:
                print(f"  ✓ 成功生成RSI列")
                # 显示RSI统计信息
                if 'RSI' in result.columns:
                    rsi_series = result['RSI']
                    print(f"  RSI统计:")
                    print(f"    有效值数量: {rsi_series.count()}")
                    print(f"    NaN值数量: {rsi_series.isnull().sum()}")
                    print(f"    值范围: [{rsi_series.min():.2f}, {rsi_series.max():.2f}]")
                    print(f"    均值: {rsi_series.mean():.2f}")
            else:
                print("  ❌ 未生成RSI列，检查数据预处理")

            return True

    except Exception as e:
        print(f"✗ 增强查询引擎测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_preprocessing():
    """专门测试数据预处理"""
    print("\n" + "=" * 60)
    print("测试数据预处理...")
    print("=" * 60)

    try:
        from src.indicators.indicator_manager import IndicatorManager
        from unittest.mock import MagicMock, patch
        import decimal

        # 创建包含Decimal和None的测试数据
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        test_df = pd.DataFrame({
            'trade_date': dates,
            'symbol': ['sh600519'] * 10,
            'open_price': [decimal.Decimal(str(100 + i)) for i in range(10)],
            'close_price': [decimal.Decimal(str(101 + i)) for i in range(10)],
            'volume': [1000 + i * 100 for i in range(10)]
        })

        # 添加一些None值
        test_df.loc[3, 'close_price'] = None
        test_df.loc[5, 'volume'] = None

        print(f"测试数据形状: {test_df.shape}")
        print(f"原始数据类型:")
        for col, dtype in test_df.dtypes.items():
            print(f"  {col}: {dtype}")

        print(f"\n包含None值的列:")
        for col in test_df.columns:
            nan_count = test_df[col].isnull().sum()
            if nan_count > 0:
                print(f"  {col}: {nan_count} 个None值")

        with patch('src.indicators.indicator_manager.QueryEngine') as MockQueryEngine, \
                patch('src.indicators.indicator_manager.StockAdjustor') as MockAdjustor:

            mock_engine = MagicMock()
            mock_engine.query_daily_data.return_value = test_df
            MockQueryEngine.return_value = mock_engine

            mock_adjustor = MagicMock()
            MockAdjustor.return_value = mock_adjustor

            manager = IndicatorManager()

            print("\n1. 测试_preprocess_data_for_calculation方法...")
            # 检查方法是否存在
            if hasattr(manager, '_preprocess_data_for_calculation'):
                preprocessed = manager._preprocess_data_for_calculation(test_df.copy())

                print(f"  预处理后数据形状: {preprocessed.shape}")
                print(f"  预处理后数据类型:")
                for col, dtype in preprocessed.dtypes.items():
                    print(f"    {col}: {dtype}")

                print(f"\n  预处理后None值统计:")
                nan_found = False
                for col in preprocessed.columns:
                    nan_count = preprocessed[col].isnull().sum()
                    if nan_count > 0:
                        print(f"    {col}: {nan_count} 个None值 (❌)")
                        nan_found = True
                    else:
                        print(f"    {col}: 无None值 (✓)")

                if not nan_found:
                    print("  ✓ 所有None值已成功处理")
                else:
                    print("  ⚠️  仍有None值存在")
            else:
                print("  ⚠️  _preprocess_data_for_calculation方法不存在")

            print("\n2. 测试实际RSI指标计算...")
            # 测试实际指标计算
            result = manager.calculate_single(
                symbol="sh600519",
                indicator_name="rsi",
                start_date="2024-01-01",
                end_date="2024-01-10",
                period=5  # 使用较小的周期
            )

            if result is not None:
                print(f"  ✓ RSI计算成功")
                print(f"  结果形状: {result.shape}")
                print(f"  结果列: {list(result.columns)}")

                # 检查是否有RSI列
                rsi_cols = [col for col in result.columns if 'RSI' in col]
                if rsi_cols:
                    print(f"  ✓ 生成RSI列: {rsi_cols}")
                    # 显示前几个RSI值
                    if 'RSI' in result.columns:
                        rsi_values = result['RSI'].head().tolist()
                        print(f"  前5个RSI值: {rsi_values}")
                else:
                    print("  ❌ 未生成RSI列")
            else:
                print("  ❌ RSI计算返回None")

            print("\n✓ 数据预处理测试完成")
            return True

    except Exception as e:
        print(f"✗ 数据预处理测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_api_integration():
    """测试API集成"""
    print("\n" + "=" * 60)
    print("测试API集成...")
    print("=" * 60)

    try:
        from src.api.indicators_api import IndicatorRequest, ValidationRequest
        print("✓ API数据模型导入成功")

        # 测试请求模型
        request = IndicatorRequest(
            symbol="sh600519",
            indicators=["moving_average", "rsi"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            use_cache=True
        )
        print(f"✓ 请求模型创建成功: {request.symbol}")

        validation_request = ValidationRequest(
            symbol="sh600519",
            indicator="macd",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        print(f"✓ 验证模型创建成功: {validation_request.indicator}")

        return True

    except Exception as e:
        print(f"✗ API测试失败: {e}")
        return False


if __name__ == "__main__":
    print("开始完整的系统测试...")

    # 先运行数据预处理测试
    success0 = test_data_preprocessing()

    # 如果数据预处理测试通过，再运行其他测试
    if success0:
        success1 = test_complete_indicator_system()
        success2 = test_enhanced_query_engine()
        success3 = test_api_integration()
    else:
        print("\n⚠️ 数据预处理测试失败，跳过其他测试")
        success1 = success2 = success3 = False

    print("\n" + "=" * 60)
    print("系统测试总结:")
    print("=" * 60)
    print(f"0. 数据预处理: {'✓ 通过' if success0 else '✗ 失败'}")
    print(f"1. 指标管理器: {'✓ 通过' if success1 else '✗ 失败'}")
    print(f"2. 增强查询引擎: {'✓ 通过' if success2 else '✗ 失败'}")
    print(f"3. API集成: {'✓ 通过' if success3 else '✗ 失败'}")

    all_passed = success0 and success1 and success2 and success3
    if all_passed:
        print("\n🎉 所有系统测试通过!")
    else:
        print("\n⚠️  部分测试失败，需要检查")
    print("=" * 60)