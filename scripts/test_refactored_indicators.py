# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_refactored_indicators.py
# File Name: test_refactored_indicators
# @ Author: mango-gh22
# @ Date：2025/12/21 11:43
"""
desc 注册类而不是实例的重构--测试验证
重构总结
✅ 主要改进：
注册类而不是实例：IndicatorManager 现在存储指标类信息，而不是固定实例
动态参数支持：每次计算时都可以传入不同的参数
工厂模式：通过 IndicatorFactory 统一创建指标实例
向后兼容：保持了原有API接口，添加了新的灵活接口

Desc: 测试重构后的指标系统
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_refactored_system():
    print("测试重构后的指标系统...")
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

    # 测试1: 直接创建指标实例
    print("测试1: 直接创建指标实例")
    try:
        from src.indicators.trend.moving_average import MovingAverage

        # 使用默认参数
        ma1 = MovingAverage()
        print(f"✓ 创建默认移动平均线: periods={ma1.periods}, type={ma1.ma_type}")

        # 使用自定义参数
        ma2 = MovingAverage(periods=[10, 20, 30], ma_type='ema')
        print(f"✓ 创建自定义移动平均线: periods={ma2.periods}, type={ma2.ma_type}")

        # 计算
        result1 = ma1.calculate(test_df.copy())
        print(f"✓ 计算默认MA成功，输出列数: {len(result1.columns)}")

        result2 = ma2.calculate(test_df.copy())
        print(f"✓ 计算EMA成功，输出列数: {len(result2.columns)}")

    except Exception as e:
        print(f"✗ 失败: {e}")
        return False

    # 测试2: 使用指标管理器
    print("\n测试2: 使用指标管理器")
    try:
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
            indicators = manager.get_available_indicators()
            print(f"✓ 可用指标数量: {len(indicators)}")

            # 测试计算单个指标（不同参数）
            print("\n测试不同参数的移动平均线:")

            # 默认参数
            result_default = manager.calculate_single(
                symbol="sh600519",
                indicator_name="moving_average",
                start_date="2024-01-01",
                end_date="2024-04-01"
            )
            print(f"  ✓ 默认参数: {len(result_default.columns)} 列")

            # 自定义参数
            result_custom = manager.calculate_single(
                symbol="sh600519",
                indicator_name="moving_average",
                start_date="2024-01-01",
                end_date="2024-04-01",
                periods=[5, 20, 60],
                ma_type='ema'
            )
            print(f"  ✓ 自定义参数: {len(result_custom.columns)} 列")

            # 测试多个指标
            print("\n测试多个指标:")
            results = manager.calculate_for_symbol(
                symbol="sh600519",
                indicator_names=["moving_average", "rsi", "macd"],
                start_date="2024-01-01",
                end_date="2024-04-01",
                indicator_params={
                    "moving_average": {"periods": [10, 20], "ma_type": "sma"},
                    "rsi": {"period": 14},
                    "macd": {"fast_period": 12, "slow_period": 26}
                }
            )
            print(f"  ✓ 成功计算 {len(results)} 个指标")
            for name, df in results.items():
                print(f"    - {name}: {len(df.columns)} 列")

    except Exception as e:
        print(f"✗ 失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = test_refactored_system()
    print("\n" + "=" * 60)
    if success:
        print("✓ 重构测试通过!")
    else:
        print("✗ 重构测试失败")
    print("=" * 60)