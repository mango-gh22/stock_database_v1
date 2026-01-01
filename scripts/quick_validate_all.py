# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\quick_validate_all.py
# File Name: quick_validate_all
# @ Author: mango-gh22
# @ Date：2025/12/21 13:20
"""
desc 
"""

"""
File: scripts/quick_validate_all.py
Desc: 快速验证所有指标
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_all_indicators():
    """测试所有指标"""
    print("测试所有指标重构...")
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

    # 测试的指标列表
    indicators_to_test = [
        ('moving_average', {'periods': [5, 10, 20], 'ma_type': 'sma'}),
        ('rsi', {'period': 14}),
        ('macd', {'fast_period': 12, 'slow_period': 26}),
        ('bollinger_bands', {'period': 20, 'std_dev': 2}),
        ('obv', {}),
        ('stochastic', {'k_period': 14, 'd_period': 3}),
        ('cci', {'period': 20}),
        ('williams_r', {'period': 14}),
    ]

    from src.indicators.indicator_manager import IndicatorFactory
    factory = IndicatorFactory()

    success_count = 0
    total_count = len(indicators_to_test)

    for indicator_name, params in indicators_to_test:
        try:
            print(f"\n测试: {indicator_name}")
            print(f"参数: {params}")

            # 创建指标实例
            indicator = factory.create_indicator(indicator_name, **params)
            print(f"✓ 创建成功: {indicator.name}")
            print(f"  描述: {indicator.description}")

            # 计算指标
            result = indicator.calculate(test_df.copy())
            print(f"✓ 计算成功: {len(result.columns)} 列")
            print(f"  输出列: {[col for col in result.columns if col not in test_df.columns][:5]}...")

            success_count += 1

        except Exception as e:
            print(f"✗ 失败: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"测试结果: {success_count}/{total_count} 个指标通过")

    if success_count == total_count:
        print("✓ 所有指标重构成功!")
    else:
        print(f"✗ 有 {total_count - success_count} 个指标需要修复")

    return success_count == total_count


if __name__ == "__main__":
    success = test_all_indicators()
    exit(0 if success else 1)