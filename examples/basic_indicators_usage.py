# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/examples\basic_indicators_usage.py
# File Name: basic_indicators_usage
# @ Author: mango-gh22
# @ Date：2025/12/21 16:38
"""
desc 创建生产环境示例
"""
"""
File: examples/basic_indicators_usage.py
Desc: 基础指标使用示例
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# 添加项目根目录
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def example_basic_usage():
    """基础使用示例"""
    print("=" * 60)
    print("技术指标基础使用示例")
    print("=" * 60)

    # 创建模拟数据
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=200, freq='D')
    base_prices = 100 + np.random.randn(200).cumsum()

    stock_data = pd.DataFrame({
        'trade_date': dates,
        'symbol': ['sh600519'] * 200,
        'open_price': base_prices + np.random.randn(200) * 2,
        'high_price': base_prices + np.abs(np.random.randn(200) * 3),
        'low_price': base_prices - np.abs(np.random.randn(200) * 3),
        'close_price': base_prices,
        'volume': np.random.randint(1000, 10000, 200),
        'amount': np.random.randint(100000, 1000000, 200)
    })

    print(f"创建测试数据: {len(stock_data)} 条记录")
    print(f"日期范围: {stock_data['trade_date'].min()} 到 {stock_data['trade_date'].max()}")
    print()

    # 示例1: 直接使用指标类
    print("1. 直接使用指标类")
    print("-" * 40)

    from src.indicators.trend.moving_average import MovingAverage
    from src.indicators.momentum.rsi import RSI
    from src.indicators.trend.macd import MACD

    # 创建不同参数的指标实例
    ma_short = MovingAverage(periods=[5, 10, 20], ma_type='sma')
    ma_long = MovingAverage(periods=[50, 100, 200], ma_type='ema')
    rsi_fast = RSI(period=7, overbought=80, oversold=20)
    rsi_slow = RSI(period=21, overbought=75, oversold=25)
    macd_fast = MACD(fast_period=8, slow_period=17, signal_period=9)

    print(f"✓ 创建了 {len([ma_short, ma_long, rsi_fast, rsi_slow, macd_fast])} 个指标实例")
    print()

    # 示例2: 使用指标管理器
    print("2. 使用指标管理器")
    print("-" * 40)

    from src.indicators.indicator_manager import IndicatorManager
    from unittest.mock import MagicMock, patch

    with patch('src.indicators.indicator_manager.QueryEngine') as MockQueryEngine, \
            patch('src.indicators.indicator_manager.StockAdjustor') as MockAdjustor:

        mock_engine = MagicMock()
        mock_engine.query_daily_data.return_value = stock_data
        MockQueryEngine.return_value = mock_engine

        mock_adjustor = MagicMock()
        MockAdjustor.return_value = mock_adjustor

        manager = IndicatorManager()

        # 获取可用指标
        indicators = manager.get_available_indicators()
        print(f"可用指标数量: {len(indicators)}")
        print("趋势指标:", [name for name, info in indicators.items() if info['type'] == 'trend'])
        print("动量指标:", [name for name, info in indicators.items() if info['type'] == 'momentum'])
        print("波动率指标:", [name for name, info in indicators.items() if info['type'] == 'volatility'])
        print("成交量指标:", [name for name, info in indicators.items() if info['type'] == 'volume'])
        print()

        # 示例3: 计算单个指标
        print("3. 计算单个指标（不同参数）")
        print("-" * 40)

        # 计算短期均线
        ma_result = manager.calculate_single(
            symbol="sh600519",
            indicator_name="moving_average",
            start_date="2024-01-01",
            end_date="2024-06-01",
            periods=[5, 10, 20, 30, 60],
            ma_type='sma'
        )

        print(f"移动平均线结果:")
        print(f"  数据行数: {len(ma_result)}")
        ma_columns = [col for col in ma_result.columns if 'MA_' in col]
        print(f"  均线列: {ma_columns}")

        # 显示最后几行的均线值
        print(f"  最新均线值:")
        last_row = ma_result.iloc[-1]
        for col in ma_columns[:3]:  # 显示前3个均线
            print(f"    {col}: {last_row[col]:.2f}")
        print()

        # 示例4: 批量计算多个指标
        print("4. 批量计算多个指标")
        print("-" * 40)

        results = manager.calculate_for_symbol(
            symbol="sh600519",
            indicator_names=["rsi", "macd", "bollinger_bands"],
            start_date="2024-01-01",
            end_date="2024-06-01",
            indicator_params={
                "rsi": {"period": 14},
                "macd": {"fast_period": 12, "slow_period": 26, "signal_period": 9},
                "bollinger_bands": {"period": 20, "std_dev": 2}
            }
        )

        print(f"批量计算结果:")
        for indicator_name, result_df in results.items():
            indicator_cols = [col for col in result_df.columns if col not in stock_data.columns]
            last_values = {}
            for col in indicator_cols[:3]:  # 取前3个指标列
                if col in result_df.columns:
                    last_values[col] = f"{result_df.iloc[-1][col]:.2f}"

            print(f"  {indicator_name}: {len(indicator_cols)} 个指标列")
            if last_values:
                print(f"    最新值: {last_values}")
        print()

        # 示例5: 使用新指标
        print("5. 使用新开发的指标")
        print("-" * 40)

        new_indicators = {
            "parabolic_sar": {"acceleration_factor": 0.02, "acceleration_max": 0.2},
            "ichimoku_cloud": {"tenkan_period": 9, "kijun_period": 26, "senkou_span_b_period": 52},
            "stochastic": {"k_period": 14, "d_period": 3, "smoothing": 3},
            "cci": {"period": 20, "constant": 0.015},
            "williams_r": {"period": 14, "overbought_level": -20, "oversold_level": -80}
        }

        for indicator_name, params in new_indicators.items():
            try:
                result = manager.calculate_single(
                    symbol="sh600519",
                    indicator_name=indicator_name,
                    start_date="2024-01-01",
                    end_date="2024-06-01",
                    **params
                )

                indicator_cols = [col for col in result.columns if col not in stock_data.columns]
                print(f"  ✓ {indicator_name}: {len(indicator_cols)} 个指标列")

                # 显示关键指标值
                if indicator_cols:
                    key_col = indicator_cols[0]  # 第一个指标列
                    if key_col in result.columns:
                        last_value = result.iloc[-1][key_col]
                        print(f"    最新{key_col}: {last_value:.2f}")

            except Exception as e:
                print(f"  ✗ {indicator_name}: 计算失败 - {str(e)[:50]}...")

        print()
        print("=" * 60)
        print("示例完成！系统功能正常。")
        print("=" * 60)


def example_strategy_signal():
    """策略信号示例"""
    print("\n" + "=" * 60)
    print("策略信号生成示例")
    print("=" * 60)

    # 创建简单策略：均线金叉 + RSI超卖
    from src.indicators.trend.moving_average import MovingAverage
    from src.indicators.momentum.rsi import RSI

    # 模拟数据
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    prices = 100 + np.random.randn(100).cumsum()

    data = pd.DataFrame({
        'trade_date': dates,
        'close_price': prices,
        'volume': np.random.randint(1000, 10000, 100)
    })

    # 计算技术指标
    ma = MovingAverage(periods=[5, 20], ma_type='sma')
    ma_result = ma.calculate(data.copy())

    rsi = RSI(period=14)
    rsi_result = rsi.calculate(data.copy())

    # 合并结果
    result = pd.concat([
        data[['trade_date', 'close_price']],
        ma_result[['MA_5', 'MA_20']],
        rsi_result[['RSI', 'RSI_Oversold']]
    ], axis=1)

    # 生成交易信号
    result['Golden_Cross'] = (result['MA_5'] > result['MA_20']) & \
                             (result['MA_5'].shift(1) <= result['MA_20'].shift(1))

    result['Buy_Signal'] = result['Golden_Cross'] & result['RSI_Oversold'].shift(1)

    # 统计信号
    buy_signals = result['Buy_Signal'].sum()
    golden_crosses = result['Golden_Cross'].sum()

    print(f"数据周期: {len(result)} 天")
    print(f"均线金叉次数: {golden_crosses}")
    print(f"买入信号次数: {buy_signals}")
    print(f"信号示例（最后10天）:")
    print(result[['trade_date', 'close_price', 'MA_5', 'MA_20', 'RSI', 'Buy_Signal']].tail(10).to_string())

    return result


if __name__ == "__main__":
    example_basic_usage()
    example_strategy_signal()