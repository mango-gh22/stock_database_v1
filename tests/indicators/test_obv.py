# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/indicators\test_obv.py
# File Name: test_obv
# @ Author: mango-gh22
# @ Date：2025/12/20 23:01
"""
desc 
"""
# tests/indicators/test_obv.py
"""
OBV指标测试
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta
from src.indicators.volume.obv import OBV


def create_test_data(days: int = 100) -> pd.DataFrame:
    """创建测试数据"""
    dates = pd.date_range(
        start=datetime.now() - timedelta(days=days),
        end=datetime.now(),
        freq='D'
    )

    # 生成价格和成交量序列
    np.random.seed(42)
    prices = 100 + np.cumsum(np.random.randn(len(dates)) * 2)
    volumes = np.random.randint(500000, 5000000, len(dates))

    df = pd.DataFrame({
        'trade_date': dates,
        'symbol': 'TEST',
        'close_price': prices,
        'volume': volumes,
        'open_price': prices + np.random.normal(0, 1, len(dates)),
        'high_price': prices + np.abs(np.random.normal(0, 2, len(dates))),
        'low_price': prices - np.abs(np.random.normal(0, 2, len(dates)))
    })

    df.set_index('trade_date', inplace=True)
    return df


class TestOBV:
    """OBV测试类"""

    def setup_method(self):
        self.test_data = create_test_data(100)
        self.obv = OBV()

    def test_obv_initialization(self):
        """测试OBV初始化"""
        assert self.obv.name == "obv"
        assert self.obv.indicator_type.value == "volume"
        assert self.obv.ma_periods == [5, 10, 20, 30]

    def test_obv_calculation(self):
        """测试OBV计算"""
        result = self.obv.calculate(self.test_data)

        # 检查基本列
        assert 'OBV' in result.columns
        assert 'OBV_ROC' in result.columns
        assert 'OBV_MOMENTUM' in result.columns

        # OBV应该是累积值
        obv_values = result['OBV'].dropna()
        if len(obv_values) > 1:
            # OBV应该有明显的变化（累积效应）
            obv_range = obv_values.max() - obv_values.min()
            assert obv_range > 0

    def test_obv_moving_averages(self):
        """测试OBV移动平均"""
        result = self.obv.calculate(self.test_data)

        # 检查MA列
        for period in self.obv.ma_periods:
            ma_col = f'OBV_MA{period}'
            signal_col = f'OBV_MA{period}_SIGNAL'

            assert ma_col in result.columns
            assert signal_col in result.columns

            # 检查信号数据类型
            assert result[signal_col].dtype == bool

    def test_obv_signals(self):
        """测试OBV信号"""
        result = self.obv.calculate(self.test_data)

        # 检查信号列
        assert 'OBV_TREND' in result.columns
        assert 'OBV_BREAKOUT' in result.columns
        assert 'OBV_PRICE_CONFIRMATION' in result.columns

        # 检查趋势列的值
        valid_trends = result['OBV_TREND'].dropna()
        if len(valid_trends) > 0:
            valid_values = ['bullish', 'bearish', 'neutral', 'strong_bullish', 'strong_bearish', 'unknown']
            assert all(val in valid_values for val in valid_trends.unique())

    def test_obv_analysis(self):
        """测试OBV分析"""
        result = self.obv.calculate(self.test_data)
        analysis = self.obv.analyze_obv_pattern(result)

        assert 'current_obv' in analysis
        assert 'trend' in analysis
        assert 'confirmation' in analysis
        assert 'signals' in analysis


if __name__ == "__main__":
    print("🧪 运行OBV测试...")

    test = TestOBV()
    test.setup_method()

    print("1. 测试初始化...")
    test.test_obv_initialization()
    print("   ✅ 通过")

    print("2. 测试计算...")
    test.test_obv_calculation()
    print("   ✅ 通过")

    print("3. 测试移动平均...")
    test.test_obv_moving_averages()
    print("   ✅ 通过")

    print("\n🎉 OBV测试完成！")