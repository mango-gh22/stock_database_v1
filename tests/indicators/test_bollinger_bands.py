# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/indicators\test_bollinger_bands.py
# File Name: test_bollinger_bands
# @ Author: mango-gh22
# @ Date：2025/12/20 23:00
"""
desc 
"""
# tests/indicators/test_bollinger_bands.py
"""
布林带指标测试
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta
from src.indicators.volatility.bollinger_bands import BollingerBands


def create_test_data(days: int = 100) -> pd.DataFrame:
    """创建测试数据"""
    dates = pd.date_range(
        start=datetime.now() - timedelta(days=days),
        end=datetime.now(),
        freq='D'
    )

    # 生成价格序列
    np.random.seed(42)
    trend = np.linspace(100, 120, len(dates))
    noise = np.random.normal(0, 3, len(dates))
    prices = trend + noise

    df = pd.DataFrame({
        'trade_date': dates,
        'symbol': 'TEST',
        'close_price': prices,
        'volume': np.random.randint(1000000, 5000000, len(dates))
    })

    df.set_index('trade_date', inplace=True)
    return df


class TestBollingerBands:
    """布林带测试类"""

    def setup_method(self):
        self.test_data = create_test_data(100)
        self.bb = BollingerBands(period=20, std_dev=2.0)

    def test_bb_initialization(self):
        """测试布林带初始化"""
        assert self.bb.name == "bollinger_bands"
        assert self.bb.indicator_type.value == "volatility"
        assert self.bb.period == 20
        assert self.bb.std_dev == 2.0

    def test_bb_calculation(self):
        """测试布林带计算"""
        result = self.bb.calculate(self.test_data)

        # 检查基本列
        assert 'BB_MIDDLE' in result.columns
        assert 'BB_UPPER' in result.columns
        assert 'BB_LOWER' in result.columns

        # 检查中轨应该是移动平均
        middle_band = result['BB_MIDDLE'].dropna()
        if len(middle_band) > 0:
            # 中轨应该在价格范围内
            price = self.test_data['close_price']
            assert middle_band.min() >= price.min() * 0.8
            assert middle_band.max() <= price.max() * 1.2

    def test_bb_signals(self):
        """测试布林带信号"""
        result = self.bb.calculate(self.test_data)

        # 检查信号列
        assert 'BB_TOUCH_UPPER' in result.columns
        assert 'BB_TOUCH_LOWER' in result.columns
        assert 'BB_BREAKOUT_UPPER' in result.columns
        assert 'BB_BREAKOUT_LOWER' in result.columns

        # 检查数据类型
        assert result['BB_TOUCH_UPPER'].dtype == bool
        assert result['BB_SQUEEZE'].dtype == bool

    def test_bb_metrics(self):
        """测试布林带指标"""
        result = self.bb.calculate(self.test_data)

        # 检查计算指标
        assert 'BB_WIDTH' in result.columns
        assert 'BB_POSITION' in result.columns

        # 宽度应该为正数
        valid_width = result['BB_WIDTH'].dropna()
        if len(valid_width) > 0:
            assert valid_width.min() >= 0

    def test_bb_analysis(self):
        """测试布林带分析"""
        result = self.bb.calculate(self.test_data)
        analysis = self.bb.analyze_band_structure(result)

        assert 'current_band_width' in analysis
        assert 'volatility_state' in analysis
        assert 'signals' in analysis


if __name__ == "__main__":
    print("🧪 运行布林带测试...")

    test = TestBollingerBands()
    test.setup_method()

    print("1. 测试初始化...")
    test.test_bb_initialization()
    print("   ✅ 通过")

    print("2. 测试计算...")
    test.test_bb_calculation()
    print("   ✅ 通过")

    print("3. 测试信号...")
    test.test_bb_signals()
    print("   ✅ 通过")

    print("\n🎉 布林带测试完成！")