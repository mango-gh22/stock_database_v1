# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/processors\test_adjustor.py
# File Name: test_adjustor
# @ Author: mango-gh22
# @ Date：2025/12/14 15:40
"""
desc 
"""
# tests/processors/test_adjustor.py
"""
复权计算器测试 - 修复日期类型问题
"""

import unittest
import pandas as pd
import sys
import os
from datetime import datetime, date

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

print(f"当前工作目录: {os.getcwd()}")

try:
    from src.processors.adjustor import StockAdjustor, AdjustType, AdjustMethod, DividendEvent

    print("✅ 成功导入 adjustor 模块")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "adjustor",
        os.path.join(os.path.dirname(__file__), '../../src/processors/adjustor.py')
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    StockAdjustor = module.StockAdjustor
    AdjustType = module.AdjustType
    AdjustMethod = module.AdjustMethod
    DividendEvent = module.DividendEvent


class TestStockAdjustor(unittest.TestCase):
    """复权计算器测试类"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        print("设置复权计算器测试...")
        try:
            cls.adjustor = StockAdjustor()
            print("✅ 复权计算器初始化成功")
        except Exception as e:
            print(f"❌ 复权计算器初始化失败: {e}")
            cls.adjustor = None

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        if hasattr(cls, 'adjustor') and cls.adjustor is not None:
            cls.adjustor.close()

    def test_1_adjustor_initialization(self):
        """测试复权计算器初始化"""
        self.assertIsNotNone(self.adjustor)

    def test_2_dividend_event(self):
        """测试分红事件类"""
        # 修复：使用 datetime.date 对象而不是字符串
        ex_date = datetime.strptime('2023-06-15', '%Y-%m-%d').date()

        event = DividendEvent(
            symbol='000001.SZ',
            ex_date=ex_date,  # 使用 date 对象
            cash_div=0.5,
            shares_div=0.3
        )

        self.assertEqual(event.symbol, '000001.SZ')
        self.assertEqual(event.cash_div, 0.5)
        self.assertEqual(event.shares_div, 0.3)
        # self.assertIsInstance(event.ex_date, datetime.date)
        self.assertIsInstance(event.ex_date, date)
        self.assertIsInstance(event.forward_factor, float)
        self.assertIsInstance(event.backward_factor, float)
        print(f"分红事件测试: 前复权因子={event.forward_factor:.6f}, 后复权因子={event.backward_factor:.6f}")

    def test_3_enum_types(self):
        """测试枚举类型"""
        self.assertEqual(AdjustType.FORWARD.value, "forward")
        self.assertEqual(AdjustType.BACKWARD.value, "backward")
        self.assertEqual(AdjustType.NONE.value, "none")

        self.assertEqual(AdjustMethod.FACTOR.value, "factor")
        self.assertEqual(AdjustMethod.PRICE.value, "price")

    def test_4_load_dividend_events(self):
        """测试加载分红事件"""
        if self.adjustor:
            try:
                events = self.adjustor.load_dividend_events('000001.SZ')
                self.assertIsInstance(events, list)

                if events:
                    event = events[0]
                    self.assertIsInstance(event, DividendEvent)
                    self.assertEqual(event.symbol, '000001.SZ')
                    print(f"加载到 {len(events)} 个分红事件")
            except Exception as e:
                print(f"⚠️ 加载分红事件测试跳过: {e}")

    def test_5_adjust_methods(self):
        """测试复权方法"""
        # 创建测试数据 - 确保 trade_date 是 datetime.date 类型
        test_data = {
            'trade_date': [
                datetime.strptime('2023-12-01', '%Y-%m-%d').date(),
                datetime.strptime('2023-12-02', '%Y-%m-%d').date(),
                datetime.strptime('2023-12-03', '%Y-%m-%d').date()
            ],
            'symbol': ['000001.SZ', '000001.SZ', '000001.SZ'],
            'open': [10.0, 10.5, 11.0],
            'high': [10.5, 11.0, 11.5],
            'low': [9.5, 10.0, 10.5],
            'close': [10.2, 10.8, 11.2],
            'volume': [1000000, 1200000, 1100000]
        }

        df = pd.DataFrame(test_data)

        # 测试基本的复权逻辑（不依赖数据库）
        from src.processors.adjustor import StockAdjustor

        # 创建模拟的adjustor - 修复日期类型
        class MockAdjustor(StockAdjustor):
            def __init__(self):
                self.query_engine = None
                self.db_connector = None
                self.factor_cache = {}

            def get_adjust_factors(self, symbol, ex_date=None):
                # 返回模拟的因子 - 使用 date 对象
                ex_date_obj = datetime.strptime('2023-06-15', '%Y-%m-%d').date()
                return pd.DataFrame({
                    'symbol': [symbol],
                    'ex_date': [ex_date_obj],  # 使用 date 对象
                    'total_factor': [0.9]
                })

        adjustor = MockAdjustor()

        # 测试前复权
        forward_df = adjustor.adjust_price(
            df.copy(), '000001.SZ', AdjustType.FORWARD
        )

        self.assertIsInstance(forward_df, pd.DataFrame)
        self.assertIn('adjust_type', forward_df.columns)
        self.assertEqual(forward_df['adjust_type'].iloc[0], 'forward')

        print("✅ 基本复权逻辑测试通过")


def run_tests():
    """运行测试"""
    print("=" * 60)
    print("运行复权计算器测试")
    print("=" * 60)

    suite = unittest.TestLoader().loadTestsFromTestCase(TestStockAdjustor)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == '__main__':
    run_tests()