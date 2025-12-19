# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/query\test_query_engine.py
# File Name: test_query_engine
# @ Author: m_mango
# @ Date：2025/12/6 16:31
"""
desc 
"""
"""
查询引擎测试
"""

import unittest
import pandas as pd
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.query.query_engine import QueryEngine


class TestQueryEngine(unittest.TestCase):
    """查询引擎测试类"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls.engine = QueryEngine()

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        if hasattr(cls, 'engine'):
            cls.engine.close()

    def test_get_stock_basic(self):
        """测试获取股票基本信息"""
        df = self.engine.get_stock_basic()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

        # 测试有条件的查询
        df_filtered = self.engine.get_stock_basic(exchange='SZ')
        if not df_filtered.empty:
            self.assertTrue(all(df_filtered['exchange'] == 'SZ'))

    def test_get_daily_data(self):
        """测试获取日线数据"""
        # 先获取股票列表
        stock_list = self.engine.get_stock_list()
        if stock_list:
            symbol = stock_list[0]
            df = self.engine.get_daily_data(symbol, limit=10)
            self.assertIsInstance(df, pd.DataFrame)
            if not df.empty:
                self.assertGreater(len(df), 0)
                self.assertIn('close', df.columns)
                self.assertIn('volume', df.columns)

    def test_get_data_statistics(self):
        """测试获取数据统计"""
        stats = self.engine.get_data_statistics()
        self.assertIsInstance(stats, dict)
        self.assertIn('stock_basic', stats)
        self.assertIn('daily_data', stats)

    def test_get_stock_list(self):
        """测试获取股票列表"""
        stock_list = self.engine.get_stock_list()
        self.assertIsInstance(stock_list, list)
        if stock_list:
            self.assertIsInstance(stock_list[0], str)

    def test_get_trading_dates(self):
        """测试获取交易日期"""
        stock_list = self.engine.get_stock_list()
        if stock_list:
            dates = self.engine.get_trading_dates(stock_list[0])
            self.assertIsInstance(dates, list)


if __name__ == '__main__':
    unittest.main()