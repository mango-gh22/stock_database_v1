# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\test_performance_modules.py
# File Name: test_performance_modules
# @ Author: mango-gh22
# @ Date：2025/12/21 22:29
"""
desc 
"""

"""
测试性能模块
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.performance.performance_manager import PerformanceManager


def test_performance_manager():
    """测试性能管理器"""
    manager = PerformanceManager()

    # 测试内存管理
    df = pd.DataFrame({
        'a': np.random.randn(10000),
        'b': np.random.randint(0, 100, 10000),
        'c': ['test'] * 10000
    })

    optimized_df = manager.optimize_dataframe(df)
    assert optimized_df.memory_usage().sum() <= df.memory_usage().sum()

    # 测试并行计算
    def square(x):
        return x * x

    data = list(range(1000))
    results = manager.parallel_calculate(square, data, batch_size=100)
    assert len(results) == 1000
    assert results[10] == 100

    # 测试缓存
    test_key = 'test_key'
    test_value = {'data': [1, 2, 3]}
    manager.set_cache(test_key, test_value, ttl=10)
    cached_value = manager.get_cache(test_key)
    assert cached_value == test_value

    print("所有性能模块测试通过")


if __name__ == '__main__':
    test_performance_manager()