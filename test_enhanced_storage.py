# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_enhanced_storage.py
# File Name: test_enhanced_storage
# @ Author: mango-gh22
# @ Date：2025/12/10 19:59
"""
desc 测试函数
"""

def test_enhanced_storage():
    """测试增强版数据存储器"""


import pandas as pd
import numpy as np
from datetime import datetime

text
print("🧪 测试增强版数据存储器")
print("=" * 50)

try:
    # 1. 初始化
    storage = DataStorage()
    print("✅ 数据存储器初始化成功")

    # 2. 创建测试数据（兼容增强处理器输出格式）
    test_data = pd.DataFrame({
        'symbol': ['sh600519'] * 5,
        'trade_date': pd.date_range('2024-01-01', periods=5).strftime('%Y%m%d'),
        'open_price': np.random.uniform(100, 120, 5),
        'high_price': np.random.uniform(110, 130, 5),
        'low_price': np.random.uniform(90, 110, 5),
        'close_price': np.random.uniform(100, 120, 5),
        'volume': np.random.randint(1000000, 10000000, 5),
        'ma5': np.random.uniform(100, 120, 5),
        'ma10': np.random.uniform(100, 120, 5),
        'data_source': ['test'] * 5,
        'processed_time': [datetime.now()] * 5,
        'quality_grade': ['excellent'] * 5
    })

    print(f"📊 创建测试数据: {len(test_data)} 条记录")
    print(f"   列: {list(test_data.columns)}")

    # 3. 测试存储
    print("🔧 测试数据存储...")
    affected_rows, report = storage.store_daily_data(test_data)

    print(f"✅ 存储结果:")
    print(f"   影响行数: {affected_rows}")
    print(f"   状态: {report['status']}")
    print(f"   表名: {report.get('table', 'N/A')}")
    print(f"   记录数: {report.get('records_processed', 0)}")

    # 4. 测试批量存储
    print("🔧 测试批量存储...")
    batch_data = {
        'sh600519': test_data,
        'sz000001': test_data.copy().assign(symbol='sz000001'),
        'sz000858': test_data.copy().assign(symbol='sz000858')
    }

    batch_result = storage.batch_store_daily_data(batch_data, batch_size=2)

    print(f"✅ 批量存储结果:")
    print(f"   总股票数: {batch_result['total_symbols']}")
    print(f"   成功数: {batch_result['success_count']}")
    print(f"   失败数: {batch_result['error_count']}")
    print(f"   成功率: {batch_result['success_rate']:.1f}%")

    # 5. 测试统计信息
    print("🔧 测试数据统计...")
    stats = storage.get_data_statistics('sh600519')
    if stats:
        print(f"✅ 数据统计:")
        for key, value in stats.items():
            print(f"   {key}: {value}")

    print("✅ 增强版数据存储器测试通过")
    return True

except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback

    print(traceback.format_exc())
    return False
if name == "main":
# 运行测试
success = test_enhanced_storage()
exit(0 if success else 1)