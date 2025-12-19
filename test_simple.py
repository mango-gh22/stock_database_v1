# test_simple.py
"""
简单验证脚本
"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

print("🧪 简单验证测试")
print("=" * 50)

try:
    # 测试导入
    print("\n1. 测试模块导入...")
    from src.query.query_engine import QueryEngine
    from src.processors.validator import DataValidator
    from src.processors.adjustor import StockAdjustor
    print("✅ 模块导入成功")

    # 测试查询引擎
    print("\n2. 测试查询引擎...")
    engine = QueryEngine()

    # 获取股票列表
    stocks = engine.get_stock_list()
    if not stocks.empty:
        symbol = stocks.iloc[0]['symbol']
        print(f"   测试股票: {symbol}")

        # 查询数据
        data = engine.query_daily_data(symbol=symbol, limit=3)
        if not data.empty:
            print(f"   ✅ 查询成功: {len(data)} 条")

            # 检查列名
            print("   检查列名:")
            for col in ['open', 'high', 'low', 'close', 'pct_change', 'volume']:
                if col in data.columns:
                    print(f"     ✓ {col}")
                else:
                    print(f"     ✗ {col} (缺失)")

            # 显示数据
            print("\n   示例数据:")
            for i in range(min(2, len(data))):
                row = data.iloc[i]
                print(f"     {row['trade_date']}: {row['close']:.2f}")
        else:
            print("   ⚠️ 查询返回空数据")
    else:
        print("   ⚠️ 无股票数据")

    engine.close()
    print("\n✅ 简单测试完成!")

except Exception as e:
    print(f"\n❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
