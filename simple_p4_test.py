
"""
简单测试脚本
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 简单测试P4功能")
    print("=" * 50)

    try:
        # 直接导入测试
        print("\n1. 导入QueryEngine...")
        from src.query.query_engine import QueryEngine
        print("✅ 导入成功")

        # 测试引擎
        print("\n2. 创建查询引擎...")
        engine = QueryEngine()

        # 测试统计
        print("\n3. 测试数据统计...")
        stats = engine.get_data_statistics()

        if stats:
            print(f"📊 统计结果:")
            print(f"  股票数量: {stats.get('total_stocks', 0)}")
            print(f"  日线记录: {stats.get('total_daily_records', 0)}")

            if stats.get('total_daily_records', 0) > 0:
                print("✅ 数据库中有数据!")

                # 测试查询
                print("\n4. 测试查询...")
                if stats.get('stock_list'):
                    test_symbol = stats['stock_list'][0]
                    print(f"  查询股票: {test_symbol}")

                    data = engine.query_daily_data(symbol=test_symbol, limit=2)
                    if not data.empty:
                        print(f"✅ 查询成功: {len(data)}条记录")
                        print(data[['trade_date', 'symbol', 'close', 'price_change']].to_string())
                    else:
                        print("⚠️  未查询到数据")

        engine.close()
        print("\n🎉 简单测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
