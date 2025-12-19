# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\performance_test.py
# File Name: performance_test
# @ Author: mango-gh22
# @ Date：2025/12/14 15:41
"""
desc 
"""
# performance_test.py
"""
性能测试脚本
"""
import time
from src.processors.validator import DataValidator
from src.processors.adjustor import StockAdjustor


def test_performance():
    """性能测试"""
    print("🚀 性能测试开始")
    print("=" * 50)

    # 初始化
    validator = DataValidator()
    adjustor = StockAdjustor()

    try:
        # 1. 获取100只股票
        stock_df = validator.query_engine.get_stock_list()
        symbols = stock_df['symbol'].head(100).tolist()

        print(f"测试股票数量: {len(symbols)}")

        # 2. 数据验证性能测试
        print("\n📊 数据验证性能测试")
        start_time = time.time()

        for i, symbol in enumerate(symbols[:10]):  # 先测试10只
            try:
                validator.validate_all(symbol)
                if (i + 1) % 5 == 0:
                    print(f"  进度: {i + 1}/10")
            except Exception as e:
                print(f"  {symbol} 验证失败: {e}")

        validation_time = time.time() - start_time
        print(f"  验证10只股票用时: {validation_time:.2f}秒")
        print(f"  预计100只股票用时: {validation_time * 10:.2f}秒")

        # 3. 复权计算性能测试
        print("\n💰 复权计算性能测试")
        start_time = time.time()

        adjustment_results = adjustor.adjust_batch(
            symbols[:20],  # 测试20只
            start_date='2023-01-01'
        )

        adjustment_time = time.time() - start_time
        print(f"  复权20只股票用时: {adjustment_time:.2f}秒")
        print(f"  成功: {len(adjustment_results)}只")

        # 4. 内存使用检查
        import psutil
        import os
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        print(f"\n💾 内存使用: {memory_mb:.2f} MB")

        # 验证性能目标
        print("\n✅ 性能目标验证:")
        print(f"  100只股票验证 < 30秒: {'✓' if validation_time * 10 < 30 else '✗'}")
        print(f"  内存使用 < 2GB: {'✓' if memory_mb < 2000 else '✗'}")

        print("\n🎉 性能测试完成!")

    except Exception as e:
        print(f"❌ 性能测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        validator.close()
        adjustor.close()


if __name__ == "__main__":
    test_performance()