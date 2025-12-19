# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_quality_模块.py
# File Name: test_quality_模块
# @ Author: mango-gh22
# @ Date：2025/12/14 17:11
"""
desc 
"""
# test_quality_模块.py
"""
测试质量模块的简化脚本
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.abspath('.'))

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("🧪 测试质量模块")
print("=" * 50)

try:
    # 测试导入
    print("\n1. 测试模块导入...")
    from src.processors.validator import DataValidator, ValidationResult, ValidationResultDetail
    from src.processors.adjustor import StockAdjustor, AdjustType, AdjustMethod, DividendEvent

    print("✅ 模块导入成功")

    # 测试验证器
    print("\n2. 测试数据验证器...")
    validator = DataValidator()

    # 检查规则加载
    print(f"   加载规则数量: {sum(len(rules) for rules in validator.rules.values())}")

    # 获取测试股票
    from src.query.query_engine import QueryEngine

    query_engine = QueryEngine()
    stock_df = query_engine.get_stock_list()

    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']
        print(f"   测试股票: {test_symbol}")

        # 运行完整性验证
        print("   运行完整性验证...")
        completeness_results = validator.validate_completeness(test_symbol)
        print(f"   完整性验证结果: {len(completeness_results)} 条")

        for result in completeness_results:
            print(f"     - {result.rule_name}: {result.result.value}")

    # 测试复权计算器
    print("\n3. 测试复权计算器...")
    adjustor = StockAdjustor()

    # 测试分红事件
    event = DividendEvent(
        symbol='000001.SZ',
        ex_date='2023-06-15',
        cash_div=0.5,
        shares_div=0.3
    )
    print(f"   分红事件测试: {event}")
    print(f"   前复权因子: {event.forward_factor:.6f}")
    print(f"   后复权因子: {event.backward_factor:.6f}")

    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']
        print(f"   为 {test_symbol} 加载分红事件...")
        events = adjustor.load_dividend_events(test_symbol)
        print(f"   加载到 {len(events)} 个分红事件")

        # 计算复权因子
        if events:
            factors_df = adjustor.calculate_adjust_factors(test_symbol, events)
            print(f"   计算 {len(factors_df)} 个复权因子")

    # 测试数据查询和简单复权
    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']
        print(f"\n4. 测试 {test_symbol} 的复权计算...")

        df = query_engine.query_daily_data(
            symbol=test_symbol,
            limit=5
        )

        if not df.empty:
            print(f"   获取到 {len(df)} 条数据")

            # 测试前复权
            forward_df = adjustor.adjust_price(
                df.copy(), test_symbol, AdjustType.FORWARD
            )
            print(f"   前复权完成: {len(forward_df)} 条")

            # 测试后复权
            backward_df = adjustor.adjust_price(
                df.copy(), test_symbol, AdjustType.BACKWARD
            )
            print(f"   后复权完成: {len(backward_df)} 条")

    print("\n✅ 所有测试完成!")

except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback

    traceback.print_exc()
finally:
    # 清理
    if 'validator' in locals():
        validator.close()
    if 'adjustor' in locals():
        adjustor.close()
    if 'query_engine' in locals():
        query_engine.close()