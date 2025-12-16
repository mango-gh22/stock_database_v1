# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_integration.py
# File Name: test_integration
# @ Author: mango-gh22
# @ Date：2025/12/14 19:30
"""
desc 
"""
# test_integration.py
"""
集成测试：验证块1和块2的集成
"""

import sys
import os
import logging

# 添加项目路径
sys.path.insert(0, os.path.abspath('.'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("🔗 集成测试：数据质量模块")
print("=" * 60)

try:
    # 测试1: 验证数据库连接
    print("\n1. 测试数据库连接...")
    from src.database.db_connector import DatabaseConnector

    db = DatabaseConnector()

    # 检查质量相关表
    tables = ['data_quality_log', 'adjust_factors', 'data_anomalies', 'quality_rules']
    existing_tables = []

    for table in tables:
        try:
            result = db.execute_query(f"SHOW TABLES LIKE '{table}'")
            if result:
                existing_tables.append(table)
        except:
            pass

    print(f"   质量相关表: {len(existing_tables)}/{len(tables)} 已创建")
    for table in existing_tables:
        print(f"   ✓ {table}")
    for table in set(tables) - set(existing_tables):
        print(f"   ✗ {table} (缺失)")

    # 测试2: 验证器集成
    print("\n2. 测试数据验证器集成...")
    from src.processors.validator import DataValidator

    validator = DataValidator()

    # 检查规则加载
    rule_count = sum(len(rules) for rules in validator.rules.values())
    print(f"   加载质量规则: {rule_count} 条")

    # 测试3: 复权计算器集成
    print("\n3. 测试复权计算器集成...")
    from src.processors.adjustor import StockAdjustor, AdjustType

    adjustor = StockAdjustor()

    # 测试枚举类型
    print(f"   复权类型: {[t.value for t in AdjustType]}")

    # 测试4: 与查询引擎集成
    print("\n4. 测试与查询引擎集成...")
    from src.query.query_engine import QueryEngine

    query_engine = QueryEngine()

    # 获取股票数据
    stock_df = query_engine.get_stock_list()
    print(f"   可用股票数量: {len(stock_df)}")

    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']
        print(f"   测试股票: {test_symbol}")

        # 测试数据查询
        data = query_engine.query_daily_data(symbol=test_symbol, limit=3)
        print(f"   获取数据: {len(data)} 条")

        if not data.empty:
            # 测试验证
            print("   运行数据验证...")
            try:
                results = validator.validate_completeness(test_symbol)
                print(f"   验证结果: {len(results)} 条")
            except Exception as e:
                print(f"   验证失败: {e}")

            # 测试复权
            print("   运行复权计算...")
            try:
                adjusted_data = adjustor.adjust_price(data.copy(), test_symbol, AdjustType.FORWARD)
                print(f"   复权完成: {len(adjusted_data)} 条")
                print(f"   复权类型: {adjusted_data['adjust_type'].iloc[0]}")
            except Exception as e:
                print(f"   复权失败: {e}")

    # 测试5: 质量监控（如果可用）
    print("\n5. 测试质量监控器...")
    try:
        from src.processors.quality_monitor import QualityMonitor

        monitor = QualityMonitor()
        print("   ✓ 质量监控器可用")

        # 运行快速检查
        report = monitor.run_daily_check()
        print(f"   每日检查完成: {len(report.get('checks', []))} 项检查")

    except ImportError:
        print("   ⚠️ 质量监控器不可用（可选模块）")

    print("\n✅ 集成测试完成!")

except Exception as e:
    print(f"❌ 集成测试失败: {e}")
    import traceback

    traceback.print_exc()

finally:
    # 清理资源
    for var in ['db', 'validator', 'adjustor', 'query_engine', 'monitor']:
        if var in locals():
            try:
                locals()[var].close()
            except:
                pass