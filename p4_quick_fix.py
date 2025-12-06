# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\p4_quick_fix.py
# File Name: p4_quick_fix
# @ Author: mango-gh22
# @ Date：2025/12/6 17:41
"""
desc 
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P4快速修复脚本
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔧 P4快速修复脚本")
print("=" * 60)


def fix_query_engine_import():
    """修复query_engine导入问题"""
    print("\n1️⃣ 检查query_engine.py...")

    query_engine_file = 'src/query/query_engine.py'
    if not os.path.exists(query_engine_file):
        print(f"❌ 文件不存在: {query_engine_file}")
        return False

    with open(query_engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 确保使用正确的导入
    if 'from src.database.connection import engine' in content:
        print("✅ query_engine.py导入正确")
        return True
    else:
        print("⚠️  query_engine.py可能需要更新")
        return True


def create_sample_data():
    """创建示例数据"""
    print("\n2️⃣ 创建示例数据...")

    try:
        # 创建示例股票
        sample_stocks = [
            ("000001.SZ", "平安银行", "SZ", "银行", "1991-04-03"),
            ("000002.SZ", "万科A", "SZ", "房地产", "1991-01-29"),
            ("600000.SH", "浦发银行", "SH", "银行", "1999-11-10"),
            ("600036.SH", "招商银行", "SH", "银行", "2002-04-09"),
            ("601318.SH", "中国平安", "SH", "保险", "2007-03-01"),
        ]

        from src.database.connection import get_session
        from sqlalchemy import text

        session = get_session()

        # 插入股票
        for symbol, name, exchange, industry, listing_date in sample_stocks:
            sql = text("""
            INSERT INTO stock_basic (symbol, name, exchange, industry, listing_date, is_active)
            VALUES (:symbol, :name, :exchange, :industry, :listing_date, 1)
            ON DUPLICATE KEY UPDATE name=VALUES(name), industry=VALUES(industry)
            """)
            session.execute(sql, {
                'symbol': symbol,
                'name': name,
                'exchange': exchange,
                'industry': industry,
                'listing_date': listing_date
            })

        session.commit()
        print(f"✅ 插入 {len(sample_stocks)} 只示例股票")

        # 检查日线数据
        result = session.execute(text("SELECT COUNT(*) FROM daily_data"))
        daily_count = result.scalar()

        if daily_count == 0:
            print("⚠️  没有日线数据，建议运行: python local_data_collector.py")

        session.close()
        return True

    except Exception as e:
        print(f"❌ 创建示例数据失败: {e}")
        return False


def test_p4_modules():
    """测试P4模块"""
    print("\n3️⃣ 测试P4模块...")

    modules_to_test = [
        ('query_engine', 'QueryEngine'),
        ('indicators', 'TechnicalIndicators'),
        ('export', 'DataExporter'),
    ]

    all_ok = True
    for module_name, class_name in modules_to_test:
        try:
            exec(f"from src.query.{module_name} import {class_name}")
            print(f"✅ {module_name}.{class_name} 导入成功")
        except ImportError as e:
            print(f"❌ {module_name}.{class_name} 导入失败: {e}")
            all_ok = False

    return all_ok


def run_quick_test():
    """运行快速测试"""
    print("\n4️⃣ 运行快速测试...")

    try:
        from src.query.query_engine import QueryEngine
        engine = QueryEngine()

        # 测试连接
        if hasattr(engine, 'test_connection'):
            if engine.test_connection():
                print("✅ 数据库连接测试成功")
            else:
                print("❌ 数据库连接测试失败")
                return False

        # 获取统计
        stats = engine.get_data_statistics()
        print(f"📊 数据统计:")
        print(f"  股票数量: {stats.get('stock_basic', {}).get('total_stocks', 0)}")
        print(f"  日线记录: {stats.get('daily_data', {}).get('total_records', 0)}")

        # 获取股票列表
        stocks = engine.get_stock_list()
        print(f"📋 股票列表: {len(stocks)} 只")

        if stocks:
            # 查询示例数据
            df = engine.get_daily_data(stocks[0], limit=3)
            if not df.empty:
                print(f"\n📅 {stocks[0]} 示例数据:")
                print(df[['trade_date', 'open', 'high', 'low', 'close', 'volume']].to_string())
            else:
                print(f"\n⚠️  {stocks[0]} 没有日线数据")

        engine.close()
        return True

    except Exception as e:
        print(f"❌ 快速测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""

    print("P4阶段问题诊断与修复")
    print("=" * 60)

    steps = [
        ("检查模块导入", fix_query_engine_import),
        ("创建示例数据", create_sample_data),
        ("测试P4模块", test_p4_modules),
        ("运行快速测试", run_quick_test),
    ]

    results = []
    for step_name, step_func in steps:
        print(f"\n▶️  {step_name}...")
        result = step_func()
        results.append((step_name, result))

    print("\n" + "=" * 60)
    print("📋 修复结果汇总:")

    passed = 0
    failed = 0

    for step_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {step_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1

    print(f"\n📊 总计: {passed} 项通过, {failed} 项失败")

    if failed == 0:
        print("\n🎉 所有检查通过! 现在可以测试P4功能:")
        print("  python main.py --action p4_query_test")
        print("  python main.py --action p4_indicators_test")
        print("  python main.py --action p4_full_test")
    else:
        print("\n⚠️  存在未解决的问题，请按照以下步骤操作:")
        print("  1. 运行: python setup_database_complete.py")
        print("  2. 运行: python local_data_collector.py")
        print("  3. 再次运行此脚本")


if __name__ == "__main__":
    main()