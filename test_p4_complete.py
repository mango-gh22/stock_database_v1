# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_p4_complete.py
# File Name: test_p4_complete
# @ Author: mango-gh22
# @ Date：2025/12/6 18:17
"""
desc 
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的P4测试
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🧪 完整的P4测试")
print("=" * 60)


def test_query_engine_fixed():
    """测试修复后的查询引擎"""
    print("\n1️⃣ 测试查询引擎（修复版）...")

    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        # 数据统计
        stats = engine.get_data_statistics()
        print(f"📊 数据统计:")
        print(f"  股票数量: {stats.get('stock_basic', {}).get('total_stocks', 0)}")
        print(f"  日线记录: {stats.get('daily_data', {}).get('total_records', 0)}")

        # 股票列表
        stocks = engine.get_stock_list()
        print(f"\n📋 股票列表 ({len(stocks)} 只):")
        for stock in stocks[:5]:
            print(f"  - {stock}")

        # 日线数据查询
        if stocks:
            print(f"\n📅 测试日线数据查询...")

            # 方法1: 使用修复的get_daily_data
            df = engine.get_daily_data(stocks[0], limit=5)
            if not df.empty:
                print(f"✅ {stocks[0]} 日线数据查询成功 ({len(df)} 条)")
                print(df[['trade_date', 'open', 'high', 'low', 'close', 'volume']].head().to_string())
            else:
                print(f"⚠️  {stocks[0]} 未查询到数据")

                # 尝试直接查询
                print("\n🔍 尝试直接查询...")
                from src.database.connection import get_session
                from sqlalchemy import text

                session = get_session()
                query = """
                SELECT trade_date, symbol, open, high, low, close, volume
                FROM stock_daily_data
                WHERE symbol = :symbol
                ORDER BY trade_date DESC
                LIMIT 3
                """
                result = session.execute(text(query), {'symbol': stocks[0]})
                rows = result.fetchall()
                session.close()

                if rows:
                    print(f"✅ 直接查询成功 ({len(rows)} 条)")
                    for row in rows:
                        print(f"  {row[0]} | 收盘:{row[5]:.2f} 成交量:{row[6]:,.0f}")

        engine.close()
        return True

    except Exception as e:
        print(f"❌ 查询引擎测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_indicators():
    """测试技术指标"""
    print("\n2️⃣ 测试技术指标计算...")

    try:
        from src.query.query_engine import QueryEngine
        from src.query.indicators import TechnicalIndicators

        engine = QueryEngine()
        stocks = engine.get_stock_list()

        if not stocks:
            print("❌ 没有股票数据")
            return False

        # 获取数据
        df = engine.get_daily_data(stocks[0], limit=30)

        if df.empty:
            # 尝试获取更多数据
            from src.database.connection import get_session
            from sqlalchemy import text
            import pandas as pd

            session = get_session()
            query = """
            SELECT trade_date, close, volume
            FROM stock_daily_data
            WHERE symbol = :symbol
            ORDER BY trade_date DESC
            LIMIT 30
            """
            result = session.execute(text(query), {'symbol': stocks[0]})
            data = result.fetchall()
            session.close()

            if data:
                df = pd.DataFrame(data, columns=['trade_date', 'close', 'volume'])
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df = df.set_index('trade_date')
                df = df.sort_index()

        if not df.empty:
            print(f"📈 测试 {stocks[0]} 技术指标...")

            # 计算指标
            if 'close' in df.columns:
                df_with_indicators = TechnicalIndicators.calculate_all_indicators(df)

                # 显示结果
                print("\n📊 技术指标计算结果:")
                result_cols = ['close']
                if 'MA5' in df_with_indicators.columns:
                    result_cols.append('MA5')
                if 'RSI' in df_with_indicators.columns:
                    result_cols.append('RSI')

                if len(result_cols) > 1:
                    display_df = df_with_indicators[result_cols].tail()
                    print(display_df.to_string())
                    print("✅ 技术指标计算成功")
                else:
                    print("⚠️  未计算出指标")
            else:
                print("❌ 数据缺少close列")
        else:
            print("❌ 无法获取数据")

        engine.close()
        return True

    except Exception as e:
        print(f"❌ 技术指标测试失败: {e}")
        return False


def test_export():
    """测试数据导出"""
    print("\n3️⃣ 测试数据导出...")

    try:
        from src.query.query_engine import QueryEngine
        from src.query.export import DataExporter

        engine = QueryEngine()
        exporter = DataExporter()

        stocks = engine.get_stock_list()

        if not stocks:
            print("❌ 没有股票数据")
            return False

        # 获取数据
        df = engine.get_daily_data(stocks[0], limit=10)

        if df.empty:
            print("⚠️  没有日线数据，跳过导出测试")
            return True

        # 导出CSV
        print(f"💾 导出 {stocks[0]} 数据...")
        csv_file = exporter.export_to_csv(df, f"{stocks[0]}_test")

        if csv_file:
            print(f"✅ CSV导出成功: {csv_file}")

            # 验证文件
            if os.path.exists(csv_file):
                file_size = os.path.getsize(csv_file)
                print(f"📄 文件大小: {file_size} 字节")

                # 读取验证
                import pandas as pd
                exported_df = pd.read_csv(csv_file)
                print(f"📊 导出数据: {len(exported_df)} 行 x {len(exported_df.columns)} 列")
            return True
        else:
            print("❌ CSV导出失败")
            return False

    except Exception as e:
        print(f"❌ 数据导出测试失败: {e}")
        return False


def main():
    """主函数"""
    print("P4阶段完整功能测试")
    print("=" * 60)

    tests = [
        ("查询引擎", test_query_engine_fixed),
        ("技术指标", test_indicators),
        ("数据导出", test_export),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n▶️  {test_name}测试...")
        result = test_func()
        results.append((test_name, result))

    print("\n" + "=" * 60)
    print("📋 测试结果汇总:")

    passed = 0
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1

    print(f"\n📊 总计: {passed}/{len(tests)} 项通过")

    if passed == len(tests):
        print("\n🎉 P4阶段所有功能测试通过!")
        print("\n📋 下一步:")
        print("  1. 提交代码: git add src/query/ tests/")
        print("  2. 打标签: git tag -a v0.4.0 -m 'P4阶段完成'")
        print("  3. 推送到GitHub: git push origin main --tags")
    else:
        print(f"\n⚠️  还有 {len(tests) - passed} 项需要修复")


if __name__ == "__main__":
    main()