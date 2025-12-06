# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\main.py
# @ Author: mango-gh22
# @ PyCharm
# @ Date：2025/12/4 23:36
"""
desc 项目入口文件
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目主入口文件 - P4阶段版本 (v0.4.0)
包含P1-P4所有功能
"""

import sys
import os
import argparse
import logging
import pandas as pd  # 添加pandas导入

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)


def setup_basic_logging():
    """基本日志设置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


# ==================== P4阶段新增函数 ====================
def test_p4_query_engine():
    """测试P4查询引擎"""
    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        print("\n" + "=" * 50)
        print("🚀 P4查询引擎测试")
        print("=" * 50)

        print("\n=== 测试1: 数据统计 ===")
        stats = engine.get_data_statistics()
        print(f"📊 数据统计:")
        print(f"  股票基本信息: {stats.get('stock_basic', {}).get('total_stocks', 0)} 条")
        print(f"  日线数据: {stats.get('daily_data', {}).get('total_records', 0)} 条")

        print("\n=== 测试2: 股票列表 ===")
        stock_list = engine.get_stock_list()
        print(f"📋 股票列表 ({len(stock_list)} 只):")
        for i, stock in enumerate(stock_list[:5]):  # 只显示前5只
            print(f"  {i + 1}. {stock}")
        if len(stock_list) > 5:
            print(f"  ... 等{len(stock_list)}只股票")

        print("\n=== 测试3: 查询日线数据 ===")
        if stock_list:
            # 查询第一只股票的最近10条数据
            df = engine.get_daily_data(stock_list[0], limit=10)
            if not df.empty:
                print(f"📅 {stock_list[0]} 最近10个交易日数据:")
                print(df[['trade_date', 'open', 'high', 'low', 'close', 'volume']].to_string())

        print("\n✅ P4查询引擎测试完成!")
        engine.close()
        return True

    except Exception as e:
        print(f"❌ P4查询引擎测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_p4_indicators():
    """测试P4技术指标"""
    try:
        from src.query.query_engine import QueryEngine
        from src.query.indicators import TechnicalIndicators

        engine = QueryEngine()

        print("\n" + "=" * 50)
        print("📈 P4技术指标测试")
        print("=" * 50)

        # 获取测试数据
        stock_list = engine.get_stock_list()
        if not stock_list:
            print("❌ 没有可用的股票数据")
            return False

        symbol = stock_list[0]
        df = engine.get_daily_data(symbol, limit=30)

        if not df.empty:
            print(f"\n=== 测试 {symbol} 的技术指标 ===")

            # 设置索引
            df = df.set_index('trade_date')

            # 计算所有指标
            indicators_df = TechnicalIndicators.calculate_all_indicators(df)

            # 显示结果
            print("\n📈 技术指标计算结果 (最近5个交易日):")
            result_cols = ['close']
            # 添加计算出的指标列
            indicator_cols = [col for col in indicators_df.columns
                              if any(x in col for x in ['MA', 'RSI', 'MACD', 'BB'])]
            result_cols.extend(indicator_cols[:8])  # 最多显示8个指标列

            # 显示最后5行
            display_df = indicators_df[result_cols].tail()
            print(display_df.to_string())
        else:
            print(f"❌ 未找到 {symbol} 的数据")
            return False

        print("\n✅ P4技术指标测试完成!")
        engine.close()
        return True

    except Exception as e:
        print(f"❌ P4技术指标测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_p4_export():
    """测试P4数据导出"""
    try:
        from src.query.query_engine import QueryEngine
        from src.query.export import DataExporter

        engine = QueryEngine()
        exporter = DataExporter()

        print("\n" + "=" * 50)
        print("💾 P4数据导出测试")
        print("=" * 50)

        # 获取测试数据
        stock_list = engine.get_stock_list()
        if not stock_list:
            print("❌ 没有可用的股票数据")
            return False

        symbol = stock_list[0]

        # 查询数据
        df = engine.get_daily_data(symbol, limit=20)
        if df.empty:
            print(f"❌ 未找到 {symbol} 的数据")
            return False

        print(f"\n=== 测试导出 {symbol} 数据 ===")

        # 导出为CSV
        csv_file = exporter.export_to_csv(df, f"{symbol}_test_export")

        if csv_file:
            print(f"✅ CSV文件已导出: {csv_file}")

            # 读取并显示导出的数据
            try:
                exported_df = pd.read_csv(csv_file)
                print(f"\n📄 导出文件内容预览 (前5行):")
                print(exported_df.head().to_string())
            except Exception as read_error:
                print(f"⚠️  读取导出文件失败: {read_error}")
        else:
            print("❌ CSV导出失败")
            return False

        print("\n✅ P4数据导出测试完成!")
        engine.close()
        return True

    except Exception as e:
        print(f"❌ P4数据导出测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def interactive_query():
    """交互式查询"""
    try:
        from src.query.query_engine import QueryEngine
        from src.query.indicators import TechnicalIndicators
        from src.query.export import DataExporter

        engine = QueryEngine()
        exporter = DataExporter()

        print("\n" + "=" * 50)
        print("🔍 股票数据库查询系统 v0.4.0")
        print("=" * 50)

        while True:
            print("\n请选择操作:")
            print("1. 查看数据统计")
            print("2. 查看股票列表")
            print("3. 查询日线数据")
            print("4. 计算技术指标")
            print("5. 导出数据")
            print("6. 退出")

            choice = input("\n请输入选项 (1-6): ").strip()

            if choice == '1':
                stats = engine.get_data_statistics()
                print(f"\n📊 数据统计:")
                print(f"  股票数量: {stats.get('stock_basic', {}).get('total_stocks', 0)}")
                print(f"  日线记录: {stats.get('daily_data', {}).get('total_records', 0)}")
                print(f"  最早日期: {stats.get('daily_data', {}).get('earliest_date', 'N/A')}")
                print(f"  最新日期: {stats.get('daily_data', {}).get('latest_date', 'N/A')}")

            elif choice == '2':
                stocks = engine.get_stock_list()
                print(f"\n📋 股票列表 ({len(stocks)} 只):")
                for i, stock in enumerate(stocks):
                    print(f"  {i + 1:3d}. {stock}")

            elif choice == '3':
                symbol = input("请输入股票代码 (如: 000001.SZ): ").strip()
                if not symbol:
                    print("⚠️  请输入有效的股票代码")
                    continue

                limit = input("请输入查询条数 (默认10): ").strip()
                limit = int(limit) if limit.isdigit() else 10

                df = engine.get_daily_data(symbol, limit=limit)
                if not df.empty:
                    print(f"\n📅 {symbol} 数据:")
                    print(df.to_string())
                else:
                    print(f"❌ 未找到 {symbol} 的数据")

            elif choice == '4':
                symbol = input("请输入股票代码 (如: 000001.SZ): ").strip()
                if not symbol:
                    print("⚠️  请输入有效的股票代码")
                    continue

                df = engine.get_daily_data(symbol, limit=50)
                if not df.empty:
                    df = df.set_index('trade_date')
                    indicators_df = TechnicalIndicators.calculate_all_indicators(df)

                    print(f"\n📈 {symbol} 技术指标 (最近10个交易日):")
                    result_cols = ['close']
                    indicator_cols = [col for col in indicators_df.columns
                                      if any(x in col for x in ['MA', 'RSI', 'MACD', 'BB'])]
                    result_cols.extend(indicator_cols[:5])  # 只显示前5个指标列

                    display_df = indicators_df[result_cols].tail(10)
                    print(display_df.to_string())
                else:
                    print(f"❌ 未找到 {symbol} 的数据")

            elif choice == '5':
                symbol = input("请输入股票代码 (如: 000001.SZ): ").strip()
                if not symbol:
                    print("⚠️  请输入有效的股票代码")
                    continue

                export_type = input("导出格式 (csv/excel/json, 默认csv): ").strip().lower()
                export_type = export_type if export_type in ['csv', 'excel', 'json'] else 'csv'

                df = engine.get_daily_data(symbol, limit=100)
                if not df.empty:
                    if export_type == 'csv':
                        filepath = exporter.export_to_csv(df, f"{symbol}_export")
                    elif export_type == 'excel':
                        filepath = exporter.export_to_excel({symbol: df}, f"{symbol}_export")
                    elif export_type == 'json':
                        filepath = exporter.export_to_json(df, f"{symbol}_export")

                    if filepath:
                        print(f"✅ 数据已导出到: {filepath}")
                    else:
                        print("❌ 导出失败")
                else:
                    print(f"❌ 未找到 {symbol} 的数据")

            elif choice == '6':
                print("👋 退出查询系统")
                break

            else:
                print("❌ 无效的选项，请重新输入")

        engine.close()
        return True

    except Exception as e:
        print(f"❌ 交互式查询失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_p4_full_test():
    """运行完整的P4测试"""
    print("\n" + "=" * 50)
    print("🧪 P4阶段完整测试")
    print("=" * 50)

    results = []

    # 测试1: 查询引擎
    print("\n1️⃣ 测试查询引擎...")
    results.append(("查询引擎", test_p4_query_engine()))

    # 测试2: 技术指标
    print("\n2️⃣ 测试技术指标...")
    results.append(("技术指标", test_p4_indicators()))

    # 测试3: 数据导出
    print("\n3️⃣ 测试数据导出...")
    results.append(("数据导出", test_p4_export()))

    # 测试结果汇总
    print("\n" + "=" * 50)
    print("📋 P4测试结果汇总")
    print("=" * 50)

    passed = 0
    failed = 0

    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
        else:
            failed += 1

    print(f"\n📊 总计: {passed} 项通过, {failed} 项失败")

    if failed == 0:
        print("\n🎉 P4阶段所有测试通过!")
        return True
    else:
        print("\n⚠️  P4阶段测试未完全通过")
        return False



def validate_data():
    """验证数据"""
    print("🔍 数据验证报告")
    print("=" * 50)

    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        try:
            # 获取统计信息
            stats = engine.get_data_statistics()

            if not stats:
                print("❌ 无法获取数据统计")
                return

            print(f"\n📊 股票基本信息:")
            print(f"  总股票数: {stats.get('total_stocks', 0)}")
            print(f"  行业数量: {stats.get('industry_count', 0)}")

            print(f"\n📅 日线数据:")
            print(f"  总记录数: {stats.get('total_daily_records', 0)}")
            print(f"  最早日期: {stats.get('earliest_date', 'N/A')}")
            print(f"  最新日期: {stats.get('latest_date', 'N/A')}")
            print(f"  有数据的股票: {stats.get('stocks_with_data', 0)}")

            if stats.get('stock_list'):
                print(f"\n📋 股票列表 ({len(stats['stock_list'])} 只):")
                for i, symbol in enumerate(stats['stock_list'][:10], 1):
                    name = stats['stock_details'].get(symbol, '')
                    print(f"  {i:2}. {symbol} {name}")
                if len(stats['stock_list']) > 10:
                    print(f"  ... 还有 {len(stats['stock_list']) - 10} 只股票")

            print("\n✅ 数据验证完成")

        finally:
            engine.close()

    except Exception as e:
        print(f"❌ 数据验证失败: {e}")
        import traceback
        traceback.print_exc()
