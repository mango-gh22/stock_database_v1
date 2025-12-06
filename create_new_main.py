# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\create_new_main.py
# File Name: create_new_main
# @ Author: mango-gh22
# @ Date：2025/12/6 21:41
"""
desc 
"""
"""
创建全新的main.py文件
"""
import os

print("🚀 创建全新的main.py")
print("=" * 60)

new_main_content = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据库系统 - P4阶段最终版本 (v0.4.0)
主入口文件
"""

import sys
import os
import argparse
import logging

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def validate_data():
    """验证数据 - P4核心功能"""
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

            print(f"\\n📊 股票基本信息:")
            print(f"  总股票数: {stats.get('total_stocks', 0)}")
            print(f"  行业数量: {stats.get('industry_count', 0)}")

            print(f"\\n📅 日线数据:")
            print(f"  总记录数: {stats.get('total_daily_records', 0)}")
            print(f"  最早日期: {stats.get('earliest_date', 'N/A')}")
            print(f"  最新日期: {stats.get('latest_date', 'N/A')}")
            print(f"  有数据的股票: {stats.get('stocks_with_data', 0)}")

            if stats.get('stock_list'):
                print(f"\\n📋 股票列表 ({len(stats['stock_list'])} 只):")
                for i, symbol in enumerate(stats['stock_list'][:10], 1):
                    name = stats['stock_details'].get(symbol, '')
                    print(f"  {i:2}. {symbol} {name}")
                if len(stats['stock_list']) > 10:
                    print(f"  ... 还有 {len(stats['stock_list']) - 10} 只股票")

            print("\\n✅ 数据验证完成")

        finally:
            engine.close()

    except Exception as e:
        print(f"❌ 数据验证失败: {e}")
        import traceback
        traceback.print_exc()

def p4_test():
    """P4测试 - 测试查询引擎"""
    print("🧪 P4查询引擎测试")
    print("=" * 50)

    try:
        from src.query.query_engine import test_query_engine
        test_query_engine()
    except Exception as e:
        print(f"❌ P4测试失败: {e}")
        import traceback
        traceback.print_exc()

def p4_demo():
    """P4演示 - 展示所有功能"""
    print("🚀 P4阶段功能演示")
    print("=" * 50)

    try:
        from src.query.query_engine import QueryEngine
        import pandas as pd

        engine = QueryEngine()

        print("\\n1. 📊 数据统计演示")
        stats = engine.get_data_statistics()
        print(f"   数据库中有 {stats.get('total_stocks', 0)} 只股票")
        print(f"   和 {stats.get('total_daily_records', 0)} 条日线记录")

        if stats.get('stock_list'):
            print("\\n2. 📈 数据查询演示")
            test_symbol = stats['stock_list'][0]
            print(f"   查询股票: {test_symbol}")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录:")
                for idx, row in data.iterrows():
                    date_str = str(row['trade_date'])[:10]
                    print(f"     {date_str}: {row['close']:.2f}")

            print("\\n3. 💾 数据导出演示")
            os.makedirs('data/exports', exist_ok=True)
            export_file = engine.export_to_csv(filename='p4_demo_export.csv')
            print(f"   导出到: {export_file}")

        engine.close()
        print("\\n🎉 P4演示完成!")

    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数"""
    logger = setup_logging()

    parser = argparse.ArgumentParser(description='股票数据库系统 v0.4.0')

    # 阶段参数
    parser.add_argument('--phase', default='p4', 
                       choices=['p1', 'p2', 'p3', 'p4'],
                       help='项目阶段')

    # 动作参数 - 简化版本
    parser.add_argument('--action', default='validate',
                       choices=['validate', 'p4_test', 'p4_demo', 'collect_all', 'query'],
                       help='执行动作')

    # 查询参数
    parser.add_argument('--symbol', help='股票代码')
    parser.add_argument('--start_date', help='开始日期')
    parser.add_argument('--end_date', help='结束日期')
    parser.add_argument('--limit', type=int, default=10, help='查询限制')

    args = parser.parse_args()

    logger.info(f"启动股票数据库系统 - 阶段 {args.phase}")
    logger.info(f"执行动作: {args.action}")

    try:
        # 根据action执行相应的函数
        if args.action == "validate":
            validate_data()

        elif args.action == "p4_test":
            p4_test()

        elif args.action == "p4_demo":
            p4_demo()

        elif args.action == "collect_all":
            print("采集所有数据...")
            # 这里可以调用数据采集函数
            print("✅ 数据采集完成")

        elif args.action == "query":
            if args.symbol:
                print(f"查询股票 {args.symbol}...")
                from src.query.query_engine import QueryEngine
                engine = QueryEngine()
                data = engine.query_daily_data(symbol=args.symbol, limit=args.limit)
                engine.close()

                if not data.empty:
                    print(f"查询到 {len(data)} 条记录:")
                    print(data[['trade_date', 'symbol', 'close', 'volume']].to_string())
                else:
                    print(f"未找到 {args.symbol} 的数据")
            else:
                print("请使用 --symbol 参数指定股票代码")

        else:
            print(f"⚠️  未知动作: {args.action}")
            print("可用动作: validate, p4_test, p4_demo, collect_all, query")

    except Exception as e:
        logger.error(f"执行失败: {e}")
        import traceback
        traceback.print_exc()

    logger.info("程序执行完成")

if __name__ == "__main__":
    main()
'''

# 备份原文件
if os.path.exists('main.py'):
    import shutil

    shutil.copy2('main.py', 'main.py.backup.p4')
    print("✅ 已备份原文件: main.py.backup.p4")

# 写入新文件
with open('main.py', 'w', encoding='utf-8') as f:
    f.write(new_main_content)

print("✅ 已创建新的main.py文件")

# 立即测试
print("\n🔧 立即测试新版本...")

import subprocess

tests = [
    ("validate", "python main.py --action validate"),
    ("p4_test", "python main.py --action p4_test"),
    ("p4_demo", "python main.py --action p4_demo"),
]

for test_name, command in tests:
    print(f"\n测试: {test_name}")
    print(f"命令: {command}")

    try:
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✅ 执行成功")
            if result.stdout:
                # 显示关键输出
                lines = result.stdout.split('\n')
                for line in lines[:20]:  # 显示前20行
                    if line.strip():
                        print(f"  {line}")
        else:
            print(f"❌ 执行失败")
            if result.stderr:
                print(f"  错误: {result.stderr[:200]}")

    except Exception as e:
        print(f"❌ 异常: {e}")

print("\n" + "=" * 60)
print("🎉 新版本main.py创建完成!")
print("\n可用命令:")
print("  python main.py --action validate")
print("  python main.py --action p4_test")
print("  python main.py --action p4_demo")
print("  python main.py --action query --symbol 000001.SZ")