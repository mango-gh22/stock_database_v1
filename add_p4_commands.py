# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\add_p4_commands.py
# File Name: add_p4_commands
# @ Author: mango-gh22
# @ Date：2025/12/6 20:06
"""
desc 
"""
"""
添加P4测试命令到main.py
"""
import re


def update_main_py():
    """更新main.py文件"""
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    print("🔧 更新main.py添加P4命令")
    print("=" * 50)

    # 1. 更新action参数
    # 查找当前的action列表
    pattern = r"choices=\[([^\]]+)\]"
    match = re.search(pattern, content)

    if match:
        current_actions = match.group(1)
        print(f"当前actions: {current_actions}")

        # 添加p4_test到列表中
        if "'p4_test'" not in current_actions:
            # 在列表末尾添加
            new_actions = current_actions.rstrip() + ", 'p4_test', 'p4_safe_test'"
            new_content = content.replace(current_actions, new_actions)

            with open('main.py', 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已添加p4_test和p4_safe_test到action列表")
        else:
            print("✅ p4_test已在action列表中")

    # 2. 添加p4_test的处理逻辑
    # 查找elif action == 的模式，在合适的位置插入
    if 'elif action == "validate":' in content:
        # 在validate后面添加p4_test
        p4_test_code = '''
    elif action == "p4_test":
        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)

        try:
            # 先测试数据库连接
            from src.database.connection import get_connection
            conn = get_connection()
            cursor = conn.cursor()

            # 测试查询
            cursor.execute("SELECT COUNT(*) FROM stock_daily_data")
            count = cursor.fetchone()[0]
            print(f"📊 数据库中有 {count} 条日线记录")

            cursor.execute("SELECT DISTINCT symbol FROM stock_daily_data LIMIT 3")
            symbols = [row[0] for row in cursor.fetchall()]
            print(f"📋 股票代码示例: {symbols}")

            cursor.close()
            conn.close()

            # 测试查询引擎
            print("\\n🚀 测试查询引擎...")
            from src.query.safe_query_engine import SafeQueryEngine
            engine = SafeQueryEngine()

            # 数据统计
            stats = engine.get_data_statistics()
            print(f"✅ 数据统计: {stats.get('total_daily_records', 0)}条记录")

            # 查询示例
            if symbols:
                data = engine.query_daily_data(symbol=symbols[0], limit=3)
                print(f"📈 查询{symbols[0]}: {len(data)}条记录")
                if not data.empty:
                    print(data[['trade_date', 'close', 'price_change']].to_string())

            engine.close()

            print("\\n🎉 P4查询引擎测试完成!")

        except Exception as e:
            print(f"❌ P4测试失败: {e}")
            import traceback
            traceback.print_exc()

    elif action == "p4_safe_test":
        print("🔒 P4安全查询引擎测试")
        print("=" * 50)

        try:
            from src.query.safe_query_engine import test_safe_engine
            test_safe_engine()
        except Exception as e:
            print(f"❌ 安全引擎测试失败: {e}")
            import traceback
            traceback.print_exc()
'''

        # 插入代码
        new_content = content.replace(
            'elif action == "validate":',
            f'''elif action == "validate":{p4_test_code}
    elif action == "validate":'''
        )

        # 移除重复的validate
        new_content = new_content.replace(
            '''elif action == "validate":        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)''',
            'elif action == "validate":'
        )

        with open('main.py', 'w', encoding='utf-8') as f:
            f.write(new_content)

        print("✅ 已添加p4_test处理逻辑")

    print("\n📋 更新完成!")
    print("\n现在可以使用以下命令:")
    print("  python main.py --action p4_test")
    print("  python main.py --action p4_safe_test")


def create_quick_test():
    """创建快速测试脚本"""
    quick_test = '''
"""
P4快速测试 - 独立脚本
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 P4阶段快速测试")
    print("=" * 50)

    try:
        # 1. 测试数据库连接
        print("\\n🔗 测试数据库连接...")
        try:
            from src.database.connection import get_connection
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"✅ 数据库连接成功，表数量: {len(tables)}")
            print(f"   表名: {tables}")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return

        # 2. 测试安全查询引擎
        print("\\n🔍 测试安全查询引擎...")
        try:
            from src.query.safe_query_engine import SafeQueryEngine
            engine = SafeQueryEngine()

            # 数据统计
            stats = engine.get_data_statistics()
            print(f"📊 数据统计:")
            print(f"   股票总数: {stats.get('total_stocks', 0)}")
            print(f"   日线记录: {stats.get('total_daily_records', 0)}")

            # 查询测试
            if stats.get('stock_list'):
                test_symbol = stats['stock_list'][0]
                print(f"\\n📈 查询测试: {test_symbol}")

                data = engine.query_daily_data(symbol=test_symbol, limit=3)
                if not data.empty:
                    print(f"✅ 查询成功: {len(data)}条记录")
                    for idx, row in data.iterrows():
                        date_str = row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
                        print(f"   {date_str}: {row['close']:.2f} ({row.get('price_change', 0):+.2f})")
                else:
                    print("⚠️  未查询到数据")

            engine.close()
            print("\\n🎉 P4测试完成!")

        except Exception as e:
            print(f"❌ 查询引擎测试失败: {e}")
            import traceback
            traceback.print_exc()

    except Exception as e:
        print(f"❌ 测试过程出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
'''

    with open('quick_p4_test.py', 'w', encoding='utf-8') as f:
        f.write(quick_test)

    print("✅ 已创建快速测试脚本: quick_p4_test.py")


if __name__ == "__main__":
    update_main_py()
    create_quick_test()

    print("\n" + "=" * 50)
    print("🎉 P4命令添加完成!")
    print("\n运行测试:")
    print("  python quick_p4_test.py")
    print("  或")
    print("  python main.py --action p4_test")