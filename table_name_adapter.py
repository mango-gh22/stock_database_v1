# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\table_name_adapter.py
# File Name: stock_basic_info_
# @ Author: mango-gh22
# @ Date：2025/12/6 17:51
"""
desc 创建适配脚本-mysql:stock_basic_info,stock_daily_data
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表名适配脚本
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔄 表名适配工具")
print("=" * 60)


def check_table_structure():
    """检查表结构"""
    try:
        from src.database.connection import get_session
        from sqlalchemy import text

        session = get_session()

        print("📊 当前数据库表结构:")
        print("-" * 40)

        # 获取所有表
        result = session.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]

        for table in tables:
            print(f"\n📋 表: {table}")
            try:
                # 获取表结构
                result = session.execute(text(f"DESCRIBE {table}"))
                columns = result.fetchall()
                for col in columns:
                    print(f"  {col[0]:20} {col[1]:15} {col[2]}")
            except Exception as e:
                print(f"  ❌ 获取结构失败: {e}")

        session.close()

        # 检查关键表
        required_tables_mapping = {
            'stock_basic': ['stock_basic_info', 'stock_basic'],
            'daily_data': ['stock_daily_data', 'daily_data']
        }

        print("\n🔍 关键表映射检查:")
        for expected_table, possible_names in required_tables_mapping.items():
            found = None
            for name in possible_names:
                if name in tables:
                    found = name
                    break

            if found:
                print(f"  ✅ {expected_table} → {found}")
            else:
                print(f"  ❌ {expected_table}: 未找到对应表")

        return tables

    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return []


def create_table_mapping():
    """创建表名映射"""
    tables = check_table_structure()

    mapping = {}

    # 基于表名推断映射
    for table in tables:
        if 'basic' in table.lower() and 'info' in table.lower():
            mapping['stock_basic'] = table
        elif 'daily' in table.lower() and 'data' in table.lower():
            mapping['daily_data'] = table
        elif 'index' in table.lower() and 'info' in table.lower():
            mapping['index_info'] = table
        elif 'financial' in table.lower():
            mapping['financial_data'] = table
        elif 'minute' in table.lower():
            mapping['minute_data'] = table

    print("\n📝 推断的表名映射:")
    for key, value in mapping.items():
        print(f"  {key:20} → {value}")

    return mapping


def create_table_aliases():
    """创建表别名（临时解决方案）"""
    mapping = {
        'stock_basic': 'stock_basic_info',
        'daily_data': 'stock_daily_data',
        'index_info': 'index_info',
        'index_components': 'stock_index_constituent',
        'financial_data': 'stock_financial_indicators',
        'minute_data': 'stock_minute_data',
        'update_log': 'data_update_log'
    }

    print("\n📋 确定的表名映射:")
    for key, value in mapping.items():
        print(f"  {key:20} → {value}")

    return mapping


def update_query_engine():
    """更新query_engine.py使用正确的表名"""
    print("\n🔄 更新query_engine.py...")

    query_engine_file = 'src/query/query_engine.py'
    if not os.path.exists(query_engine_file):
        print(f"❌ 文件不存在: {query_engine_file}")
        return False

    mapping = create_table_aliases()

    # 读取文件
    with open(query_engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 替换表名
    original_content = content
    for old_name, new_name in mapping.items():
        content = content.replace(f'FROM {old_name}', f'FROM {new_name}')
        content = content.replace(f'INSERT INTO {old_name}', f'INSERT INTO {new_name}')
        content = content.replace(f'UPDATE {old_name}', f'UPDATE {new_name}')
        content = content.replace(f'DELETE FROM {old_name}', f'DELETE FROM {new_name}')

    if content != original_content:
        # 备份原文件
        import shutil
        shutil.copy2(query_engine_file, query_engine_file + '.backup')

        # 写入新内容
        with open(query_engine_file, 'w', encoding='utf-8') as f:
            f.write(content)

        print("✅ query_engine.py已更新")

        # 显示修改内容
        print("\n📝 修改内容预览:")
        lines1 = original_content.split('\n')
        lines2 = content.split('\n')

        for i, (line1, line2) in enumerate(zip(lines1, lines2)):
            if line1 != line2:
                print(f"行 {i + 1}:")
                print(f"  原: {line1[:50]}...")
                print(f"  新: {line2[:50]}...")

        return True
    else:
        print("⚠️  未发现需要替换的表名")
        return False


def test_updated_engine():
    """测试更新后的查询引擎"""
    print("\n🧪 测试更新后的查询引擎...")

    try:
        # 重新加载模块
        import importlib
        import sys

        if 'src.query.query_engine' in sys.modules:
            del sys.modules['src.query.query_engine']

        from src.query.query_engine import QueryEngine
        engine = QueryEngine()

        print("1️⃣ 测试连接...")
        if hasattr(engine, 'test_connection'):
            if engine.test_connection():
                print("✅ 数据库连接正常")
            else:
                print("❌ 数据库连接失败")
                return False

        print("\n2️⃣ 测试数据统计...")
        stats = engine.get_data_statistics()
        print(f"📊 股票数量: {stats.get('stock_basic', {}).get('total_stocks', 0)}")
        print(f"📅 日线记录: {stats.get('daily_data', {}).get('total_records', 0)}")

        print("\n3️⃣ 测试股票列表...")
        stocks = engine.get_stock_list()
        print(f"📋 股票列表: {len(stocks)} 只")
        if stocks:
            for stock in stocks[:3]:
                print(f"  - {stock}")
            if len(stocks) > 3:
                print(f"  ... 等{len(stocks)}只股票")

        print("\n4️⃣ 测试日线查询...")
        if stocks:
            df = engine.get_daily_data(stocks[0], limit=3)
            if not df.empty:
                print(f"✅ 成功查询到 {len(df)} 条数据")
            else:
                print("⚠️  未查询到数据（可能需要先导入数据）")

        engine.close()
        print("\n🎉 查询引擎测试通过!")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def import_sample_data_with_correct_tables():
    """使用正确的表名导入示例数据"""
    print("\n📥 导入示例数据（使用正确表名）...")

    try:
        from src.database.connection import get_session
        from sqlalchemy import text

        session = get_session()

        # 检查表
        result = session.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]

        # 确定表名
        basic_table = 'stock_basic_info' if 'stock_basic_info' in tables else 'stock_basic'
        daily_table = 'stock_daily_data' if 'stock_daily_data' in tables else 'daily_data'

        print(f"使用表: {basic_table}, {daily_table}")

        # 检查是否已有数据
        result = session.execute(text(f"SELECT COUNT(*) FROM {basic_table}"))
        count = result.scalar()

        if count > 0:
            print(f"⚠️  已有 {count} 条股票数据，跳过导入")
            session.close()
            return True

        # 导入中证A50示例数据
        print("  导入中证A50示例数据...")
        a50_stocks = [
            ("000001.SZ", "平安银行", "SZ", "银行", "1991-04-03", 1),
            ("000858.SZ", "五粮液", "SZ", "食品饮料", "1998-04-27", 1),
            ("000333.SZ", "美的集团", "SZ", "家用电器", "2013-09-18", 1),
            ("002594.SZ", "比亚迪", "SZ", "汽车", "2011-06-30", 1),
            ("600519.SH", "贵州茅台", "SH", "食品饮料", "2001-08-27", 1),
        ]

        insert_sql = text(f"""
        INSERT INTO {basic_table} (symbol, stock_name, exchange, industry, listing_date, is_active)
        VALUES (:symbol, :name, :exchange, :industry, :listing_date, :is_active)
        ON DUPLICATE KEY UPDATE stock_name=VALUES(stock_name), industry=VALUES(industry)
        """)

        for symbol, name, exchange, industry, listing_date, is_active in a50_stocks:
            session.execute(insert_sql, {
                'symbol': symbol,
                'name': name,
                'exchange': exchange,
                'industry': industry,
                'listing_date': listing_date,
                'is_active': is_active
            })

        session.commit()
        print(f"✅ 导入 {len(a50_stocks)} 只股票")

        # 检查日线数据表结构
        print("\n🔍 检查日线数据表结构...")
        try:
            result = session.execute(text(f"DESCRIBE {daily_table}"))
            columns = [row[0] for row in result.fetchall()]
            print(f"日线表列: {columns}")
        except Exception as e:
            print(f"⚠️  检查表结构失败: {e}")

        session.close()
        return True

    except Exception as e:
        print(f"❌ 导入数据失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    print("🔄 表名适配解决方案")
    print("=" * 60)

    steps = [
        ("检查表结构", check_table_structure),
        ("创建表名映射", create_table_aliases),
        ("更新查询引擎", update_query_engine),
        ("导入示例数据", import_sample_data_with_correct_tables),
        ("测试更新后引擎", test_updated_engine),
    ]

    results = []
    for step_name, step_func in steps:
        print(f"\n▶️  {step_name}...")
        result = step_func()
        results.append((step_name, result))

    print("\n" + "=" * 60)
    print("📋 适配结果汇总:")

    passed = 0
    for step_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {step_name}: {status}")
        if result:
            passed += 1

    if passed == len(steps):
        print("\n🎉 表名适配完成! 现在可以测试P4功能:")
        print("  python main.py --action p4_query_test")
        print("  python main.py --action validate")
    else:
        print(f"\n⚠️  适配未完全成功 ({passed}/{len(steps)})")
        print("建议检查表结构并手动调整SQL查询。")


if __name__ == "__main__":
    main()