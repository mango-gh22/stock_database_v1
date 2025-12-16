# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_columns_simple.py
# File Name: fix_columns_simple
# @ Author: mango-gh22
# @ Date：2025/12/14 18:32
"""
desc 
"""
# fix_columns_simple.py
"""
简单直接的列名修复脚本
"""

import sys
import os
import shutil

sys.path.insert(0, os.path.abspath('.'))

print("🔧 简单列名修复脚本")
print("=" * 60)


def main():
    # 1. 修复查询引擎
    print("\n1. 修复查询引擎 (src/query/query_engine.py)...")

    query_engine_path = 'src/query/query_engine.py'

    if not os.path.exists(query_engine_path):
        print(f"  ❌ 文件不存在: {query_engine_path}")
        return

    # 备份
    backup_path = query_engine_path + '.backup'
    shutil.copy2(query_engine_path, backup_path)
    print(f"  ✅ 已备份到: {backup_path}")

    # 读取文件
    with open(query_engine_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找并修复查询语句
    # 查找包含 pct_change 的查询语句
    if 'pct_change' in content and 'change_percent' not in content:
        print("  🔍 发现需要修复的列名...")

        # 修复方法1：替换 SELECT 语句中的 pct_change
        if "pct_change," in content:
            content = content.replace("pct_change,", "change_percent as pct_change,")
            print("  ✅ 修复了 pct_change 列")

        # 修复方法2：替换 change_amount
        if "change_amount as pct_change" in content:
            content = content.replace("change_amount as pct_change", "change_percent as pct_change")
            print("  ✅ 修复了 change_amount -> change_percent 映射")

        # 保存修复
        with open(query_engine_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print("  ✅ 查询引擎修复完成")
    else:
        print("  ✅ 查询引擎似乎已经是正确的")

    # 2. 创建测试验证器
    print("\n2. 创建验证测试脚本...")

    test_script = '''# test_simple.py
"""
简单验证脚本
"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

print("🧪 简单验证测试")
print("=" * 50)

try:
    # 测试导入
    print("\\n1. 测试模块导入...")
    from src.query.query_engine import QueryEngine
    from src.processors.validator import DataValidator
    from src.processors.adjustor import StockAdjustor
    print("✅ 模块导入成功")

    # 测试查询引擎
    print("\\n2. 测试查询引擎...")
    engine = QueryEngine()

    # 获取股票列表
    stocks = engine.get_stock_list()
    if not stocks.empty:
        symbol = stocks.iloc[0]['symbol']
        print(f"   测试股票: {symbol}")

        # 查询数据
        data = engine.query_daily_data(symbol=symbol, limit=3)
        if not data.empty:
            print(f"   ✅ 查询成功: {len(data)} 条")

            # 检查列名
            print("   检查列名:")
            for col in ['open', 'high', 'low', 'close', 'pct_change', 'volume']:
                if col in data.columns:
                    print(f"     ✓ {col}")
                else:
                    print(f"     ✗ {col} (缺失)")

            # 显示数据
            print("\\n   示例数据:")
            for i in range(min(2, len(data))):
                row = data.iloc[i]
                print(f"     {row['trade_date']}: {row['close']:.2f}")
        else:
            print("   ⚠️ 查询返回空数据")
    else:
        print("   ⚠️ 无股票数据")

    engine.close()
    print("\\n✅ 简单测试完成!")

except Exception as e:
    print(f"\\n❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
'''

    with open('test_simple.py', 'w', encoding='utf-8') as f:
        f.write(test_script)

    print("  ✅ 测试脚本已创建: test_simple.py")

    print("\n" + "=" * 60)
    print("🎉 修复完成！请运行测试:")
    print("  python test_simple.py")
    print("=" * 60)


if __name__ == "__main__":
    main()