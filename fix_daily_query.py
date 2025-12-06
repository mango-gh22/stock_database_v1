# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_daily_query.py
# File Name: fix_daily_query
# @ Author: mango-gh22
# @ Date：2025/12/6 18:11
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复日线数据查询
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔧 修复日线数据查询")
print("=" * 60)


def check_table_columns():
    """检查日线数据表的列名"""
    try:
        from src.database.connection import get_session
        from sqlalchemy import text

        session = get_session()

        print("📋 检查stock_daily_data表结构:")
        result = session.execute(text("DESCRIBE stock_daily_data"))
        columns = []
        for row in result:
            print(f"  {row[0]:20} {row[1]:15} {'YES' if row[2] == 'YES' else 'NO'} {row[3] or ''}")
            columns.append(row[0])

        session.close()

        print(f"\n📊 总列数: {len(columns)}")
        print(f"📝 列名列表: {columns}")

        # 检查是否有change列（保留关键字）
        if 'change' in columns or 'Change' in [c.lower() for c in columns]:
            print("⚠️  发现'change'列（MySQL保留关键字），需要特殊处理")

        return columns

    except Exception as e:
        print(f"❌ 检查表结构失败: {e}")
        return []


def create_safe_query():
    """创建安全的查询语句"""
    print("\n🔍 创建安全的查询语句...")

    columns = check_table_columns()

    if not columns:
        print("❌ 无法获取列信息")
        return

    # 安全的查询模板
    safe_query = """
SELECT 
    {columns}
FROM stock_daily_data
WHERE 1=1
    {symbol_filter}
    {date_filters}
ORDER BY trade_date {order}
    {limit_clause}
"""

    # 构建列列表，处理保留关键字
    column_list = []
    for col in columns:
        if col.lower() in ['change', 'desc', 'key', 'index', 'table']:  # MySQL保留关键字
            column_list.append(f"`{col}`")
        else:
            column_list.append(col)

    columns_str = ",\n    ".join(column_list)

    print(f"\n📝 安全的查询语句模板:")
    print("-" * 50)
    print(safe_query.format(
        columns=columns_str,
        symbol_filter="AND symbol = :symbol",
        date_filters="AND trade_date >= :start_date\n    AND trade_date <= :end_date",
        order="DESC",
        limit_clause="LIMIT :limit"
    ))
    print("-" * 50)

    # 常用查询
    print("\n📊 常用查询:")

    # 1. 基础查询
    basic_columns = ['trade_date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    basic_query = f"""
SELECT 
    {', '.join([f'`{c}`' if c.lower() == 'change' else c for c in basic_columns])}
FROM stock_daily_data
WHERE symbol = :symbol
ORDER BY trade_date DESC
LIMIT 10
"""
    print("1. 基础查询（最近10条）:")
    print(basic_query)

    return True


def update_query_engine():
    """更新query_engine.py"""
    print("\n🔄 更新query_engine.py...")

    query_engine_file = 'src/query/query_engine.py'
    if not os.path.exists(query_engine_file):
        print(f"❌ 文件不存在: {query_engine_file}")
        return False

    # 读取文件
    with open(query_engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否需要修复
    if '`change`' in content or 'change' in content:
        print("✅ query_engine.py已处理保留关键字")

        # 显示相关代码
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'change' in line.lower() and 'SELECT' in line.upper():
                print(f"行 {i + 1}: {line.strip()[:80]}...")
    else:
        print("⚠️  可能需要更新查询语句")

    return True


def test_fixed_query():
    """测试修复后的查询"""
    print("\n🧪 测试修复后的查询...")

    try:
        from src.database.connection import get_session
        from sqlalchemy import text

        session = get_session()

        # 获取一只股票
        result = session.execute(text("SELECT symbol FROM stock_basic_info LIMIT 1"))
        stock_row = result.fetchone()

        if not stock_row:
            print("❌ 没有找到股票数据")
            return False

        symbol = stock_row[0]
        print(f"测试股票: {symbol}")

        # 测试1: 安全查询
        print("\n1️⃣ 测试安全查询...")
        safe_query = """
        SELECT 
            trade_date, symbol, open, high, low, close, volume, amount, pct_change
        FROM stock_daily_data
        WHERE symbol = :symbol
        ORDER BY trade_date DESC
        LIMIT 3
        """

        result = session.execute(text(safe_query), {'symbol': symbol})
        rows = result.fetchall()

        if rows:
            print(f"✅ 成功查询到 {len(rows)} 条数据")
            for row in rows[:2]:  # 显示前2条
                print(f"  {row[0]} | 开盘:{row[2]:.2f} 收盘:{row[5]:.2f} 成交量:{row[6]:,.0f}")
        else:
            print("⚠️  未查询到数据")

        # 测试2: 检查是否有change列
        print("\n2️⃣ 检查change列...")
        try:
            result = session.execute(text("SELECT `change` FROM stock_daily_data LIMIT 1"))
            print("✅ 存在change列（带反引号）")

            # 测试带反引号的查询
            change_query = """
            SELECT 
                trade_date, symbol, `change`, pct_change
            FROM stock_daily_data
            WHERE symbol = :symbol
            LIMIT 2
            """
            result = session.execute(text(change_query), {'symbol': symbol})
            rows = result.fetchall()
            if rows:
                print(f"✅ 成功查询change列数据")
        except Exception as e:
            if 'change' in str(e):
                print("❌ change列查询失败（可能是保留关键字问题）")
            else:
                print(f"⚠️  change列检查异常: {e}")

        session.close()
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_quick_fix_patch():
    """创建快速修复补丁"""
    print("\n🔧 创建快速修复补丁...")

    patch_content = '''
# query_engine.py 快速修复补丁
# 用于修复MySQL保留关键字'change'的问题

def get_daily_data_fixed(self,
                        symbol: str = None,
                        start_date: str = None,
                        end_date: str = None,
                        limit: int = None) -> pd.DataFrame:
    """
    修复版的日线数据查询（处理保留关键字）
    """
    # 使用安全的列名，避免保留关键字
    query = """
    SELECT 
        trade_date, symbol, 
        open, high, low, close,
        volume, amount,
        pct_change
    FROM stock_daily_data
    WHERE 1=1
    """
    params = {}

    if symbol:
        query += " AND symbol = :symbol"
        params['symbol'] = symbol
    if start_date:
        query += " AND trade_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND trade_date <= :end_date"
        params['end_date'] = end_date

    query += " ORDER BY trade_date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        df = pd.read_sql_query(text(query), self.engine, params=params)
        if not df.empty and 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')
        self.logger.info(f"查询日线数据，返回{len(df)}条记录")
        return df
    except Exception as e:
        self.logger.error(f"查询日线数据失败: {e}")
        return pd.DataFrame()
'''

    patch_file = 'query_engine_fix.py'
    with open(patch_file, 'w', encoding='utf-8') as f:
        f.write(patch_content)

    print(f"✅ 补丁文件已创建: {patch_file}")

    print("\n📋 使用方法:")
    print("1. 将上面的函数添加到query_engine.py的QueryEngine类中")
    print("2. 或者在get_daily_data方法中调用这个函数")
    print("3. 或者在main.py中使用这个函数临时修复")

    return True


def main():
    """主函数"""
    print("日线数据查询修复工具")
    print("=" * 60)

    steps = [
        ("检查表结构", check_table_columns),
        ("创建安全查询", create_safe_query),
        ("测试修复查询", test_fixed_query),
        ("创建修复补丁", create_quick_fix_patch),
    ]

    for step_name, step_func in steps:
        print(f"\n▶️  {step_name}...")
        step_func()

    print("\n" + "=" * 60)
    print("📋 修复建议:")
    print("1. 在查询中避免使用'change'列名，或使用反引号`change`")
    print("2. 使用pct_change代替change（如果可用）")
    print("3. 修改查询使用安全的列名列表")

    print("\n✅ 修复完成！")


if __name__ == "__main__":
    main()