# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_reserved_keywords.py
# File Name: fix_reserved_keywords
# @ Author: mango-gh22
# @ Date：2025/12/6 19:31
"""
desc 
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复MySQL保留关键字问题
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔧 修复MySQL保留关键字问题")
print("=" * 60)


def fix_query_engine_for_reserved_keywords():
    """修复查询引擎中的保留关键字"""
    query_engine_file = 'src/query/query_engine.py'

    if not os.path.exists(query_engine_file):
        print(f"❌ 文件不存在: {query_engine_file}")
        return False

    print("📄 修复查询引擎...")

    with open(query_engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # MySQL保留关键字列表（需要加反引号）
    reserved_keywords = ['change', 'open', 'close', 'date', 'desc', 'key', 'index', 'table']

    # 备份原文件
    import shutil
    shutil.copy2(query_engine_file, query_engine_file + '.backup_reserved')

    # 修复get_daily_data方法中的字段列表
    if 'def get_daily_data(' in content:
        print("🔍 找到get_daily_data方法，修复字段列表...")

        # 找到字段列表部分
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'fields = [' in line and 'def get_daily_data' in '\n'.join(lines[max(0, i - 5):i]):
                # 找到字段定义开始
                for j in range(i, min(i + 20, len(lines))):
                    if ']' in lines[j]:
                        # 修复这一行
                        for keyword in reserved_keywords:
                            if f"'{keyword}'" in lines[j] or f'"{keyword}"' in lines[j]:
                                # 在字段名前后添加反引号
                                lines[j] = lines[j].replace(f"'{keyword}'", f"'`{keyword}`'")
                                lines[j] = lines[j].replace(f'"{keyword}"', f'"`{keyword}`"')
                                print(f"  ✅ 修复保留关键字: {keyword}")
                        break

        # 重新组合内容
        content = '\n'.join(lines)

    # 写入修复后的文件
    with open(query_engine_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ 查询引擎已修复")

    # 创建更安全的查询引擎版本
    create_safe_query_engine()

    return True


def create_safe_query_engine():
    """创建安全的查询引擎"""
    print("\n📝 创建安全的查询引擎...")

    safe_code = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安全查询引擎 - 处理MySQL保留关键字
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text
from src.database.connection import engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SafeQueryEngine:
    """安全查询引擎（处理保留关键字）"""

    def __init__(self):
        self.engine = engine
        self.logger = get_logger(__name__)

        # MySQL保留关键字（需要加反引号）
        self.reserved_keywords = {
            'change', 'open', 'close', 'date', 'desc', 
            'key', 'index', 'table', 'select', 'where',
            'group', 'order', 'limit', 'offset'
        }

    def _safe_field(self, field: str) -> str:
        """处理字段名，为保留关键字加反引号"""
        if field.lower() in self.reserved_keywords:
            return f'`{field}`'
        return field

    def _safe_fields(self, fields: List[str]) -> str:
        """处理字段列表"""
        safe_fields = [self._safe_field(field) for field in fields]
        return ', '.join(safe_fields)

    def get_daily_data(self,
                      symbol: str = None,
                      start_date: str = None,
                      end_date: str = None,
                      fields: List[str] = None,
                      limit: int = None) -> pd.DataFrame:
        """
        获取日线数据（安全版）
        """
        # 默认字段
        if fields is None:
            fields = [
                'trade_date', 'symbol', 
                'open', 'high', 'low', 'close',
                'volume', 'amount', 'pct_change', 
                'change', 'pre_close', 'turnover_rate'
            ]

        # 安全处理字段名
        field_str = self._safe_fields(fields)

        query = f"""
        SELECT {field_str}
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

            if not df.empty:
                # 处理日期列
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    df = df.sort_values('trade_date')

            self.logger.info(f"查询日线数据，返回{len(df)}条记录")
            return df

        except Exception as e:
            self.logger.error(f"查询日线数据失败: {e}")

            # 尝试更简单的查询
            return self._simple_safe_query(symbol, limit)

    def _simple_safe_query(self, symbol=None, limit=10):
        """简单的安全查询"""
        # 使用安全的字段名
        safe_fields = self._safe_fields(['trade_date', 'symbol', 'open', 'high', 'low', 'close', 'volume'])

        query = f"""
        SELECT {safe_fields}
        FROM stock_daily_data
        WHERE 1=1
        """
        params = {}

        if symbol:
            query += " AND symbol = :symbol"
            params['symbol'] = symbol

        query += " ORDER BY trade_date DESC"

        if limit:
            query += f" LIMIT {limit}"

        try:
            df = pd.read_sql_query(text(query), self.engine, params=params)

            if not df.empty and 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df = df.sort_values('trade_date')

            return df

        except Exception as e:
            self.logger.error(f"简单查询也失败: {e}")
            return pd.DataFrame()

    def get_data_statistics(self) -> Dict:
        """获取数据统计"""
        # 使用现有查询引擎的统计方法
        from src.query.query_engine import QueryEngine
        engine = QueryEngine()
        stats = engine.get_data_statistics()
        engine.close()
        return stats

    def get_stock_list(self) -> List[str]:
        """获取股票列表"""
        from src.query.query_engine import QueryEngine
        engine = QueryEngine()
        stocks = engine.get_stock_list()
        engine.close()
        return stocks

def test_safe_engine():
    """测试安全引擎"""
    engine = SafeQueryEngine()

    print("🧪 测试安全查询引擎")
    print("=" * 50)

    # 获取股票列表
    stocks = engine.get_stock_list()
    print(f"📋 股票列表: {len(stocks)} 只")

    if stocks:
        # 测试日线查询
        print(f"\n📅 查询 {stocks[0]} 日线数据...")
        df = engine.get_daily_data(stocks[0], limit=3)

        if not df.empty:
            print(f"✅ 查询成功 ({len(df)} 条)")
            print(df.head().to_string())
        else:
            print("❌ 查询失败")

    print("\n✅ 测试完成")

if __name__ == "__main__":
    test_safe_engine()
'''

    with open('src/query/simple_query_engine.py', 'w', encoding='utf-8') as f:
        f.write(safe_code)

    print("✅ 安全查询引擎已创建: src/query/simple_query_engine.py")

    return True


def quick_test_fix():
    """快速测试修复"""
    print("\n⚡ 快速测试修复...")

    test_code = '''
import sys
sys.path.insert(0, '.')
from src.database.connection import engine
from sqlalchemy import text
import pandas as pd

print("1️⃣ 测试直接查询（处理保留关键字）...")

# 方法1: 使用反引号
query1 = """
SELECT 
    trade_date, symbol, 
    `open`, `high`, `low`, `close`,
    volume, amount, pct_change, 
    `change`, pre_close
FROM stock_daily_data
WHERE symbol = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 3
"""

try:
    df1 = pd.read_sql_query(text(query1), engine)
    print(f"✅ 方法1成功: {len(df1)} 条数据")
    print(df1.head().to_string())
except Exception as e:
    print(f"❌ 方法1失败: {e}")

print("\\n2️⃣ 测试不使用保留关键字...")

# 方法2: 避免使用保留关键字
query2 = """
SELECT 
    trade_date, symbol, 
    open, high, low, close,
    volume, amount, pct_change,
    pre_close, turnover_rate
FROM stock_daily_data
WHERE symbol = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 3
"""

try:
    df2 = pd.read_sql_query(text(query2), engine)
    print(f"✅ 方法2成功: {len(df2)} 条数据")
    print(df2.head().to_string())
except Exception as e:
    print(f"❌ 方法2失败: {e}")

print("\\n3️⃣ 检查表实际列名...")

# 查看表结构
with engine.connect() as conn:
    result = conn.execute(text("DESCRIBE stock_daily_data"))
    columns = []
    for row in result:
        columns.append(row[0])

    print(f"📋 实际列名 ({len(columns)}):")
    # 显示包含'change'的列
    change_cols = [col for col in columns if 'change' in col.lower()]
    print(f"  包含'change'的列: {change_cols}")

    # 显示所有列
    for i in range(0, len(columns), 5):
        print(f"  {columns[i:i+5]}")
'''

    exec(test_code)
    return True


def update_main_for_export_test():
    """更新main.py的导出测试"""
    print("\n🔄 更新main.py的导出测试...")

    main_file = 'main.py'

    if not os.path.exists(main_file):
        print(f"❌ 文件不存在: {main_file}")
        return False

    with open(main_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找test_p4_export_test函数
    if 'def test_p4_export_test():' in content:
        print("🔍 找到导出测试函数，进行修复...")

        # 备份
        import shutil
        shutil.copy2(main_file, main_file + '.backup_export')

        # 添加安全查询导入
        import_statement = 'from src.query.safe_query_engine import SafeQueryEngine'
        if import_statement not in content:
            # 在文件顶部添加导入
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'from src.query.query_engine import QueryEngine' in line:
                    lines.insert(i + 1, import_statement)
                    break

            content = '\n'.join(lines)

        # 修复导出测试函数，使用安全查询
        lines = content.split('\n')
        in_export_test = False

        for i, line in enumerate(lines):
            if 'def test_p4_export_test():' in line:
                in_export_test = True
            elif in_export_test and line.strip() and not line.startswith(' ') and not line.startswith('\t'):
                in_export_test = False

            if in_export_test and 'QueryEngine()' in line:
                # 替换为SafeQueryEngine
                lines[i] = lines[i].replace('QueryEngine()', 'SafeQueryEngine()')
                print("  ✅ 替换为SafeQueryEngine")

        content = '\n'.join(lines)

        # 写入更新后的文件
        with open(main_file, 'w', encoding='utf-8') as f:
            f.write(content)

        print("✅ main.py导出测试已更新")
    else:
        print("⚠️  未找到导出测试函数")

    return True


def create_quick_fix_sql():
    """创建快速修复SQL"""
    print("\n📝 创建快速修复SQL...")

    sql_fixes = [
        "-- 方法1: 重命名change列为其他名称",
        "ALTER TABLE stock_daily_data CHANGE COLUMN `change` price_change DECIMAL(10,4);",
        "",
        "-- 方法2: 创建视图使用别名",
        "CREATE OR REPLACE VIEW daily_data_view AS",
        "SELECT",
        "    trade_date, symbol,",
        "    open, high, low, close,",
        "    volume, amount, pct_change,",
        "    `change` as price_change,  -- 使用别名",
        "    pre_close, turnover_rate",
        "FROM stock_daily_data;",
        "",
        "-- 方法3: 直接查询使用反引号",
        "SELECT trade_date, symbol, `open`, `high`, `low`, `close`, `change` FROM stock_daily_data;"
    ]

    with open('fix_reserved_keywords.sql', 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_fixes))

    print("✅ 快速修复SQL已保存到: fix_reserved_keywords.sql")
    print("\n📋 SQL预览:")
    for line in sql_fixes[:10]:
        print(f"  {line}")

    return True


def main():
    """主函数"""
    print("MySQL保留关键字修复方案")
    print("=" * 60)

    print("问题: 'change'是MySQL保留关键字，需要加反引号`change`")
    print("解决方案:")
    print("  1. 修复查询引擎处理保留关键字")
    print("  2. 创建安全查询引擎")
    print("  3. 更新导出测试使用安全引擎")

    choice = input("\n是否执行修复？(y/n): ").strip().lower()

    if choice != 'y':
        print("已取消")
        return

    steps = [
        ("修复查询引擎", fix_query_engine_for_reserved_keywords),
        ("创建安全引擎", create_safe_query_engine),
        ("快速测试", quick_test_fix),
        ("更新导出测试", update_main_for_export_test),
        ("创建修复SQL", create_quick_fix_sql),
    ]

    for step_name, step_func in steps:
        print(f"\n{'=' * 50}")
        print(f"▶️  {step_name}")
        print(f"{'=' * 50}")

        try:
            step_func()
        except Exception as e:
            print(f"⚠️  {step_name} 出错: {e}")

    print(f"\n{'=' * 60}")
    print("🎉 修复完成!")
    print("\n📋 现在测试:")
    print("  1. python main.py --action p4_export_test")
    print("  2. python main.py --action p4_query_test")
    print("  3. 或者直接使用: from src.query.safe_query_engine import SafeQueryEngine")


if __name__ == "__main__":
    main()