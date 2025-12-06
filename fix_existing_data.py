#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复现有数据 - 将非标准列名改为标准列名
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import engine
from sqlalchemy import text
import pandas as pd

print("🔧 修复现有数据列名")
print("=" * 60)

def check_current_data():
    """检查当前数据"""
    print("🔍 检查当前数据...")

    try:
        with engine.connect() as conn:
            # 1. 查看表结构
            print("📋 stock_daily_data表结构:")
            result = conn.execute(text("DESCRIBE stock_daily_data"))
            columns = []
            for row in result:
                print(f"  {row[0]:20} {row[1]:20}")
                columns.append(row[0])

            # 2. 检查列名
            non_standard_cols = []
            standard_cols = []

            for col in columns:
                if col in ['open_price', 'close_price', 'high_price', 'low_price', 'change_amount', 'pre_close_price']:
                    non_standard_cols.append(col)
                elif col in ['open', 'close', 'high', 'low', 'change', 'pre_close']:
                    standard_cols.append(col)

            print(f"📝 非标准列名: {non_standard_cols}")
            print(f"📝 标准列名: {standard_cols}")

            # 3. 查看数据
            if non_standard_cols:
                print(f"📊 查看非标准列数据示例:")
                query = f"SELECT {', '.join(non_standard_cols[:3])} FROM stock_daily_data LIMIT 3"
                result = conn.execute(text(query))
                for row in result:
                    print(f"  {row}")

            return columns, non_standard_cols

    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return [], []

def fix_column_names():
    """修复列名"""
    print("🔄 修复列名...")

    rename_operations = [
        ('open_price', 'open'),
        ('high_price', 'high'),
        ('low_price', 'low'),
        ('close_price', 'close'),
        ('pre_close_price', 'pre_close'),
        ('change_amount', 'change'),
        ('created_time', 'created_at'),
        ('updated_time', 'updated_at')
    ]

    try:
        with engine.connect() as conn:
            for old_name, new_name in rename_operations:
                try:
                    # 先检查列是否存在
                    check_sql = f"""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'stock_daily_data' 
                    AND COLUMN_NAME = '{old_name}'
                    """
                    result = conn.execute(text(check_sql))
                    exists = result.scalar() > 0

                    if exists:
                        # 获取列定义
                        describe_sql = f"DESCRIBE stock_daily_data {old_name}"
                        result = conn.execute(text(f"DESCRIBE stock_daily_data"))
                        col_info = None
                        for row in result:
                            if row[0] == old_name:
                                col_info = row
                                break

                        if col_info:
                            col_type = col_info[1]
                            nullable = 'NOT NULL' if col_info[2] == 'NO' else ''
                            default = f"DEFAULT '{col_info[4]}'" if col_info[4] else ''
                            extra = col_info[5] or ''

                            # 执行重命名
                            alter_sql = f"""
                            ALTER TABLE stock_daily_data 
                            CHANGE COLUMN {old_name} {new_name} {col_type} {nullable} {default} {extra}
                            """

                            conn.execute(text(alter_sql))
                            print(f"  ✅ {old_name} → {new_name}")
                    else:
                        print(f"  ⚠️  {old_name} 不存在，跳过")

                except Exception as e:
                    print(f"  ❌ 重命名 {old_name} 失败: {e}")

            conn.commit()
            print("✅ 列名修复完成")

    except Exception as e:
        print(f"❌ 修复失败: {e}")

def test_after_fix():
    """修复后测试"""
    print("🧪 修复后测试...")

    try:
        # 测试查询
        test_query = """
        SELECT 
            trade_date, symbol, 
            open, high, low, close,
            volume, pct_change
        FROM stock_daily_data
        WHERE symbol = '000001.SZ'
        ORDER BY trade_date DESC
        LIMIT 3
        """

        with engine.connect() as conn:
            result = conn.execute(text(test_query))
            rows = result.fetchall()

            if rows:
                print(f"✅ 查询成功，返回 {len(rows)} 条数据")
                print("📊 数据示例:")
                for row in rows:
                    print(f"  {row[0]} | 开:{row[2]:.2f} 收:{row[5]:.2f} 量:{row[6]:,.0f}")
            else:
                print("⚠️  查询成功但无数据")

    except Exception as e:
        print(f"❌ 测试失败: {e}")

def main():
    print("现有数据修复工具")
    print("=" * 60)

    # 检查当前状态
    columns, non_standard = check_current_data()

    if non_standard:
        choice = input(f"发现 {len(non_standard)} 个非标准列名，是否修复？(y/n): ")
        if choice.lower() == 'y':
            fix_column_names()
            test_after_fix()
    else:
        print("✅ 所有列名已经是标准名称")

    print("🎉 修复完成")

if __name__ == "__main__":
    main()
