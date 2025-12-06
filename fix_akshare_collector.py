# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_akshare_collector.py
# File Name: fix_akshare_collector
# @ Author: mango-gh22
# @ Date：2025/12/6 19:18
"""
desc 
"""
# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复AKShare采集器使用标准列名
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔧 修复AKShare采集器")
print("=" * 60)


def fix_akshare_column_names():
    """修复AKShare采集器的列名"""
    akshare_file = 'src/data/akshare_collector.py'

    if not os.path.exists(akshare_file):
        print(f"❌ 文件不存在: {akshare_file}")
        return False

    print("📄 读取AKShare采集器文件...")
    with open(akshare_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 备份原文件
    import shutil
    backup_file = akshare_file + '.backup_nonstandard'
    shutil.copy2(akshare_file, backup_file)
    print(f"📦 已备份到: {backup_file}")

    # 检查并修复列名映射
    if 'open_price' in content and 'close_price' in content:
        print("🔄 发现非标准列名，进行修复...")

        # 修复列名映射
        old_mapping = """column_mapping = {
                '日期': 'trade_date',
                '开盘': 'open_price',
                '收盘': 'close_price',
                '最高': 'high_price',
                '最低': 'low_price',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            }"""

        new_mapping = """column_mapping = {
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover_rate'
            }"""

        if old_mapping in content:
            content = content.replace(old_mapping, new_mapping)
            print("✅ 修复列名映射")
        else:
            # 尝试其他格式
            content = content.replace("'开盘': 'open_price'", "'开盘': 'open'")
            content = content.replace("'收盘': 'close_price'", "'收盘': 'close'")
            content = content.replace("'最高': 'high_price'", "'最高': 'high'")
            content = content.replace("'最低': 'low_price'", "'最低': 'low'")
            content = content.replace("'涨跌额': 'change_amount'", "'涨跌额': 'change'")
            print("✅ 直接替换列名")

    # 写入修复后的文件
    with open(akshare_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print("✅ AKShare采集器已修复为使用标准列名")

    # 显示修复内容
    print("\n📝 修复后的列名映射:")
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'column_mapping' in line:
            for j in range(i, min(i + 15, len(lines))):
                print(f"  {lines[j]}")
            break

    return True


def create_standard_akshare_collector():
    """创建标准化的AKShare采集器"""
    print("\n📝 创建标准化的AKShare采集器模板...")

    standard_template = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AKShare数据采集器（标准化版）- 使用标准列名
"""

import akshare as ak
import pandas as pd
from typing import Optional, Dict, Any
import logging
from datetime import datetime

from .data_collector import BaseDataCollector

logger = logging.getLogger(__name__)

class StandardAKShareCollector(BaseDataCollector):
    """标准AKShare数据采集器（使用标准列名）"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        super().__init__(config_path)
        logger.info("标准AKShare采集器初始化完成")

    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取日线数据（使用标准列名）"""
        try:
            # 转换symbol格式
            if symbol.endswith('.SH') or symbol.endswith('.SZ'):
                stock_code = symbol[:-3]
            else:
                stock_code = symbol

            logger.info(f"获取日线数据: {{symbol}} ({{start_date}} 至 {{end_date}})")

            # 使用AKShare获取数据
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust="qfq"
            )

            if df.empty:
                logger.warning(f"未获取到数据: {{symbol}}")
                return None

            # 标准列名映射
            column_mapping = {
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover_rate'
            }

            df = df.rename(columns=column_mapping)
            df['symbol'] = symbol

            # 添加其他标准列（如果数据中有）
            if '换手率' in df.columns:
                df['turnover_rate'] = df['换手率']

            # 计算前收盘价（如果没有）
            if 'pre_close' not in df.columns and 'change' in df.columns and 'close' in df.columns:
                df['pre_close'] = df['close'] - df['change']

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])

            # 选择标准列
            standard_columns = [
                'trade_date', 'symbol', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'pct_change', 'change',
                'pre_close', 'turnover_rate', 'amplitude'
            ]

            # 只保留存在的列
            available_columns = [col for col in standard_columns if col in df.columns]
            df = df[available_columns]

            logger.info(f"成功获取 {{symbol}} 日线数据 {{len(df)}} 条")
            return df

        except Exception as e:
            logger.error(f"获取日线数据失败 {{symbol}}: {{e}}")
            return None

    def fetch_basic_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        try:
            # 简单返回
            return {
                'symbol': symbol,
                'name': '待从AKShare获取',
                'source': 'akshare'
            }
        except Exception as e:
            logger.error(f"获取基本信息失败 {{symbol}}: {{e}}")
            return None

    def fetch_minute_data(self, symbol: str, trade_date: str, freq: str = '1min') -> Optional[pd.DataFrame]:
        """获取分钟线数据"""
        return None

if __name__ == "__main__":
    # 测试标准采集器
    import sys
    sys.path.insert(0, '.')
    collector = StandardAKShareCollector()
    print("标准AKShare采集器创建成功")
'''

    # 保存为标准模板
    with open('src/data/akshare_collector_standard.py', 'w', encoding='utf-8') as f:
        f.write(standard_template)

    print("✅ 标准化采集器模板已保存到 src/data/akshare_collector_standard.py")

    return True


def update_data_storage_for_standard():
    """更新数据存储使用标准列名"""
    print("\n💾 更新数据存储模块...")

    storage_file = 'src/data/data_storage.py'

    if not os.path.exists(storage_file):
        print(f"❌ 文件不存在: {storage_file}")
        return False

    with open(storage_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找并修复插入SQL
    if 'INSERT INTO stock_daily_data' in content:
        print("🔍 发现插入SQL，检查列名...")

        # 备份
        import shutil
        shutil.copy2(storage_file, storage_file + '.backup')

        # 修复列名
        old_column_patterns = [
            'open_price', 'close_price', 'high_price', 'low_price',
            'change_amount', 'pre_close_price'
        ]

        new_column_names = [
            'open', 'close', 'high', 'low',
            'change', 'pre_close'
        ]

        for old, new in zip(old_column_patterns, new_column_names):
            if old in content:
                content = content.replace(old, new)
                print(f"  ✅ {old} → {new}")

        # 写入修复后的文件
        with open(storage_file, 'w', encoding='utf-8') as f:
            f.write(content)

        print("✅ 数据存储模块已更新")
    else:
        print("✅ 数据存储模块可能已经使用标准列名")

    return True


def test_standard_collector():
    """测试标准化采集器"""
    print("\n🧪 测试标准化采集器...")

    try:
        # 临时导入标准采集器
        test_code = '''
import sys
sys.path.insert(0, '.')
import akshare as ak
import pandas as pd

# 测试AKShare数据获取
symbol = "000001"
df = ak.stock_zh_a_hist(
    symbol=symbol,
    period="daily",
    start_date="20240101",
    end_date="20240110",
    adjust="qfq"
)

print(f"获取到 {len(df)} 条数据")
print("原始列名:", df.columns.tolist())

# 标准列名映射
column_mapping = {
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount',
    '振幅': 'amplitude',
    '涨跌幅': 'pct_change',
    '涨跌额': 'change',
    '换手率': 'turnover_rate'
}

df = df.rename(columns=column_mapping)
print("标准列名:", [col for col in column_mapping.values() if col in df.columns])
print("数据示例:")
print(df[['trade_date', 'open', 'high', 'low', 'close', 'volume']].head())
'''

        exec(test_code)
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False


def create_fix_data_script():
    """创建修复现有数据的脚本"""
    print("\n🔄 创建数据修复脚本...")

    fix_script = '''#!/usr/bin/env python3
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

            print(f"\n📝 非标准列名: {non_standard_cols}")
            print(f"📝 标准列名: {standard_cols}")

            # 3. 查看数据
            if non_standard_cols:
                print(f"\n📊 查看非标准列数据示例:")
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
    print("\n🔄 修复列名...")

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
            print("\n✅ 列名修复完成")

    except Exception as e:
        print(f"❌ 修复失败: {e}")

def test_after_fix():
    """修复后测试"""
    print("\n🧪 修复后测试...")

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
        choice = input(f"\n发现 {len(non_standard)} 个非标准列名，是否修复？(y/n): ")
        if choice.lower() == 'y':
            fix_column_names()
            test_after_fix()
    else:
        print("\n✅ 所有列名已经是标准名称")

    print("\n🎉 修复完成")

if __name__ == "__main__":
    main()
'''

    with open('fix_existing_data.py', 'w', encoding='utf-8') as f:
        f.write(fix_script)

    print("✅ 数据修复脚本已创建: fix_existing_data.py")
    print("\n📋 使用方法:")
    print("  python fix_existing_data.py")

    return True


def main():
    """主函数"""
    print("AKShare采集器标准化解决方案")
    print("=" * 60)

    steps = [
        ("修复AKShare采集器列名", fix_akshare_column_names),
        ("创建标准化采集器模板", create_standard_akshare_collector),
        ("更新数据存储模块", update_data_storage_for_standard),
        ("测试标准化采集器", test_standard_collector),
        ("创建数据修复脚本", create_fix_data_script),
    ]

    results = []
    for step_name, step_func in steps:
        print(f"\n{'=' * 50}")
        print(f"▶️  {step_name}")
        print(f"{'=' * 50}")

        try:
            result = step_func()
            results.append((step_name, result))
        except Exception as e:
            print(f"❌ {step_name} 执行出错: {e}")
            results.append((step_name, False))

    print(f"\n{'=' * 60}")
    print("📋 修复完成汇总")
    print(f"{'=' * 60}")

    success_count = 0
    for step_name, result in results:
        status = "✅ 成功" if result else "❌ 失败"
        print(f"  {step_name:25}: {status}")
        if result:
            success_count += 1

    print(f"\n📊 完成度: {success_count}/{len(steps)}")

    print("\n📋 后续步骤:")
    print("  1. 运行数据修复: python fix_existing_data.py")
    print("  2. 测试查询引擎: python main.py --action p4_query_test")
    print("  3. 测试数据导出: python main.py --action p4_export_test")


if __name__ == "__main__":
    main()