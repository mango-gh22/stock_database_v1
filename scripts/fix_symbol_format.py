# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\fix_symbol_format.py
# File Name: fix_symbol_format
# @ Author: mango-gh22
# @ Date：2026/1/10 10:03
"""
desc 
"""

# _*_ coding: utf-8 _*_
"""
清理并统一数据库中的股票代码格式
将 sh.601318 格式统一为 sh601318
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.utils.code_converter import normalize_stock_code
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_symbol_format():
    """修复股票代码格式"""
    print("=" * 70)
    print("🔧 修复股票代码格式（sh.601318 → sh601318）")
    print("=" * 70)

    db = DatabaseConnector()

    with db.get_connection() as conn:
        with conn.cursor() as cursor:
            # 1. 统计当前格式分布
            print("\n📊 统计当前格式分布...")
            cursor.execute("""
                SELECT symbol, COUNT(*) as count 
                FROM stock_daily_data 
                WHERE symbol LIKE 'sh.%' OR symbol LIKE 'sz.%'
                GROUP BY symbol
                ORDER BY count DESC
                LIMIT 20
            """)
            dot_symbols = cursor.fetchall()

            print(f"发现 {len(dot_symbols)} 个带点的股票代码")
            for symbol, count in dot_symbols[:10]:
                print(f"  {symbol}: {count} 条记录")

            # 2. 修复数据
            print("\n🛠️  开始修复...")
            fixed_count = 0

            for symbol, count in dot_symbols:
                try:
                    # 标准化格式
                    normalized_symbol = normalize_stock_code(symbol)

                    if normalized_symbol != symbol:
                        # 检查目标符号是否已存在
                        cursor.execute(
                            "SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s",
                            (normalized_symbol,)
                        )
                        existing_count = cursor.fetchone()[0]

                        if existing_count > 0:
                            # 如果目标已存在，合并数据（删除旧的）
                            print(f"  合并 {symbol} → {normalized_symbol} ({existing_count}条已存在)")
                            cursor.execute(
                                "DELETE FROM stock_daily_data WHERE symbol = %s",
                                (symbol,)
                            )
                        else:
                            # 直接更新
                            cursor.execute(
                                "UPDATE stock_daily_data SET symbol = %s WHERE symbol = %s",
                                (normalized_symbol, symbol)
                            )

                        conn.commit()
                        fixed_count += 1
                        print(f"  已修复: {symbol} → {normalized_symbol}")

                except Exception as e:
                    logger.error(f"修复失败 {symbol}: {e}")
                    conn.rollback()

            # 3. 验证修复结果
            print("\n✅ 修复完成，验证结果...")
            cursor.execute("""
                SELECT COUNT(DISTINCT symbol) as unique_symbols,
                       COUNT(*) as total_records,
                       SUM(CASE WHEN symbol LIKE 'sh.%' OR symbol LIKE 'sz.%' THEN 1 ELSE 0 END) as dot_format_count
                FROM stock_daily_data
            """)
            result = cursor.fetchone()

            print(f"  唯一股票数: {result[0]}")
            print(f"  总记录数: {result[1]:,}")
            print(f"  剩余点格式: {result[2]} 条")

            return fixed_count


if __name__ == "__main__":
    fixed = fix_symbol_format()
    print(f"\n🎉 完成！共修复 {fixed} 个股票代码格式")
    sys.exit(0 if fixed >= 0 else 1)