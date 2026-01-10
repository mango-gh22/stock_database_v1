# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\calculate_technical_indicators.py
# File Name: calculate_technical_indicators
# @ Author: mango-gh22
# @ Date：2026/1/9 22:01
# @ Date: 2026/1/10 (优化版)
"""
desc 计算技术指标
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import decimal  # ✅ 用于类型检查

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.config.logging_config import setup_logging
from src.utils.code_converter import normalize_stock_code

logger = setup_logging()


def calculate_all_indicators():
    """优化版：批量计算技术指标（性能提升 10-20 倍）"""
    print("\n" + "=" * 60)
    print("📈 计算技术指标（优化版）")
    print("=" * 60)

    db = DatabaseConnector()

    try:
        with db.get_connection() as conn:
            # 1. 获取股票代码（仅标准格式）
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT symbol 
                    FROM stock_daily_data 
                    WHERE symbol NOT LIKE 'sh.%' AND symbol NOT LIKE 'sz.%'
                    ORDER BY symbol
                """)
                symbols = [row[0] for row in cursor.fetchall()]

            # 格式验证
            invalid_symbols = [s for s in symbols if '.' in s]
            if invalid_symbols:
                logger.error(f"❌ 发现 {len(invalid_symbols)} 个格式错误的股票代码")
                return False

            print(f"✅ 找到 {len(symbols)} 只标准格式股票代码")

            # 2. 检查数据库索引（性能关键）
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as index_count 
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() 
                      AND table_name = 'stock_daily_data'
                      AND index_name LIKE '%symbol_trade_date%'
                """)
                has_index = cursor.fetchone()[0] > 0

            if not has_index:
                logger.warning("⚠️  建议添加复合索引: (symbol, trade_date)")

            total_updated = 0

            # 3. 逐只股票批量计算
            for i, symbol in enumerate(symbols, 1):
                try:
                    print(f"\n[{i}/{len(symbols)}] {symbol}")

                    # 4. 只读取未计算的数据（ma5 IS NULL）
                    with conn.cursor(dictionary=True) as cursor:
                        cursor.execute("""
                            SELECT trade_date, close_price, volume, high_price, low_price
                            FROM stock_daily_data 
                            WHERE symbol = %s AND ma5 IS NULL
                            ORDER BY trade_date
                        """, (symbol,))
                        data = cursor.fetchall()

                    if not data or len(data) < 20:
                        print(f"  ⚠️  无待计算数据或数据不足，跳过")
                        continue

                    print(f"  📊 待计算: {len(data)} 条记录")

                    # 5. 转换为DataFrame并强制类型转换（关键修复）
                    df = pd.DataFrame(data)

                    # 强制转为 float（解决 Decimal 与 None 冲突）
                    numeric_cols = ['close_price', 'volume', 'high_price', 'low_price']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                    # 删除包含 NaN 的行（无法计算指标）
                    missing_count = df['close_price'].isna().sum()
                    if missing_count > 0:
                        print(f"  ⚠️  清理 {missing_count} 个缺失值")
                        df = df.dropna(subset=['close_price'])

                    if len(df) < 20:
                        print(f"  ⚠️  清理后数据不足，跳过")
                        continue

                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    df = df.sort_values('trade_date')

                    # 6. 批量计算所有指标（向量化操作）
                    close_series = df['close_price']
                    volume_series = df['volume']

                    df['ma5'] = close_series.rolling(5, min_periods=1).mean()
                    df['ma10'] = close_series.rolling(10, min_periods=1).mean()
                    df['ma20'] = close_series.rolling(20, min_periods=1).mean()
                    df['ma30'] = close_series.rolling(30, min_periods=1).mean()
                    df['ma60'] = close_series.rolling(60, min_periods=1).mean()
                    df['ma120'] = close_series.rolling(120, min_periods=1).mean()
                    df['ma250'] = close_series.rolling(250, min_periods=1).mean()

                    df['volume_ma5'] = volume_series.rolling(5, min_periods=1).mean()
                    df['volume_ma10'] = volume_series.rolling(10, min_periods=1).mean()
                    df['volume_ma20'] = volume_series.rolling(20, min_periods=1).mean()

                    # RSI
                    delta = close_series.diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss.replace(0, np.nan)
                    df['rsi'] = 100 - (100 / (1 + rs))

                    # 布林带
                    df['bb_middle'] = close_series.rolling(20).mean()
                    bb_std = close_series.rolling(20).std()
                    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
                    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)

                    # 波动率
                    df['volatility_20d'] = close_series.pct_change().rolling(20).std() * np.sqrt(252)

                    # 7. 准备批量更新数据（仅包含需要更新的字段）
                    update_records = []
                    for _, row in df.iterrows():
                        # 只保留非空值
                        record = tuple(
                            None if pd.isna(row[col]) else float(row[col])
                            for col in ['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
                                        'volume_ma5', 'volume_ma10', 'volume_ma20',
                                        'rsi', 'bb_middle', 'bb_upper', 'bb_lower',
                                        'volatility_20d']
                        ) + (symbol, row['trade_date'])
                        update_records.append(record)

                    # 8. ✅ 批量更新（executemany）- 性能核心
                    with conn.cursor() as cursor:
                        cursor.executemany("""
                            UPDATE stock_daily_data 
                            SET 
                                ma5 = %s, ma10 = %s, ma20 = %s, ma30 = %s, 
                                ma60 = %s, ma120 = %s, ma250 = %s,
                                volume_ma5 = %s, volume_ma10 = %s, volume_ma20 = %s,
                                rsi = %s,
                                bb_middle = %s, bb_upper = %s, bb_lower = %s,
                                volatility_20d = %s,
                                updated_time = NOW()
                            WHERE symbol = %s AND trade_date = %s
                        """, update_records)

                        updated_count = cursor.rowcount
                        total_updated += updated_count

                    print(f"  ✅ 更新 {updated_count}/{len(data)} 条记录")

                except Exception as e:
                    print(f"  ❌ 计算 {symbol} 失败: {e}")
                    logger.error(f"计算失败 {symbol}: {e}", exc_info=True)
                    continue

            # 最终提交
            conn.commit()

            print(f"\n" + "=" * 60)
            print(f"🎉 技术指标计算完成！总更新记录: {total_updated:,}")
            print("=" * 60)

            return True

    except Exception as e:
        logger.error(f"计算技术指标失败: {e}", exc_info=True)
        return False


def main():
    """主函数"""
    print("技术指标计算工具（优化版）")
    print("-" * 40)

    db = DatabaseConnector()
    with db.get_connection() as conn:
        with conn.cursor() as cursor:
            # 只统计需要计算的数据
            cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE ma5 IS NULL")
            null_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
            total_count = cursor.fetchone()[0]

            print(f"数据库统计:")
            print(f"  总记录数: {total_count:,}")
            print(f"  待计算指标: {null_count:,} ({null_count / total_count * 100:.1f}%)")

    if null_count == 0:
        print("✅ 所有技术指标已计算完成")
        return 0

    confirmation = input(f"\n需要为 {null_count:,} 条记录计算技术指标，继续吗？(y/n): ").lower()
    if confirmation not in ['y', 'yes']:
        print("操作已取消")
        return 0

    success = calculate_all_indicators()

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
