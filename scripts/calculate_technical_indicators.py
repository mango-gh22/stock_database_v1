# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\calculate_technical_indicators.py
# File Name: calculate_technical_indicators
# @ Author: mango-gh22
# @ Date：2026/1/9 22:01
# @ Date: 2026/1/10 (优化版)
"""
desc 计算技术指标批量计算脚本 v1.1.0--优化性能 + 进度显示 + 断点续算
v1.1.1 (修复版)--除错误的 db.close() 调用
技术指标批量计算脚本 --v1.1.2 (修复游标连接)
技术指标批量计算脚本 v1.1.3 (修复作用域错误)
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.config.logging_config import setup_logging
from src.utils.code_converter import normalize_stock_code

logger = setup_logging()


def calculate_for_symbol(symbol, db=None):
    """单只股票指标计算（修复作用域错误）"""
    if db is None:
        db = DatabaseConnector()

    updated_count = 0  # 初始化返回值

    try:
        with db.get_connection() as conn:
            # 查询未计算的数据
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT trade_date, close_price, volume, high_price, low_price
                    FROM stock_daily_data 
                    WHERE symbol = %s AND ma5 IS NULL
                    ORDER BY trade_date
                """, (symbol,))
                data = cursor.fetchall()

            if not data or len(data) < 20:
                return 0

            # 转换为DataFrame
            df = pd.DataFrame(data)

            # 类型转换
            numeric_cols = ['close_price', 'volume', 'high_price', 'low_price']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df = df.dropna(subset=['close_price'])

            if len(df) < 20:
                return 0

            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')

            # 批量计算
            close_series = df['close_price']
            volume_series = df['volume']

            # 移动平均线
            df['ma5'] = close_series.rolling(5, min_periods=1).mean()
            df['ma10'] = close_series.rolling(10, min_periods=1).mean()
            df['ma20'] = close_series.rolling(20, min_periods=1).mean()
            df['ma30'] = close_series.rolling(30, min_periods=1).mean()
            df['ma60'] = close_series.rolling(60, min_periods=1).mean()
            df['ma120'] = close_series.rolling(120, min_periods=1).mean()
            df['ma250'] = close_series.rolling(250, min_periods=1).mean()

            # 成交量均线
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

            # ✅ 修复：在正确的作用域内准备更新数据
            update_records = []
            for _, row in df.iterrows():
                record = tuple(
                    None if pd.isna(row[col]) else float(row[col])
                    for col in ['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
                                'volume_ma5', 'volume_ma10', 'volume_ma20',
                                'rsi', 'bb_middle', 'bb_upper', 'bb_lower',
                                'volatility_20d']
                ) + (symbol, row['trade_date'].strftime('%Y-%m-%d'))
                update_records.append(record)

            # ✅ 修复：确保 update_records 在作用域内
            if update_records:
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

                conn.commit()

            return updated_count

    except Exception as e:
        logger.error(f"计算失败 {symbol}: {e}", exc_info=True)
        return 0


def calculate_all_indicators():
    """批量计算所有股票（修复连接管理）"""
    print("\n" + "=" * 60)
    print("📈 批量计算技术指标")
    print("=" * 60)

    db = DatabaseConnector()

    try:
        # 获取需要计算的股票列表
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT DISTINCT symbol FROM stock_daily_data WHERE ma5 IS NULL"
                )
                symbols_to_calc = [row[0] for row in cursor.fetchall()]

            total_symbols = len(symbols_to_calc)

            if total_symbols == 0:
                print("✅ 所有技术指标已计算完成")
                return True

            print(f"发现 {total_symbols} 只股票需要计算")

            # 统计待计算记录数
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stock_daily_data WHERE ma5 IS NULL")
                total_pending = cursor.fetchone()[0]

            print(f"总待计算记录: {total_pending:,} 条")

            confirm = input("\n开始计算吗？(y/n): ").lower()
            if confirm not in ['y', 'yes']:
                print("操作已取消")
                return True

        # 逐只股票计算，创建独立连接
        success_count = 0
        total_updated = 0

        for i, symbol in enumerate(symbols_to_calc, 1):
            try:
                print(f"\n[{i}/{total_symbols}] {symbol}", end=' ')

                # 每次调用创建独立连接
                updated = calculate_for_symbol(symbol)
                total_updated += updated

                print(f"✅ 更新 {updated} 条")

                if updated > 0:
                    success_count += 1

            except Exception as e:
                logger.error(f"计算失败 {symbol}: {e}", exc_info=True)
                print(f"❌ 失败: {e}")

        # 最终统计
        print("\n" + "=" * 60)
        print(f"🎉 计算完成！")
        print(f"成功股票: {success_count}/{total_symbols}")
        print(f"总更新记录: {total_updated:,}")
        print("=" * 60)

        return success_count > 0

    finally:
        # 不需要关闭 db
        pass


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description='技术指标计算')
    parser.add_argument('--symbol', help='指定股票代码')

    args = parser.parse_args()

    if args.symbol:
        # 单只股票计算
        print(f"计算股票: {args.symbol}")
        updated = calculate_for_symbol(args.symbol)
        print(f"更新 {updated} 条记录")
        return 0 if updated > 0 else 1
    else:
        # 全部计算
        success = calculate_all_indicators()
        return 0 if success else 1


if __name__ == "__main__":
    import argparse

    sys.exit(main())
