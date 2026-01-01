# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\brute_force_fix.py
# File Name: brute_force_fix
# @ Author: mango-gh22
# @ Date：2026/1/1 11:07
"""
desc 
"""
# brute_force_fix.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("💥 暴力修复 - 绕过所有问题逻辑")
print("=" * 60)


def brute_force_update():
    """完全绕过现有逻辑，直接更新数据"""

    # 导入必要的库
    import pandas as pd
    from src.data.baostock_collector import BaostockCollector
    from src.database.db_connector import DatabaseConnector
    import mysql.connector
    from datetime import datetime
    import time

    # 要更新的股票列表（可以从配置文件读取）
    symbols = [
        "sh.600000", "sh.600028", "sh.600036", "sh.600048", "sh.600050",
        "sz.000001", "sz.000002", "sz.000063", "sz.000333", "sz.000858"
    ]

    # 更新日期范围
    start_date = "2025-12-25"
    end_date = "2025-12-31"

    print(f"📋 更新计划:")
    print(f"   股票数量: {len(symbols)}")
    print(f"   日期范围: {start_date} 到 {end_date}")
    print("-" * 60)

    # 初始化采集器
    collector = BaostockCollector()
    db = DatabaseConnector()

    total_inserted = 0
    total_skipped = 0
    total_failed = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"\n[{i}/{len(symbols)}] 处理 {symbol}...")

            # 1. 采集数据
            print("   1. 采集数据...")
            data = collector.fetch_daily_data(symbol, start_date, end_date)

            if data is None or data.empty:
                print("   ⚠️  没有数据，跳过")
                total_skipped += 1
                continue

            print(f"     采集到 {len(data)} 条记录")

            # 2. 连接到数据库
            conn = db.get_connection()
            cursor = conn.cursor(dictionary=True)

            # 3. 获取当前数据状态
            clean_symbol = symbol.replace('.', '')
            cursor.execute("SELECT trade_date FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
            existing_dates = {row['trade_date'].strftime('%Y-%m-%d') for row in cursor.fetchall()}

            print(f"     数据库中已有 {len(existing_dates)} 天的数据")

            # 4. 插入新数据
            inserted = 0
            for _, row in data.iterrows():
                try:
                    # 解析日期
                    trade_date = None
                    if 'date' in row:
                        trade_date = str(row['date'])
                    elif 'trade_date' in row:
                        trade_date = str(row['trade_date'])

                    if not trade_date:
                        continue

                    # 检查是否已存在
                    if trade_date in existing_dates:
                        # print(f"     跳过已存在数据: {trade_date}")
                        continue

                    # 准备插入数据
                    sql = """
                        INSERT INTO stock_daily_data 
                        (symbol, trade_date, open_price, high_price, low_price, 
                         close_price, pre_close_price, volume, turnover,
                         turnover_rate, turnover_rate_f, amplitude, 
                         change_percent, data_source, created_time, updated_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """

                    # 准备参数（处理缺失值）
                    params = (
                        clean_symbol,
                        trade_date,
                        row.get('open', row.get('open_price')),
                        row.get('high', row.get('high_price')),
                        row.get('low', row.get('low_price')),
                        row.get('close', row.get('close_price')),
                        row.get('preclose', row.get('pre_close_price', row.get('close'))),
                        int(row.get('volume', 0)),
                        float(row.get('amount', row.get('turnover', 0))),
                        float(row.get('turn', row.get('turnover_rate', 0))),
                        float(row.get('turnover', row.get('turnover_rate_f', 0))),
                        float(row.get('amplitude', 0)),
                        float(row.get('pctChg', row.get('change_percent', 0))),
                        'baostock'
                    )

                    cursor.execute(sql, params)
                    inserted += 1

                except mysql.connector.errors.IntegrityError as e:
                    # 重复数据，跳过
                    if "Duplicate entry" in str(e):
                        continue
                    else:
                        raise
                except Exception as e:
                    print(f"     插入记录时出错: {e}")

            # 5. 提交事务
            conn.commit()

            # 6. 验证结果
            cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
            total_count = cursor.fetchone()['count']

            cursor.close()
            conn.close()

            print(f"     成功插入 {inserted} 条新记录")
            print(f"     现共有 {total_count} 条记录")

            total_inserted += inserted

            # 避免请求过于频繁
            time.sleep(1)

        except Exception as e:
            print(f"   ❌ 处理 {symbol} 失败: {e}")
            total_failed += 1

    # 输出总结
    print("\n" + "=" * 60)
    print("📊 更新总结")
    print("=" * 60)
    print(f"总股票数: {len(symbols)}")
    print(f"成功插入: {total_inserted} 条记录")
    print(f"跳过: {total_skipped} 只股票")
    print(f"失败: {total_failed} 只股票")

    if total_inserted > 0:
        print("\n🎉 暴力更新成功！数据库已更新。")
    else:
        print("\n⚠️  没有插入新数据，可能所有数据都已存在。")


# 执行
if __name__ == "__main__":
    brute_force_update()