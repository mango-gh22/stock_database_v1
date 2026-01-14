# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\debug_factor_update.py
# File Name: debug_factor_update
# @ Author: mango-gh22
# @ Date：2026/1/11 11:51
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子更新调试脚本
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.utils.code_converter import normalize_stock_code


def debug_single_stock(symbol: str = 'sh600519'):
    """调试单只股票因子更新"""
    print(f"\n🔍 调试股票: {symbol}")
    print("=" * 60)

    # 1. 下载数据
    downloader = BaostockPBFactorDownloader()
    storage = FactorStorageManager()

    try:
        # 下载最近30天
        import datetime
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

        print(f"1. 下载因子数据: {start_date} ~ {end_date}")

        df_downloaded = downloader.fetch_factor_data(symbol, start_date, end_date)

        print(f"   下载结果: {len(df_downloaded)} 条记录")

        if df_downloaded.empty:
            print("   ❌ 下载为空！")
            return False

        # 显示前3条数据
        print("\n   数据样本:")
        print(df_downloaded.head(3).to_string())

        # 显示列名
        print(f"\n   数据列名: {list(df_downloaded.columns)}")

        # 2. 准备存储
        print("\n2. 准备存储数据...")

        # 确保symbol列存在
        df_downloaded['symbol'] = symbol

        # 标准化日期格式
        if 'trade_date' in df_downloaded.columns:
            df_downloaded['trade_date'] = pd.to_datetime(df_downloaded['trade_date']).dt.strftime('%Y-%m-%d')

        print(f"   准备存储: {len(df_downloaded)} 条")
        print(f"   因子字段统计:")

        # 检查因子字段
        factor_fields = ['pb', 'pe_ttm', 'ps_ttm', 'pb_ttm', 'dv_ttm']
        for field in factor_fields:
            if field in df_downloaded.columns:
                non_null = df_downloaded[field].notna().sum()
                print(f"     {field}: {non_null} 条非空")
            else:
                print(f"     {field}: ❌ 列不存在")

        # 3. 存储数据
        print("\n3. 存储到数据库...")

        # 检查表结构
        with storage.db_connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DESCRIBE stock_daily_data")
            columns = [col[0] for col in cursor.fetchall()]
            print(f"   数据库表字段: {columns}")

        # 执行存储
        affected_rows, report = storage.store_factor_data(df_downloaded)

        print(f"   存储结果: {affected_rows} 条受影响")
        print(f"   状态: {report.get('status')}")
        print(f"   详情: {report}")

        if report.get('error'):
            print(f"   ❌ 错误: {report['error']}")

        # 4. 验证数据库
        print("\n4. 验证数据库存储...")

        with storage.db_connector.get_connection() as conn:
            df_db = pd.read_sql(
                f"SELECT * FROM stock_daily_data WHERE symbol = '{symbol}' AND pb IS NOT NULL LIMIT 3",
                conn
            )

            if df_db.empty:
                print("   ❌ 数据库中因子数据为空！")
            else:
                print("   ✅ 数据库中找到因子数据:")
                print(df_db[['trade_date', 'symbol', 'pb', 'pe_ttm']].to_string())

        return True

    finally:
        downloader.logout()


if __name__ == '__main__':
    debug_single_stock()