# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_factor_download.py
# File Name: test_factor_download
# @ Author: mango-gh22
# @ Date：2026/1/11 11:12
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试因子下载（绕过存储，直接查看原始数据）
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader


def test_factor_download(symbol: str = 'sh600519'):
    """测试下载因子数据"""
    print(f"\n🔍 测试因子下载: {symbol}")
    print("=" * 60)

    downloader = BaostockPBFactorDownloader()

    try:
        # 下载最近5天数据
        import datetime
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y%m%d')

        print(f"日期范围: {start_date} ~ {end_date}")

        df = downloader.fetch_factor_data(symbol, start_date, end_date)

        print(f"\n1. 下载结果:")
        print(f"   记录数: {len(df)}")
        print(f"   列名: {list(df.columns)}")

        if df.empty:
            print("   ❌ 下载为空！")
            return False

        print(f"\n2. 数据样本:")
        print(df.head(3).to_string())

        print(f"\n3. 因子字段统计:")
        factor_fields = ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm', 'turnover_rate_f']  # 移除 pb_ttm, dv_ttm

        # 添加明确的注释说明
        print("\n3. 因子字段统计:")
        print("   ✅ 以下字段来自Baostock：pb, pe_ttm, ps_ttm, pcf_ttm, turnover_rate_f")
        print("   ❌ 以下字段Baostock不支持：pb_ttm, dv_ttm, dv_ratio")

        for field in factor_fields:
            if field in df.columns:
                non_null = df[field].notna().sum()
                print(f"   {field}: {non_null} / {len(df)} 条非空")
                # 显示唯一值
                unique_vals = df[field].dropna().unique()[:3]
                print(f"       示例值: {unique_vals}")
            else:
                print(f"   ❌ {field}: 列不存在")

        # 4. 检查数据类型
        print(f"\n4. 数据类型:")
        print(df.dtypes)

        return True

    finally:
        downloader.logout()


if __name__ == '__main__':
    test_factor_download()