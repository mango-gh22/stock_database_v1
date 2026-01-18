# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\diagnose_factor_gaps.py
# File Name: diagnose_factor_gaps
# @ Author: mango-gh22
# @ Date：2026/1/18 13:33
"""
desc 因子缺失模式诊断工具
识别因子缺失的分布规律（时间、股票、原因）
"""

import pandas as pd
import sys
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.db_connector import DatabaseConnector
from src.utils.stock_pool_loader import load_a50_components


def diagnose_factor_gaps():
    """诊断因子缺失模式"""
    print("\n🔍 因子缺失模式诊断")
    print("=" * 70)

    db = DatabaseConnector()

    with db.get_connection() as conn:
        # 1. 查询缺失分布
        query = """
            SELECT 
                symbol,
                COUNT(*) as total_records,
                SUM(CASE WHEN pb IS NULL OR pb = 0 THEN 1 ELSE 0 END) as pb_missing,
                SUM(CASE WHEN pe_ttm IS NULL OR pe_ttm = 0 THEN 1 ELSE 0 END) as pe_missing,
                MIN(trade_date) as earliest_date,
                MAX(trade_date) as latest_date
            FROM stock_daily_data
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
            GROUP BY symbol
            HAVING pb_missing > 0 OR pe_missing > 0
        """

        df_stats = pd.read_sql(query, conn)

        if df_stats.empty:
            print("✅ 最近90天无因子缺失")
            return

        print(f"发现 {len(df_stats)} 只股票存在因子缺失")
        print("\n📈 缺失统计预览:")
        print(df_stats.head(10).to_string(index=False))

        # 2. 时间分布分析
        query_time = """
            SELECT 
                trade_date,
                COUNT(*) as total_records,
                SUM(CASE WHEN pb IS NULL OR pb = 0 THEN 1 ELSE 0 END) as pb_missing,
                SUM(CASE WHEN pe_ttm IS NULL OR pe_ttm = 0 THEN 1 ELSE 0 END) as pe_missing
            FROM stock_daily_data
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY trade_date
            ORDER BY trade_date
        """

        df_time = pd.read_sql(query_time, conn)

        # 绘制缺失趋势图
        plt.figure(figsize=(14, 6))
        plt.subplot(1, 2, 1)
        plt.plot(df_time['trade_date'], df_time['pb_missing'], label='PB缺失', marker='o')
        plt.title('PB因子缺失时间分布')
        plt.xticks(rotation=45)
        plt.legend()

        plt.subplot(1, 2, 2)
        plt.plot(df_time['trade_date'], df_time['pe_missing'], label='PE缺失', color='orange', marker='s')
        plt.title('PE因子缺失时间分布')
        plt.xticks(rotation=45)
        plt.legend()

        plt.tight_layout()
        plt.savefig('reports/factor_missing_trend.png', dpi=300)
        print("\n📊 图表已保存: reports/factor_missing_trend.png")

        # 3. 诊断报告
        summary = {
            'total_affected_stocks': len(df_stats),
            'total_missing_pb': df_stats['pb_missing'].sum(),
            'total_missing_pe': df_stats['pe_missing'].sum(),
            'avg_missing_rate_pb': df_stats['pb_missing'].sum() / df_stats['total_records'].sum() * 100,
            'avg_missing_rate_pe': df_stats['pe_missing'].sum() / df_stats['total_records'].sum() * 100
        }

        print("\n" + "=" * 70)
        print("📋 诊断总结")
        print("=" * 70)
        print(f"受影响股票: {summary['total_affected_stocks']} 只")
        print(f"PB缺失记录: {summary['total_missing_pb']:,} 条")
        print(f"PE缺失记录: {summary['total_missing_pe']:,} 条")
        print(f"PB缺失率: {summary['avg_missing_rate_pb']:.2f}%")
        print(f"PE缺失率: {summary['avg_missing_rate_pe']:.2f}%")

        # 4. 保存详细报告
        df_stats.to_csv('reports/factor_missing_detail.csv', index=False)
        print(f"\n📄 详细报告已保存: reports/factor_missing_detail.csv")

        return df_stats


if __name__ == "__main__":
    diagnose_factor_gaps()