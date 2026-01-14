# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\verify_factor_storage.py
# File Name: verify_factor_storage
# @ Author: mango-gh22
# @ Date：2026/1/3 19:37
"""
desc 验证因子数据存储情况
检查：存储记录数、数据完整性、日期范围等
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.factor_storage_manager import FactorStorageManager
from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.config.logging_config import setup_logging

logger = setup_logging()


def verify_factor_storage(symbol: str = '600519'):
    """验证因子数据存储情况"""
    print(f"\n🔍 验证因子数据存储: {symbol}")
    print("=" * 50)

    try:
        # 初始化
        storage = FactorStorageManager()
        downloader = BaostockPBFactorDownloader()

        # 1. 检查最后更新日期
        last_date = storage.get_last_factor_date(symbol)
        print(f"1️⃣ 最后更新日期: {last_date or '无数据'}")

        # 2. 从数据库查询实际存储的因子数据
        clean_symbol = str(symbol).replace('.', '')

        with storage.db_connector.get_connection() as conn:
            # 查询总记录数
            df_total = pd.read_sql_query(
                f"SELECT COUNT(*) as total_count FROM stock_daily_data WHERE symbol = '{clean_symbol}'",
                conn
            )
            total_count = df_total['total_count'].iloc[0]

            # 查询有因子数据的记录数
            df_factor = pd.read_sql_query(
                f"""SELECT COUNT(*) as factor_count 
                    FROM stock_daily_data 
                    WHERE symbol = '{clean_symbol}' 
                    AND (pb IS NOT NULL OR pe_ttm IS NOT NULL OR ps_ttm IS NOT NULL)""",
                conn
            )
            factor_count = df_factor['factor_count'].iloc[0]

            # 查询因子数据的日期范围
            df_range = pd.read_sql_query(
                f"""SELECT MIN(trade_date) as first_date, MAX(trade_date) as last_date
                    FROM stock_daily_data 
                    WHERE symbol = '{clean_symbol}' 
                    AND pb IS NOT NULL""",
                conn
            )

            # 查询因子字段的统计信息
            df_stats = pd.read_sql_query(
                f"""SELECT 
                        COUNT(pb) as pb_count,
                        COUNT(pe_ttm) as pe_ttm_count,
                        COUNT(ps_ttm) as ps_ttm_count,
                        AVG(pb) as avg_pb,
                        AVG(pe_ttm) as avg_pe_ttm
                    FROM stock_daily_data 
                    WHERE symbol = '{clean_symbol}'""",
                conn
            )

        print(f"2️⃣ 数据统计:")
        print(f"   总记录数: {total_count}")
        print(f"   有因子记录数: {factor_count}")
        print(f"   因子覆盖率: {factor_count / total_count * 100:.1f}%" if total_count > 0 else "   无数据")

        if not df_range.empty and df_range['first_date'].iloc[0]:
            print(f"3️⃣ 因子日期范围:")
            print(f"   最早: {df_range['first_date'].iloc[0]}")
            print(f"   最晚: {df_range['last_date'].iloc[0]}")

        if not df_stats.empty:
            print(f"4️⃣ 因子统计:")
            print(f"   PB记录数: {df_stats['pb_count'].iloc[0]}")
            print(f"   PE_TTM记录数: {df_stats['pe_ttm_count'].iloc[0]}")
            print(f"   PS_TTM记录数: {df_stats['ps_ttm_count'].iloc[0]}")
            if df_stats['avg_pb'].iloc[0]:
                print(f"   平均PB: {df_stats['avg_pb'].iloc[0]:.2f}")
            if df_stats['avg_pe_ttm'].iloc[0]:
                print(f"   平均PE_TTM: {df_stats['avg_pe_ttm'].iloc[0]:.2f}")

        # 5. 检查数据质量
        with storage.db_connector.get_connection() as conn:
            # 检查空值和异常值
            df_quality = pd.read_sql_query(
                f"""SELECT 
                        SUM(CASE WHEN pb IS NULL THEN 1 ELSE 0 END) as pb_null,
                        SUM(CASE WHEN pb <= 0 THEN 1 ELSE 0 END) as pb_non_positive,
                        SUM(CASE WHEN pe_ttm IS NULL THEN 1 ELSE 0 END) as pe_ttm_null,
                        SUM(CASE WHEN pe_ttm <= 0 THEN 1 ELSE 0 END) as pe_ttm_non_positive
                    FROM stock_daily_data 
                    WHERE symbol = '{clean_symbol}'""",
                conn
            )

        if not df_quality.empty:
            print(f"5️⃣ 数据质量检查:")
            print(f"   PB空值: {df_quality['pb_null'].iloc[0]}")
            print(f"   PB非正值: {df_quality['pb_non_positive'].iloc[0]}")
            print(f"   PE_TTM空值: {df_quality['pe_ttm_null'].iloc[0]}")
            print(f"   PE_TTM非正值: {df_quality['pe_ttm_non_positive'].iloc[0]}")

        # 6. 下载最新数据对比
        print(f"6️⃣ 下载最新数据对比:")
        try:
            # 下载最近30天数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

            downloaded_data = downloader.fetch_factor_data(symbol, start_date, end_date)
            print(f"   下载记录数: {len(downloaded_data)}")

            if not downloaded_data.empty:
                # 与数据库对比
                db_dates = set()
                with storage.db_connector.get_connection() as conn:
                    df_db = pd.read_sql_query(
                        f"""SELECT trade_date, pb, pe_ttm 
                            FROM stock_daily_data 
                            WHERE symbol = '{clean_symbol}' 
                            AND trade_date >= '{start_date}' 
                            AND trade_date <= '{end_date}'""",
                        conn
                    )
                    db_dates = set(df_db['trade_date'].astype(str).tolist())

                downloaded_dates = set(downloaded_data['trade_date'].astype(str).tolist())

                missing_in_db = downloaded_dates - db_dates
                extra_in_db = db_dates - downloaded_dates

                print(f"   数据库已有: {len(db_dates)} 个日期")
                print(f"   下载数据: {len(downloaded_dates)} 个日期")
                print(f"   缺失日期: {len(missing_in_db)} 个")
                print(f"   多余日期: {len(extra_in_db)} 个")

                if missing_in_db:
                    print(f"   具体缺失: {sorted(list(missing_in_db))[:5]}...")

        except Exception as e:
            print(f"   下载对比失败: {e}")

        print("\n✅ 验证完成")
        return True

    except Exception as e:
        print(f"❌ 验证失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def analyze_storage_discrepancy():
    """分析存储记录数不匹配的问题"""
    print("\n🔬 分析存储记录数不匹配问题")
    print("=" * 50)

    try:
        storage = FactorStorageManager()

        with storage.db_connector.get_connection() as conn:
            # 检查stock_daily_data表的结构
            cursor = conn.cursor()
            cursor.execute("DESCRIBE stock_daily_data")
            columns = cursor.fetchall()

            print("表结构检查:")
            print(f"  总列数: {len(columns)}")

            # 找出有唯一约束的列
            cursor.execute("""
                SELECT COLUMN_NAME, CONSTRAINT_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'stock_daily_data'
                AND CONSTRAINT_NAME = 'PRIMARY' OR CONSTRAINT_NAME LIKE '%unique%'
            """)
            constraints = cursor.fetchall()

            print(f"  约束条件: {constraints}")

            # 检查股票600519的重复记录
            cursor.execute("""
                SELECT symbol, trade_date, COUNT(*) as count
                FROM stock_daily_data
                WHERE symbol LIKE '%600519%'
                GROUP BY symbol, trade_date
                HAVING count > 1
                ORDER BY count DESC
                LIMIT 10
            """)
            duplicates = cursor.fetchall()

            if duplicates:
                print(f"\n⚠️  发现重复记录:")
                for dup in duplicates:
                    print(f"  {dup[0]} {dup[1]}: {dup[2]} 条重复")
            else:
                print(f"\n✅ 无重复记录")

            # 检查不同symbol格式的记录
            cursor.execute("""
                SELECT DISTINCT symbol
                FROM stock_daily_data
                WHERE symbol LIKE '%600519%'
            """)
            symbols = cursor.fetchall()

            print(f"\n不同的symbol格式:")
            for sym in symbols:
                print(f"  {sym[0]}")

        return True

    except Exception as e:
        print(f"❌ 分析失败: {e}")
        return False


def main():
    """主函数"""
    print("\n📊 PB因子数据存储验证工具")
    print("=" * 60)

    # 验证主要股票
    test_symbols = ['600519', '000001', '000858']

    for symbol in test_symbols[:1]:  # 只验证第一个
        verify_factor_storage(symbol)

    # 分析存储记录数问题
    analyze_storage_discrepancy()

    print("\n" + "=" * 60)
    print("🎉 验证工具运行完成")
    print("=" * 60)


if __name__ == "__main__":
    main()