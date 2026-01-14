# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\fixed_batch_download.py
# File Name: fixed_batch_download
# @ Author: mango-gh22
# @ Date：2026/1/6 20:18
"""
desc 
"""

# File Path: E:/MyFile/stock_database_v1/scripts/fixed_batch_download.py
"""
修复的批量下载脚本 - 确保日期范围和symbol格式正确
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
import logging
from typing import List, Optional, Tuple, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.utils.code_converter import normalize_stock_code
from src.config.logging_config import setup_logging

logger = setup_logging()


def fix_symbol_format(symbol: str) -> str:
    """修复symbol格式为数据库格式"""
    if not symbol:
        return symbol

    # 移除.SH/.SZ后缀
    if '.' in symbol:
        code, exchange = symbol.split('.')
        if exchange == 'SH':
            return f"sh{code}"
        elif exchange == 'SZ':
            return f"sz{code}"

    # 如果是纯数字
    if symbol.isdigit():
        if symbol.startswith('6'):
            return f"sh{symbol}"
        elif symbol.startswith(('0', '3')):
            return f"sz{symbol}"

    return symbol


def verify_database_data(symbols: List[str]):
    """验证数据库中的数据（修复版）"""
    try:
        from src.database.db_connector import DatabaseConnector

        db = DatabaseConnector()

        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                print("\n📊 数据库数据验证:")
                print("-" * 60)

                for symbol in symbols:
                    # 修复symbol格式
                    clean_symbol = fix_symbol_format(symbol).replace('.', '')

                    # 查询总记录数
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_count,
                            MIN(trade_date) as first_date,
                            MAX(trade_date) as last_date
                        FROM stock_daily_data 
                        WHERE symbol = %s
                    """, (clean_symbol,))

                    result = cursor.fetchone()
                    if result:
                        total, first_date, last_date = result
                        print(f"  {symbol} -> {clean_symbol}:")
                        print(f"    总记录数: {total:,}条")
                        if first_date and last_date:
                            print(f"    日期范围: {first_date} 到 {last_date}")
                        else:
                            print(f"    无数据")

                        # 查询因子数据
                        cursor.execute("""
                            SELECT 
                                SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,
                                SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count,
                                SUM(CASE WHEN ps_ttm IS NOT NULL THEN 1 ELSE 0 END) as ps_count
                            FROM stock_daily_data 
                            WHERE symbol = %s
                        """, (clean_symbol,))

                        factor_result = cursor.fetchone()
                        if factor_result:
                            pb_count, pe_count, ps_count = factor_result
                            print(f"    有PB数据: {pb_count or 0}条")
                            print(f"    有PE数据: {pe_count or 0}条")
                            print(f"    有PS数据: {ps_count or 0}条")

                            # 如果有数据，显示最近5天的因子值
                            if pb_count or pe_count or ps_count:
                                cursor.execute("""
                                    SELECT trade_date, pb, pe_ttm, ps_ttm 
                                    FROM stock_daily_data 
                                    WHERE symbol = %s 
                                    AND (pb IS NOT NULL OR pe_ttm IS NOT NULL OR ps_ttm IS NOT NULL)
                                    ORDER BY trade_date DESC 
                                    LIMIT 5
                                """, (clean_symbol,))

                                recent_data = cursor.fetchall()
                                if recent_data:
                                    print(f"    最近因子数据:")
                                    for date, pb, pe, ps in recent_data:
                                        pb_str = f"PB={pb:.2f}" if pb else "PB=None"
                                        pe_str = f"PE={pe:.2f}" if pe else "PE=None"
                                        ps_str = f"PS={ps:.2f}" if ps else "PS=None"
                                        print(f"      {date}: {pb_str}, {pe_str}, {ps_str}")

                        print()

    except Exception as e:
        print(f"数据库验证失败: {e}")


def download_full_history(symbols: List[str], start_date: str = '2005-01-01'):
    """
    下载完整历史数据（修复日期范围问题）
    """
    print("\n" + "=" * 60)
    print("📥 下载完整历史因子数据")
    print("=" * 60)

    try:
        # 初始化下载器和存储器
        downloader = BaostockPBFactorDownloader()
        storage = FactorStorageManager()

        # 登录
        downloader._ensure_logged_in()

        total_records = 0
        successful_symbols = []
        failed_symbols = []

        # 设置合理的结束日期（昨天）
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        for i, symbol in enumerate(symbols, 1):
            try:
                print(f"\n[{i}/{len(symbols)}] 处理 {symbol}")

                # 1. 下载数据
                print(f"  下载范围: {start_date} - {end_date}")
                df = downloader.fetch_factor_data(symbol, start_date, end_date)

                if df is None or df.empty:
                    print(f"  ⚠️  无数据")
                    failed_symbols.append((symbol, "无数据"))
                    continue

                print(f"  ✅ 下载成功: {len(df)} 条记录")

                # 2. 存储数据
                affected_rows, report = storage.store_factor_data(df)

                if affected_rows > 0:
                    print(f"  ✅ 存储成功: {affected_rows} 条记录")
                    total_records += affected_rows
                    successful_symbols.append(symbol)

                    # 显示数据摘要
                    if 'symbol' in df.columns:
                        symbol_in_db = df['symbol'].iloc[0]
                        print(f"  数据库symbol: {symbol_in_db}")

                    if 'trade_date' in df.columns:
                        dates = df['trade_date'].tolist()
                        if dates:
                            print(f"  日期范围: {min(dates)} 到 {max(dates)}")

                    # 检查是否有因子数据
                    factor_fields = ['pb', 'pe_ttm', 'ps_ttm']
                    for field in factor_fields:
                        if field in df.columns:
                            non_null_count = df[field].notna().sum()
                            if non_null_count > 0:
                                print(f"  有{field.upper()}数据: {non_null_count}条")

                else:
                    print(f"  ⚠️  存储0条记录（可能已存在）")
                    # 即使存储0条，也算成功（因为数据已存在）
                    successful_symbols.append(symbol)

                # 请求间隔
                if i < len(symbols):
                    time.sleep(2)

            except Exception as e:
                print(f"  ❌ 处理失败: {e}")
                failed_symbols.append((symbol, str(e)))

        # 退出登录
        downloader.logout()

        # 输出结果
        print("\n" + "=" * 60)
        print("📊 下载完成报告")
        print("=" * 60)
        print(f"总股票数: {len(symbols)}")
        print(f"成功: {len(successful_symbols)}")
        print(f"失败: {len(failed_symbols)}")
        print(f"总记录数: {total_records}")

        if failed_symbols:
            print(f"\n❌ 失败股票:")
            for symbol, error in failed_symbols[:5]:  # 只显示前5个
                print(f"  {symbol}: {error}")
            if len(failed_symbols) > 5:
                print(f"  还有 {len(failed_symbols) - 5} 只失败股票...")

        # 验证数据库数据
        verify_database_data(symbols)

        return len(successful_symbols) > 0

    except Exception as e:
        print(f"❌ 下载失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='修复的批量下载 - 完整历史数据')

    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        required=True,
        help='股票代码列表，如: 600519 000001 000858'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        default='2005-01-01',
        help='开始日期 (YYYY-MM-DD)，默认: 2005-01-01'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='测试模式，只处理前2只股票'
    )

    args = parser.parse_args()

    symbols = args.symbols

    if args.test:
        symbols = symbols[:2]
        print(f"🧪 测试模式，处理前 {len(symbols)} 只股票")

    print(f"股票列表: {symbols}")

    success = download_full_history(symbols, args.start_date)

    if success:
        print("\n🎉 批量下载完成！")
        print("\n💡 下一步：")
        print("1. 检查数据库中是否有PB、PE、PS等因子数据")
        print("2. 可以使用以下SQL查询验证:")
        print("   SELECT symbol, COUNT(*) as count, ")
        print("          SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,")
        print("          SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count")
        print("   FROM stock_daily_data")
        print("   GROUP BY symbol;")
    else:
        print("\n❌ 批量下载失败")

    return 0 if success else 1


if __name__ == "__main__":
    import time
    from typing import List

    exit_code = main()
    sys.exit(exit_code)