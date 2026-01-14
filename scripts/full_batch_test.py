# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\full_batch_test.py
# File Name: full_batch_test
# @ Author: mango-gh22
# @ Date：2026/1/6 20:47
"""
desc 
"""

# File Path: E:/MyFile/stock_database_v1/scripts/full_batch_test.py
"""
完整的批量下载测试 - 从零开始下载
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
import logging
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.database.db_connector import DatabaseConnector
from src.config.logging_config import setup_logging

logger = setup_logging()


def check_database_empty():
    """检查数据库是否为空"""
    try:
        db = DatabaseConnector()
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
                count = cursor.fetchone()[0]
                return count == 0
    except Exception as e:
        logger.error(f"检查数据库失败: {e}")
        return False


def download_stock_with_retry(downloader, storage, symbol: str, start_date: str, end_date: str, max_retries: int = 3):
    """带重试的股票下载"""
    for attempt in range(max_retries):
        try:
            logger.info(f"  尝试 {attempt + 1}/{max_retries}: {symbol}")

            # 下载数据
            df = downloader.fetch_factor_data(symbol, start_date, end_date)

            if df is None or df.empty:
                logger.warning(f"  {symbol}: 无数据")
                return 0

            logger.info(f"  {symbol}: 下载 {len(df)} 条记录")

            # 存储数据
            affected_rows, report = storage.store_factor_data(df)

            if affected_rows > 0:
                logger.info(f"  {symbol}: 存储 {affected_rows} 条记录")
                return affected_rows
            else:
                logger.warning(f"  {symbol}: 存储0条记录")
                return 0

        except Exception as e:
            logger.error(f"  {symbol}: 尝试 {attempt + 1} 失败: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # 递增等待时间
                logger.info(f"  {symbol}: {wait_time}秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error(f"  {symbol}: 所有重试失败")
                return -1

    return -1


def run_full_batch_test(symbols: list, batch_size: int = 3):
    """运行完整批量下载测试"""
    print("\n" + "=" * 70)
    print("🚀 完整批量下载测试 - 从零开始")
    print("=" * 70)

    # 1. 检查数据库是否为空
    print("\n1️⃣ 检查数据库状态...")
    if not check_database_empty():
        print("❌ 数据库不为空，请先清空数据")
        print("   运行: python scripts/clean_database.py")
        return False

    print("✅ 数据库为空，可以开始测试")

    # 2. 初始化组件
    print("\n2️⃣ 初始化下载器和存储器...")
    downloader = BaostockPBFactorDownloader()
    storage = FactorStorageManager()

    # 登录
    downloader._ensure_logged_in()

    # 3. 设置下载参数
    start_date = '2005-01-01'  # 从2005年开始
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # 到昨天

    print(f"📅 下载范围: {start_date} - {end_date}")
    print(f"📊 股票数量: {len(symbols)} 只")
    print(f"⚙️  批次大小: {batch_size}")

    # 4. 分批下载
    total_records = 0
    successful_symbols = []
    failed_symbols = []

    print("\n3️⃣ 开始批量下载...")
    print("-" * 70)

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size

        print(f"\n📦 批次 {batch_num}/{total_batches}: {len(batch)} 只股票")
        print("   " + ", ".join(batch))

        for j, symbol in enumerate(batch, 1):
            symbol_num = i + j
            print(f"\n  [{symbol_num}/{len(symbols)}] 处理 {symbol}")

            records = download_stock_with_retry(downloader, storage, symbol, start_date, end_date)

            if records > 0:
                total_records += records
                successful_symbols.append(symbol)
                print(f"  ✅ 成功: {records} 条记录")
            elif records == 0:
                failed_symbols.append((symbol, "无数据"))
                print(f"  ⚠️  无数据")
            else:
                failed_symbols.append((symbol, "下载失败"))
                print(f"  ❌ 失败")

        # 批次间等待（避免API限制）
        if i + batch_size < len(symbols):
            wait_time = 10
            print(f"\n⏳ 等待 {wait_time} 秒后处理下一批...")
            time.sleep(wait_time)

    # 5. 退出登录
    downloader.logout()

    # 6. 输出结果
    print("\n" + "=" * 70)
    print("📊 批量下载完成报告")
    print("=" * 70)

    print(f"📈 统计信息:")
    print(f"   总股票数: {len(symbols)}")
    print(f"   成功: {len(successful_symbols)}")
    print(f"   失败: {len(failed_symbols)}")
    print(f"   总记录数: {total_records:,}")

    if successful_symbols:
        print(f"\n✅ 成功股票 ({len(successful_symbols)} 只):")
        for i, symbol in enumerate(successful_symbols[:10], 1):
            print(f"   {i:2d}. {symbol}")
        if len(successful_symbols) > 10:
            print(f"   ... 还有 {len(successful_symbols) - 10} 只")

    if failed_symbols:
        print(f"\n❌ 失败股票 ({len(failed_symbols)} 只):")
        for i, (symbol, reason) in enumerate(failed_symbols[:10], 1):
            print(f"   {i:2d}. {symbol}: {reason}")
        if len(failed_symbols) > 10:
            print(f"   ... 还有 {len(failed_symbols) - 10} 只")

    # 7. 验证结果
    print("\n4️⃣ 验证下载结果...")
    verify_download_results(successful_symbols)

    return len(successful_symbols) > 0


def verify_download_results(symbols: list):
    """验证下载结果"""
    try:
        db = DatabaseConnector()

        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                print("\n📋 数据库验证:")
                print("-" * 50)

                # 总体统计
                cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
                total = cursor.fetchone()[0]
                print(f"总记录数: {total:,} 条")

                cursor.execute("SELECT COUNT(DISTINCT symbol) as symbols FROM stock_daily_data")
                symbol_count = cursor.fetchone()[0]
                print(f"股票数量: {symbol_count} 只")

                # 各股票统计
                print("\n📊 各股票数据统计:")
                for symbol in symbols[:5]:  # 只显示前5个
                    # 转换为数据库格式
                    if '.' in symbol:
                        code, exchange = symbol.split('.')
                        db_symbol = f"{exchange.lower()}{code}"
                    else:
                        if symbol.startswith('6'):
                            db_symbol = f"sh{symbol}"
                        else:
                            db_symbol = f"sz{symbol}"

                    cursor.execute("""
                        SELECT 
                            COUNT(*) as count,
                            MIN(trade_date) as first_date,
                            MAX(trade_date) as last_date,
                            SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count
                        FROM stock_daily_data 
                        WHERE symbol = %s
                    """, (db_symbol,))

                    result = cursor.fetchone()
                    if result:
                        count, first_date, last_date, pb_count = result
                        print(f"  {symbol}: {count:,}条, {first_date} 到 {last_date}, PB数据: {pb_count or 0}条")

    except Exception as e:
        print(f"验证失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='完整批量下载测试')

    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        default=['600519', '000001', '000858', '000333', '600036', '601318', '300750', '002415'],
        help='股票代码列表'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=3,
        help='批次大小（默认: 3）'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='测试模式，只处理前3只股票'
    )

    args = parser.parse_args()

    symbols = args.symbols

    if args.test:
        symbols = symbols[:3]
        print(f"🧪 测试模式，处理前 {len(symbols)} 只股票")

    print(f"📋 测试股票列表 ({len(symbols)} 只):")
    for i, symbol in enumerate(symbols, 1):
        print(f"  {i:2d}. {symbol}")

    # 确认
    confirmation = input("\n⚠️  确认开始批量下载测试吗？(输入 'YES' 继续): ")
    if confirmation != 'YES':
        print("操作已取消")
        return 0

    # 运行测试
    success = run_full_batch_test(symbols, args.batch_size)

    if success:
        print("\n" + "=" * 70)
        print("🎉 批量下载测试完成！")
        print("=" * 70)

        print("\n💡 验证建议:")
        print("1. 检查数据库中的总记录数:")
        print("   mysql -u root -p -e \"USE stock_database; SELECT COUNT(*) FROM stock_daily_data;\"")
        print("\n2. 检查各股票的因子数据:")
        print(
            "   mysql -u root -p -e \"USE stock_database; SELECT symbol, COUNT(*), MIN(trade_date), MAX(trade_date), SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count FROM stock_daily_data GROUP BY symbol;\"")
    else:
        print("\n❌ 批量下载测试失败")

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)