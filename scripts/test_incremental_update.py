# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_incremental_update.py
# File Name: test_incremental_update
# @ Author: mango-gh22
# @ Date：2026/1/6 20:42
"""
desc 
"""

# File Path: E:/MyFile/stock_database_v1/scripts/test_incremental_update.py
"""
测试增量更新功能
"""

import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.factor_batch_processor import FactorBatchProcessor
from src.config.logging_config import setup_logging

logger = setup_logging()


def test_incremental_update():
    """测试增量更新"""
    print("\n" + "=" * 60)
    print("🧪 测试增量更新功能")
    print("=" * 60)

    try:
        # 初始化处理器
        processor = FactorBatchProcessor()

        # 测试股票列表
        test_symbols = ['600519', '000001']

        print(f"测试股票: {test_symbols}")
        print("模式: incremental")

        # 执行增量更新
        report = processor.process_symbol_list(
            symbols=test_symbols,
            mode='incremental'
        )

        # 分析结果
        summary = report['summary']
        print(f"\n📊 更新结果:")
        print(f"  总股票数: {summary['total_symbols']}")
        print(f"  成功: {summary['successful']}")
        print(f"  失败: {summary['failed']}")
        print(f"  跳过: {summary['skipped']}")
        print(f"  总记录数: {summary['total_records']}")

        # 详细结果
        print(f"\n🔍 详细结果:")
        for result in report['detailed_results']:
            symbol = result.get('symbol', 'unknown')
            status = result.get('status', 'unknown')
            reason = result.get('reason', '')

            if status == 'skipped':
                print(f"  ⚠️  {symbol}: 跳过 - {reason}")
            elif status == 'success':
                records = result.get('records_stored', 0)
                print(f"  ✅ {symbol}: 成功存储 {records} 条记录")
            elif status == 'error':
                error = result.get('error', '未知错误')
                print(f"  ❌ {symbol}: 错误 - {error}")

        # 清理
        processor.cleanup()

        print("\n" + "=" * 60)
        print("✅ 测试完成")
        print("=" * 60)

        # 解释结果
        print("\n💡 结果解释:")
        if summary['skipped'] > 0:
            print("  有股票被跳过，说明数据已经是最新的，无需更新")
            print("  这是正常情况，说明增量更新逻辑正确工作")

        if summary['successful'] > 0:
            print("  有股票成功更新，说明发现了新数据并成功存储")

        return True

    except Exception as e:
        logger.error(f"测试失败: {e}")
        print(f"\n❌ 错误: {e}")
        return False


def main():
    """主函数"""
    print("检查数据库中已有的数据...")

    # 先检查现有数据
    from src.database.db_connector import DatabaseConnector

    db = DatabaseConnector()
    with db.get_connection() as conn:
        with conn.cursor() as cursor:
            print("\n📊 数据库最新数据:")
            print("-" * 40)

            for symbol in ['sh600519', 'sz000001']:
                cursor.execute("""
                    SELECT MAX(trade_date) as last_date 
                    FROM stock_daily_data 
                    WHERE symbol = %s AND pb IS NOT NULL
                """, (symbol,))

                last_date = cursor.fetchone()[0]
                print(f"  {symbol}: 最后PB数据日期 = {last_date}")

    # 当前日期
    today = datetime.now().date()
    print(f"\n📅 当前日期: {today}")

    # 运行测试
    success = test_incremental_update()

    if success:
        print("\n🎉 增量更新测试完成！")
        print("\n💡 建议:")
        print("1. 数据已最新，无需批量重新下载")
        print("2. 可以定期运行增量更新保持数据新鲜")
        print("3. 如果要强制重新下载，使用full模式")
    else:
        print("\n❌ 测试失败")

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)