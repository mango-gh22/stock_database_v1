# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\run_batch_direct.py
# File Name: run_batch_direct
# @ Author: mango-gh22
# @ Date：2026/1/4 0:51
"""
desc 直接批量运行脚本
不依赖配置文件，直接指定股票列表
"""

import sys
import os
import argparse
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.factor_batch_processor import FactorBatchProcessor
from src.config.logging_config import setup_logging

logger = setup_logging()


def get_a50_symbols() -> list:
    """获取A50成分股列表（硬编码，绕过配置问题）"""
    a50_symbols = [
        # 消费
        "600519.SH",  # 贵州茅台
        "000858.SZ",  # 五粮液
        "000333.SZ",  # 美的集团
        "002304.SZ",  # 洋河股份
        "600887.SH",  # 伊利股份

        # 金融
        "601318.SH",  # 中国平安
        "600036.SH",  # 招商银行
        "000001.SZ",  # 平安银行
        "601166.SH",  # 兴业银行
        "601328.SH",  # 交通银行

        # 新能源
        "300750.SZ",  # 宁德时代
        "002594.SZ",  # 比亚迪
        "601012.SH",  # 隆基绿能

        # 医药
        "600276.SH",  # 恒瑞医药
        "000538.SZ",  # 云南白药

        # 科技
        "002415.SZ",  # 海康威视
        "000977.SZ",  # 浪潮信息
        "603259.SH",  # 药明康德

        # 其他
        "600900.SH",  # 长江电力
        "601088.SH",  # 中国神华
        "601857.SH",  # 中国石油
    ]

    logger.info(f"加载 {len(a50_symbols)} 只A50成分股")
    return a50_symbols


def get_csi_300_sample() -> list:
    """获取沪深300样本股"""
    csi_300_symbols = [
        "600519.SH", "000858.SZ", "000333.SZ", "601318.SH", "600036.SH",
        "000001.SZ", "300750.SZ", "002594.SZ", "601012.SH", "600276.SH",
        "002415.SZ", "600900.SH", "601166.SH", "601328.SH", "600887.SH",
        "600030.SH", "601688.SH", "601998.SH", "600016.SH", "600000.SH",
    ]

    logger.info(f"加载 {len(csi_300_symbols)} 只沪深300样本股")
    return csi_300_symbols


def run_batch_update(symbols: list, mode: str = 'incremental',
                     batch_size: int = 10, test_mode: bool = False):
    """
    运行批量更新

    Args:
        symbols: 股票代码列表
        mode: 更新模式
        batch_size: 批次大小
        test_mode: 测试模式（限制数量）
    """
    print("\n" + "=" * 60)
    print("🚀 直接批量更新")
    print("=" * 60)

    if test_mode:
        symbols = symbols[:5]  # 测试模式只处理5只
        print(f"🧪 测试模式，处理 {len(symbols)} 只股票")

    print(f"📋 股票列表 ({len(symbols)} 只):")
    for i, symbol in enumerate(symbols[:10], 1):  # 只显示前10个
        print(f"  [{i}] {symbol}")
    if len(symbols) > 10:
        print(f"  ... 还有 {len(symbols) - 10} 只")

    try:
        # 初始化处理器
        processor = FactorBatchProcessor()
        processor.batch_size = batch_size  # 设置批次大小

        # 进度回调
        def progress_callback(progress, current, total):
            print(f"📈 进度: {progress:.1f}% ({current}/{total})", end='\r')

        # 执行批量处理
        print(f"\n⚙️  开始批量处理 ({mode}模式，批次大小: {batch_size})...")
        start_time = datetime.now()

        report = processor.process_symbol_list(
            symbols=symbols,
            mode=mode,
            progress_callback=progress_callback
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # 输出结果
        print("\n" + "=" * 60)
        print("📊 更新完成报告")
        print("=" * 60)

        summary = report['summary']
        print(f"总股票数: {summary['total_symbols']}")
        print(f"成功更新: {summary['successful']}")
        print(f"更新失败: {summary['failed']}")
        print(f"已跳过: {summary['skipped']}")
        print(f"总记录数: {summary['total_records']:,}")
        print(f"成功率: {summary['success_rate']}%")
        print(f"总耗时: {duration:.2f}秒")

        # 性能统计
        perf = report['performance']
        print(f"\n⚡ 性能指标:")
        print(f"处理速度: {perf['symbols_per_second']:.2f} 只/秒")
        print(f"记录速度: {perf['records_per_second']:.2f} 条/秒")

        # 失败股票
        failed_symbols = report.get('failed_symbols', [])
        if failed_symbols:
            print(f"\n❌ 失败股票 ({len(failed_symbols)} 只):")
            for symbol in failed_symbols[:10]:
                print(f"  {symbol}")
            if len(failed_symbols) > 10:
                print(f"  ... 还有 {len(failed_symbols) - 10} 只")

        # 成功示例
        successful_symbols = report.get('successful_symbols', [])
        if successful_symbols:
            print(f"\n✅ 成功示例 (前5只):")
            for symbol in successful_symbols[:5]:
                for detail in report['detailed_results']:
                    if detail.get('symbol') == symbol and detail.get('status') == 'success':
                        records = detail.get('records_stored', 0)
                        print(f"  {symbol}: {records} 条记录")
                        break

        print("\n" + "=" * 60)
        print("💡 详细报告已保存至: data/reports/factors/")
        print("=" * 60)

        # 清理
        processor.cleanup()

        return True

    except Exception as e:
        logger.error(f"❌ 批量更新失败: {e}", exc_info=True)
        print(f"\n错误: {e}")
        return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='直接批量更新脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 测试模式运行A50
  python run_batch_direct.py --group a50 --test

  # 完整运行A50（增量模式）
  python run_batch_direct.py --group a50 --mode incremental

  # 运行沪深300样本
  python run_batch_direct.py --group csi300

  # 指定股票列表
  python run_batch_direct.py --symbols 600519 000001 000858 --mode full

  # 自定义批次大小
  python run_batch_direct.py --group a50 --batch-size 5
        """
    )

    parser.add_argument(
        '--group',
        type=str,
        choices=['a50', 'csi300', 'custom'],
        default='a50',
        help='股票分组: a50(A50成分股), csi300(沪深300样本), custom(自定义)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        help='自定义股票代码列表，如: 600519 000001 000858'
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['incremental', 'full'],
        default='incremental',
        help='更新模式: incremental(增量, 默认), full(全量)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='批次大小（默认: 10）'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='测试模式，只处理少量股票'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别'
    )

    args = parser.parse_args()

    # 设置日志级别
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if numeric_level:
        logging.getLogger().setLevel(numeric_level)

    # 确定股票列表
    symbols = []

    if args.symbols:
        # 自定义股票列表
        symbols = args.symbols
        print(f"使用自定义股票列表: {len(symbols)} 只")
    elif args.group == 'a50':
        # A50成分股
        symbols = get_a50_symbols()
    elif args.group == 'csi300':
        # 沪深300样本
        symbols = get_csi_300_sample()
    else:
        # 默认使用A50
        symbols = get_a50_symbols()

    if not symbols:
        print("❌ 未指定有效的股票列表")
        return 1

    # 运行批量更新
    success = run_batch_update(
        symbols=symbols,
        mode=args.mode,
        batch_size=args.batch_size,
        test_mode=args.test
    )

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)