# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\run.py
# @ Author: mango-gh22
# @ Date：2025/12/7 20:57
"""
desc 股票数据库系统 - 统一主入口 v1.0.0
整合P4查询/P6性能监控/数据下载/因子更新/指标计算
修复上下文管理器问题
"""

import sys
import os
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# 统一日志配置
def setup_logging(log_level=logging.INFO):
    """设置日志（控制台+文件）"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / f"stock_database_{datetime.now().strftime('%Y%m%d')}.log",
                encoding='utf-8'
            )
        ]
    )
    return logging.getLogger(__name__)


# 统一的参数解析器
def create_base_parser():
    """创建基础参数解析器（所有子命令复用）"""
    parser = argparse.ArgumentParser(
        description='股票数据库系统 v1.0.1 - 统一入口',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 数据操作
  python run.py download --group a50 --mode incremental     # 增量下载A50
  python run.py factor-update --symbols 600519 000001      # 更新因子
  python run.py indicator-calc --symbol 600519            # 计算指标

  # 查询与验证
  python run.py validate                                   # 验证数据库
  python run.py query --symbol 600519 --limit 5           # 查询数据

  # 性能监控
  python run.py monitor --duration 300                    # 监控5分钟
  python run.py report                                    # 生成性能报告
        """
    )
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    return parser


# 子命令：validate
# 替换 run.py 中的 cmd_validate 函数

def cmd_validate(args):
    """数据验证（完整修复版）"""
    logger = logging.getLogger(__name__)
    logger.info("🔍 启动数据验证")

    try:
        from src.query.query_engine import QueryEngine
        from src.utils.stock_pool_loader import load_a50_components
        from src.utils.code_converter import normalize_stock_code

        engine = QueryEngine()

        try:
            stats = engine.get_data_statistics()

            with engine.db_connector.get_connection() as conn:
                import pandas as pd

                # 获取所有股票并标准化
                df_symbols = pd.read_sql(
                    "SELECT DISTINCT symbol FROM stock_daily_data ORDER BY symbol",
                    conn
                )

                db_symbols_raw = df_symbols['symbol'].tolist()
                db_symbols = []
                conversion_errors = []

                for symbol in db_symbols_raw:
                    try:
                        normalized = normalize_stock_code(symbol)
                        db_symbols.append(normalized)
                    except ValueError as e:
                        conversion_errors.append(f"{symbol}: {e}")
                        db_symbols.append(symbol)  # 保留原值

                db_symbols = sorted(set(db_symbols))

                # 获取配置中的A50
                config_symbols = load_a50_components()

                # 对比
                db_set = set(db_symbols)
                config_set = set(config_symbols)

                intersection = db_set & config_set
                only_in_db = db_set - config_set
                only_in_config = config_set - db_set

            print("\n" + "=" * 70)
            print("📊 数据验证报告")
            print("=" * 70)

            # 数据库状态判断
            if len(db_symbols) == 0:
                print("⚠️  数据库为空！请先下载数据")
            else:
                print(f"数据库股票（去重后）: {len(db_symbols):,}只")

            print(f"配置文件A50股票: {len(config_symbols):,}只")

            if len(db_symbols) > 0:
                print(f"交集（两者都有）: {len(intersection):,}只")

            if len(db_symbols) > 0:
                print(f"\n📅 数据时间范围: {stats.get('earliest_date')} ~ {stats.get('latest_date')}")
                print(f"📈 总日线记录: {stats.get('total_daily_records', 0):,}")

            if conversion_errors:
                print(f"\n⚠️  代码转换警告（{len(conversion_errors)}个）:")
                for err in conversion_errors[:3]:
                    print(f"  - {err}")

            if only_in_db:
                print(f"\n📦 仅在数据库中（{len(only_in_db)}只）:")
                sample = sorted(list(only_in_db))[:5]
                print(f"  示例: {sample}")

            if only_in_config:
                print(f"\n⬇️  仅在配置中（{len(only_in_config)}只）- 需要下载:")
                print(f"  {sorted(list(only_in_config))}")

            # 因子覆盖率（修复空值问题）
            with engine.db_connector.get_connection() as conn:
                factor_check = pd.read_sql("""
                    SELECT 
                        SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,
                        SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count,
                        COUNT(*) as total
                    FROM stock_daily_data
                """, conn)

                if not factor_check.empty and factor_check['total'].iloc[0] > 0:
                    total = factor_check['total'].iloc[0]
                    pb_val = factor_check['pb_count'].iloc[0] or 0  # ✅ 修复None值
                    pe_val = factor_check['pe_count'].iloc[0] or 0  # ✅ 修复None值

                    pb_pct = pb_val / total * 100
                    pe_pct = pe_val / total * 100

                    print(f"\n📈 因子覆盖率:")
                    print(f"  PB: {pb_pct:.1f}% ({pb_val:,}/{total:,})")
                    print(f"  PE: {pe_pct:.1f}% ({pe_val:,}/{total:,})")
                else:
                    print(f"\n📈 因子覆盖率: 暂无数据")

            print("\n" + "=" * 70)
            print("✅ 验证完成")

            # 给出建议
            print("\n💡 下一步建议:")
            if len(db_symbols) == 0:
                print("  1. 首次运行，请执行: python run.py download --group a50")
            elif len(only_in_config) > 0:
                print(f"  1. 需下载 {len(only_in_config)} 只A50股票: python run.py download --group a50")
            else:
                print("  1. 数据完整，可执行: python run.py indicator-calc")

            return True

        finally:
            engine.close()

    except Exception as e:
        logger.error(f"验证失败: {e}", exc_info=True)
        return False


# 子命令：download
def cmd_download(args):
    """数据下载"""
    logger = logging.getLogger(__name__)

    # 获取股票列表
    symbols = []
    if args.symbols:
        symbols = args.symbols
    elif args.group:
        from src.utils.stock_pool_loader import load_a50_components
        symbols = load_a50_components()
        logger.info(f"从{args.group}加载{len(symbols)}只股票")

    if not symbols:
        logger.error("未指定股票列表")
        return False

    logger.info(f"开始下载{len(symbols)}只股票 ({args.mode}模式)")

    # 路由到具体脚本
    if args.mode == 'incremental':
        from scripts.collect_a50_daily import main as incremental_main
        return incremental_main(symbols)
    else:
        from scripts.download_a50_complete import download_batch
        return download_batch(symbols, args.mode)


# 子命令：factor-update
# 在 run.py 中找到 cmd_factor_update 函数并替换

def cmd_factor_update(args):
    """因子更新（支持从数据库读取）"""
    logger = logging.getLogger(__name__)

    # 确定股票来源
    if args.symbols:
        symbols = args.symbols
        source = 'manual'
    elif args.source:
        symbols = None  # 由脚本内部根据source加载
        source = args.source
    else:
        symbols = None
        source = 'db'  # 默认从数据库

    logger.info(f"因子更新 - 来源: {source}, 模式: {args.mode}")

    from scripts.run_factor_update import update_batch

    success = update_batch(
        symbols=symbols,
        mode=args.mode,
        test_mode=args.test,
        source=source
    )

    return success


# 子命令：indicator-calc
def cmd_indicator_calc(args):
    """指标计算"""
    from scripts.calculate_technical_indicators import calculate_for_symbol, calculate_all_indicators

    if args.symbol:
        # 单只股票
        updated = calculate_for_symbol(args.symbol)
        print(f"更新 {updated} 条记录")
        return updated > 0
    else:
        # 全部计算
        return calculate_all_indicators()


# 子命令：query
def cmd_query(args):
    """数据查询"""
    try:
        from src.query.query_engine import QueryEngine

        engine = QueryEngine()

        try:
            data = engine.query_daily_data(
                symbol=args.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit
            )

            if not data.empty:
                print("\n查询结果:")
                print(data.to_string(index=False))
                return True
            else:
                print("未找到数据")
                return False

        finally:
            engine.close()

    except Exception as e:
        logging.getLogger(__name__).error(f"查询失败: {e}")
        return False


# 子命令：monitor
def cmd_monitor(args):
    """性能监控"""
    try:
        from src.monitoring.performance_monitor import PerformanceMonitor
        from src.config.config_loader import ConfigLoader

        config = ConfigLoader.load_yaml_config('config/performance.yaml')
        monitor = PerformanceMonitor(config.get('monitoring', {}))
        monitor.start()

        print(f"\n监控已启动，持续{args.duration}秒，按Ctrl+C停止...")

        import time
        for i in range(args.duration):
            metrics = monitor.get_current_metrics()
            print(f"\rCPU: {metrics.get('cpu_percent', 0):5.1f}% | "
                  f"内存: {metrics.get('memory_percent', 0):5.1f}% | "
                  f"线程: {metrics.get('active_threads', 0):3d}",
                  end='', flush=True)
            time.sleep(1)

        monitor.stop()
        print("\n监控已停止")
        return True

    except KeyboardInterrupt:
        print("\n用户中断")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"监控失败: {e}")
        return False


# 子命令：report
def cmd_report(args):
    """生成报告"""
    try:
        from src.performance.performance_manager import PerformanceManager

        pm = PerformanceManager()
        report = pm.get_performance_report()

        if report:
            print("\n" + "=" * 60)
            print("📈 性能报告")
            print("=" * 60)
            print(json.dumps(report, indent=2, ensure_ascii=False))

        pm.stop()
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"报告生成失败: {e}")
        return False


# 工具函数：加载股票列表
def load_symbols(source):
    """统一股票代码加载"""
    if isinstance(source, list):
        return source

    if source == 'a50':
        from src.utils.stock_pool_loader import load_a50_components
        return load_a50_components()

    # 配置文件
    config_file = Path('config/symbols.yaml')
    if config_file.exists():
        import yaml
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config.get('csi_a50', [])

    return []


# 主入口
def main():
    parser = create_base_parser()

    # 添加子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # validate 命令
    parser_validate = subparsers.add_parser('validate', help='验证数据完整性')

    # download 命令
    parser_download = subparsers.add_parser('download', help='下载股票数据')
    parser_download.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser_download.add_argument('--symbols', nargs='+', help='股票代码')
    parser_download.add_argument('--group', choices=['a50', 'csi300'], help='股票分组')

    # factor-update 命令
    parser_factor = subparsers.add_parser('factor-update', help='更新估值因子')
    parser_factor.add_argument('--mode', choices=['incremental', 'full'], default='incremental')
    parser_factor.add_argument('--symbols', nargs='+')
    parser_factor.add_argument('--group', choices=['a50', 'csi300'])
    parser_factor.add_argument('--test', action='store_true')
    # 在 factor-update 子命令参数中添加
    parser_factor.add_argument('--source', choices=['db', 'config'], default='db',
                               help='代码来源: db(数据库,默认), config(配置文件)')

    # indicator-calc 命令
    parser_calc = subparsers.add_parser('indicator-calc', help='计算技术指标')
    parser_calc.add_argument('--symbol', help='指定股票代码，留空则计算全部')

    # query 命令
    parser_query = subparsers.add_parser('query', help='查询数据')
    parser_query.add_argument('--symbol', required=True)
    parser_query.add_argument('--start-date', help='开始日期 (YYYYMMDD)')
    parser_query.add_argument('--end-date', help='结束日期 (YYYYMMDD)')
    parser_query.add_argument('--limit', type=int, default=10)

    # monitor 命令
    parser_monitor = subparsers.add_parser('monitor', help='性能监控')
    parser_monitor.add_argument('--duration', type=int, default=60, help='监控时长(秒)')

    # report 命令
    parser_report = subparsers.add_parser('report', help='生成报告')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # 设置日志
    logger = setup_logging(getattr(logging, args.log_level.upper()))
    logger.info(f"执行命令: {args.command}")

    # 路由到子命令
    cmd_map = {
        'validate': cmd_validate,
        'download': cmd_download,
        'factor-update': cmd_factor_update,
        'indicator-calc': cmd_indicator_calc,
        'query': cmd_query,
        'monitor': cmd_monitor,
        'report': cmd_report,
    }

    success = cmd_map[args.command](args)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
