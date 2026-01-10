# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\adjustment_factor_manager.py
# File Name: adjustment_factor_manager
# @ Author: mango-gh22
# @ Date：2025/12/15 0:30
"""
desc 复权因子管理器 - 负责采集、存储和管理复权因子
修复版本: v0.5.1-fix - 添加缺失的fetch_factors方法
复权因子管理器 - P6阶段完整实现
负责：下载 → 计算 → 存储 → 查询 全链路
单线程约束：通过BaostockAdjustmentFactorDownloader强制实现
P7/P8预留：--daemon 和 --batch 参数解析接口
运行：python -m src.data.adjustment_factor_manager --mode incremental

核心约束：单线程执行（thread_num=1），P7/P8可平滑扩展
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union
import logging
import argparse
import sys
import time
from pathlib import Path
import threading
import os

from src.data.baostock_adjustment_factor_downloader import BaostockAdjustmentFactorDownloader
from src.data.adjustment_factor_storage import AdjustmentFactorStorage
from src.data.adjustment_factor_date_calculator import AdjustmentFactorDateCalculator
from src.utils.code_converter import normalize_stock_code
from src.utils.logger import get_logger
from src.monitoring.calculation_logger import CalculationLogger

logger = get_logger(__name__)


class AdjustmentFactorManager:
    """
    复权因子全生命周期管理器
    P6阶段：强制单线程，稳健优先
    P7/P8阶段：通过配置切换为多线程/守护进程
    """

    def __init__(self, config_path: str = 'config/adjustment_factor_config.yaml'):
        """
        初始化管理器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()

        # 核心组件（按执行顺序初始化）
        self.downloader = BaostockAdjustmentFactorDownloader(config_path)
        self.storage = AdjustmentFactorStorage(self.config.get('database_config', 'config/database.yaml'))
        self.date_calculator = AdjustmentFactorDateCalculator(self.storage)

        # 监控日志器
        self.calc_logger = self._init_calculation_logger()

        # 统计信息（按流程阶段细分）
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_symbols': 0,
            'successful_download': 0,
            'failed_download': 0,
            'successful_calculate': 0,
            'failed_calculate': 0,
            'successful_store': 0,
            'failed_store': 0,
            'total_records_downloaded': 0,
            'total_records_stored': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'duration_ms': 0
        }

        # 单线程锁（P6阶段强制使用）
        self._operation_lock = threading.Lock()

        logger.info("✅ 复权因子管理器初始化完成")
        logger.info(f"   单线程模式: {'ENABLED' if self.is_single_thread() else 'DISABLED'}")

    def _load_config(self) -> Dict[str, Any]:
        """加载配置，支持环境变量覆盖"""
        try:
            from src.config.config_loader import ConfigLoader
            loader = ConfigLoader()
            config = loader.load_yaml_config(self.config_path)

            # 合并环境变量（如DB_PASSWORD）
            from dotenv import load_dotenv
            load_dotenv()

            if os.getenv('ADJUSTMENT_FACTOR_THREAD_NUM'):
                config['download']['thread_num'] = int(os.getenv('ADJUSTMENT_FACTOR_THREAD_NUM'))

            return config.get('adjustment_factors', {})
        except Exception as e:
            logger.warning(f"加载配置失败 {self.config_path}: {e}，使用默认值")
            return {
                'download': {'thread_num': 1, 'incremental_mode': True, 'enable_cache': False},
                'storage': {'batch_size': 500, 'use_upsert': True}
            }

    def _init_calculation_logger(self) -> CalculationLogger:
        """初始化性能监控日志器"""
        log_config = {
            'enabled': self.config.get('logging', {}).get('enable_performance_monitoring', True),
            'log_level': self.config.get('logging', {}).get('log_level', 'INFO'),
            'log_dir': 'logs/adjustment_factors',
            'log_queries': True,
            'log_results': False,
            'log_performance': True,
            'buffer_size': 50,
            'flush_interval': 30,
            'max_log_size': 100 * 1024 * 1024  # 100MB
        }
        return CalculationLogger(log_config)

    def is_single_thread(self) -> bool:
        """P6阶段：始终返回True"""
        # P7阶段：返回 self.config.get('download', {}).get('thread_num', 1) == 1
        return True

    def download_batch(self, symbols: List[str], start_date: str = None,
                       end_date: str = None, mode: str = 'incremental') -> Dict[str, pd.DataFrame]:
        """
        批量下载并存储复权因子（P6阶段单线程实现）

        执行流程：
        1. 计算下载范围 → 2. 下载复权因子 → 3. 存储数据库 → 4. 记录日志

        Args:
            symbols: 股票代码列表（标准化格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            mode: 日期计算模式 ('incremental', 'full', 'specific')

        Returns:
            Dict[str, pd.DataFrame]: 成功处理的符号 -> 因子DataFrame
        """
        # 强制单线程执行（P6约束）
        with self._operation_lock:
            return self._execute_batch_sync(symbols, start_date, end_date, mode)

    def _execute_batch_sync(self, symbols: List[str], start_date: str = None,
                            end_date: str = None, mode: str = 'incremental') -> Dict[str, pd.DataFrame]:
        """同步批量执行核心逻辑（内部方法）"""
        self.stats['start_time'] = datetime.now()
        self.stats['total_symbols'] = len(symbols)

        logger.info(f"🚀 开始批量处理复权因子: {len(symbols)} 只股票")
        logger.info(f"📅 模式: {mode} | 范围: {start_date or 'auto'} - {end_date or 'auto'}")
        logger.info(f"⚙️  单线程模式: {'强制启用' if self.is_single_thread() else '已关闭'}")

        results = {}

        # P6：单线程顺序处理
        for i, symbol in enumerate(symbols, 1):
            success = self._process_single_symbol(
                symbol, i, len(symbols), start_date, end_date, mode, results
            )

        # 更新统计
        self.stats['end_time'] = datetime.now()
        self.stats['duration_ms'] = int((self.stats['end_time'] - self.stats['start_time']).total_seconds() * 1000)

        # 输出总结
        self._print_batch_summary()

        return results

    def _process_single_symbol(self, symbol: str, index: int, total: int,
                               start_date: str, end_date: str, mode: str,
                               results: Dict) -> bool:
        """处理单只股票（原子操作）"""
        log_id = self.calc_logger.log_calculation_start(
            indicator_name="adjustment_factor",
            symbol=str(symbol),
            period="daily",
            calculation_type="download_calculate_store",
            parameters={
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "config": self.config
            },
            input_data_shape=(0, 0)
        )

        start_ms = time.time() * 1000

        try:
            logger.info(f"[{index}/{total}] 处理 {symbol}")

            # Step 1: 计算下载范围
            date_range = self.date_calculator.calculate_download_range(
                symbol, mode=mode, custom_params={'start_date': start_date, 'end_date': end_date}
            )

            if not date_range:
                logger.info(f"  {symbol} 数据已最新，跳过")
                self.stats['cache_hits'] += 1
                self.calc_logger.log_calculation_end(
                    log_id=log_id, success=True, output_data_shape=(0, 0),
                    duration_ms=int(time.time() * 1000 - start_ms), cache_hit=True
                )
                return True

            start, end = date_range
            logger.info(f"  下载范围: {start} - {end}")

            # Step 2: 下载复权因子数据（接口已返回计算好的因子）
            factor_df = self.downloader.fetch_adjustment_factor_data(symbol, start, end)

            if factor_df.empty:
                logger.warning(f"  {symbol} 无复权因子数据")
                self.stats['failed_download'] += 1
                self.calc_logger.log_calculation_end(
                    log_id=log_id, success=True, output_data_shape=(0, 0),
                    duration_ms=int(time.time() * 1000 - start_ms), cache_hit=False
                )
                return True  # 无数据不算失败

            self.stats['successful_download'] += 1
            self.stats['total_records_downloaded'] += len(factor_df)
            logger.info(f"  ✅ 下载成功: {len(factor_df)} 条")

            # Step 3: 直接使用数据（无需计算）
            factors_df = factor_df.copy()

            if factors_df.empty:
                logger.warning(f"  {symbol} 无复权因子数据")
                self.calc_logger.log_calculation_end(
                    log_id=log_id, success=True, output_data_shape=(0, 0),
                    duration_ms=int(time.time() * 1000 - start_ms), cache_hit=False
                )
                return True

            self.stats['successful_calculate'] += 1
            logger.info(f"  ✅ 数据准备成功: {len(factors_df)} 条")

            # Step 4: 存储到数据库
            affected_rows, report = self.storage.store_adjustment_factors(factors_df)

            if affected_rows > 0:
                results[symbol] = factors_df
                self.stats['successful_store'] += 1
                self.stats['total_records_stored'] += affected_rows
                logger.info(f"  ✅ 存储成功: {affected_rows}/{len(factors_df)} 条")

                end_ms = time.time() * 1000
                self.calc_logger.log_calculation_end(
                    log_id=log_id, success=True, output_data_shape=factors_df.shape,
                    duration_ms=int(end_ms - start_ms), performance_metrics=report
                )
                return True
            else:
                logger.warning(f"  {symbol}: 存储失败 - {report.get('reason', 'unknown')}")
                self.stats['failed_store'] += 1

                end_ms = time.time() * 1000
                self.calc_logger.log_calculation_end(
                    log_id=log_id, success=False, output_data_shape=factors_df.shape,
                    duration_ms=int(end_ms - start_ms), error_message=report.get('error', 'Storage failed')
                )
                return False

        except Exception as e:
            logger.error(f"  ❌ {symbol} 处理异常: {e}", exc_info=True)
            self.stats['failed_download'] += 1  # 统归为下载失败

            end_ms = time.time() * 1000
            self.calc_logger.log_calculation_end(
                log_id=log_id, success=False, output_data_shape=None,
                duration_ms=int(end_ms - start_ms), error_message=str(e)
            )
            return False

# -------4-----------------------------
    def update_symbol(self, symbol: str, mode: str = 'incremental') -> bool:
        """
        更新单只股票复权因子（智能增量）

        Args:
            symbol: 股票代码
            mode: 更新模式

        Returns:
            是否成功
        """
        try:
            normalized_symbol = normalize_stock_code(symbol)
            logger.info(f"更新单只股票: {normalized_symbol}")

            result = self.download_batch([normalized_symbol], mode=mode)
            return len(result) > 0

        except Exception as e:
            logger.error(f"单只股票更新失败 {symbol}: {e}")
            return False

    def get_adjustment_factor(
            self,
            symbol: str,
            date: Union[str, datetime],
            factor_type: str = 'forward'
    ) -> Optional[float]:
        """
        核心查询接口：获取指定日期复权因子

        算法：
        1. 查询小于等于目标日期的最新除权记录
        2. 若无记录，返回1.0（无除权）

        Args:
            symbol: 股票代码
            date: 日期（YYYY-MM-DD 或 YYYYMMDD 或 datetime）
            factor_type: 'forward', 'backward', 'total'

        Returns:
            复权因子值（异常返回None）
        """
        try:
            # 日期标准化
            date_str = self._normalize_date(date)

            normalized_symbol = normalize_stock_code(symbol)

            # 查询最接近的除权日（小于等于目标日期）
            query = f"""
                SELECT {factor_type}_factor, ex_date 
                FROM {self.storage.factor_table}
                WHERE symbol = %s AND ex_date <= %s
                ORDER BY ex_date DESC 
                LIMIT 1
            """

            with self.storage.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (normalized_symbol, date_str))
                    result = cursor.fetchone()

            if result and result[0] is not None:
                factor = float(result[0])
                logger.debug(f"查询复权因子: {symbol} {date_str} {factor_type}={factor:.6f}")
                return factor

            # 无除权事件，返回1.0
            logger.debug(f"无除权事件，返回1.0: {symbol} {date_str}")
            return 1.0

        except Exception as e:
            logger.error(f"查询复权因子失败 {symbol} {date}: {e}")
            return None

    def get_factors_for_symbol(self, symbol: str, start_date: str = None,
                               end_date: str = None) -> pd.DataFrame:
        """
        获取指定股票的所有复权因子（兼容adjustor.py）

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            复权因子DataFrame
        """
        return self.storage.get_factors_by_symbol(symbol, start_date, end_date)

    def _normalize_date(self, date: Union[str, datetime]) -> str:
        """日期格式标准化 -> YYYY-MM-DD"""
        if isinstance(date, datetime):
            return date.strftime('%Y-%m-%d')
        elif isinstance(date, str):
            return f"{date[:4]}-{date[4:6]}-{date[6:8]}" if len(date) == 8 else date
        else:
            raise ValueError(f"不支持的日期类型: {type(date)}")

    def _print_batch_summary(self):
        """打印批量处理总结报告"""
        if not self.stats['start_time']:
            return

        print("\n" + "=" * 80)
        print("📊 复权因子批量处理总结报告")
        print("-" * 80)
        print(f"  执行时间: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  结束时间: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  总耗时: {self.stats['duration_ms']}ms")
        print("-" * 80)
        print(f"  📥 下载阶段:")
        print(f"     成功: {self.stats['successful_download']} / {self.stats['total_symbols']}")
        print(f"     失败: {self.stats['failed_download']}")
        print(f"     记录数: {self.stats['total_records_downloaded']:,}")
        print(f"  🧮 计算阶段:")
        print(f"     成功: {self.stats['successful_calculate']}")
        print(f"     失败: {self.stats['failed_calculate']}")
        print(f"  💾 存储阶段:")
        print(f"     成功: {self.stats['successful_store']}")
        print(f"     失败: {self.stats['failed_store']}")
        print(f"     记录数: {self.stats['total_records_stored']:,}")
        print("-" * 80)
        print(f"  缓存命中: {self.stats['cache_hits']}")
        print(f"  缓存未命中: {self.stats['cache_misses']}")
        success_rate = self.stats['successful_store'] / self.stats['total_symbols'] * 100 if self.stats[
                                                                                                 'total_symbols'] > 0 else 0
        print(f"  整体成功率: {success_rate:.1f}%")
        print("=" * 80)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息（深拷贝）"""
        return self.stats.copy()

    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_symbols': 0,
            'successful_download': 0,
            'failed_download': 0,
            'successful_calculate': 0,
            'failed_calculate': 0,
            'successful_store': 0,
            'failed_store': 0,
            'total_records_downloaded': 0,
            'total_records_stored': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'duration_ms': 0
        }

    def cleanup(self):
        """清理资源（幂等）"""
        try:
            self.downloader.logout()
            self.calc_logger.flush_buffer()
            logger.info("复权因子管理器清理完成")
        except Exception as e:
            logger.warning(f"清理资源异常: {e}")

    def __del__(self):
        """析构函数（确保资源释放）"""
        self.cleanup()

    # ========== P7/P8阶段扩展接口 ==========

    def download_batch_parallel(self, symbols: List[str], start_date: str = None,
                                end_date: str = None, mode: str = 'incremental',
                                max_workers: int = 3) -> Dict[str, pd.DataFrame]:
        """
        P7阶段：多线程批量下载（预留接口）
        当前P6阶段调用此方法会降级为单线程
        """
        logger.warning("P6阶段：多线程模式已降级为单线程")
        return self.download_batch(symbols, start_date, end_date, mode)

    def run_daemon_mode(self, interval_hours: int = 24):
        """
        P7阶段：守护进程模式（预留接口）
        """
        logger.info(f"🔄 守护进程模式将在P7阶段实现（当前间隔: {interval_hours}h）")
        # TODO: P7实现
        pass

    def run_batch_job(self, job_file: str):
        """
        P8阶段：批量任务模式（预留接口）
        """
        logger.info(f"📦 批量任务模式将在P8阶段实现（配置文件: {job_file}）")
        # TODO: P8实现
        pass

    def export_factors(self, symbol: str, format: str = 'csv',
                       output_path: Optional[str] = None) -> str:
        """
        P8阶段：导出复权因子数据（预留接口）

        Args:
            symbol: 股票代码
            format: 导出格式（csv, json, parquet）
            output_path: 输出路径

        Returns:
            导出文件路径
        """
        logger.info(f"📤 导出功能将在P8阶段实现: {symbol} -> {format}")
        return ""


# ========== 命令行接口（P7/P8独立运行入口） ==========
# P6阶段：仅支持单次执行
# P7阶段：支持 --daemon
# P8阶段：支持 --batch

def main():
    """命令行主入口"""
    parser = argparse.ArgumentParser(
        description="复权因子管理器 - P6阶段单线程模式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # P6：单次运行（默认）
  python adjustment_factor_manager.py --symbols sh600519 sz000001 --mode incremental

  # P6：从文件加载股票列表
  python adjustment_factor_manager.py --symbols-file config/symbols.yaml --mode full

  # P7：守护进程模式（暂未实现）
  python adjustment_factor_manager.py --daemon --interval 24

  # P8：批量任务模式（暂未实现）
  python adjustment_factor_manager.py --batch job_config.yaml
        """
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        help="股票代码列表（如 sh600519 sz000001）"
    )
    parser.add_argument(
        "--symbols-file",
        help="股票代码文件路径（YAML格式）"
    )
    parser.add_argument(
        "--start-date",
        help="开始日期 YYYYMMDD"
    )
    parser.add_argument(
        "--end-date",
        help="结束日期 YYYYMMDD"
    )
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=["incremental", "full", "specific"],
        help="下载模式（默认: incremental）"
    )
    parser.add_argument(
        "--config",
        default="config/adjustment_factor_config.yaml",
        help="配置文件路径"
    )

    # P7/P8预留参数
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="P7: 守护进程模式（暂未实现）"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=24,
        help="P7: 守护进程执行间隔（小时）"
    )
    parser.add_argument(
        "--batch",
        help="P8: 批量任务配置文件（暂未实现）"
    )

    args = parser.parse_args()

    # P7/P8模式检查
    if args.daemon:
        print("🔄 P7守护进程模式将在后续版本实现")
        sys.exit(0)

    if args.batch:
        print(f"📦 P8批量任务模式将在后续版本实现: {args.batch}")
        sys.exit(0)

    # P6阶段：单次执行
    print("🚀 P6阶段单线程模式启动")

    manager = AdjustmentFactorManager(args.config)

    # 加载股票列表
    symbols = []
    if args.symbols:
        symbols = [normalize_stock_code(s) for s in args.symbols]
    elif args.symbols_file:
        from src.data.symbol_manager import get_symbol_manager
        sm = get_symbol_manager()
        symbols = sm.get_symbols_from_file(args.symbols_file)
    else:
        # 默认使用CSI A50
        from src.data.symbol_manager import get_symbol_manager
        sm = get_symbol_manager()
        symbols = sm.get_symbols('csi_a50')
        print(f"📋 未指定股票，默认使用CSI A50: {len(symbols)} 只")

    if not symbols:
        print("❌ 错误: 未提供有效股票代码")
        sys.exit(1)

    print(f"📊 准备处理 {len(symbols)} 只股票")
    print(f"📅 日期范围: {args.start_date or '自动'} - {args.end_date or '自动'}")
    print(f"⚙️  模式: {args.mode}")
    print("-" * 60)

    try:
        # 核心执行
        results = manager.download_batch(
            symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=args.mode
        )

        # 输出结果
        print("\n" + "=" * 60)
        print("✅ 执行完成")
        print(f"📈 成功处理: {len(results)} 只股票")
        print(f"⏱️  总耗时: {manager.stats['duration_ms']}ms")
        print("=" * 60)

        sys.exit(0)

    except KeyboardInterrupt:
        print("\n⚠️  用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 致命错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        manager.cleanup()


if __name__ == "__main__":
    main()
