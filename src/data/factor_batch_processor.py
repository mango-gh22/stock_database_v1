# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_batch_processor.py
# File Name: factor_batch_processor
# @ Author: mango-gh22
# @ Date：2026/1/3 22:43
"""
desc 
"""

# File Path: E:/MyFile/stock_database_v1/src/data/factor_batch_processor.py
"""
批量因子处理器 - 高效处理大量股票的因子数据下载和存储
支持分批处理、进度监控、错误恢复
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import time
import sys
import os
from tqdm import tqdm
import json
from pathlib import Path

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 添加项目路径
# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.utils.enhanced_trade_date_manager import EnhancedTradeDateManager
from src.config.logging_config import setup_logging
from src.data.a50_fixer import A50SymbolFixer

logger = setup_logging()


class FactorBatchProcessor:
    """
    批量因子处理器 - 专门处理大量股票的因子数据
    特性：
    1. 分批处理，避免内存溢出
    2. 进度监控和报告
    3. 错误恢复和重试
    4. 性能统计和优化
    """

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化批量处理器

        Args:
            config_path: 配置文件路径
        """
        self.downloader = BaostockPBFactorDownloader()
        self.storage = FactorStorageManager(config_path)
        self.trade_date_manager = EnhancedTradeDateManager()

        # 配置
        self.batch_size = 10  # 每批处理的股票数
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 5  # 重试延迟（秒）

        # 统计信息
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_symbols': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0,
            'total_downloaded': 0,
            'total_stored': 0,
            'retry_count': 0,
            'cache_hits': 0,
            'duration_seconds': 0
        }

        # 报告目录
        self.report_dir = Path('data/reports/factors')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        logger.info("✅ 批量因子处理器初始化完成")

    def _extract_symbol_from_item(self, item):
        """
        从配置项中提取股票代码

        Args:
            item: 配置项（可能是字符串、字典或其他格式）

        Returns:
            标准化的股票代码
        """
        if isinstance(item, dict):
            # 字典格式：{'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38}
            if 'symbol' in item:
                symbol = item['symbol']
                return self._normalize_symbol(symbol)
            else:
                raise ValueError(f"字典中缺少symbol字段: {item}")

        elif isinstance(item, str):
            # 字符串格式："600519.SH" 或 "sh600519" 或 "600519"
            return self._normalize_symbol(item)

        else:
            raise ValueError(f"不支持的配置项格式: {type(item)}")

    def _normalize_symbol(self, symbol: str) -> str:
        """
        标准化股票代码格式

        Args:
            symbol: 原始股票代码

        Returns:
            标准化的Baostock格式股票代码
        """
        if not symbol:
            raise ValueError("股票代码为空")

        symbol = str(symbol).strip().upper()

        # 如果已经是Baostock格式（sh600519/sz000001），直接返回
        if symbol.startswith(('SH', 'SZ')):
            return symbol.lower()  # 转换为小写

        # 处理带交易所后缀的格式：600519.SH
        if '.' in symbol:
            code, exchange = symbol.split('.')
            if exchange == 'SH':
                return f'sh{code}'
            elif exchange == 'SZ':
                return f'sz{code}'
            else:
                return f'{exchange.lower()}{code}'

        # 纯数字代码，需要判断市场
        if symbol.isdigit():
            if symbol.startswith('6'):
                return f'sh{symbol}'
            elif symbol.startswith(('0', '3')):
                return f'sz{symbol}'
            else:
                raise ValueError(f"无法判断股票市场: {symbol}")

        # 其他格式，直接返回
        return symbol.lower()


    def process_symbol_list(self, symbols: List[str], mode: str = 'incremental',
                            start_date: str = None, end_date: str = None,
                            progress_callback=None) -> Dict[str, Any]:
        """
        处理股票列表

        Args:
            symbols: 股票代码列表
            mode: 更新模式 ('incremental', 'full', 'specific')
            start_date: 特定开始日期
            end_date: 特定结束日期
            progress_callback: 进度回调函数

        Returns:
            处理结果报告
        """
        self.stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_symbols': len(symbols),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0,
            'total_downloaded': 0,
            'total_stored': 0,
            'retry_count': 0,
            'cache_hits': 0,
            'duration_seconds': 0
        }

        detailed_results = []
        failed_symbols = []

        logger.info(f"🚀 开始批量处理 {len(symbols)} 只股票")
        logger.info(f"⚙️  模式: {mode}, 批次大小: {self.batch_size}")

        # 分批处理
        total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size

        for batch_num in range(total_batches):
            batch_start = batch_num * self.batch_size
            batch_end = min((batch_num + 1) * self.batch_size, len(symbols))
            batch_symbols = symbols[batch_start:batch_end]

            logger.info(f"批次 {batch_num + 1}/{total_batches}: 处理 {len(batch_symbols)} 只股票")

            # 处理当前批次
            batch_results = self._process_batch(
                batch_symbols, mode, start_date, end_date, batch_num
            )

            # 收集结果
            for result in batch_results:
                detailed_results.append(result)

                if result['status'] == 'success':
                    self.stats['successful'] += 1
                    self.stats['total_records'] += result.get('records_stored', 0)
                    self.stats['total_downloaded'] += result.get('records_downloaded', 0)
                    self.stats['total_stored'] += result.get('records_stored', 0)
                elif result['status'] == 'skipped':
                    self.stats['skipped'] += 1
                    self.stats['cache_hits'] += 1
                elif result['status'] == 'no_data':
                    self.stats['skipped'] += 1
                elif result['status'] == 'error':
                    self.stats['failed'] += 1
                    failed_symbols.append(result['symbol'])

            # 更新进度
            if progress_callback:
                progress = (batch_end / len(symbols)) * 100
                progress_callback(progress, batch_end, len(symbols))

            # 批次间延迟，避免API限制
            if batch_num < total_batches - 1:
                time.sleep(2)

        # 完成统计
        self.stats['end_time'] = datetime.now()
        self.stats['duration_seconds'] = (
                self.stats['end_time'] - self.stats['start_time']
        ).total_seconds()

        # 生成报告
        report = self._generate_report(detailed_results)

        # 保存报告
        self._save_report(report)

        logger.info(f"✅ 批量处理完成")
        logger.info(f"   成功: {self.stats['successful']}, 失败: {self.stats['failed']}, 跳过: {self.stats['skipped']}")
        logger.info(f"   下载记录: {self.stats['total_downloaded']}, 存储记录: {self.stats['total_stored']}")
        logger.info(f"   耗时: {self.stats['duration_seconds']:.2f}秒")

        return report

    def _process_batch(self, symbols: List[Any], mode: str,
                       start_date: str, end_date: str, batch_num: int) -> List[Dict]:
        """
        处理单个批次
        """
        batch_results = []

        for i, item in enumerate(symbols, 1):
            try:
                # 使用A50修复器处理符号
                try:
                    normalized_symbol = A50SymbolFixer.fix_symbol(item)
                    symbol_info = A50SymbolFixer.extract_symbol_info(item)
                except Exception as e:
                    logger.error(f"符号处理失败 {item}: {e}")
                    batch_results.append({
                        'symbol': str(item),
                        'status': 'error',
                        'error': f'符号处理失败: {e}',
                        'retry_count': 0
                    })
                    continue

                logger.debug(f"[批次{batch_num + 1}] 处理 {normalized_symbol} ({i}/{len(symbols)})")

                # 处理股票
                result = self._process_single_symbol_with_retry(
                    normalized_symbol, mode, start_date, end_date
                )

                # 添加符号信息
                result['symbol_info'] = symbol_info
                result['original_item'] = str(item)

                batch_results.append(result)

                # 延迟，避免API限制
                if i < len(symbols):
                    time.sleep(1.5)

            except Exception as e:
                logger.error(f"处理股票失败 {item}: {e}")
                batch_results.append({
                    'symbol': str(item),
                    'status': 'error',
                    'error': str(e),
                    'retry_count': 0
                })

        return batch_results

    def _process_single_symbol_with_retry(self, symbol: str, mode: str,
                                          start_date: str, end_date: str) -> Dict:
        """
        带重试的处理单只股票

        Args:
            symbol: 股票代码
            mode: 更新模式
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            处理结果
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                result = self._process_single_symbol(symbol, mode, start_date, end_date)

                if result['status'] != 'error':
                    if attempt > 0:
                        self.stats['retry_count'] += 1
                        logger.info(f"  {symbol}: 第{attempt + 1}次重试成功")
                    return result

            except Exception as e:
                last_error = e
                logger.warning(f"  {symbol}: 第{attempt + 1}次尝试失败: {e}")

                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (attempt + 1)
                    logger.info(f"  {symbol}: {delay}秒后重试...")
                    time.sleep(delay)

        # 所有重试都失败
        logger.error(f"  {symbol}: 所有重试失败，最终错误: {last_error}")
        return {
            'symbol': symbol,
            'status': 'error',
            'error': str(last_error),
            'retry_count': self.max_retries
        }

    # def _process_single_symbol(self, symbol: str, mode: str,
    #                            start_date: str, end_date: str) -> Dict:
    #     """
    #     处理单只股票
    #
    #     Args:
    #         symbol: 股票代码
    #         mode: 更新模式
    #         start_date: 开始日期
    #         end_date: 结束日期
    #
    #     Returns:
    #         处理结果
    #     """
    #     result = {
    #         'symbol': symbol,
    #         'mode': mode,
    #         'status': 'pending',
    #         'records_downloaded': 0,
    #         'records_stored': 0,
    #         'error': None,
    #         'execution_time': 0,
    #         'retry_count': 0
    #     }
    #
    #     start_time = time.time()
    #
    #     try:
    #         # 1. 检查是否需要更新（增量模式）
    #         if mode == 'incremental':
    #             start_date, end_date = self.storage.calculate_incremental_range(symbol)
    #             if not start_date or not end_date:
    #                 result['status'] = 'skipped'
    #                 result['reason'] = '数据已最新'
    #                 result['execution_time'] = time.time() - start_time
    #                 return result
    #
    #         # 2. 调整日期范围
    #         if start_date and end_date:
    #             start_date, end_date = self._adjust_date_range(start_date, end_date)
    #             logger.debug(f"  {symbol}: 下载范围 {start_date} - {end_date}")
    #
    #         # 3. 下载数据
    #         logger.debug(f"  {symbol}: 开始下载")
    #         factor_data = self.downloader.fetch_factor_data(symbol, start_date, end_date)
    #
    #         if factor_data.empty:
    #             result['status'] = 'no_data'
    #             result['reason'] = '无数据'
    #             result['execution_time'] = time.time() - start_time
    #             return result
    #
    #         result['records_downloaded'] = len(factor_data)
    #         logger.debug(f"  {symbol}: 下载 {len(factor_data)} 条记录")
    #
    #         # 4. 存储数据
    #         logger.debug(f"  {symbol}: 开始存储")
    #         affected_rows, storage_report = self.storage.store_factor_data(factor_data)
    #
    #         result['records_stored'] = affected_rows
    #         result['storage_report'] = storage_report
    #
    #         if affected_rows > 0:
    #             result['status'] = 'success'
    #             logger.debug(f"  {symbol}: 存储 {affected_rows} 条记录")
    #         else:
    #             result['status'] = 'skipped'
    #             result['reason'] = '数据已存在'
    #             logger.debug(f"  {symbol}: 无新记录")
    #
    #         # 5. 清理缓存
    #         self.storage.clear_cache(symbol)
    #
    #     except Exception as e:
    #         result['status'] = 'error'
    #         result['error'] = str(e)
    #         logger.error(f"  {symbol}: 处理失败: {e}")
    #
    #     finally:
    #         result['execution_time'] = time.time() - start_time
    #
    #     return result

    # 在_factor_batch_processor.py中修改_process_single_symbol方法：

    def _process_single_symbol(self, symbol: str, mode: str,
                               start_date: str, end_date: str) -> Dict:
        """
        修复的单只股票处理 - 修复日期范围问题
        """
        result = {
            'symbol': symbol,
            'mode': mode,
            'status': 'pending',
            'records_downloaded': 0,
            'records_stored': 0,
            'error': None,
            'execution_time': 0,
            'retry_count': 0
        }

        start_time = time.time()

        try:
            # 修复：确保在full模式下有正确的日期范围
            if mode == 'full':
                # 如果没有提供日期，使用默认范围
                if not start_date:
                    start_date = '20050101'  # 从2005年开始
                if not end_date:
                    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # 到昨天

            # 1. 检查是否需要更新（增量模式）
            if mode == 'incremental':
                start_date, end_date = self.storage.calculate_incremental_range(symbol)
                if not start_date or not end_date:
                    result['status'] = 'skipped'
                    result['reason'] = '数据已最新'
                    result['execution_time'] = time.time() - start_time
                    return result

            # 2. 调整日期范围
            if start_date and end_date:
                start_date, end_date = self._adjust_date_range(start_date, end_date)
                logger.info(f"  {symbol}: 下载范围 {start_date} - {end_date}")  # 改为info级别

            # 3. 下载数据
            logger.info(f"  {symbol}: 开始下载")  # 改为info级别
            factor_data = self.downloader.fetch_factor_data(symbol, start_date, end_date)

            if factor_data.empty:
                result['status'] = 'no_data'
                result['reason'] = '无数据'
                result['execution_time'] = time.time() - start_time
                logger.warning(f"  {symbol}: 无数据")  # 改为warning级别
                return result

            result['records_downloaded'] = len(factor_data)
            logger.info(f"  {symbol}: 下载 {len(factor_data)} 条记录")

            # 4. 存储数据
            logger.debug(f"  {symbol}: 开始存储")
            affected_rows, storage_report = self.storage.store_factor_data(factor_data)

            result['records_stored'] = affected_rows
            result['storage_report'] = storage_report

            if affected_rows > 0:
                result['status'] = 'success'
                logger.info(f"  {symbol}: 存储 {affected_rows} 条记录")
            else:
                result['status'] = 'skipped'
                result['reason'] = '数据已存在'
                logger.info(f"  {symbol}: 无新记录")

            # 5. 清理缓存
            self.storage.clear_cache(symbol)

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"  {symbol}: 处理失败: {e}")

        finally:
            result['execution_time'] = time.time() - start_time

        return result


    def _adjust_date_range(self, start_date: str, end_date: str) -> Tuple[str, str]:
        """
        调整日期范围为交易日
        """
        try:
            # 调整开始日期
            if hasattr(self.trade_date_manager, 'adjust_to_trade_date'):
                adjusted_start = self.trade_date_manager.adjust_to_trade_date(start_date, 'backward')
                if adjusted_start != start_date:
                    start_date = adjusted_start

            # 调整结束日期
            if hasattr(self.trade_date_manager, 'adjust_to_trade_date'):
                adjusted_end = self.trade_date_manager.adjust_to_trade_date(end_date, 'backward')
                if adjusted_end != end_date:
                    end_date = adjusted_end

            return start_date, end_date

        except Exception as e:
            logger.warning(f"日期范围调整失败: {e}")
            return start_date, end_date

    def _generate_report(self, detailed_results: List[Dict]) -> Dict[str, Any]:
        """
        生成详细报告
        """
        # 计算成功率
        success_rate = (self.stats['successful'] / self.stats['total_symbols'] * 100
                        if self.stats['total_symbols'] > 0 else 0)

        # 计算平均处理时间
        avg_execution_time = np.mean([
            r.get('execution_time', 0) for r in detailed_results
            if r.get('execution_time', 0) > 0
        ]) if detailed_results else 0

        report = {
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'summary': {
                'total_symbols': self.stats['total_symbols'],
                'successful': self.stats['successful'],
                'failed': self.stats['failed'],
                'skipped': self.stats['skipped'],
                'success_rate': round(success_rate, 2),
                'total_records': self.stats['total_records'],
                'total_downloaded': self.stats['total_downloaded'],
                'total_stored': self.stats['total_stored'],
                'cache_hits': self.stats['cache_hits'],
                'retry_count': self.stats['retry_count']
            },
            'performance': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': self.stats['end_time'].isoformat(),
                'duration_seconds': round(self.stats['duration_seconds'], 2),
                'avg_execution_time_per_symbol': round(avg_execution_time, 2),
                'symbols_per_second': round(self.stats['total_symbols'] / self.stats['duration_seconds'], 3)
                if self.stats['duration_seconds'] > 0 else 0,
                'records_per_second': round(self.stats['total_records'] / self.stats['duration_seconds'], 2)
                if self.stats['duration_seconds'] > 0 else 0
            },
            'configuration': {
                'batch_size': self.batch_size,
                'max_retries': self.max_retries,
                'retry_delay': self.retry_delay
            },
            'detailed_results': detailed_results,
            'failed_symbols': [
                r['symbol'] for r in detailed_results if r['status'] == 'error'
            ],
            'successful_symbols': [
                r['symbol'] for r in detailed_results if r['status'] == 'success'
            ]
        }

        return report

    def _save_report(self, report: Dict):
        """
        保存报告到文件
        """
        try:
            report_file = self.report_dir / f"{report['batch_id']}.json"

            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"📄 报告已保存: {report_file}")

            # 同时保存简版文本报告
            self._save_text_report(report, report_file.with_suffix('.txt'))

        except Exception as e:
            logger.error(f"保存报告失败: {e}")

    def _save_text_report(self, report: Dict, filepath: Path):
        """
        保存文本格式报告
        """
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("📊 PB因子批量处理报告\n")
                f.write("=" * 60 + "\n\n")

                # 汇总信息
                summary = report['summary']
                f.write("汇总统计:\n")
                f.write("-" * 40 + "\n")
                f.write(f"总股票数: {summary['total_symbols']}\n")
                f.write(f"成功: {summary['successful']}\n")
                f.write(f"失败: {summary['failed']}\n")
                f.write(f"跳过: {summary['skipped']}\n")
                f.write(f"成功率: {summary['success_rate']}%\n")
                f.write(f"总记录数: {summary['total_records']:,}\n")
                f.write(f"下载记录: {summary['total_downloaded']:,}\n")
                f.write(f"存储记录: {summary['total_stored']:,}\n")
                f.write(f"缓存命中: {summary['cache_hits']}\n")
                f.write(f"重试次数: {summary['retry_count']}\n\n")

                # 性能信息
                perf = report['performance']
                f.write("性能统计:\n")
                f.write("-" * 40 + "\n")
                f.write(f"开始时间: {perf['start_time']}\n")
                f.write(f"结束时间: {perf['end_time']}\n")
                f.write(f"总耗时: {perf['duration_seconds']}秒\n")
                f.write(f"平均处理时间: {perf['avg_execution_time_per_symbol']}秒/只\n")
                f.write(f"处理速度: {perf['symbols_per_second']}只/秒\n")
                f.write(f"记录速度: {perf['records_per_second']}条/秒\n\n")

                # 失败股票列表
                failed_symbols = report.get('failed_symbols', [])
                if failed_symbols:
                    f.write("失败股票列表:\n")
                    f.write("-" * 40 + "\n")
                    for symbol in failed_symbols[:20]:  # 最多显示20个
                        f.write(f"  {symbol}\n")
                    if len(failed_symbols) > 20:
                        f.write(f"  还有 {len(failed_symbols) - 20} 只...\n")
                    f.write("\n")

                # 成功股票示例
                successful_symbols = report.get('successful_symbols', [])
                if successful_symbols:
                    f.write("成功股票示例 (前10只):\n")
                    f.write("-" * 40 + "\n")
                    for symbol in successful_symbols[:10]:
                        # 找到该股票的详细结果
                        for detail in report['detailed_results']:
                            if detail.get('symbol') == symbol and detail.get('status') == 'success':
                                records = detail.get('records_stored', 0)
                                f.write(f"  {symbol}: {records} 条记录\n")
                                break
                    f.write("\n")

                f.write("=" * 60 + "\n")
                f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n")

            logger.debug(f"文本报告已保存: {filepath}")

        except Exception as e:
            logger.warning(f"保存文本报告失败: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息"""
        return self.stats.copy()

    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_symbols': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0,
            'total_downloaded': 0,
            'total_stored': 0,
            'retry_count': 0,
            'cache_hits': 0,
            'duration_seconds': 0
        }

    def cleanup(self):
        """清理资源"""
        try:
            self.downloader.logout()
            logger.info("批量处理器清理完成")
        except Exception as e:
            logger.warning(f"清理资源异常: {e}")


# 在_factor_batch_processor.py中添加以下修复

class FixedFactorBatchProcessor(FactorBatchProcessor):
    """修复的批量处理器 - 支持强制下载"""

    def __init__(self, config_path: str = 'config/database.yaml', force_download: bool = False):
        """
        初始化，添加force_download参数

        Args:
            force_download: 是否强制重新下载（忽略最后更新检查）
        """
        super().__init__(config_path)
        self.force_download = force_download
        logger.info(f"批量处理器初始化完成，强制下载模式: {force_download}")

    def _process_single_symbol(self, symbol: str, mode: str,
                               start_date: str, end_date: str) -> Dict:
        """
        修复的单只股票处理 - 支持强制下载
        """
        result = {
            'symbol': symbol,
            'mode': mode,
            'status': 'pending',
            'records_downloaded': 0,
            'records_stored': 0,
            'error': None,
            'execution_time': 0,
            'retry_count': 0
        }

        start_time = time.time()

        try:
            # 1. 检查是否需要更新（增量模式）- 修复逻辑
            if mode == 'incremental' and not self.force_download:
                # 使用改进的增量范围计算
                start_date, end_date = self.storage.calculate_improved_incremental_range(symbol)
                if not start_date or not end_date:
                    # 即使数据已最新，也检查数据完整性
                    if self._should_force_refresh(symbol):
                        logger.info(f"  {symbol}: 数据完整性检查失败，强制刷新")
                        start_date = '20050101'  # 从2005年开始
                        end_date = datetime.now().strftime('%Y%m%d')
                    else:
                        result['status'] = 'skipped'
                        result['reason'] = '数据已最新'
                        result['execution_time'] = time.time() - start_time
                        logger.info(f"  {symbol}: 数据已最新，跳过")
                        return result

            # 2. 如果是强制下载或全量模式
            elif mode == 'full' or self.force_download:
                start_date = start_date or '20050101'
                end_date = end_date or datetime.now().strftime('%Y%m%d')
                logger.info(f"  {symbol}: 强制下载模式，范围 {start_date} - {end_date}")

            # 3. 调整日期范围
            if start_date and end_date:
                start_date, end_date = self._adjust_date_range(start_date, end_date)
                logger.debug(f"  {symbol}: 下载范围 {start_date} - {end_date}")

            # 4. 下载数据
            logger.debug(f"  {symbol}: 开始下载")
            factor_data = self.downloader.fetch_factor_data(symbol, start_date, end_date)

            if factor_data.empty:
                result['status'] = 'no_data'
                result['reason'] = '无数据'
                result['execution_time'] = time.time() - start_time
                logger.warning(f"  {symbol}: 无数据")
                return result

            result['records_downloaded'] = len(factor_data)
            logger.info(f"  {symbol}: 下载 {len(factor_data)} 条记录")

            # 5. 存储数据
            logger.debug(f"  {symbol}: 开始存储")
            affected_rows, storage_report = self.storage.store_factor_data(factor_data)

            result['records_stored'] = affected_rows
            result['storage_report'] = storage_report

            if affected_rows > 0:
                result['status'] = 'success'
                logger.info(f"  {symbol}: 存储 {affected_rows} 条记录")
            else:
                # 检查为什么没有新记录
                existing_count = self._check_existing_data(symbol)
                if existing_count > 0:
                    result['status'] = 'skipped'
                    result['reason'] = '数据已存在'
                    logger.info(f"  {symbol}: 无新记录（数据库中已有 {existing_count} 条）")
                else:
                    result['status'] = 'error'
                    result['error'] = '下载成功但存储失败'
                    logger.error(f"  {symbol}: 下载成功但存储0条记录")

            # 6. 清理缓存
            self.storage.clear_cache(symbol)

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"  {symbol}: 处理失败: {e}")

        finally:
            result['execution_time'] = time.time() - start_time

        return result

    def _should_force_refresh(self, symbol: str) -> bool:
        """
        检查是否需要强制刷新数据

        Returns:
            True: 需要强制刷新
            False: 数据完整，无需刷新
        """
        try:
            # 1. 检查数据库中是否有数据
            data_exists = self._check_data_exists(symbol)
            if not data_exists:
                logger.info(f"  {symbol}: 数据库中无数据，需要下载")
                return True

            # 2. 检查数据完整性（是否有PB、PE等关键因子）
            factor_complete = self._check_factor_completeness(symbol)
            if not factor_complete:
                logger.warning(f"  {symbol}: 因子数据不完整，需要刷新")
                return True

            # 3. 检查最后更新日期是否太旧
            last_date = self.storage.get_last_factor_date(symbol)
            if last_date:
                last_dt = datetime.strptime(str(last_date), '%Y-%m-%d')
                today = datetime.now().date()
                days_diff = (today - last_dt.date()).days

                # 如果超过7天没有更新，强制刷新
                if days_diff > 7:
                    logger.info(f"  {symbol}: 最后更新于 {last_date}，已超过{days_diff}天，需要刷新")
                    return True

            return False

        except Exception as e:
            logger.warning(f"检查强制刷新条件失败 {symbol}: {e}")
            return True  # 检查失败时默认强制刷新

    def _check_data_exists(self, symbol: str) -> bool:
        """检查数据库中是否有该股票的数据"""
        try:
            with self.storage.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s",
                        (symbol.replace('.', ''),)
                    )
                    count = cursor.fetchone()[0]
                    return count > 0
        except Exception as e:
            logger.warning(f"检查数据存在失败 {symbol}: {e}")
            return False

    def _check_factor_completeness(self, symbol: str) -> bool:
        """检查因子数据是否完整（是否有PB、PE等关键因子）"""
        try:
            with self.storage.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 检查最近10个交易日是否有PB、PE数据
                    cursor.execute("""
                        SELECT 
                            SUM(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_count,
                            SUM(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_count
                        FROM stock_daily_data 
                        WHERE symbol = %s 
                        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        LIMIT 10
                    """, (symbol.replace('.', ''),))

                    result = cursor.fetchone()
                    if result:
                        pb_count, pe_count = result
                        # 如果最近10条记录都没有PB或PE数据，说明不完整
                        if pb_count == 0 or pe_count == 0:
                            return False

                    return True
        except Exception as e:
            logger.warning(f"检查因子完整性失败 {symbol}: {e}")
            return False

    def _check_existing_data(self, symbol: str) -> int:
        """检查数据库中已有数据数量"""
        try:
            clean_symbol = symbol.replace('.', '')
            with self.storage.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s",
                        (clean_symbol,)
                    )
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"检查现有数据失败 {symbol}: {e}")
            return 0


# 测试函数
def test_batch_processor():
    """测试批量处理器"""
    print("\n🧪 测试批量因子处理器")
    print("=" * 50)

    try:
        # 初始化
        print("初始化FactorBatchProcessor...")
        processor = FactorBatchProcessor()
        print("✅ 初始化成功")

        # 测试股票列表
        test_symbols = ['600519', '000001', '000858', '601318', '000333']

        # 进度回调函数
        def progress_callback(progress, current, total):
            print(f"  进度: {progress:.1f}% ({current}/{total})")

        print(f"\n处理 {len(test_symbols)} 只股票...")

        # 处理股票列表
        report = processor.process_symbol_list(
            symbols=test_symbols,
            mode='incremental',
            progress_callback=progress_callback
        )

        # 显示报告摘要
        print(f"\n📋 处理报告摘要:")
        summary = report['summary']
        print(f"   成功: {summary['successful']}/{summary['total_symbols']}")
        print(f"   失败: {summary['failed']}")
        print(f"   跳过: {summary['skipped']}")
        print(f"   总记录: {summary['total_records']:,}")
        print(f"   成功率: {summary['success_rate']}%")

        # 显示性能统计
        perf = report['performance']
        print(f"\n⚡ 性能统计:")
        print(f"   总耗时: {perf['duration_seconds']}秒")
        print(f"   处理速度: {perf['symbols_per_second']:.2f}只/秒")
        print(f"   记录速度: {perf['records_per_second']:.2f}条/秒")

        # 清理
        processor.cleanup()

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_batch_processor()

    if success:
        print("\n✅ 批量因子处理器测试通过！")
    else:
        print("\n❌ 批量因子处理器测试失败")

    exit(0 if success else 1)