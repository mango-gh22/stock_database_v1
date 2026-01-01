# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/api\async_calculator.py
# File Name: async_calculator
# @ Author: mango-gh22
# @ Date：2025/12/21 19:15
"""
Desc: 异步计算器 - 支持并发指标计算
"""
import asyncio
import concurrent.futures
from typing import Dict, List, Optional, Any, Tuple, Callable
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import time
from dataclasses import dataclass
from enum import Enum
import json
import hashlib

from src.indicators.indicator_manager import IndicatorManager
from src.query.data_pipeline import DataPipeline, DataQualityReport
from src.query.result_formatter import ResultFormatter

logger = logging.getLogger(__name__)


class CalculationStatus(Enum):
    """计算状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CalculationTask:
    """计算任务"""
    task_id: str
    symbol: str
    indicators: List[str]
    start_date: str
    end_date: str
    parameters: Dict[str, Dict]
    status: CalculationStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0
    result: Optional[Dict] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'task_id': self.task_id,
            'symbol': self.symbol,
            'indicators': self.indicators,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'status': self.status.value,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'progress': self.progress,
            'result_available': self.result is not None,
            'error': self.error
        }


class AsyncIndicatorCalculator:
    """异步指标计算器"""

    def __init__(self,
                 max_workers: int = 4,
                 cache_enabled: bool = True,
                 timeout: int = 300):
        """
        初始化异步计算器

        Args:
            max_workers: 最大工作线程数
            cache_enabled: 是否启用缓存
            timeout: 任务超时时间（秒）
        """
        self.max_workers = max_workers
        self.cache_enabled = cache_enabled
        self.timeout = timeout

        # 核心组件
        self.indicator_manager = IndicatorManager()
        self.data_pipeline = DataPipeline()
        self.result_formatter = ResultFormatter()

        # 任务管理
        self.tasks: Dict[str, CalculationTask] = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

        # 缓存结果（短期）
        self.result_cache: Dict[str, Tuple[datetime, Dict]] = {}
        self.cache_ttl = timedelta(minutes=30)

        # 统计信息
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'active_tasks': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }

        logger.info(f"初始化异步计算器，最大线程数: {max_workers}, 缓存: {cache_enabled}")

    def _generate_task_id(self, symbol: str, indicators: List[str],
                          start_date: str, end_date: str,
                          parameters: Dict) -> str:
        """生成任务ID"""
        task_str = f"{symbol}_{json.dumps(indicators, sort_keys=True)}_\
                    {start_date}_{end_date}_{json.dumps(parameters, sort_keys=True)}"
        return hashlib.md5(task_str.encode()).hexdigest()[:12]

    def _get_cached_result(self, task_id: str) -> Optional[Dict]:
        """获取缓存结果"""
        if not self.cache_enabled:
            return None

        if task_id in self.result_cache:
            cached_time, result = self.result_cache[task_id]
            if datetime.now() - cached_time < self.cache_ttl:
                self.stats['cache_hits'] += 1
                logger.debug(f"缓存命中: {task_id}")
                return result

        self.stats['cache_misses'] += 1
        return None

    def _set_cached_result(self, task_id: str, result: Dict):
        """设置缓存结果"""
        if self.cache_enabled:
            self.result_cache[task_id] = (datetime.now(), result)

            # 清理过期缓存
            self._cleanup_expired_cache()

    def _cleanup_expired_cache(self):
        """清理过期缓存"""
        current_time = datetime.now()
        expired_keys = []

        for task_id, (cached_time, _) in self.result_cache.items():
            if current_time - cached_time > self.cache_ttl:
                expired_keys.append(task_id)

        for key in expired_keys:
            self.result_cache.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    async def calculate_async(self, symbol: str,
                              indicators: List[str],
                              start_date: str,
                              end_date: str,
                              parameters: Optional[Dict[str, Dict]] = None,
                              use_cache: bool = True) -> str:
        """
        异步计算指标

        Args:
            symbol: 股票代码
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            parameters: 指标参数
            use_cache: 是否使用缓存

        Returns:
            任务ID
        """
        if parameters is None:
            parameters = {}

        # 生成任务ID
        task_id = self._generate_task_id(symbol, indicators, start_date, end_date, parameters)

        # 检查缓存
        if use_cache:
            cached_result = self._get_cached_result(task_id)
            if cached_result is not None:
                # 创建已完成的任务
                task = CalculationTask(
                    task_id=task_id,
                    symbol=symbol,
                    indicators=indicators,
                    start_date=start_date,
                    end_date=end_date,
                    parameters=parameters,
                    status=CalculationStatus.COMPLETED,
                    created_at=datetime.now(),
                    started_at=datetime.now(),
                    completed_at=datetime.now(),
                    progress=1.0,
                    result=cached_result
                )
                self.tasks[task_id] = task
                return task_id

        # 创建新任务
        task = CalculationTask(
            task_id=task_id,
            symbol=symbol,
            indicators=indicators,
            start_date=start_date,
            end_date=end_date,
            parameters=parameters,
            status=CalculationStatus.PENDING,
            created_at=datetime.now()
        )

        self.tasks[task_id] = task
        self.stats['total_tasks'] += 1
        self.stats['active_tasks'] += 1

        # 提交到线程池
        future = self.executor.submit(
            self._calculate_sync,
            task_id,
            symbol,
            indicators,
            start_date,
            end_date,
            parameters,
            use_cache
        )

        # 设置超时
        future.add_done_callback(
            lambda f: self._handle_task_completion(task_id, f)
        )

        logger.info(f"创建异步计算任务: {task_id}, 符号: {symbol}, 指标: {indicators}")

        return task_id

    def _calculate_sync(self, task_id: str,
                        symbol: str,
                        indicators: List[str],
                        start_date: str,
                        end_date: str,
                        parameters: Dict[str, Dict],
                        use_cache: bool) -> Dict[str, Any]:
        """
        同步计算（在后台线程中运行）

        Args:
            同 calculate_async

        Returns:
            计算结果
        """
        task = self.tasks.get(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")

        try:
            # 更新任务状态
            task.status = CalculationStatus.PROCESSING
            task.started_at = datetime.now()

            logger.info(f"开始计算任务: {task_id}")

            # 获取数据（这里需要实际的数据源，简化处理）
            # 在实际系统中，这里应该调用 query_engine
            from unittest.mock import MagicMock
            import sys
            from pathlib import Path

            # 创建模拟数据
            dates = pd.date_range(start_date, end_date, freq='D')
            test_df = pd.DataFrame({
                'trade_date': dates,
                'symbol': [symbol] * len(dates),
                'open_price': np.random.randn(len(dates)).cumsum() + 100,
                'high_price': np.random.randn(len(dates)).cumsum() + 105,
                'low_price': np.random.randn(len(dates)).cumsum() + 95,
                'close_price': np.random.randn(len(dates)).cumsum() + 100,
                'volume': np.random.randint(1000, 10000, len(dates)),
                'amount': np.random.randint(100000, 1000000, len(dates))
            })

            # 更新进度
            task.progress = 0.3

            # 数据预处理
            processed_df, quality_report = self.data_pipeline.process(
                test_df, symbol, start_date, end_date, use_cache=False
            )

            task.progress = 0.5

            # 计算指标
            results = {}
            total_indicators = len(indicators)

            for i, indicator in enumerate(indicators):
                try:
                    # 获取指标参数
                    indicator_params = parameters.get(indicator, {})

                    # 计算指标
                    result_df = self.indicator_manager.calculate_single(
                        symbol=symbol,
                        indicator_name=indicator,
                        start_date=start_date,
                        end_date=end_date,
                        **indicator_params
                    )

                    if result_df is not None:
                        results[indicator] = result_df

                    # 更新进度
                    task.progress = 0.5 + (i + 1) / total_indicators * 0.4

                except Exception as e:
                    logger.error(f"计算指标 {indicator} 失败: {e}")
                    # 继续计算其他指标

            task.progress = 0.9

            # 格式化结果
            formatted_result = self.result_formatter.format_indicator_results(
                results, symbol, start_date, end_date
            )

            # 添加质量报告
            formatted_result['quality_report'] = {
                'score': quality_report.quality_score,
                'level': quality_report.quality_level.value,
                'suggestions': quality_report.suggestions,
                'processed_rows': quality_report.processed_rows
            }

            task.progress = 1.0
            task.result = formatted_result
            task.status = CalculationStatus.COMPLETED

            # 缓存结果
            if use_cache:
                self._set_cached_result(task_id, formatted_result)

            logger.info(f"计算任务完成: {task_id}")

            return formatted_result

        except Exception as e:
            logger.error(f"计算任务失败 {task_id}: {e}")
            task.status = CalculationStatus.FAILED
            task.error = str(e)
            raise

    def _handle_task_completion(self, task_id: str, future: concurrent.futures.Future):
        """处理任务完成"""
        task = self.tasks.get(task_id)
        if not task:
            return

        try:
            task.completed_at = datetime.now()

            if future.exception():
                task.status = CalculationStatus.FAILED
                task.error = str(future.exception())
                self.stats['failed_tasks'] += 1
            else:
                task.status = CalculationStatus.COMPLETED
                self.stats['completed_tasks'] += 1

            self.stats['active_tasks'] -= 1

            logger.info(f"任务处理完成: {task_id}, 状态: {task.status.value}")

        except Exception as e:
            logger.error(f"处理任务完成回调失败 {task_id}: {e}")
            task.status = CalculationStatus.FAILED
            task.error = f"回调处理失败: {str(e)}"

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态

        Args:
            task_id: 任务ID

        Returns:
            任务状态信息
        """
        task = self.tasks.get(task_id)
        if not task:
            return None

        return task.to_dict()

    async def get_task_result(self, task_id: str, wait: bool = False,
                              timeout: int = 30) -> Optional[Dict[str, Any]]:
        """
        获取任务结果

        Args:
            task_id: 任务ID
            wait: 是否等待任务完成
            timeout: 等待超时时间（秒）

        Returns:
            任务结果或None
        """
        task = self.tasks.get(task_id)
        if not task:
            return None

        # 如果任务已完成，直接返回结果
        if task.status == CalculationStatus.COMPLETED:
            return task.result

        # 如果任务失败，返回错误信息
        if task.status == CalculationStatus.FAILED:
            return {
                'success': False,
                'error': task.error,
                'task_id': task_id
            }

        # 如果需要等待任务完成
        if wait:
            start_time = time.time()
            while time.time() - start_time < timeout:
                if task.status in [CalculationStatus.COMPLETED, CalculationStatus.FAILED]:
                    break
                await asyncio.sleep(0.1)

        if task.status == CalculationStatus.COMPLETED:
            return task.result
        elif task.status == CalculationStatus.FAILED:
            return {
                'success': False,
                'error': task.error,
                'task_id': task_id
            }
        else:
            return {
                'success': False,
                'error': f"任务仍在处理中，状态: {task.status.value}",
                'task_id': task_id,
                'progress': task.progress
            }

    async def batch_calculate(self, symbols: List[str],
                              indicators: List[str],
                              start_date: str,
                              end_date: str,
                              parameters: Optional[Dict[str, Dict]] = None,
                              max_concurrent: int = 5) -> Dict[str, List[str]]:
        """
        批量计算（多股票）

        Args:
            symbols: 股票代码列表
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            parameters: 指标参数
            max_concurrent: 最大并发数

        Returns:
            {symbol: [task_ids]} 字典
        """
        if parameters is None:
            parameters = {}

        logger.info(f"开始批量计算: {len(symbols)} 只股票, {len(indicators)} 个指标")

        # 控制并发数量
        semaphore = asyncio.Semaphore(max_concurrent)
        tasks = {}

        async def calculate_with_limit(symbol):
            async with semaphore:
                task_id = await self.calculate_async(
                    symbol, indicators, start_date, end_date, parameters
                )
                return symbol, task_id

        # 创建所有任务
        tasks_list = [calculate_with_limit(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks_list, return_exceptions=True)

        # 处理结果
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"批量计算任务创建失败: {result}")
                continue

            symbol, task_id = result
            if symbol not in tasks:
                tasks[symbol] = []
            tasks[symbol].append(task_id)

        logger.info(f"批量计算任务创建完成，共 {len(tasks)} 只股票")

        return tasks

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        status_counts = {
            status.value: 0 for status in CalculationStatus
        }

        for task in self.tasks.values():
            status_counts[task.status.value] += 1

        return {
            'tasks': {
                'total': self.stats['total_tasks'],
                'by_status': status_counts,
                'active': self.stats['active_tasks']
            },
            'cache': {
                'hits': self.stats['cache_hits'],
                'misses': self.stats['cache_misses'],
                'hit_rate': (self.stats['cache_hits'] /
                             (self.stats['cache_hits'] + self.stats['cache_misses'])
                             if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0),
                'cached_results': len(self.result_cache)
            },
            'performance': {
                'max_workers': self.max_workers,
                'timeout': self.timeout
            }
        }

    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        task = self.tasks.get(task_id)
        if not task:
            return False

        if task.status in [CalculationStatus.COMPLETED, CalculationStatus.FAILED]:
            return False

        task.status = CalculationStatus.CANCELLED
        task.completed_at = datetime.now()
        self.stats['active_tasks'] -= 1

        logger.info(f"取消任务: {task_id}")

        return True

    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """
        清理旧任务

        Args:
            max_age_hours: 最大保留时间（小时）
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        old_task_ids = []

        for task_id, task in self.tasks.items():
            if task.created_at < cutoff_time:
                old_task_ids.append(task_id)

        for task_id in old_task_ids:
            self.tasks.pop(task_id, None)

        if old_task_ids:
            logger.info(f"清理了 {len(old_task_ids)} 个旧任务")

    async def shutdown(self):
        """关闭计算器"""
        logger.info("关闭异步计算器...")

        # 取消所有未完成的任务
        for task_id, task in self.tasks.items():
            if task.status in [CalculationStatus.PENDING, CalculationStatus.PROCESSING]:
                self.cancel_task(task_id)

        # 关闭执行器
        self.executor.shutdown(wait=True)

        logger.info("异步计算器已关闭")


class AsyncBatchProcessor:
    """异步批处理器（高级功能）"""

    def __init__(self, calculator: AsyncIndicatorCalculator):
        self.calculator = calculator

    async def process_batch_with_callback(self, symbols: List[str],
                                          indicators: List[str],
                                          start_date: str,
                                          end_date: str,
                                          callback: Callable[[str, Dict], None],
                                          batch_size: int = 10):
        """
        带回调的批量处理

        Args:
            symbols: 股票代码列表
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            callback: 回调函数 (symbol, result) -> None
            batch_size: 批次大小
        """
        total_symbols = len(symbols)

        for i in range(0, total_symbols, batch_size):
            batch = symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_symbols + batch_size - 1) // batch_size

            logger.info(f"处理批次 {batch_num}/{total_batches}, 大小: {len(batch)}")

            # 创建批处理任务
            tasks_dict = await self.calculator.batch_calculate(
                batch, indicators, start_date, end_date
            )

            # 等待并处理结果
            for symbol, task_ids in tasks_dict.items():
                for task_id in task_ids:
                    try:
                        result = await self.calculator.get_task_result(
                            task_id, wait=True, timeout=120
                        )

                        if result and result.get('success', False):
                            callback(symbol, result)
                        else:
                            logger.warning(f"处理 {symbol} 失败: {result.get('error', '未知错误')}")

                    except Exception as e:
                        logger.error(f"处理 {symbol} 回调失败: {e}")

            logger.info(f"批次 {batch_num} 处理完成")

    async def process_with_progress(self, symbols: List[str],
                                    indicators: List[str],
                                    start_date: str,
                                    end_date: str,
                                    progress_callback: Callable[[int, int, float], None]):
        """
        带进度报告的批量处理

        Args:
            symbols: 股票代码列表
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            progress_callback: 进度回调 (completed, total, percentage) -> None
        """
        total_symbols = len(symbols)
        completed = 0

        # 创建所有任务
        tasks_dict = await self.calculator.batch_calculate(
            symbols, indicators, start_date, end_date
        )

        all_task_ids = []
        for task_ids in tasks_dict.values():
            all_task_ids.extend(task_ids)

        # 监控进度
        while completed < total_symbols:
            await asyncio.sleep(1)

            # 检查已完成的任务
            new_completed = 0
            for task_id in all_task_ids:
                status = await self.calculator.get_task_status(task_id)
                if status and status['status'] in ['completed', 'failed', 'cancelled']:
                    new_completed += 1

            if new_completed > completed:
                completed = new_completed
                percentage = (completed / total_symbols) * 100

                # 调用进度回调
                progress_callback(completed, total_symbols, percentage)

                logger.info(f"进度: {completed}/{total_symbols} ({percentage:.1f}%)")