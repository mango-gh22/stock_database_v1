# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\parallel_calculator.py
# File Name: parallel_calculator
# @ Author: mango-gh22
# @ Date：2025/12/21 21:17
"""
desc 
"""

"""
File: src/performance/parallel_calculator.py
Desc: 并行计算器 - 高性能并行指标计算
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
import multiprocessing
from multiprocessing import Pool, cpu_count
import threading
from queue import Queue, Empty
import hashlib
import json

logger = logging.getLogger(__name__)


class ParallelMode(Enum):
    """并行模式"""
    THREAD = "thread"  # 线程并行（I/O密集型）
    PROCESS = "process"  # 进程并行（CPU密集型）
    ASYNC = "async"  # 异步并行（混合型）


@dataclass
class ParallelTask:
    """并行任务"""
    task_id: str
    symbol: str
    indicator: str
    parameters: Dict
    data: pd.DataFrame
    mode: ParallelMode
    priority: int = 1
    created_at: datetime = None
    result: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class ParallelCalculator:
    """并行计算器"""

    def __init__(self,
                 max_workers: Optional[int] = None,
                 mode: ParallelMode = ParallelMode.THREAD,
                 cache_enabled: bool = True,
                 timeout: int = 300):
        """
        初始化并行计算器

        Args:
            max_workers: 最大工作线程/进程数
            mode: 并行模式
            cache_enabled: 是否启用缓存
            timeout: 任务超时时间（秒）
        """
        self.mode = mode
        self.cache_enabled = cache_enabled
        self.timeout = timeout

        # 设置工作线程数
        if max_workers is None:
            if mode == ParallelMode.PROCESS:
                self.max_workers = max(1, cpu_count() - 1)
            else:
                self.max_workers = min(32, cpu_count() * 4)
        else:
            self.max_workers = max_workers

        # 执行器
        self.executor = None
        self._init_executor()

        # 任务队列
        self.task_queue = Queue()
        self.completed_tasks: Dict[str, ParallelTask] = {}
        self.running_tasks: Dict[str, ParallelTask] = {}

        # 缓存
        self.cache: Dict[str, Tuple[datetime, pd.DataFrame]] = {}
        self.cache_ttl = timedelta(minutes=30)

        # 统计信息
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'cancelled_tasks': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_execution_time': 0.0,
            'avg_execution_time': 0.0
        }

        # 性能监控
        self.performance_stats = {
            'throughput': 0.0,  # 任务/秒
            'utilization': 0.0,  # 资源利用率
            'queue_size': 0,  # 队列长度
            'worker_status': []  # 工作线程状态
        }

        # 启动监控线程
        self.monitor_thread = None
        self.running = False

        logger.info(f"初始化并行计算器，模式: {mode.value}, 工作数: {self.max_workers}")

    def _init_executor(self):
        """初始化执行器"""
        if self.mode == ParallelMode.PROCESS:
            self.executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=self.max_workers
            )
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            )

    def _generate_task_id(self, symbol: str, indicator: str,
                          parameters: Dict, data_hash: str) -> str:
        """生成任务ID"""
        task_str = f"{symbol}_{indicator}_{json.dumps(parameters, sort_keys=True)}_{data_hash}"
        return hashlib.md5(task_str.encode()).hexdigest()[:16]

    def _get_data_hash(self, df: pd.DataFrame) -> str:
        """生成数据哈希"""
        # 使用列名和形状作为哈希基础
        data_info = f"{list(df.columns)}_{df.shape}_{df.index[-10:].sum() if len(df) > 10 else 0}"
        return hashlib.md5(data_info.encode()).hexdigest()[:8]

    def _get_cached_result(self, task_id: str) -> Optional[pd.DataFrame]:
        """获取缓存结果"""
        if not self.cache_enabled:
            return None

        if task_id in self.cache:
            cached_time, result = self.cache[task_id]
            if datetime.now() - cached_time < self.cache_ttl:
                self.stats['cache_hits'] += 1
                logger.debug(f"缓存命中: {task_id}")
                return result

        self.stats['cache_misses'] += 1
        return None

    def _set_cached_result(self, task_id: str, result: pd.DataFrame):
        """设置缓存结果"""
        if self.cache_enabled:
            self.cache[task_id] = (datetime.now(), result)
            self._cleanup_expired_cache()

    def _cleanup_expired_cache(self):
        """清理过期缓存"""
        current_time = datetime.now()
        expired_keys = []

        for task_id, (cached_time, _) in self.cache.items():
            if current_time - cached_time > self.cache_ttl:
                expired_keys.append(task_id)

        for key in expired_keys:
            self.cache.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    def start_monitoring(self, interval: int = 5):
        """启动性能监控"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.running = True
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop,
                args=(interval,),
                daemon=True
            )
            self.monitor_thread.start()
            logger.info(f"启动性能监控，间隔: {interval}秒")

    def _monitor_loop(self, interval: int):
        """监控循环"""
        while self.running:
            try:
                self._update_performance_stats()
                time.sleep(interval)
            except Exception as e:
                logger.error(f"监控循环错误: {e}")

    def _update_performance_stats(self):
        """更新性能统计"""
        # 队列大小
        self.performance_stats['queue_size'] = self.task_queue.qsize()

        # 吞吐量（最近1分钟）
        recent_tasks = [
            t for t in self.completed_tasks.values()
            if t.created_at > datetime.now() - timedelta(minutes=1)
        ]

        if recent_tasks:
            completion_times = [
                (t.created_at, t.execution_time or 0)
                for t in recent_tasks if t.execution_time
            ]

            if completion_times:
                total_time = sum(et for _, et in completion_times)
                avg_time = total_time / len(completion_times)
                throughput = len(completion_times) / 60  # 任务/秒

                self.performance_stats['throughput'] = throughput
                self.performance_stats['avg_execution_time'] = avg_time

        # 资源利用率
        if self.executor:
            # 简单估算：运行任务数 / 最大工作数
            utilization = len(self.running_tasks) / self.max_workers
            self.performance_stats['utilization'] = min(utilization, 1.0)

    def stop_monitoring(self):
        """停止性能监控"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            logger.info("性能监控已停止")

    def submit_task(self, symbol: str, indicator: str,
                    data: pd.DataFrame, parameters: Dict = None,
                    priority: int = 1) -> str:
        """
        提交计算任务

        Args:
            symbol: 股票代码
            indicator: 指标名称
            data: 计算数据
            parameters: 指标参数
            priority: 任务优先级（1-10，越高越优先）

        Returns:
            任务ID
        """
        if parameters is None:
            parameters = {}

        # 生成任务ID
        data_hash = self._get_data_hash(data)
        task_id = self._generate_task_id(symbol, indicator, parameters, data_hash)

        # 检查缓存
        cached_result = self._get_cached_result(task_id)
        if cached_result is not None:
            # 创建已完成的任务
            task = ParallelTask(
                task_id=task_id,
                symbol=symbol,
                indicator=indicator,
                parameters=parameters,
                data=data,
                mode=self.mode,
                priority=priority,
                result=cached_result,
                execution_time=0.0
            )
            self.completed_tasks[task_id] = task
            self.stats['completed_tasks'] += 1
            return task_id

        # 检查是否已有相同任务在运行
        if task_id in self.running_tasks:
            logger.debug(f"任务已在运行: {task_id}")
            return task_id

        # 创建新任务
        task = ParallelTask(
            task_id=task_id,
            symbol=symbol,
            indicator=indicator,
            parameters=parameters,
            data=data,
            mode=self.mode,
            priority=priority
        )

        # 添加到队列
        self.task_queue.put((priority, task))
        self.stats['total_tasks'] += 1

        # 启动工作线程（如果需要）
        self._ensure_workers_running()

        logger.debug(f"提交任务: {task_id}, 指标: {indicator}, 优先级: {priority}")

        return task_id

    def _ensure_workers_running(self):
        """确保工作线程在运行"""
        # 这里简化处理，实际应该维护工作线程池
        pass

    def _worker_loop(self):
        """工作线程循环"""
        while True:
            try:
                # 获取任务（带超时）
                priority, task = self.task_queue.get(timeout=1)

                # 标记为运行中
                self.running_tasks[task.task_id] = task

                try:
                    # 执行计算
                    start_time = time.time()

                    if self.mode == ParallelMode.PROCESS:
                        result = self._calculate_in_process(task)
                    else:
                        result = self._calculate_in_thread(task)

                    execution_time = time.time() - start_time

                    # 更新任务结果
                    task.result = result
                    task.execution_time = execution_time

                    # 缓存结果
                    self._set_cached_result(task.task_id, result)

                    # 移动到已完成
                    self.completed_tasks[task.task_id] = task
                    self.running_tasks.pop(task.task_id, None)

                    # 更新统计
                    self.stats['completed_tasks'] += 1
                    self.stats['total_execution_time'] += execution_time

                    logger.debug(f"任务完成: {task.task_id}, 耗时: {execution_time:.2f}秒")

                except Exception as e:
                    logger.error(f"任务执行失败 {task.task_id}: {e}")
                    task.error = str(e)
                    self.stats['failed_tasks'] += 1

                finally:
                    self.task_queue.task_done()

            except Empty:
                # 队列为空，继续等待
                continue
            except Exception as e:
                logger.error(f"工作线程错误: {e}")

    def _calculate_in_thread(self, task: ParallelTask) -> pd.DataFrame:
        """在线程中计算"""
        # 这里应该调用实际的指标计算
        # 简化实现，返回模拟数据
        logger.debug(f"在线程中计算: {task.task_id}")

        # 模拟计算延迟
        time.sleep(0.1)

        # 返回模拟结果
        result = pd.DataFrame({
            f'{task.indicator}_result': np.random.randn(len(task.data))
        }, index=task.data.index)

        return result

    def _calculate_in_process(self, task: ParallelTask) -> pd.DataFrame:
        """在进程中计算"""
        # 进程间传递数据可能需要序列化
        # 这里简化处理
        logger.debug(f"在进程中计算: {task.task_id}")

        # 模拟计算延迟
        time.sleep(0.2)

        # 返回模拟结果
        result = pd.DataFrame({
            f'{task.indicator}_result': np.random.randn(len(task.data))
        }, index=task.data.index)

        return result

    def get_task_result(self, task_id: str, wait: bool = False,
                        timeout: int = 30) -> Optional[pd.DataFrame]:
        """
        获取任务结果

        Args:
            task_id: 任务ID
            wait: 是否等待任务完成
            timeout: 等待超时时间（秒）

        Returns:
            任务结果或None
        """
        # 检查是否已完成
        if task_id in self.completed_tasks:
            task = self.completed_tasks[task_id]
            if task.error:
                raise RuntimeError(f"任务失败: {task.error}")
            return task.result

        # 检查是否在运行中
        if task_id in self.running_tasks:
            if wait:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    if task_id in self.completed_tasks:
                        task = self.completed_tasks[task_id]
                        if task.error:
                            raise RuntimeError(f"任务失败: {task.error}")
                        return task.result
                    time.sleep(0.1)

                raise TimeoutError(f"等待任务超时: {task_id}")
            else:
                return None

        # 任务不存在
        raise KeyError(f"任务不存在: {task_id}")

    def batch_submit(self, tasks: List[Tuple[str, str, pd.DataFrame, Dict]]) -> List[str]:
        """
        批量提交任务

        Args:
            tasks: 任务列表 [(symbol, indicator, data, parameters), ...]

        Returns:
            任务ID列表
        """
        task_ids = []

        for symbol, indicator, data, parameters in tasks:
            if parameters is None:
                parameters = {}

            task_id = self.submit_task(symbol, indicator, data, parameters)
            task_ids.append(task_id)

        logger.info(f"批量提交 {len(task_ids)} 个任务")

        return task_ids

    def wait_for_completion(self, task_ids: List[str], timeout: int = 300) -> Dict[str, Any]:
        """
        等待任务完成

        Args:
            task_ids: 任务ID列表
            timeout: 总超时时间（秒）

        Returns:
            完成统计
        """
        start_time = time.time()
        completed = set()
        results = {}

        while len(completed) < len(task_ids):
            if time.time() - start_time > timeout:
                raise TimeoutError(f"等待任务完成超时，已完成: {len(completed)}/{len(task_ids)}")

            for task_id in task_ids:
                if task_id in completed:
                    continue

                try:
                    result = self.get_task_result(task_id, wait=False)
                    if result is not None:
                        results[task_id] = result
                        completed.add(task_id)
                except (KeyError, RuntimeError) as e:
                    # 任务失败或不存在
                    results[task_id] = {'error': str(e)}
                    completed.add(task_id)

            # 更新进度
            if len(completed) < len(task_ids):
                progress = len(completed) / len(task_ids) * 100
                logger.debug(f"等待进度: {progress:.1f}% ({len(completed)}/{len(task_ids)})")
                time.sleep(0.5)

        # 统计结果
        success_count = sum(1 for r in results.values() if not isinstance(r, dict) or 'error' not in r)
        failed_count = len(results) - success_count

        return {
            'total': len(task_ids),
            'completed': len(completed),
            'successful': success_count,
            'failed': failed_count,
            'success_rate': success_count / len(task_ids) if task_ids else 0,
            'results': results
        }

    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        # 检查是否在队列中
        # 这里简化处理，实际需要更复杂的队列操作
        if task_id in self.running_tasks:
            # 标记为取消
            task = self.running_tasks[task_id]
            task.error = "Cancelled by user"
            self.running_tasks.pop(task_id, None)
            self.stats['cancelled_tasks'] += 1
            logger.info(f"取消任务: {task_id}")
            return True

        return False

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        # 计算平均执行时间
        if self.stats['completed_tasks'] > 0:
            avg_time = self.stats['total_execution_time'] / self.stats['completed_tasks']
            self.stats['avg_execution_time'] = avg_time

        return {
            'tasks': self.stats.copy(),
            'performance': self.performance_stats.copy(),
            'cache': {
                'size': len(self.cache),
                'hits': self.stats['cache_hits'],
                'misses': self.stats['cache_misses'],
                'hit_rate': (
                    self.stats['cache_hits'] /
                    (self.stats['cache_hits'] + self.stats['cache_misses'])
                    if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0
                )
            },
            'system': {
                'max_workers': self.max_workers,
                'mode': self.mode.value,
                'queue_size': self.task_queue.qsize(),
                'running_tasks': len(self.running_tasks),
                'completed_tasks': len(self.completed_tasks)
            }
        }

    def clear_cache(self):
        """清理缓存"""
        self.cache.clear()
        logger.info("清理所有缓存")

    def reset_statistics(self):
        """重置统计信息"""
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'cancelled_tasks': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_execution_time': 0.0,
            'avg_execution_time': 0.0
        }
        logger.info("重置统计信息")

    def shutdown(self):
        """关闭计算器"""
        logger.info("关闭并行计算器...")

        # 停止监控
        self.stop_monitoring()

        # 关闭执行器
        if self.executor:
            self.executor.shutdown(wait=True)

        # 清理资源
        self.clear_cache()

        logger.info("并行计算器已关闭")


class AsyncParallelCalculator(ParallelCalculator):
    """异步并行计算器"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mode = ParallelMode.ASYNC
        self.loop = asyncio.new_event_loop()
        self.async_tasks: Dict[str, asyncio.Task] = {}

    async def submit_async_task(self, symbol: str, indicator: str,
                                data: pd.DataFrame, parameters: Dict = None) -> str:
        """
        异步提交任务

        Args:
            symbol: 股票代码
            indicator: 指标名称
            data: 计算数据
            parameters: 指标参数

        Returns:
            任务ID
        """
        # 生成任务ID
        data_hash = self._get_data_hash(data)
        task_id = self._generate_task_id(symbol, indicator, parameters or {}, data_hash)

        # 检查缓存
        cached_result = self._get_cached_result(task_id)
        if cached_result is not None:
            return task_id

        # 创建异步任务
        task = asyncio.create_task(
            self._calculate_async(symbol, indicator, data, parameters or {})
        )

        # 存储任务
        self.async_tasks[task_id] = task

        # 添加回调
        task.add_done_callback(
            lambda t: self._handle_async_completion(task_id, t)
        )

        logger.debug(f"提交异步任务: {task_id}")

        return task_id

    async def _calculate_async(self, symbol: str, indicator: str,
                               data: pd.DataFrame, parameters: Dict) -> pd.DataFrame:
        """异步计算"""
        # 这里应该调用实际的指标计算
        # 简化实现，返回模拟数据
        await asyncio.sleep(0.1)  # 模拟异步延迟

        # 返回模拟结果
        result = pd.DataFrame({
            f'{indicator}_async_result': np.random.randn(len(data))
        }, index=data.index)

        return result

    def _handle_async_completion(self, task_id: str, future: asyncio.Future):
        """处理异步任务完成"""
        try:
            result = future.result()
            self._set_cached_result(task_id, result)
            logger.debug(f"异步任务完成: {task_id}")
        except Exception as e:
            logger.error(f"异步任务失败 {task_id}: {e}")
        finally:
            self.async_tasks.pop(task_id, None)

    async def wait_for_async_tasks(self, task_ids: List[str]) -> Dict[str, pd.DataFrame]:
        """等待异步任务完成"""
        tasks = [self.async_tasks[tid] for tid in task_ids if tid in self.async_tasks]

        if not tasks:
            return {}

        # 等待所有任务完成
        completed = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        results = {}
        for task_id, result in zip(task_ids, completed):
            if isinstance(result, Exception):
                logger.error(f"异步任务 {task_id} 失败: {result}")
            else:
                results[task_id] = result

        return results


class ParallelStrategy:
    """并行策略管理器"""

    @staticmethod
    def select_mode(data_size: int, indicator_complexity: str) -> ParallelMode:
        """
        选择并行模式

        Args:
            data_size: 数据大小（行数）
            indicator_complexity: 指标复杂度 ('simple', 'medium', 'complex')

        Returns:
            推荐的并行模式
        """
        if data_size < 100:
            # 小数据量，使用线程模式
            return ParallelMode.THREAD

        elif indicator_complexity == 'complex':
            # 复杂计算，使用进程模式
            return ParallelMode.PROCESS

        elif data_size > 10000:
            # 大数据量，使用进程模式
            return ParallelMode.PROCESS

        else:
            # 中等情况，使用异步模式
            return ParallelMode.ASYNC

    @staticmethod
    def optimize_workers(mode: ParallelMode, available_memory: float = None) -> int:
        """
        优化工作线程/进程数

        Args:
            mode: 并行模式
            available_memory: 可用内存（GB）

        Returns:
            推荐的工作数
        """
        cpu_cores = cpu_count()

        if mode == ParallelMode.PROCESS:
            # 进程模式：考虑CPU核心和内存
            if available_memory:
                # 每个进程大约需要200MB内存
                memory_limit = int(available_memory * 1024 / 200)
                return min(cpu_cores - 1, memory_limit, 8)
            else:
                return max(1, cpu_cores - 1)

        elif mode == ParallelMode.THREAD:
            # 线程模式：I/O密集型，可以更多
            return min(32, cpu_cores * 4)

        else:  # ASYNC
            # 异步模式：混合型
            return min(16, cpu_cores * 2)