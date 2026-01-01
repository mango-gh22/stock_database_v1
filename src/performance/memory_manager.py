# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\memory_manager.py
# File Name: memory_manager
# @ Author: mango-gh22
# @ Date：2025/12/21 21:26
"""
desc 
"""

"""
File: src/performance/memory_manager.py
Desc: 内存管理器 - 内存使用监控和优化
"""
import psutil
import gc
import tracemalloc
import threading
import time
from typing import Dict, List, Optional, Any, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import weakref
import sys
import os
import pandas as pd
import numpy as np
from collections import defaultdict, deque
import warnings

logger = logging.getLogger(__name__)


class MemoryThreshold(Enum):
    """内存阈值"""
    LOW = "low"  # 内存使用率 < 60%
    MEDIUM = "medium"  # 内存使用率 60-80%
    HIGH = "high"  # 内存使用率 80-90%
    CRITICAL = "critical"  # 内存使用率 > 90%


@dataclass
class MemorySnapshot:
    """内存快照"""
    timestamp: datetime
    total_memory: float  # 总内存 (MB)
    used_memory: float  # 已用内存 (MB)
    available_memory: float  # 可用内存 (MB)
    memory_percent: float  # 内存使用率 (%)
    process_memory: float  # 进程内存 (MB)
    objects_count: int  # 对象数量
    gc_collections: Tuple[int, int, int]  # GC收集统计
    threshold: MemoryThreshold  # 当前阈值

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'total_memory': self.total_memory,
            'used_memory': self.used_memory,
            'available_memory': self.available_memory,
            'memory_percent': self.memory_percent,
            'process_memory': self.process_memory,
            'objects_count': self.objects_count,
            'gc_collections': self.gc_collections,
            'threshold': self.threshold.value
        }


@dataclass
class MemoryLeakDetection:
    """内存泄漏检测"""
    object_type: str
    count_increase: int
    size_increase: float  # MB
    stack_trace: List[str]
    detected_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'object_type': self.object_type,
            'count_increase': self.count_increase,
            'size_increase': self.size_increase,
            'stack_trace': self.stack_trace[:5],  # 只取前5行
            'detected_at': self.detected_at.isoformat()
        }


class MemoryOptimizer:
    """内存优化器"""

    @staticmethod
    def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        优化DataFrame内存使用

        Args:
            df: 输入的DataFrame

        Returns:
            优化后的DataFrame
        """
        if df.empty:
            return df

        optimized_df = df.copy()

        # 优化数值类型
        for col in optimized_df.select_dtypes(include=['int64']).columns:
            col_min = optimized_df[col].min()
            col_max = optimized_df[col].max()

            if col_min >= 0:
                if col_max < 255:
                    optimized_df[col] = optimized_df[col].astype(np.uint8)
                elif col_max < 65535:
                    optimized_df[col] = optimized_df[col].astype(np.uint16)
                elif col_max < 4294967295:
                    optimized_df[col] = optimized_df[col].astype(np.uint32)
            else:
                if col_min > -128 and col_max < 127:
                    optimized_df[col] = optimized_df[col].astype(np.int8)
                elif col_min > -32768 and col_max < 32767:
                    optimized_df[col] = optimized_df[col].astype(np.int16)
                elif col_min > -2147483648 and col_max < 2147483647:
                    optimized_df[col] = optimized_df[col].astype(np.int32)

        # 优化浮点类型
        for col in optimized_df.select_dtypes(include=['float64']).columns:
            optimized_df[col] = optimized_df[col].astype(np.float32)

        # 优化字符串类型
        for col in optimized_df.select_dtypes(include=['object']).columns:
            num_unique = optimized_df[col].nunique()
            num_total = len(optimized_df[col])

            if num_unique / num_total < 0.5:  # 低基数
                optimized_df[col] = optimized_df[col].astype('category')

        # 清理内存
        del df
        gc.collect()

        return optimized_df

    @staticmethod
    def compress_array(arr: np.ndarray) -> np.ndarray:
        """
        压缩数组

        Args:
            arr: 输入数组

        Returns:
            压缩后的数组
        """
        if arr.dtype == np.float64:
            return arr.astype(np.float32)
        elif arr.dtype == np.int64:
            # 检查值范围
            min_val = arr.min()
            max_val = arr.max()

            if min_val >= 0:
                if max_val < 256:
                    return arr.astype(np.uint8)
                elif max_val < 65536:
                    return arr.astype(np.uint16)
                elif max_val < 4294967296:
                    return arr.astype(np.uint32)
            else:
                if min_val > -128 and max_val < 127:
                    return arr.astype(np.int8)
                elif min_val > -32768 and max_val < 32767:
                    return arr.astype(np.int16)
                elif min_val > -2147483648 and max_val < 2147483647:
                    return arr.astype(np.int32)

        return arr

    @staticmethod
    def release_unused_memory():
        """释放未使用的内存"""
        # 强制垃圾回收
        collected = gc.collect()

        # 清理可能的内存缓存
        try:
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except ImportError:
            pass

        # 清理numpy缓存
        np.save.__defaults__ = (None, None, True)  # 允许覆盖

        logger.debug(f"释放未使用内存，垃圾回收: {collected} 个对象")

        return collected


class MemoryProfiler:
    """内存分析器"""

    def __init__(self):
        self.snapshots: List[MemorySnapshot] = []
        self.leak_detections: List[MemoryLeakDetection] = []
        self.object_tracker = ObjectTracker()

        # 启用tracemalloc
        tracemalloc.start()

        # 性能统计
        self.stats = {
            'total_snapshots': 0,
            'max_memory_usage': 0.0,
            'avg_memory_usage': 0.0,
            'leaks_detected': 0
        }

        logger.info("初始化内存分析器")

    def take_snapshot(self) -> MemorySnapshot:
        """获取内存快照"""
        # 系统内存信息
        vm = psutil.virtual_memory()

        # 进程内存信息
        process = psutil.Process()
        process_memory = process.memory_info().rss / 1024 / 1024  # MB

        # GC统计
        gc_collections = gc.get_count()

        # 对象数量
        objects_count = len(gc.get_objects())

        # 确定阈值
        memory_percent = vm.percent
        if memory_percent < 60:
            threshold = MemoryThreshold.LOW
        elif memory_percent < 80:
            threshold = MemoryThreshold.MEDIUM
        elif memory_percent < 90:
            threshold = MemoryThreshold.HIGH
        else:
            threshold = MemoryThreshold.CRITICAL

        # 创建快照
        snapshot = MemorySnapshot(
            timestamp=datetime.now(),
            total_memory=vm.total / 1024 / 1024,
            used_memory=vm.used / 1024 / 1024,
            available_memory=vm.available / 1024 / 1024,
            memory_percent=memory_percent,
            process_memory=process_memory,
            objects_count=objects_count,
            gc_collections=gc_collections,
            threshold=threshold
        )

        # 保存快照
        self.snapshots.append(snapshot)
        self.stats['total_snapshots'] += 1

        # 更新最大内存使用
        if process_memory > self.stats['max_memory_usage']:
            self.stats['max_memory_usage'] = process_memory

        # 更新平均内存使用
        total_memory = sum(s.process_memory for s in self.snapshots)
        self.stats['avg_memory_usage'] = total_memory / len(self.snapshots)

        # 检测内存泄漏
        self._detect_leaks()

        return snapshot

    def _detect_leaks(self):
        """检测内存泄漏"""
        if len(self.snapshots) < 2:
            return

        # 获取最近两个快照
        recent_snapshots = self.snapshots[-2:]
        prev_snapshot, curr_snapshot = recent_snapshots

        # 检查进程内存增长
        memory_increase = curr_snapshot.process_memory - prev_snapshot.process_memory

        if memory_increase > 10:  # 增长超过10MB
            # 获取tracemalloc快照
            snapshot = tracemalloc.take_snapshot()

            # 分析对象类型
            object_stats = self._analyze_object_growth()

            for obj_type, (count_diff, size_diff) in object_stats.items():
                if size_diff > 1:  # 增长超过1MB
                    leak = MemoryLeakDetection(
                        object_type=obj_type,
                        count_increase=count_diff,
                        size_increase=size_diff,
                        stack_trace=self._get_stack_trace(obj_type),
                        detected_at=datetime.now()
                    )

                    self.leak_detections.append(leak)
                    self.stats['leaks_detected'] += 1

                    logger.warning(
                        f"检测到可能的内存泄漏: {obj_type}, "
                        f"大小增长: {size_diff:.2f}MB, 数量增长: {count_diff}"
                    )

    def _analyze_object_growth(self) -> Dict[str, Tuple[int, float]]:
        """分析对象增长"""
        object_stats = defaultdict(lambda: [0, 0])  # [count, size]

        for obj in gc.get_objects():
            obj_type = type(obj).__name__
            object_stats[obj_type][0] += 1

            # 估算对象大小
            try:
                size = sys.getsizeof(obj)
                object_stats[obj_type][1] += size / 1024 / 1024  # MB
            except:
                pass

        return {k: (v[0], v[1]) for k, v in object_stats.items()}

    def _get_stack_trace(self, obj_type: str) -> List[str]:
        """获取堆栈跟踪"""
        try:
            import traceback
            # 获取当前调用栈
            stack = traceback.format_stack()
            return stack[-10:]  # 返回最近10行
        except:
            return ["无法获取堆栈跟踪"]

    def get_summary(self) -> Dict[str, Any]:
        """获取分析摘要"""
        if not self.snapshots:
            return {}

        latest = self.snapshots[-1]

        return {
            'current_memory': latest.process_memory,
            'memory_threshold': latest.threshold.value,
            'system_memory_usage': latest.memory_percent,
            'objects_count': latest.objects_count,
            'gc_stats': {
                'generation0': latest.gc_collections[0],
                'generation1': latest.gc_collections[1],
                'generation2': latest.gc_collections[2]
            },
            'analysis_stats': self.stats.copy(),
            'leaks_detected': len(self.leak_detections)
        }

    def get_trend_analysis(self, window: int = 10) -> Dict[str, Any]:
        """
        获取趋势分析

        Args:
            window: 分析窗口大小

        Returns:
            趋势分析结果
        """
        if len(self.snapshots) < 2:
            return {}

        # 使用最近的快照
        recent_snapshots = self.snapshots[-window:] if len(self.snapshots) > window else self.snapshots

        # 计算趋势
        memory_values = [s.process_memory for s in recent_snapshots]
        timestamps = [s.timestamp for s in recent_snapshots]

        # 线性回归计算趋势
        if len(memory_values) > 1:
            time_numeric = [(t - timestamps[0]).total_seconds() for t in timestamps]

            # 计算斜率（趋势）
            x_mean = np.mean(time_numeric)
            y_mean = np.mean(memory_values)

            numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(time_numeric, memory_values))
            denominator = sum((x - x_mean) ** 2 for x in time_numeric)

            if denominator != 0:
                slope = numerator / denominator
            else:
                slope = 0

            # 趋势判断
            if slope > 0.1:
                trend = "上升"
            elif slope < -0.1:
                trend = "下降"
            else:
                trend = "稳定"
        else:
            slope = 0
            trend = "稳定"

        return {
            'trend': trend,
            'slope': slope,
            'memory_change': memory_values[-1] - memory_values[0],
            'avg_memory': np.mean(memory_values),
            'std_memory': np.std(memory_values),
            'min_memory': min(memory_values),
            'max_memory': max(memory_values),
            'analysis_window': len(recent_snapshots)
        }

    def clear_snapshots(self):
        """清理快照"""
        self.snapshots.clear()
        self.stats['total_snapshots'] = 0
        logger.info("清理内存快照")

    def stop(self):
        """停止分析"""
        tracemalloc.stop()
        logger.info("停止内存分析")


class ObjectTracker:
    """对象跟踪器"""

    def __init__(self):
        self.tracked_objects = weakref.WeakKeyDictionary()
        self.object_registry = defaultdict(list)
        self.lock = threading.RLock()

    def track(self, obj: Any, name: str = None):
        """
        跟踪对象

        Args:
            obj: 要跟踪的对象
            name: 对象名称
        """
        with self.lock:
            obj_id = id(obj)
            obj_type = type(obj).__name__

            if name is None:
                name = f"{obj_type}_{obj_id}"

            # 创建弱引用
            obj_ref = weakref.ref(obj, self._finalize_callback)

            # 记录对象
            self.tracked_objects[obj_ref] = {
                'id': obj_id,
                'type': obj_type,
                'name': name,
                'created_at': datetime.now(),
                'size': self._estimate_size(obj)
            }

            self.object_registry[obj_type].append(obj_ref)

    def _finalize_callback(self, ref):
        """对象被垃圾回收时的回调"""
        with self.lock:
            if ref in self.tracked_objects:
                obj_info = self.tracked_objects.pop(ref)
                obj_type = obj_info['type']

                # 从注册表中移除
                if obj_type in self.object_registry:
                    self.object_registry[obj_type] = [
                        r for r in self.object_registry[obj_type]
                        if r() is not None
                    ]

    def _estimate_size(self, obj: Any) -> int:
        """估算对象大小"""
        try:
            return sys.getsizeof(obj)
        except:
            return 0

    def get_tracked_objects(self) -> Dict[str, List[Dict]]:
        """获取跟踪的对象"""
        with self.lock:
            result = {}

            for obj_type, refs in self.object_registry.items():
                objects_info = []

                for ref in refs:
                    obj = ref()
                    if obj is not None and ref in self.tracked_objects:
                        obj_info = self.tracked_objects[ref].copy()
                        obj_info['alive'] = True
                        objects_info.append(obj_info)

                if objects_info:
                    result[obj_type] = objects_info

            return result

    def get_object_count(self) -> Dict[str, int]:
        """获取对象数量统计"""
        with self.lock:
            counts = {}

            for obj_type, refs in self.object_registry.items():
                alive_count = sum(1 for ref in refs if ref() is not None)
                if alive_count > 0:
                    counts[obj_type] = alive_count

            return counts

    def clear_dead_objects(self):
        """清理已死亡的对象"""
        with self.lock:
            for obj_type in list(self.object_registry.keys()):
                self.object_registry[obj_type] = [
                    ref for ref in self.object_registry[obj_type]
                    if ref() is not None
                ]

                # 移除空列表
                if not self.object_registry[obj_type]:
                    del self.object_registry[obj_type]

            # 清理跟踪字典
            dead_refs = [
                ref for ref in self.tracked_objects.keys()
                if ref() is None
            ]
            for ref in dead_refs:
                self.tracked_objects.pop(ref, None)


class MemoryManager:
    """内存管理器"""

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化内存管理器

        Args:
            config: 配置字典
        """
        self.config = config or {}

        # 初始化组件
        self.profiler = MemoryProfiler()
        self.optimizer = MemoryOptimizer()
        self.object_tracker = ObjectTracker()

        # 监控线程
        self.monitor_thread = None
        self.monitoring = False
        self.monitor_interval = self.config.get('monitor_interval', 5)

        # 阈值配置
        self.threshold_config = self.config.get('thresholds', {
            'low': 60,
            'medium': 80,
            'high': 90,
            'critical': 95
        })

        # 事件回调
        self.threshold_callbacks = {
            MemoryThreshold.LOW: [],
            MemoryThreshold.MEDIUM: [],
            MemoryThreshold.HIGH: [],
            MemoryThreshold.CRITICAL: []
        }

        # 内存使用历史
        self.memory_history = deque(maxlen=1000)

        # 性能统计
        self.stats = {
            'optimizations_performed': 0,
            'memory_saved': 0.0,
            'gc_collections_triggered': 0,
            'threshold_warnings': 0
        }

        logger.info("初始化内存管理器")

    def start_monitoring(self):
        """开始内存监控"""
        if self.monitoring:
            logger.warning("内存监控已在运行")
            return

        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self.monitor_thread.start()

        logger.info(f"开始内存监控，间隔: {self.monitor_interval}秒")

    def _monitoring_loop(self):
        """监控循环"""
        last_threshold = None

        while self.monitoring:
            try:
                # 获取内存快照
                snapshot = self.profiler.take_snapshot()

                # 记录历史
                self.memory_history.append(snapshot)

                # 检查阈值变化
                if last_threshold != snapshot.threshold:
                    self._handle_threshold_change(snapshot.threshold, snapshot)
                    last_threshold = snapshot.threshold

                # 根据阈值采取行动
                self._take_action_based_on_threshold(snapshot)

                # 定期清理
                if len(self.memory_history) % 20 == 0:
                    self._perform_maintenance()

                time.sleep(self.monitor_interval)

            except Exception as e:
                logger.error(f"内存监控循环错误: {e}")
                time.sleep(self.monitor_interval)

    def _handle_threshold_change(self, new_threshold: MemoryThreshold, snapshot: MemorySnapshot):
        """处理阈值变化"""
        logger.info(f"内存阈值变化: {new_threshold.value} ({snapshot.memory_percent:.1f}%)")

        # 触发回调
        callbacks = self.threshold_callbacks.get(new_threshold, [])
        for callback in callbacks:
            try:
                callback(new_threshold, snapshot)
            except Exception as e:
                logger.error(f"阈值回调执行失败: {e}")

        self.stats['threshold_warnings'] += 1

    def _take_action_based_on_threshold(self, snapshot: MemorySnapshot):
        """根据阈值采取行动"""
        if snapshot.threshold == MemoryThreshold.HIGH:
            # 高内存使用，进行轻度优化
            self._perform_light_optimization()

        elif snapshot.threshold == MemoryThreshold.CRITICAL:
            # 临界内存使用，进行紧急优化
            self._perform_emergency_optimization()

            # 触发警告
            logger.warning(
                f"内存使用临界! 系统: {snapshot.memory_percent:.1f}%, "
                f"进程: {snapshot.process_memory:.1f}MB"
            )

    def _perform_light_optimization(self):
        """执行轻度优化"""
        # 触发垃圾回收
        collected = gc.collect()

        # 清理对象跟踪器
        self.object_tracker.clear_dead_objects()

        # 释放numpy缓存
        try:
            import numpy as np
            np.save.__defaults__ = (None, None, True)
        except:
            pass

        logger.debug(f"轻度内存优化，回收 {collected} 个对象")

        self.stats['gc_collections_triggered'] += 1

    def _perform_emergency_optimization(self):
        """执行紧急优化"""
        # 强制垃圾回收
        collected = gc.collect(2)  # 完整回收

        # 清理所有可能的缓存
        MemoryOptimizer.release_unused_memory()

        # 清理对象跟踪器
        self.object_tracker.clear_dead_objects()

        # 尝试释放更多内存
        self._free_large_objects()

        logger.warning(f"紧急内存优化，回收 {collected} 个对象")

        self.stats['gc_collections_triggered'] += 1

    def _free_large_objects(self):
        """释放大对象"""
        # 查找并释放大对象
        large_objects = []

        for obj in gc.get_objects():
            try:
                size = sys.getsizeof(obj)
                if size > 1024 * 1024:  # 大于1MB
                    large_objects.append((type(obj).__name__, size))
            except:
                pass

        if large_objects:
            logger.warning(f"发现大对象: {large_objects}")

            # 这里可以添加特定的大对象清理逻辑
            # 例如：清理缓存、释放数据等

    def _perform_maintenance(self):
        """执行维护任务"""
        # 清理旧的快照
        if len(self.profiler.snapshots) > 100:
            self.profiler.snapshots = self.profiler.snapshots[-50:]

        # 清理历史记录
        if len(self.memory_history) > 500:
            # 保留最近500条记录
            self.memory_history = deque(
                list(self.memory_history)[-500:],
                maxlen=1000
            )

    def stop_monitoring(self):
        """停止内存监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)

        logger.info("停止内存监控")

    def register_threshold_callback(self, threshold: MemoryThreshold, callback: Callable):
        """
        注册阈值回调

        Args:
            threshold: 阈值级别
            callback: 回调函数 (threshold, snapshot) -> None
        """
        if threshold in self.threshold_callbacks:
            self.threshold_callbacks[threshold].append(callback)
            logger.debug(f"注册 {threshold.value} 阈值回调")

    def optimize_data(self, data: Any) -> Any:
        """
        优化数据内存使用

        Args:
            data: 输入数据

        Returns:
            优化后的数据
        """
        if isinstance(data, pd.DataFrame):
            optimized = self.optimizer.optimize_dataframe(data)

            # 计算节省的内存
            original_memory = data.memory_usage(deep=True).sum() / 1024 / 1024
            optimized_memory = optimized.memory_usage(deep=True).sum() / 1024 / 1024
            saved = original_memory - optimized_memory

            if saved > 0:
                self.stats['memory_saved'] += saved
                self.stats['optimizations_performed'] += 1
                logger.info(f"DataFrame优化节省 {saved:.2f}MB 内存")

            return optimized

        elif isinstance(data, np.ndarray):
            return self.optimizer.compress_array(data)

        else:
            return data

    def track_object(self, obj: Any, name: str = None):
        """
        跟踪对象

        Args:
            obj: 要跟踪的对象
            name: 对象名称
        """
        self.object_tracker.track(obj, name)

    def get_memory_info(self) -> Dict[str, Any]:
        """获取内存信息"""
        if not self.memory_history:
            return {}

        latest = self.memory_history[-1]

        return {
            'system': {
                'total_memory': latest.total_memory,
                'used_memory': latest.used_memory,
                'available_memory': latest.available_memory,
                'memory_percent': latest.memory_percent,
                'threshold': latest.threshold.value
            },
            'process': {
                'memory_usage': latest.process_memory,
                'objects_count': latest.objects_count,
                'gc_stats': latest.gc_collections
            },
            'history': {
                'record_count': len(self.memory_history),
                'avg_memory': np.mean([s.process_memory for s in self.memory_history]),
                'trend': self.profiler.get_trend_analysis()
            }
        }

    def get_optimization_stats(self) -> Dict[str, Any]:
        """获取优化统计"""
        return self.stats.copy()

    def get_leak_detections(self) -> List[Dict[str, Any]]:
        """获取泄漏检测"""
        return [leak.to_dict() for leak in self.profiler.leak_detections]

    def get_object_tracking_info(self) -> Dict[str, Any]:
        """获取对象跟踪信息"""
        tracked = self.object_tracker.get_tracked_objects()
        counts = self.object_tracker.get_object_count()

        return {
            'tracked_objects': tracked,
            'object_counts': counts,
            'total_tracked': sum(counts.values())
        }

    def generate_report(self) -> Dict[str, Any]:
        """生成内存报告"""
        memory_info = self.get_memory_info()
        optimization_stats = self.get_optimization_stats()
        leak_detections = self.get_leak_detections()
        tracking_info = self.get_object_tracking_info()

        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'status': 'critical' if self.profiler.leak_detections else 'normal',
                'memory_usage': memory_info.get('process', {}).get('memory_usage', 0),
                'threshold': memory_info.get('system', {}).get('threshold', 'low')
            },
            'memory_info': memory_info,
            'optimization_stats': optimization_stats,
            'leak_detections': {
                'count': len(leak_detections),
                'details': leak_detections
            },
            'object_tracking': tracking_info,
            'recommendations': self._generate_recommendations(memory_info)
        }

        return report

    def _generate_recommendations(self, memory_info: Dict) -> List[str]:
        """生成优化建议"""
        recommendations = []

        # 检查内存使用
        memory_percent = memory_info.get('system', {}).get('memory_percent', 0)
        process_memory = memory_info.get('process', {}).get('memory_usage', 0)

        if memory_percent > 80:
            recommendations.append("系统内存使用过高，建议增加物理内存或优化应用")

        if process_memory > 1000:  # 进程使用超过1GB
            recommendations.append("进程内存使用过高，建议优化数据结构和算法")

        # 检查对象数量
        objects_count = memory_info.get('process', {}).get('objects_count', 0)
        if objects_count > 100000:
            recommendations.append("对象数量过多，建议使用更高效的数据结构")

        # 检查泄漏
        if self.profiler.leak_detections:
            recommendations.append("检测到可能的内存泄漏，请检查相关代码")

        # 检查GC频率
        if self.stats['gc_collections_triggered'] > 10:
            recommendations.append("频繁触发垃圾回收，可能存在内存管理问题")

        return recommendations

    def save_report(self, filepath: str):
        """
        保存报告到文件

        Args:
            filepath: 文件路径
        """
        report = self.generate_report()

        try:
            import json
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            logger.info(f"内存报告已保存到: {filepath}")

        except Exception as e:
            logger.error(f"保存报告失败: {e}")

    def shutdown(self):
        """关闭内存管理器"""
        self.stop_monitoring()
        self.profiler.stop()

        # 清理资源
        self.memory_history.clear()
        self.profiler.clear_snapshots()

        logger.info("内存管理器已关闭")


class MemoryGuard:
    """内存守卫（上下文管理器）"""

    def __init__(self, memory_manager: MemoryManager,
                 memory_limit: float = 1024,  # MB
                 name: str = None):
        """
        初始化内存守卫

        Args:
            memory_manager: 内存管理器实例
            memory_limit: 内存限制（MB）
            name: 守卫名称
        """
        self.memory_manager = memory_manager
        self.memory_limit = memory_limit
        self.name = name or f"guard_{id(self)}"

        self.start_memory = 0
        self.max_memory = 0

    def __enter__(self):
        """进入上下文"""
        # 记录开始时的内存使用
        snapshot = self.memory_manager.profiler.take_snapshot()
        self.start_memory = snapshot.process_memory
        self.max_memory = self.start_memory

        logger.debug(f"内存守卫 '{self.name}' 启动，起始内存: {self.start_memory:.1f}MB")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文"""
        # 记录结束时的内存使用
        snapshot = self.memory_manager.profiler.take_snapshot()
        end_memory = snapshot.process_memory

        # 计算内存变化
        memory_change = end_memory - self.start_memory

        # 检查内存泄漏
        if memory_change > self.memory_limit:
            logger.warning(
                f"内存守卫 '{self.name}' 检测到可能的内存泄漏: "
                f"增加 {memory_change:.1f}MB (限制: {self.memory_limit}MB)"
            )

            # 触发内存优化
            self.memory_manager._perform_light_optimization()

        logger.debug(
            f"内存守卫 '{self.name}' 结束，内存变化: {memory_change:.1f}MB, "
            f"峰值: {self.max_memory:.1f}MB"
        )

    def check_memory(self):
        """检查当前内存使用"""
        snapshot = self.memory_manager.profiler.take_snapshot()
        current_memory = snapshot.process_memory

        # 更新峰值
        if current_memory > self.max_memory:
            self.max_memory = current_memory

        # 检查是否超过限制
        memory_used = current_memory - self.start_memory
        if memory_used > self.memory_limit:
            logger.warning(
                f"内存守卫 '{self.name}' 内存使用超标: "
                f"{memory_used:.1f}MB > {self.memory_limit}MB"
            )

            return False

        return True


# 使用示例函数
def memory_safe_execute(func: Callable, *args, **kwargs):
    """
    安全执行函数（带内存保护）

    Args:
        func: 要执行的函数
        *args: 函数参数
        **kwargs: 函数关键字参数

    Returns:
        函数执行结果
    """
    # 创建内存管理器
    memory_manager = MemoryManager()

    try:
        # 启动监控
        memory_manager.start_monitoring()

        # 使用内存守卫执行函数
        with MemoryGuard(memory_manager, memory_limit=512, name=func.__name__):
            result = func(*args, **kwargs)

            # 优化结果
            if isinstance(result, (pd.DataFrame, np.ndarray)):
                result = memory_manager.optimize_data(result)

            return result

    finally:
        # 停止监控并生成报告
        memory_manager.stop_monitoring()

        # 生成报告
        report = memory_manager.generate_report()

        # 如果有问题，记录警告
        if report['summary']['status'] == 'critical':
            logger.warning(f"函数 '{func.__name__}' 执行期间检测到内存问题")

        memory_manager.shutdown()