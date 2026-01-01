# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/monitoring\performance_monitor.py
# File Name: performance_monnitor
# @ Author: mango-gh22
# @ Date：2025/12/21 22:20
"""
desc
性能监控模块 - 修复版
实时监控系统性能指标，收集统计信息，提供性能分析报告
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/monitoring/performance_monitor.py

"""
性能监控模块 - CPU 修复版
修复：初始采样 + 进程级 CPU 计算
"""

import psutil
import time
import threading
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from collections import deque
import logging

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """性能指标数据类"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_usage_percent: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_io_sent_mb: float
    network_io_recv_mb: float
    active_threads: int
    open_files: int
    cache_hit_rate: float = 0.0
    query_latency_ms: float = 0.0
    parallel_efficiency: float = 0.0


class PerformanceMonitor:
    """性能监控器 - 修复版"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics_history = deque(maxlen=config.get('history_size', 1000))
        self.running = False
        self.monitor_thread = None
        self.lock = threading.Lock()

        # ✅ 修复1：初始化当前进程
        self.process = psutil.Process()
        self.last_cpu_times: Optional[Tuple[float, float]] = None
        self.last_update: float = time.time()

        # ✅ 修复2：初始化 IO 计数器
        self.last_disk_io = psutil.disk_io_counters()
        self.last_net_io = psutil.net_io_counters()

        # ✅ 修复3：执行初始采样
        self._perform_initial_sample()

    def _perform_initial_sample(self):
        """执行初始采样，建立基准值"""
        try:
            self.last_cpu_times = self._get_cpu_times()
            self.last_disk_io = psutil.disk_io_counters()
            self.last_net_io = psutil.net_io_counters()
            self.last_update = time.time()
            time.sleep(0.1)  # 确保第二次采样有效
            logger.debug("初始采样完成")
        except Exception as e:
            logger.warning(f"初始采样失败: {e}")
            self.last_cpu_times = (0.0, 0.0)

    def _get_cpu_times(self) -> Tuple[float, float]:
        """获取进程 CPU 时间"""
        try:
            cpu_times = self.process.cpu_times()
            return (cpu_times.user, cpu_times.system)
        except:
            return (0.0, 0.0)

    def start(self):
        """启动监控"""
        if self.running:
            return

        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="PerformanceMonitor"
        )
        self.monitor_thread.start()
        logger.info("性能监控已启动")

    def stop(self):
        """停止监控"""
        logger.info("正在停止性能监控...")
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)
        logger.info("性能监控已停止")

    def _monitor_loop(self):
        """监控循环"""
        interval = self.config.get('interval', 10)

        while self.running:
            try:
                metrics = self._safe_collect_metrics()
                if metrics:
                    with self.lock:
                        self.metrics_history.append(metrics)

                self._check_thresholds(metrics)

            except Exception as e:
                logger.error(f"监控循环异常: {e}", exc_info=True)

            time.sleep(interval)

    def _safe_collect_metrics(self) -> PerformanceMetrics:
        """安全收集指标（隔离异常）"""
        try:
            return self.collect_metrics()
        except Exception as e:
            logger.error(f"收集指标失败: {e}")
            return self._get_empty_metrics()

    def _get_empty_metrics(self) -> PerformanceMetrics:
        """获取空指标"""
        return PerformanceMetrics(
            timestamp=datetime.now(),
            cpu_percent=0.0,
            memory_percent=0.0,
            memory_used_mb=0.0,
            disk_usage_percent=0.0,
            disk_io_read_mb=0.0,
            disk_io_write_mb=0.0,
            network_io_sent_mb=0.0,
            network_io_recv_mb=0.0,
            active_threads=threading.active_count(),
            open_files=0
        )

    def collect_metrics(self) -> PerformanceMetrics:
        """收集当前性能指标 - 修复版"""
        now = datetime.now()

        # ✅ 修复：获取当前 CPU 时间
        current_cpu_times = self._get_cpu_times()

        # ✅ 修复：计算 CPU 使用率
        cpu_percent = 0.0
        if self.last_cpu_times and current_cpu_times:
            try:
                time_diff = time.time() - self.last_update
                if time_diff > 0.001:  # 确保时间差足够大
                    # CPU 时间差
                    last_total = self.last_cpu_times[0] + self.last_cpu_times[1]
                    current_total = current_cpu_times[0] + current_cpu_times[1]

                    # CPU 使用率 = (CPU时间差 / 实际时间差) * 100
                    cpu_usage = (current_total - last_total) / time_diff * 100

                    # 限制范围
                    cpu_percent = max(0.0, min(cpu_usage, 100.0))

                    logger.debug(f"CPU计算: {cpu_percent:.2f}% (last={last_total:.4f}, current={current_total:.4f}, diff={time_diff:.4f})")
            except Exception as e:
                logger.error(f"CPU计算失败: {e}")

        # 更新上一次的值
        self.last_cpu_times = current_cpu_times
        self.last_update = time.time()

        # 收集其他指标（独立异常捕获）
        try:
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_mb = memory.used / (1024 * 1024)
        except:
            memory_percent = 0.0
            memory_used_mb = 0.0

        try:
            disk_usage = psutil.disk_usage('/')
            disk_usage_percent = disk_usage.percent
        except:
            disk_usage_percent = 0.0

        # IO 指标
        disk_read_mb = disk_write_mb = net_sent_mb = net_recv_mb = 0.0
        try:
            disk_io = psutil.disk_io_counters()
            net_io = psutil.net_io_counters()

            current_time = time.time()
            time_diff = current_time - self.last_update

            if time_diff > 0:
                if disk_io and self.last_disk_io:
                    disk_read_mb = (disk_io.read_bytes - self.last_disk_io.read_bytes) / (1024 * 1024) / time_diff
                    disk_write_mb = (disk_io.write_bytes - self.last_disk_io.write_bytes) / (1024 * 1024) / time_diff

                if net_io and self.last_net_io:
                    net_sent_mb = (net_io.bytes_sent - self.last_net_io.bytes_sent) / (1024 * 1024) / time_diff
                    net_recv_mb = (net_io.bytes_recv - self.last_net_io.bytes_recv) / (1024 * 1024) / time_diff

            self.last_disk_io = disk_io
            self.last_net_io = net_io
        except Exception as e:
            logger.debug(f"IO监控失败: {e}")

        try:
            open_files = len(self.process.open_files())
        except:
            open_files = 0

        return PerformanceMetrics(
            timestamp=now,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_used_mb=memory_used_mb,
            disk_usage_percent=disk_usage_percent,
            disk_io_read_mb=disk_read_mb,
            disk_io_write_mb=disk_write_mb,
            network_io_sent_mb=net_sent_mb,
            network_io_recv_mb=net_recv_mb,
            active_threads=threading.active_count(),
            open_files=open_files
        )

    def _check_thresholds(self, metrics: PerformanceMetrics):
        """检查性能阈值"""
        alert_config = self.config.get('alerts', {})
        thresholds = alert_config.get('thresholds', {})

        alerts = []

        if metrics.cpu_percent > thresholds.get('cpu_usage', 90):
            alerts.append(f"CPU使用率过高: {metrics.cpu_percent:.1f}%")

        if metrics.memory_percent > thresholds.get('memory_usage', 85):
            alerts.append(f"内存使用率过高: {metrics.memory_percent:.1f}%")

        if alerts:
            self._trigger_alerts(alerts, metrics)

    def _trigger_alerts(self, alerts: List[str], metrics: PerformanceMetrics):
        """触发警报"""
        alert_config = self.config.get('alerts', {})
        channels = alert_config.get('channels', ['log'])

        alert_message = f"性能警报 ({metrics.timestamp}):\n" + "\n".join(alerts)

        if 'log' in channels:
            logger.warning(alert_message)

        if 'file' in channels:
            alert_file = self.config.get('alert_log_file', 'logs/performance_alerts.log')
            try:
                with open(alert_file, 'a', encoding='utf-8') as f:
                    f.write(f"{datetime.now().isoformat()} - {alert_message}\n")
            except Exception as e:
                logger.error(f"写入警报日志失败: {e}")

    def get_current_metrics(self) -> Dict[str, Any]:
        """获取当前性能指标"""
        with self.lock:
            if not self.metrics_history:
                return {}
            latest = self.metrics_history[-1]
            return asdict(latest)

    def get_metrics_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取性能指标历史"""
        with self.lock:
            history = list(self.metrics_history)[-limit:]
            return [asdict(m) for m in history]

    def generate_report(self) -> Dict[str, Any]:
        """生成性能报告"""
        with self.lock:
            if not self.metrics_history:
                return {
                    'timestamp': datetime.now().isoformat(),
                    'cpu': {'avg': 0.0, 'max': 0.0, 'min': 0.0, 'current': 0.0},
                    'memory': {'avg': 0.0, 'max': 0.0, 'min': 0.0, 'current': 0.0},
                    'sample_count': 0,
                    'alerts': []
                }

            metrics_list = list(self.metrics_history)

            # 计算统计信息
            cpu_values = [m.cpu_percent for m in metrics_list]
            memory_values = [m.memory_percent for m in metrics_list]

            report = {
                'timestamp': datetime.now().isoformat(),
                'duration_seconds': (metrics_list[-1].timestamp - metrics_list[0].timestamp).total_seconds(),
                'sample_count': len(metrics_list),
                'cpu': {
                    'avg': sum(cpu_values) / len(cpu_values),
                    'max': max(cpu_values),
                    'min': min(cpu_values),
                    'current': cpu_values[-1]
                },
                'memory': {
                    'avg': sum(memory_values) / len(memory_values),
                    'max': max(memory_values),
                    'min': min(memory_values),
                    'current': memory_values[-1]
                },
                'alerts': []
            }

            return report