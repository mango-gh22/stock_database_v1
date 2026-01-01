# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/monitoring\calculation_logger.py
# File Name: calculation_logger
# @ Author: mango-gh22
# @ Date：2025/12/21 22:23
"""
desc
计算日志模块
记录技术指标计算的过程和结果，用于调试和审计
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/monitoring/calculation_logger.py

"""
计算日志模块 - 修复版
修复 log_calculation_end 参数问题
"""

import json
import logging
import threading
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from pathlib import Path
import gzip
from enum import Enum

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """日志级别"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class CalculationLogEntry:
    """计算日志条目 - 修复版"""
    log_id: str
    timestamp: datetime
    level: LogLevel
    indicator_name: str
    symbol: str
    period: str
    calculation_type: str
    parameters: Dict[str, Any]
    start_time: datetime
    end_time: datetime
    duration_ms: float  # ✅ 修复：毫秒
    success: bool
    error_message: Optional[str] = None
    input_data_shape: Optional[tuple] = None
    output_data_shape: Optional[tuple] = None
    cache_hit: bool = False
    cache_key: Optional[str] = None
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    memory_usage_mb: float = 0.0
    tags: List[str] = field(default_factory=list)
    extra_info: Dict[str, Any] = field(default_factory=dict)


class CalculationLogger:
    """计算日志器 - 修复版"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.log_level = LogLevel(config.get('log_level', 'INFO'))
        self.enabled = config.get('enabled', True)
        self.log_queries = config.get('log_queries', True)
        self.log_results = config.get('log_results', False)
        self.log_performance = config.get('log_performance', True)

        # 设置日志目录
        self.log_dir = Path(config.get('log_dir', 'logs/calculations'))
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 日志文件管理
        self.max_log_size = config.get('max_log_size', 100) * 1024 * 1024
        self.compression = config.get('compression', True)
        self.rotation_size = config.get('rotation_size', 10) * 1024 * 1024

        # 内存缓冲区
        self.buffer = []
        self.buffer_size = config.get('buffer_size', 100)
        self.buffer_lock = threading.Lock()

        # 启动定期刷新线程
        self.flush_thread = threading.Thread(target=self._auto_flush, daemon=True)
        self.flush_thread.start()

    def _auto_flush(self):
        """自动刷新缓冲区"""
        flush_interval = self.config.get('flush_interval', 60)
        while True:
            time.sleep(flush_interval)
            self.flush_buffer()

    def log_calculation_start(self, indicator_name: str, symbol: str,
                              period: str, calculation_type: str,
                              parameters: Dict[str, Any],
                              input_data_shape: Optional[tuple] = None) -> str:
        """记录计算开始"""
        if not self.enabled:
            return ""

        log_id = f"{indicator_name}_{symbol}_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

        entry = CalculationLogEntry(
            log_id=log_id,
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            indicator_name=indicator_name,
            symbol=symbol,
            period=period,
            calculation_type=calculation_type,
            parameters=parameters,
            start_time=datetime.now(),
            end_time=datetime.now(),  # 临时值
            duration_ms=0.0,
            success=False,
            input_data_shape=input_data_shape,
            tags=["start"]
        )

        self._add_to_buffer(entry)
        return log_id

    def log_calculation_end(self, log_id: str, success: bool,
                           output_data_shape: Optional[tuple] = None,
                           error_message: Optional[str] = None,
                           cache_hit: bool = False,
                           cache_key: Optional[str] = None,
                           performance_metrics: Optional[Dict[str, float]] = None,
                           memory_usage_mb: float = 0.0,
                           tags: Optional[List[str]] = None,
                           duration_ms: Optional[float] = None):  # ✅ 修复：添加 duration_ms 参数
        """记录计算结束 - 修复版"""
        if not self.enabled:
            return

        # 计算持续时间
        end_time = datetime.now()
        if duration_ms is None:
            duration_ms = 0.0  # 如果没有提供，使用0

        entry = CalculationLogEntry(
            log_id=log_id,
            timestamp=end_time,
            level=LogLevel.INFO if success else LogLevel.ERROR,
            indicator_name=log_id.split('_')[0],
            symbol=log_id.split('_')[1],
            period=log_id.split('_')[2],
            calculation_type="unknown",
            parameters={},
            start_time=end_time - timedelta(milliseconds=duration_ms),
            end_time=end_time,
            duration_ms=duration_ms,  # ✅ 修复：使用传入的 duration_ms
            success=success,
            error_message=error_message,
            output_data_shape=output_data_shape,
            cache_hit=cache_hit,
            cache_key=cache_key,
            performance_metrics=performance_metrics or {},
            memory_usage_mb=memory_usage_mb,
            tags=tags or ["end"]
        )

        self._add_to_buffer(entry)

    def log_calculation(self, indicator_name: str, symbol: str, period: str,
                       calculation_type: str, parameters: Dict[str, Any],
                       duration_ms: float, success: bool, **kwargs):
        """记录完整计算过程"""
        if not self.enabled:
            return

        entry = CalculationLogEntry(
            log_id=f"{indicator_name}_{symbol}_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
            timestamp=datetime.now(),
            level=LogLevel.INFO if success else LogLevel.ERROR,
            indicator_name=indicator_name,
            symbol=symbol,
            period=period,
            calculation_type=calculation_type,
            parameters=parameters,
            start_time=datetime.now() - timedelta(milliseconds=duration_ms),
            end_time=datetime.now(),
            duration_ms=duration_ms,
            success=success,
            **kwargs
        )

        self._add_to_buffer(entry)

    def _add_to_buffer(self, entry: CalculationLogEntry):
        """添加日志条目到缓冲区"""
        if self._should_log(entry.level):
            with self.buffer_lock:
                self.buffer.append(entry)

                # 缓冲区满时自动刷新
                if len(self.buffer) >= self.buffer_size:
                    self.flush_buffer()

    def _should_log(self, level: LogLevel) -> bool:
        """检查是否应该记录此级别的日志"""
        level_priority = {LogLevel.DEBUG: 10, LogLevel.INFO: 20,
                          LogLevel.WARNING: 30, LogLevel.ERROR: 40, LogLevel.CRITICAL: 50}

        config_priority = level_priority.get(self.log_level, 20)
        entry_priority = level_priority.get(level, 20)

        return entry_priority >= config_priority

    def flush_buffer(self):
        """刷新缓冲区到磁盘"""
        if not self.buffer:
            return

        with self.buffer_lock:
            if not self.buffer:
                return

            entries = self.buffer.copy()
            self.buffer.clear()

        try:
            self._write_entries(entries)
        except Exception as e:
            logger.error(f"写入计算日志失败: {e}")
            # 将失败的条目放回缓冲区（避免数据丢失）
            with self.buffer_lock:
                self.buffer.extend(entries)

    def _write_entries(self, entries: List[CalculationLogEntry]):
        """写入日志条目"""
        # 按日期分组
        entries_by_date = {}
        for entry in entries:
            date_str = entry.timestamp.strftime('%Y%m%d')
            if date_str not in entries_by_date:
                entries_by_date[date_str] = []
            entries_by_date[date_str].append(entry)

        # 写入每个日期的文件
        for date_str, date_entries in entries_by_date.items():
            log_file = self.log_dir / f"calculation_{date_str}.jsonl"

            # 检查文件大小，如果需要则轮转
            if log_file.exists() and log_file.stat().st_size > self.rotation_size:
                self._rotate_file(log_file)

            # 写入条目
            mode = 'a' if log_file.exists() else 'w'
            with open(log_file, mode, encoding='utf-8') as f:
                for entry in date_entries:
                    # 转换为可序列化的字典
                    entry_dict = asdict(entry)
                    entry_dict['timestamp'] = entry_dict['timestamp'].isoformat()
                    entry_dict['start_time'] = entry_dict['start_time'].isoformat()
                    entry_dict['end_time'] = entry_dict['end_time'].isoformat()
                    entry_dict['level'] = entry_dict['level'].value

                    # 根据配置决定是否记录结果数据
                    if not self.log_results:
                        if 'extra_info' in entry_dict and 'result_data' in entry_dict['extra_info']:
                            entry_dict['extra_info']['result_data'] = '[DATA OMITTED]'

                    f.write(json.dumps(entry_dict, ensure_ascii=False) + '\n')

            # 如果需要压缩旧文件
            if self.compression:
                self._compress_old_files()

    def _rotate_file(self, file_path: Path):
        """轮转日志文件"""
        if not file_path.exists():
            return

        # 查找现有的轮转文件
        counter = 1
        while True:
            rotated_file = file_path.parent / f"{file_path.stem}_{counter}.jsonl"
            if not rotated_file.exists():
                break
            counter += 1

        # 重命名文件
        file_path.rename(rotated_file)

        # 如果需要压缩
        if self.compression:
            compressed_file = rotated_file.with_suffix('.jsonl.gz')
            with open(rotated_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    f_out.write(f_in.read())
            rotated_file.unlink()

    def _compress_old_files(self):
        """压缩旧的日志文件"""
        cutoff_date = datetime.now() - timedelta(days=1)
        cutoff_str = cutoff_date.strftime('%Y%m%d')

        for log_file in self.log_dir.glob("calculation_*.jsonl"):
            try:
                date_str = log_file.stem.split('_')[1]
                if date_str < cutoff_str:
                    compressed_file = log_file.with_suffix('.jsonl.gz')
                    if not compressed_file.exists():
                        with open(log_file, 'rb') as f_in:
                            with gzip.open(compressed_file, 'wb') as f_out:
                                f_out.write(f_in.read())
                        log_file.unlink()
            except Exception as e:
                logger.debug(f"压缩旧日志失败: {e}")

    def query_logs(self, start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None,
                   indicator_name: Optional[str] = None,
                   symbol: Optional[str] = None,
                   level: Optional[LogLevel] = None,
                   success: Optional[bool] = None,
                   limit: int = 1000) -> List[Dict[str, Any]]:
        """查询日志"""
        # ... 保持原有代码 ...

    def _read_log_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """读取日志文件 - 保持原有代码"""
        # ... 保持原有代码 ...

    def generate_statistics(self, start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """生成统计报告"""
        logs = self.query_logs(start_time, end_time, limit=10000)

        if not logs:
            return {}

        # 基本统计
        total = len(logs)
        success_count = sum(1 for log in logs if log['success'])

        # 按指标统计
        indicator_stats = {}
        for log in logs:
            indicator = log['indicator_name']
            if indicator not in indicator_stats:
                indicator_stats[indicator] = {
                    'count': 0,
                    'success_count': 0,
                    'total_duration_ms': 0.0,
                    'avg_duration_ms': 0.0,
                    'max_duration_ms': 0.0,
                    'min_duration_ms': float('inf')
                }

            stats = indicator_stats[indicator]
            stats['count'] += 1
            if log['success']:
                stats['success_count'] += 1

            duration = log.get('duration_ms', 0)
            stats['total_duration_ms'] += duration
            stats['max_duration_ms'] = max(stats['max_duration_ms'], duration)
            stats['min_duration_ms'] = min(stats['min_duration_ms'], duration)

        # 计算平均值
        for stats in indicator_stats.values():
            if stats['count'] > 0:
                stats['avg_duration_ms'] = stats['total_duration_ms'] / stats['count']
            if stats['min_duration_ms'] == float('inf'):
                stats['min_duration_ms'] = 0.0

        # 缓存命中率
        cache_hits = sum(1 for log in logs if log.get('cache_hit', False))
        cache_misses = total - cache_hits

        return {
            'summary': {
                'total_calculations': total,
                'success_rate': success_count / total if total > 0 else 0,
                'cache_hit_rate': cache_hits / total if total > 0 else 0
            },
            'indicator_statistics': indicator_stats,
            'cache': {'hits': cache_hits, 'misses': cache_misses}
        }