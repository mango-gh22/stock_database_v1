# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\cache_strategy.py
# File Name: cache_strategy
# @ Author: mango-gh22
# @ Date：2025/12/21 21:20
"""
desc 
"""

"""
File: src/performance/cache_strategy.py
Desc: 缓存策略 - 智能缓存管理和优化
"""
import json
import pickle
import hashlib
import zlib
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import threading
from pathlib import Path
import pandas as pd
import numpy as np
from collections import OrderedDict, defaultdict
import time

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """缓存级别"""
    MEMORY = "memory"  # 内存缓存（最快）
    DISK = "disk"  # 磁盘缓存（中等）
    DISTRIBUTED = "distributed"  # 分布式缓存（最慢）


class CachePolicy(Enum):
    """缓存策略"""
    LRU = "lru"  # 最近最少使用
    LFU = "lfu"  # 最不经常使用
    FIFO = "fifo"  # 先进先出
    ARC = "arc"  # 自适应替换缓存
    TTL = "ttl"  # 生存时间


@dataclass
class CacheItem:
    """缓存项"""
    key: str
    value: Any
    size: int
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    ttl: Optional[timedelta] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.last_accessed is None:
            self.last_accessed = self.created_at

    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.ttl:
            return datetime.now() - self.created_at > self.ttl
        return False

    def touch(self):
        """更新访问时间"""
        self.last_accessed = datetime.now()
        self.access_count += 1


class BaseCacheStrategy:
    """基础缓存策略"""

    def __init__(self, max_size: int = 1000):
        """
        初始化缓存策略

        Args:
            max_size: 最大缓存项数
        """
        self.max_size = max_size
        self.cache: Dict[str, CacheItem] = {}
        self.lock = threading.RLock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'size': 0,
            'total_items': 0
        }

    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存项

        Args:
            key: 缓存键

        Returns:
            缓存值或None
        """
        with self.lock:
            if key in self.cache:
                item = self.cache[key]

                # 检查是否过期
                if item.is_expired():
                    self._remove(key)
                    self.stats['misses'] += 1
                    return None

                # 更新访问信息
                item.touch()
                self.stats['hits'] += 1
                return item.value
            else:
                self.stats['misses'] += 1
                return None

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None,
            metadata: Optional[Dict] = None):
        """
        设置缓存项

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 生存时间
            metadata: 元数据
        """
        with self.lock:
            # 计算大小
            size = self._calculate_size(value)

            # 创建缓存项
            item = CacheItem(
                key=key,
                value=value,
                size=size,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl=ttl,
                metadata=metadata or {}
            )

            # 检查是否需要清理空间
            if self._needs_cleanup(size):
                self._cleanup(size)

            # 添加或更新缓存
            self.cache[key] = item
            self.stats['size'] += size
            self.stats['total_items'] += 1

    def delete(self, key: str) -> bool:
        """
        删除缓存项

        Args:
            key: 缓存键

        Returns:
            是否成功删除
        """
        with self.lock:
            return self._remove(key)

    def clear(self):
        """清理所有缓存"""
        with self.lock:
            self.cache.clear()
            self.stats['size'] = 0
            self.stats['total_items'] = 0
            logger.info("清理所有缓存")

    def _calculate_size(self, value: Any) -> int:
        """计算值的大小"""
        try:
            # 尝试序列化估算大小
            if isinstance(value, pd.DataFrame):
                return value.memory_usage(deep=True).sum()
            elif isinstance(value, np.ndarray):
                return value.nbytes
            else:
                return len(pickle.dumps(value))
        except:
            return 1  # 默认大小

    def _needs_cleanup(self, new_size: int) -> bool:
        """检查是否需要清理"""
        return self.stats['size'] + new_size > self.max_size * 1024 * 1024  # MB

    def _cleanup(self, required_space: int):
        """清理缓存空间（子类实现）"""
        raise NotImplementedError

    def _remove(self, key: str) -> bool:
        """移除缓存项"""
        if key in self.cache:
            item = self.cache.pop(key)
            self.stats['size'] -= item.size
            self.stats['total_items'] -= 1
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            stats = self.stats.copy()
            stats['hit_rate'] = (
                stats['hits'] / (stats['hits'] + stats['misses'])
                if (stats['hits'] + stats['misses']) > 0 else 0
            )
            stats['current_items'] = len(self.cache)
            stats['avg_size'] = (
                stats['size'] / stats['current_items']
                if stats['current_items'] > 0 else 0
            )
            return stats

    def get_keys(self) -> List[str]:
        """获取所有缓存键"""
        with self.lock:
            return list(self.cache.keys())


class LRUCacheStrategy(BaseCacheStrategy):
    """LRU缓存策略"""

    def __init__(self, max_size: int = 1000):
        super().__init__(max_size)
        self.access_order = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        """获取缓存项（LRU）"""
        with self.lock:
            if key in self.cache:
                # 更新访问顺序
                self.access_order.move_to_end(key)
                return super().get(key)
            return None

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None,
            metadata: Optional[Dict] = None):
        """设置缓存项（LRU）"""
        with self.lock:
            super().set(key, value, ttl, metadata)
            self.access_order[key] = datetime.now()

    def _cleanup(self, required_space: int):
        """LRU清理策略"""
        removed_size = 0
        keys_to_remove = []

        # 按访问顺序清理
        for key in list(self.access_order.keys()):
            if removed_size >= required_space:
                break

            if key in self.cache:
                item = self.cache[key]
                keys_to_remove.append(key)
                removed_size += item.size

        # 移除选中的键
        for key in keys_to_remove:
            self._remove(key)
            self.access_order.pop(key, None)

        if keys_to_remove:
            self.stats['evictions'] += len(keys_to_remove)
            logger.debug(f"LRU清理: 移除 {len(keys_to_remove)} 个项，释放 {removed_size} 字节")


class LFUCacheStrategy(BaseCacheStrategy):
    """LFU缓存策略"""

    def __init__(self, max_size: int = 1000):
        super().__init__(max_size)
        self.access_freq = defaultdict(int)

    def get(self, key: str) -> Optional[Any]:
        """获取缓存项（LFU）"""
        with self.lock:
            result = super().get(key)
            if result is not None:
                self.access_freq[key] += 1
            return result

    def _cleanup(self, required_space: int):
        """LFU清理策略"""
        removed_size = 0
        keys_to_remove = []

        # 按访问频率排序
        sorted_keys = sorted(
            self.cache.keys(),
            key=lambda k: (self.access_freq.get(k, 0), self.cache[k].created_at)
        )

        # 清理低频项
        for key in sorted_keys:
            if removed_size >= required_space:
                break

            item = self.cache[key]
            keys_to_remove.append(key)
            removed_size += item.size

        # 移除选中的键
        for key in keys_to_remove:
            self._remove(key)
            self.access_freq.pop(key, None)

        if keys_to_remove:
            self.stats['evictions'] += len(keys_to_remove)
            logger.debug(f"LFU清理: 移除 {len(keys_to_remove)} 个项，释放 {removed_size} 字节")


class TTLCacheStrategy(BaseCacheStrategy):
    """TTL缓存策略"""

    def __init__(self, max_size: int = 1000, default_ttl: timedelta = None):
        super().__init__(max_size)
        self.default_ttl = default_ttl or timedelta(hours=1)
        self.expiry_times = {}

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None,
            metadata: Optional[Dict] = None):
        """设置缓存项（TTL）"""
        with self.lock:
            ttl = ttl or self.default_ttl
            super().set(key, value, ttl, metadata)
            self.expiry_times[key] = datetime.now() + ttl

    def _cleanup(self, required_space: int):
        """TTL清理策略"""
        removed_size = 0
        keys_to_remove = []
        current_time = datetime.now()

        # 清理过期项
        expired_keys = [
            k for k, expiry in self.expiry_times.items()
            if expiry < current_time
        ]

        for key in expired_keys:
            if key in self.cache:
                item = self.cache[key]
                keys_to_remove.append(key)
                removed_size += item.size

        # 如果过期项不够，按创建时间清理
        if removed_size < required_space:
            sorted_keys = sorted(
                self.cache.keys(),
                key=lambda k: self.cache[k].created_at
            )

            for key in sorted_keys:
                if key in keys_to_remove:
                    continue

                if removed_size >= required_space:
                    break

                item = self.cache[key]
                keys_to_remove.append(key)
                removed_size += item.size

        # 移除选中的键
        for key in keys_to_remove:
            self._remove(key)
            self.expiry_times.pop(key, None)

        if keys_to_remove:
            self.stats['evictions'] += len(keys_to_remove)
            logger.debug(f"TTL清理: 移除 {len(keys_to_remove)} 个项，释放 {removed_size} 字节")


class AdaptiveCacheStrategy(BaseCacheStrategy):
    """自适应缓存策略（ARC）"""

    def __init__(self, max_size: int = 1000):
        super().__init__(max_size)
        # T1: 最近访问的项
        self.t1 = OrderedDict()
        # T2: 频繁访问的项
        self.t2 = OrderedDict()
        # B1: T1的幽灵项
        self.b1 = OrderedDict()
        # B2: T2的幽灵项
        self.b2 = OrderedDict()
        self.p = 0  # 目标大小

    def get(self, key: str) -> Optional[Any]:
        """获取缓存项（ARC）"""
        with self.lock:
            # 检查T1
            if key in self.t1:
                # 移动到T2
                self.t1.pop(key)
                self.t2[key] = datetime.now()
                return super().get(key)

            # 检查T2
            elif key in self.t2:
                # 更新访问时间
                self.t2.move_to_end(key)
                return super().get(key)

            # 检查B1
            elif key in self.b1:
                # 自适应调整p
                self.p = min(self.p + max(1, len(self.b2) // len(self.b1)), self.max_size)
                self._replace(True)
                self.b1.pop(key)
                self.t2[key] = datetime.now()
                return None

            # 检查B2
            elif key in self.b2:
                # 自适应调整p
                self.p = max(self.p - max(1, len(self.b1) // len(self.b2)), 0)
                self._replace(False)
                self.b2.pop(key)
                self.t2[key] = datetime.now()
                return None

            else:
                return None

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None,
            metadata: Optional[Dict] = None):
        """设置缓存项（ARC）"""
        with self.lock:
            # 检查是否已存在
            if key in self.t1 or key in self.t2:
                # 更新值
                super().set(key, value, ttl, metadata)
                return

            # 添加到T1
            super().set(key, value, ttl, metadata)
            self.t1[key] = datetime.now()

            # 检查是否需要替换
            if len(self.t1) + len(self.b1) == self.max_size:
                if len(self.t1) < self.max_size:
                    self.b1.popitem(last=False)
                    self._replace(True)
                else:
                    self._remove_from_cache(list(self.t1.keys())[0])
                    self.t1.popitem(last=False)
            elif len(self.t1) + len(self.b1) > self.max_size:
                if len(self.t1) == self.max_size:
                    self._remove_from_cache(list(self.t1.keys())[0])
                    self.t1.popitem(last=False)
                else:
                    self.b1.popitem(last=False)
                    self._replace(True)

    def _replace(self, in_b1: bool):
        """ARC替换算法"""
        if len(self.t1) >= max(1, self.p):
            # 从T1中移除
            if list(self.t1.keys()):
                key = list(self.t1.keys())[0]
                self.t1.pop(key)
                self.b1[key] = datetime.now()
        else:
            # 从T2中移除
            if list(self.t2.keys()):
                key = list(self.t2.keys())[0]
                self.t2.pop(key)
                self.b2[key] = datetime.now()

    def _remove_from_cache(self, key: str):
        """从基础缓存中移除"""
        super()._remove(key)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = super().get_stats()
        stats.update({
            't1_size': len(self.t1),
            't2_size': len(self.t2),
            'b1_size': len(self.b1),
            'b2_size': len(self.b2),
            'p_value': self.p
        })
        return stats


class MultiLevelCache:
    """多级缓存系统"""

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化多级缓存

        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.caches = {}
        self._init_caches()

        # 统计信息
        self.stats = defaultdict(int)

        logger.info("初始化多级缓存系统")

    def _init_caches(self):
        """初始化各级缓存"""
        # L1: 内存缓存（最快）
        l1_config = self.config.get('l1', {
            'strategy': 'lru',
            'max_size': 100  # MB
        })
        self.caches[CacheLevel.MEMORY] = self._create_cache(
            CacheLevel.MEMORY, l1_config
        )

        # L2: 磁盘缓存（中等）
        l2_config = self.config.get('l2', {
            'strategy': 'lfu',
            'max_size': 1000,  # MB
            'cache_dir': 'data/cache/disk'
        })
        self.caches[CacheLevel.DISK] = self._create_cache(
            CacheLevel.DISK, l2_config
        )

        # L3: 分布式缓存（可选）
        if 'l3' in self.config:
            l3_config = self.config['l3']
            self.caches[CacheLevel.DISTRIBUTED] = self._create_cache(
                CacheLevel.DISTRIBUTED, l3_config
            )

    def _create_cache(self, level: CacheLevel, config: Dict) -> BaseCacheStrategy:
        """创建缓存实例"""
        strategy_name = config.get('strategy', 'lru').upper()

        if strategy_name == 'LRU':
            strategy_class = LRUCacheStrategy
        elif strategy_name == 'LFU':
            strategy_class = LFUCacheStrategy
        elif strategy_name == 'TTL':
            strategy_class = TTLCacheStrategy
        elif strategy_name == 'ARC':
            strategy_class = AdaptiveCacheStrategy
        else:
            strategy_class = LRUCacheStrategy

        max_size = config.get('max_size', 1000)

        if level == CacheLevel.MEMORY:
            return strategy_class(max_size)
        elif level == CacheLevel.DISK:
            return DiskCache(strategy_class, max_size, config.get('cache_dir'))
        else:
            # 分布式缓存（简化实现）
            return strategy_class(max_size)

    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存项（多级）

        Args:
            key: 缓存键

        Returns:
            缓存值或None
        """
        # 尝试从L1获取
        value = self.caches[CacheLevel.MEMORY].get(key)
        if value is not None:
            self.stats['l1_hits'] += 1
            return value

        # 尝试从L2获取
        value = self.caches[CacheLevel.DISK].get(key)
        if value is not None:
            self.stats['l2_hits'] += 1
            # 写回到L1
            self.caches[CacheLevel.MEMORY].set(key, value)
            return value

        # 尝试从L3获取（如果存在）
        if CacheLevel.DISTRIBUTED in self.caches:
            value = self.caches[CacheLevel.DISTRIBUTED].get(key)
            if value is not None:
                self.stats['l3_hits'] += 1
                # 写回到L2和L1
                self.caches[CacheLevel.DISK].set(key, value)
                self.caches[CacheLevel.MEMORY].set(key, value)
                return value

        self.stats['misses'] += 1
        return None

    def set(self, key: str, value: Any, level: CacheLevel = None,
            ttl: Optional[timedelta] = None):
        """
        设置缓存项（多级）

        Args:
            key: 缓存键
            value: 缓存值
            level: 缓存级别（None表示所有级别）
            ttl: 生存时间
        """
        if level is None:
            # 写入所有级别
            for cache_level, cache in self.caches.items():
                cache.set(key, value, ttl)
            self.stats['sets'] += 1
        else:
            # 只写入指定级别
            if level in self.caches:
                self.caches[level].set(key, value, ttl)
                self.stats['sets'] += 1

    def delete(self, key: str, level: CacheLevel = None) -> int:
        """
        删除缓存项

        Args:
            key: 缓存键
            level: 缓存级别（None表示所有级别）

        Returns:
            删除的缓存项数
        """
        deleted = 0

        if level is None:
            # 从所有级别删除
            for cache in self.caches.values():
                if cache.delete(key):
                    deleted += 1
        else:
            # 从指定级别删除
            if level in self.caches:
                if self.caches[level].delete(key):
                    deleted = 1

        if deleted > 0:
            self.stats['deletes'] += deleted

        return deleted

    def clear(self, level: CacheLevel = None):
        """清理缓存"""
        if level is None:
            for cache in self.caches.values():
                cache.clear()
        elif level in self.caches:
            self.caches[level].clear()

        logger.info(f"清理缓存: {level.value if level else 'all'}")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = dict(self.stats)

        # 添加各级缓存统计
        for level, cache in self.caches.items():
            level_stats = cache.get_stats()
            stats[f'{level.value}_stats'] = level_stats

        # 计算命中率
        total_hits = (stats.get('l1_hits', 0) +
                      stats.get('l2_hits', 0) +
                      stats.get('l3_hits', 0))
        total_access = total_hits + stats.get('misses', 0)

        if total_access > 0:
            stats['overall_hit_rate'] = total_hits / total_access
            stats['l1_hit_rate'] = (
                stats.get('l1_hits', 0) / total_access
                if total_access > 0 else 0
            )
        else:
            stats['overall_hit_rate'] = 0
            stats['l1_hit_rate'] = 0

        return stats

    def prefetch(self, keys: List[str]):
        """预取缓存项"""
        # 这里可以实现智能预取逻辑
        # 例如：基于访问模式预测需要的数据
        pass

    def optimize(self):
        """优化缓存配置"""
        # 基于访问模式调整缓存策略
        stats = self.get_stats()

        # 示例：如果L1命中率低，可能需要调整L1大小
        if stats.get('l1_hit_rate', 0) < 0.3:
            logger.info("L1命中率低，考虑调整缓存策略")

        # 示例：清理不常用的缓存
        for level, cache in self.caches.items():
            if isinstance(cache, BaseCacheStrategy):
                cache_stats = cache.get_stats()
                if cache_stats.get('hit_rate', 0) < 0.1:
                    logger.info(f"{level.value} 缓存命中率低，进行清理")
                    cache.clear()


class DiskCache(BaseCacheStrategy):
    """磁盘缓存实现"""

    def __init__(self, strategy_class, max_size: int, cache_dir: str):
        """
        初始化磁盘缓存

        Args:
            strategy_class: 缓存策略类
            max_size: 最大大小（MB）
            cache_dir: 缓存目录
        """
        super().__init__(max_size)
        self.strategy = strategy_class(max_size)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 索引文件
        self.index_file = self.cache_dir / 'index.json'
        self.index = self._load_index()

        # 启动清理线程
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True
        )
        self.cleanup_thread.start()

    def _load_index(self) -> Dict[str, Dict]:
        """加载索引"""
        if self.index_file.exists():
            try:
                with open(self.index_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_index(self):
        """保存索引"""
        try:
            with open(self.index_file, 'w') as f:
                json.dump(self.index, f)
        except Exception as e:
            logger.error(f"保存索引失败: {e}")

    def _get_cache_path(self, key: str) -> Path:
        """获取缓存文件路径"""
        # 使用哈希作为文件名
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"

    def get(self, key: str) -> Optional[Any]:
        """获取缓存项"""
        # 先检查内存索引
        if key not in self.index:
            self.stats['misses'] += 1
            return None

        # 检查是否过期
        item_info = self.index[key]
        created_at = datetime.fromisoformat(item_info['created_at'])
        ttl = timedelta(seconds=item_info.get('ttl', 0))

        if ttl and datetime.now() - created_at > ttl:
            # 已过期，删除
            self.delete(key)
            self.stats['misses'] += 1
            return None

        # 从磁盘加载
        cache_path = self._get_cache_path(key)
        if not cache_path.exists():
            self.delete(key)
            self.stats['misses'] += 1
            return None

        try:
            with open(cache_path, 'rb') as f:
                # 解压缩
                compressed_data = f.read()
                data = zlib.decompress(compressed_data)
                value = pickle.loads(data)

            # 更新访问信息
            item_info['last_accessed'] = datetime.now().isoformat()
            item_info['access_count'] = item_info.get('access_count', 0) + 1
            self._save_index()

            self.stats['hits'] += 1
            return value

        except Exception as e:
            logger.error(f"加载缓存失败 {key}: {e}")
            self.delete(key)
            self.stats['misses'] += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None,
            metadata: Optional[Dict] = None):
        """设置缓存项"""
        try:
            # 序列化和压缩数据
            data = pickle.dumps(value)
            compressed_data = zlib.compress(data)

            # 计算大小
            size = len(compressed_data)

            # 检查空间
            if self._needs_cleanup(size):
                self._cleanup(size)

            # 保存到磁盘
            cache_path = self._get_cache_path(key)
            with open(cache_path, 'wb') as f:
                f.write(compressed_data)

            # 更新索引
            self.index[key] = {
                'created_at': datetime.now().isoformat(),
                'last_accessed': datetime.now().isoformat(),
                'size': size,
                'ttl': ttl.total_seconds() if ttl else None,
                'metadata': metadata or {},
                'access_count': 0
            }
            self._save_index()

            # 更新统计
            self.stats['size'] += size
            self.stats['total_items'] += 1

        except Exception as e:
            logger.error(f"保存缓存失败 {key}: {e}")

    def delete(self, key: str) -> bool:
        """删除缓存项"""
        try:
            # 删除文件
            cache_path = self._get_cache_path(key)
            if cache_path.exists():
                cache_path.unlink()

            # 删除索引
            if key in self.index:
                item_info = self.index.pop(key)
                self.stats['size'] -= item_info.get('size', 0)
                self.stats['total_items'] -= 1
                self._save_index()
                return True

            return False

        except Exception as e:
            logger.error(f"删除缓存失败 {key}: {e}")
            return False

    def clear(self):
        """清理所有缓存"""
        try:
            # 删除所有缓存文件
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink()

            # 删除索引文件
            if self.index_file.exists():
                self.index_file.unlink()

            # 重置索引和统计
            self.index = {}
            self.stats['size'] = 0
            self.stats['total_items'] = 0

            logger.info("清理磁盘缓存")

        except Exception as e:
            logger.error(f"清理磁盘缓存失败: {e}")

    def _cleanup(self, required_space: int):
        """清理磁盘空间"""
        try:
            # 按访问时间排序
            sorted_items = sorted(
                self.index.items(),
                key=lambda x: x[1].get('last_accessed', '')
            )

            removed_size = 0
            keys_to_remove = []

            for key, item_info in sorted_items:
                if removed_size >= required_space:
                    break

                keys_to_remove.append(key)
                removed_size += item_info.get('size', 0)

            # 删除选中的项
            for key in keys_to_remove:
                self.delete(key)

            if keys_to_remove:
                self.stats['evictions'] += len(keys_to_remove)
                logger.debug(f"磁盘缓存清理: 移除 {len(keys_to_remove)} 个项")

        except Exception as e:
            logger.error(f"磁盘缓存清理失败: {e}")

    def _cleanup_loop(self):
        """定期清理循环"""
        while True:
            time.sleep(3600)  # 每小时清理一次

            try:
                # 清理过期项
                current_time = datetime.now()
                expired_keys = []

                for key, item_info in self.index.items():
                    created_at = datetime.fromisoformat(item_info['created_at'])
                    ttl = timedelta(seconds=item_info.get('ttl', 0))

                    if ttl and current_time - created_at > ttl:
                        expired_keys.append(key)

                # 批量删除过期项
                for key in expired_keys:
                    self.delete(key)

                if expired_keys:
                    logger.debug(f"清理了 {len(expired_keys)} 个过期缓存项")

            except Exception as e:
                logger.error(f"定期清理失败: {e}")


class CacheManager:
    """缓存管理器"""

    def __init__(self, config: Optional[Dict] = None):
        """
        初始化缓存管理器

        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.multi_level_cache = MultiLevelCache(config)

        # 缓存组（用于批量操作）
        self.cache_groups: Dict[str, Set[str]] = defaultdict(set)

        logger.info("初始化缓存管理器")

    def get(self, key: str, group: str = None) -> Optional[Any]:
        """
        获取缓存项

        Args:
            key: 缓存键
            group: 缓存组名

        Returns:
            缓存值或None
        """
        value = self.multi_level_cache.get(key)

        # 更新组信息
        if group and value is not None:
            self.cache_groups[group].add(key)

        return value

    def set(self, key: str, value: Any,
            level: CacheLevel = None,
            ttl: Optional[timedelta] = None,
            group: str = None,
            metadata: Optional[Dict] = None):
        """
        设置缓存项

        Args:
            key: 缓存键
            value: 缓存值
            level: 缓存级别
            ttl: 生存时间
            group: 缓存组名
            metadata: 元数据
        """
        self.multi_level_cache.set(key, value, level, ttl)

        # 更新组信息
        if group:
            self.cache_groups[group].add(key)

    def delete(self, key: str, level: CacheLevel = None) -> bool:
        """删除缓存项"""
        deleted = self.multi_level_cache.delete(key, level) > 0

        # 从所有组中移除
        if deleted:
            for group_keys in self.cache_groups.values():
                group_keys.discard(key)

        return deleted

    def delete_group(self, group: str) -> int:
        """
        删除缓存组

        Args:
            group: 缓存组名

        Returns:
            删除的缓存项数
        """
        if group not in self.cache_groups:
            return 0

        deleted = 0
        keys = list(self.cache_groups[group])

        for key in keys:
            if self.delete(key):
                deleted += 1

        # 移除组
        self.cache_groups.pop(group, None)

        logger.info(f"删除缓存组 '{group}'，移除了 {deleted} 个项")

        return deleted

    def clear_group(self, group: str):
        """清理缓存组（只清空，不删除组）"""
        if group in self.cache_groups:
            self.cache_groups[group].clear()

    def get_group_stats(self, group: str) -> Dict[str, Any]:
        """获取缓存组统计"""
        if group not in self.cache_groups:
            return {'size': 0, 'keys': []}

        keys = self.cache_groups[group]
        return {
            'size': len(keys),
            'keys': list(keys)
        }

    def get_all_groups(self) -> Dict[str, Set[str]]:
        """获取所有缓存组"""
        return dict(self.cache_groups)

    def prefetch_group(self, group: str):
        """预取缓存组"""
        if group in self.cache_groups:
            keys = self.cache_groups[group]
            self.multi_level_cache.prefetch(list(keys))

    def optimize(self):
        """优化缓存"""
        self.multi_level_cache.optimize()

        # 清理空组
        empty_groups = [
            group for group, keys in self.cache_groups.items()
            if not keys
        ]
        for group in empty_groups:
            self.cache_groups.pop(group)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.multi_level_cache.get_stats()
        stats['groups'] = {
            'total': len(self.cache_groups),
            'sizes': {g: len(k) for g, k in self.cache_groups.items()}
        }
        return stats

    def export_config(self) -> Dict[str, Any]:
        """导出配置"""
        return {
            'config': self.config,
            'stats': self.get_stats(),
            'groups': list(self.cache_groups.keys())
        }

    def import_config(self, config: Dict[str, Any]):
        """导入配置"""
        self.config.update(config.get('config', {}))
        logger.info("导入缓存配置")