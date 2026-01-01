# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\cache_strategy_fixed.py
# File Name: cache_strategy_fixed
# @ Author: mango-gh22
# @ Date：2025/12/22 0:54
"""
desc 
"""

# src/performance/cache_strategy_fixed.py
"""
修复的缓存管理器
"""
import logging
import time
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)


class CacheManagerFixed:
    def __init__(self, config):
        # 确保配置值是标量
        self.enabled = bool(config.get('enabled', True))
        self.max_size = int(config.get('max_size', 1000))
        self.ttl = int(config.get('ttl', 3600))

        self.cache = {}
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'evictions': 0
        }

        logger.info(f"缓存管理器初始化: enabled={self.enabled}, max_size={self.max_size}")

    def get(self, key: str) -> Optional[Any]:
        if not self.enabled:
            return None

        if key in self.cache:
            item = self.cache[key]
            expire_time = item.get('expire', 0)

            if expire_time > time.time():  # 这里比较的是两个数字
                self.stats['hits'] += 1
                return item.get('value')
            else:
                del self.cache[key]
                self.stats['evictions'] += 1

        self.stats['misses'] += 1
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None, group: Optional[str] = None) -> bool:
        if not self.enabled:
            return False

        # 检查缓存大小（比较两个整数）
        if len(self.cache) >= self.max_size:
            # 淘汰最旧的
            if self.cache:
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
                self.stats['evictions'] += 1

        expire_time = time.time() + (ttl or self.ttl)
        self.cache[key] = {
            'value': value,
            'expire': expire_time,
            'group': group,
            'created': time.time()
        }
        self.stats['sets'] += 1
        return True

    def get_cache_stats(self) -> Dict[str, Any]:
        total = self.stats['hits'] + self.stats['misses']
        hit_rate = self.stats['hits'] / total if total > 0 else 0

        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'hit_rate': hit_rate,
            'enabled': self.enabled
        }