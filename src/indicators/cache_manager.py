# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\cache_manager.py
# File Name: cache_manager
# @ Author: mango-gh22
# @ Date：2025/12/21 8:58
"""
desc 完善缓存管理系统
File: src/indicators/cache_manager.py
Desc: 指标缓存管理器 - 支持磁盘缓存、内存缓存和过期策略
"""
import os
import json
import pickle
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class IndicatorCacheManager:
    """指标缓存管理器"""

    def __init__(self, cache_dir: str = "data/cache/indicators"):
        """
        初始化缓存管理器

        Args:
            cache_dir: 缓存目录路径
        """
        self.cache_dir = Path(cache_dir)
        self.memory_cache: Dict[str, Any] = {}
        self.cache_metadata: Dict[str, Dict] = {}

        # 确保缓存目录存在
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 缓存配置
        self.memory_ttl = 3600  # 内存缓存1小时
        self.disk_ttl = 86400  # 磁盘缓存24小时
        self.max_memory_items = 1000  # 最大内存缓存项数

        # 加载缓存元数据
        self._load_metadata()

    def _get_cache_key(self, symbol: str, indicator_name: str,
                       parameters: Dict, start_date: str, end_date: str) -> str:
        """生成缓存键"""
        cache_str = f"{symbol}_{indicator_name}_{json.dumps(parameters, sort_keys=True)}_\
                     {start_date}_{end_date}"
        return hashlib.md5(cache_str.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """获取缓存文件路径"""
        return self.cache_dir / f"{cache_key}.pkl"

    def _get_metadata_path(self) -> Path:
        """获取元数据文件路径"""
        return self.cache_dir / "metadata.json"

    def _load_metadata(self):
        """加载缓存元数据"""
        metadata_path = self._get_metadata_path()
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    self.cache_metadata = json.load(f)
            except Exception as e:
                logger.warning(f"加载缓存元数据失败: {e}")
                self.cache_metadata = {}

    def _save_metadata(self):
        """保存缓存元数据"""
        try:
            with open(self._get_metadata_path(), 'w') as f:
                json.dump(self.cache_metadata, f, indent=2)
        except Exception as e:
            logger.error(f"保存缓存元数据失败: {e}")

    def _is_cache_valid(self, cache_key: str, cache_type: str = 'disk') -> bool:
        """检查缓存是否有效"""
        if cache_key not in self.cache_metadata:
            return False

        metadata = self.cache_metadata[cache_key]
        created_at = datetime.fromisoformat(metadata['created_at'])

        # 检查过期时间
        ttl = self.memory_ttl if cache_type == 'memory' else self.disk_ttl
        if datetime.now() - created_at > timedelta(seconds=ttl):
            return False

        return True

    def get(self, symbol: str, indicator_name: str,
            parameters: Dict, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取缓存数据

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            parameters: 指标参数
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            缓存的数据或None
        """
        cache_key = self._get_cache_key(symbol, indicator_name,
                                        parameters, start_date, end_date)

        # 1. 检查内存缓存
        if cache_key in self.memory_cache and self._is_cache_valid(cache_key, 'memory'):
            logger.debug(f"从内存缓存获取: {cache_key}")
            return self.memory_cache[cache_key]

        # 2. 检查磁盘缓存
        cache_path = self._get_cache_path(cache_key)
        if cache_path.exists() and self._is_cache_valid(cache_key, 'disk'):
            try:
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)

                # 放入内存缓存
                self.memory_cache[cache_key] = data
                logger.debug(f"从磁盘缓存获取: {cache_key}")
                return data
            except Exception as e:
                logger.warning(f"读取磁盘缓存失败: {e}")

        return None

    def set(self, symbol: str, indicator_name: str, parameters: Dict,
            start_date: str, end_date: str, data: pd.DataFrame):
        """
        设置缓存数据

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            parameters: 指标参数
            start_date: 开始日期
            end_date: 结束日期
            data: 要缓存的数据
        """
        cache_key = self._get_cache_key(symbol, indicator_name,
                                        parameters, start_date, end_date)

        # 1. 设置内存缓存
        self.memory_cache[cache_key] = data

        # 管理内存缓存大小
        if len(self.memory_cache) > self.max_memory_items:
            # 移除最早的缓存项
            keys = list(self.memory_cache.keys())
            for key in keys[:100]:  # 一次移除100个
                self.memory_cache.pop(key, None)

        # 2. 设置磁盘缓存
        try:
            cache_path = self._get_cache_path(cache_key)
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)

            # 更新元数据
            self.cache_metadata[cache_key] = {
                'symbol': symbol,
                'indicator': indicator_name,
                'parameters': parameters,
                'start_date': start_date,
                'end_date': end_date,
                'created_at': datetime.now().isoformat(),
                'data_shape': data.shape,
                'data_columns': list(data.columns)
            }

            self._save_metadata()
            logger.debug(f"设置缓存: {cache_key}, 形状: {data.shape}")

        except Exception as e:
            logger.error(f"设置磁盘缓存失败: {e}")

    def clear(self, cache_type: str = 'all'):
        """
        清理缓存

        Args:
            cache_type: 缓存类型，'memory'、'disk' 或 'all'
        """
        if cache_type in ['memory', 'all']:
            self.memory_cache.clear()
            logger.info("清理内存缓存")

        if cache_type in ['disk', 'all']:
            # 删除所有缓存文件
            for cache_file in self.cache_dir.glob("*.pkl"):
                try:
                    cache_file.unlink()
                except Exception as e:
                    logger.error(f"删除缓存文件失败 {cache_file}: {e}")

            # 删除元数据
            metadata_path = self._get_metadata_path()
            if metadata_path.exists():
                metadata_path.unlink()

            self.cache_metadata.clear()
            logger.info("清理磁盘缓存")

    def get_cache_stats(self) -> Dict:
        """
        获取缓存统计信息

        Returns:
            缓存统计字典
        """
        memory_count = len(self.memory_cache)
        disk_count = len(self.cache_metadata)

        # 计算缓存命中率（简化版本）
        total_hits = sum(meta.get('hits', 0) for meta in self.cache_metadata.values())

        return {
            'memory_cache_items': memory_count,
            'disk_cache_items': disk_count,
            'total_hits': total_hits,
            'cache_dir': str(self.cache_dir),
            'memory_ttl': self.memory_ttl,
            'disk_ttl': self.disk_ttl
        }

    def cleanup_expired(self):
        """清理过期的缓存"""
        expired_keys = []
        current_time = datetime.now()

        for cache_key, metadata in self.cache_metadata.items():
            created_at = datetime.fromisoformat(metadata['created_at'])
            if current_time - created_at > timedelta(seconds=self.disk_ttl):
                expired_keys.append(cache_key)

        for cache_key in expired_keys:
            # 从内存缓存移除
            self.memory_cache.pop(cache_key, None)

            # 从磁盘缓存移除
            cache_path = self._get_cache_path(cache_key)
            if cache_path.exists():
                try:
                    cache_path.unlink()
                except Exception as e:
                    logger.error(f"删除过期缓存文件失败 {cache_path}: {e}")

            # 从元数据移除
            self.cache_metadata.pop(cache_key, None)

        if expired_keys:
            self._save_metadata()
            logger.info(f"清理了 {len(expired_keys)} 个过期缓存项")