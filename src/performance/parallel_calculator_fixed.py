# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\parallel_calculator_fixed.py
# File Name: parallel_calculator_fixed
# @ Author: mango-gh22
# @ Date：2025/12/22 0:55
"""
desc 
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance/parallel_calculator_fixed.py

from typing import Callable, List, Any, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class ParallelCalculatorFixed:
    """修复的并行计算器 - 支持字典配置"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # 支持两种调用方式：字典或标量
        if isinstance(config, dict):
            # 从字典中提取配置
            self.enabled = bool(config.get('enabled', True))
            self.max_workers = int(config.get('max_workers', 4))
            self.timeout = int(config.get('timeout', 300))
            self.mode = config.get('mode', 'thread')
            self.batch_size = int(config.get('batch_size', 50))
        else:
            # 传统方式
            self.enabled = True
            self.max_workers = 4
            self.timeout = 300
            self.mode = 'thread'
            self.batch_size = 50

        logger.info(f"并行计算器初始化: enabled={self.enabled}, workers={self.max_workers}")

    def calculate(self, func: Callable, data: List, *args, **kwargs):
        if not self.enabled or len(data) <= 1 or self.max_workers <= 1:
            # 串行计算
            return [func(item, *args, **kwargs) for item in data]

        try:
            # 简单的并行实现
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任务
                future_to_item = {
                    executor.submit(func, item, *args, **kwargs): item
                    for item in data
                }

                # 收集结果
                results = []
                for future in as_completed(future_to_item, timeout=self.timeout):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"并行任务失败: {e}")
                        # 对于失败的任务，使用串行计算
                        item = future_to_item[future]
                        results.append(func(item, *args, **kwargs))

                return results
        except Exception as e:
            logger.error(f"并行计算异常: {e}")
            return [func(item, *args, **kwargs) for item in data]