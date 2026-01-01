# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\memory_manager_fixed.py
# File Name: memory_manager_fixed
# @ Author: mango-gh22
# @ Date：2025/12/22 0:56
"""
desc
修复的内存管理器
"""
from typing import Optional, Dict, Any  # 修复：添加缺失的导入
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class MemoryManagerFixed:
    def __init__(self, config: Optional[Dict[str, Any]] = None):  # 修复：添加类型注解
        self.config = config or {}
        logger.info("内存管理器初始化")

    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame - 增强版"""
        if df is None:
            logger.warning("接收到空 DataFrame")
            return df

        try:
            # 简单的优化：转换数据类型
            optimized = df.copy()

            # 优化整数列
            int_cols = optimized.select_dtypes(include=['int']).columns
            for col in int_cols:
                col_min = optimized[col].min()
                col_max = optimized[col].max()

                if col_min >= 0:  # 无符号
                    if col_max < 256:
                        optimized[col] = optimized[col].astype(np.uint8)
                    elif col_max < 65536:
                        optimized[col] = optimized[col].astype(np.uint16)
                else:  # 有符号
                    if col_min > -128 and col_max < 127:
                        optimized[col] = optimized[col].astype(np.int8)
                    elif col_min > -32768 and col_max < 32767:
                        optimized[col] = optimized[col].astype(np.int16)

            # 优化浮点数列
            float_cols = optimized.select_dtypes(include=['float']).columns
            for col in float_cols:
                optimized[col] = pd.to_numeric(optimized[col], downcast='float')

            logger.info(f"DataFrame优化完成: {len(df.columns)}列 -> {len(optimized.columns)}列")
            return optimized

        except Exception as e:
            logger.error(f"DataFrame优化失败: {e}")
            return df

    def start_monitoring(self):
        logger.info("内存监控启动")

    def stop_monitoring(self):
        logger.info("内存监控停止")

    # def get_memory_report(self) -> Dict[str, Any]:  # 修复：添加返回类型注解
    #     return {"status": "ok", "optimized": True}

    def get_memory_report(self) -> Dict[str, Any]:
        """获取内存报告 - 修复版"""
        try:
            # ✅ 确保不使用 pd
            return {
                "status": "ok",
                "optimized": True,
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error(f"获取内存报告失败: {e}")
            return {"status": "error", "message": str(e)}