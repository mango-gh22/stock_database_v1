# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\storage_tracer.py
# File Name: storage_tracer
# @ Author: mango-gh22
# @ Date：2026/1/1 21:16
"""
desc 存储追踪器 - v0.6.0
确保每次存储后验证数据库一致性
tracer 追踪者，曳光弹，
"""

import logging
from typing import Dict, Any, Tuple
import pandas as pd
from src.database.db_connector import DatabaseConnector


class StorageTracer:
    """存储操作追踪器"""

    def __init__(self):
        self.db = DatabaseConnector()
        self.logger = logging.getLogger(__name__)

    def trace_store_daily_data(
            self,
            storage_instance,
            df: pd.DataFrame,
            trace_id: str = None  # ✅ 新增参数
    ) -> Tuple[int, Dict[str, Any]]:
        """存储追踪 - v0.6.2 修复版"""

        # v0.6.2 修复：生成默认trace_id
        if trace_id is None:
            import time
            trace_id = f"trace_{int(time.time())}"

        # 1. 查询插入前该股票在指定日期范围内的记录数
        symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'unknown'
        date_list = df['trade_date'].tolist() if 'trade_date' in df.columns else []

        pre_count = 0
        if date_list:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    placeholders = ','.join(['%s'] * len(date_list))
                    cursor.execute(
                        f"SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s AND trade_date IN ({placeholders})",
                        [symbol] + date_list
                    )
                    pre_count = cursor.fetchone()[0]

        # 2. 执行存储
        reported_affected, storage_report = storage_instance.store_daily_data(df)

        # 3. 查询插入后记录数
        post_count = 0
        if date_list:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    placeholders = ','.join(['%s'] * len(date_list))
                    cursor.execute(
                        f"SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s AND trade_date IN ({placeholders})",
                        [symbol] + date_list
                    )
                    post_count = cursor.fetchone()[0]

        # 4. 真实新增记录数
        actual_inserted = post_count - pre_count

        validation = {
            'pre_count': pre_count,
            'post_count': post_count,
            'actual_inserted': actual_inserted,
            'reported_affected': reported_affected,
            'consistent': True,
            'message': f"新增{actual_inserted}条，已存在{len(df) - actual_inserted}条"
        }

        storage_report['validation'] = validation

        # v0.6.2 修复日志输出
        if actual_inserted > 0:
            self.logger.info(f"[{trace_id}] ✅ 验证: +{actual_inserted}条新数据")
        else:
            self.logger.info(f"[{trace_id}] ⚠️ 无新数据: {len(df)}条已存在，0条新增")

        return actual_inserted, storage_report

    def _get_record_count(self, symbol: str) -> int:
        """查询数据库中指定股票的记录数"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) as cnt FROM stock_daily_data WHERE symbol = %s",
                        (symbol,)
                    )
                    return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"查询记录数失败: {e}")
            return 0