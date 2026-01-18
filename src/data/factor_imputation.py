# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_imputation.py
# File Name: factor_imputation
# @ Author: mango-gh22
# @ Date：2026/1/18 13:41
"""
desc
imputation 归责，归因 统计学中，指缺失数据填补技术
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_imputation.py
"""
因子智能补全引擎
基于业务规则决定是否补全，并记录审计日志
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
import yaml
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np

from src.database.db_connector import DatabaseConnector
from src.utils.enhanced_trade_date_manager import get_enhanced_trade_date_manager

logger = logging.getLogger(__name__)


class FactorImputationEngine:
    """因子智能补全引擎"""

    def __init__(self, config_path: str = 'config/factor_imputation.yaml'):
        self.config = self._load_config(config_path)
        self.db = DatabaseConnector()
        self.trade_manager = get_enhanced_trade_date_manager()
        logger.info(f"初始化因子补全引擎，模式: {self.config['imputation_mode']}")

    def _load_config(self, config_path: str) -> Dict:
        """加载配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"加载配置失败 {config_path}: {e}，使用默认配置")
            return {
                'imputation_mode': 'conditional',
                'allowable_scenarios': {
                    'single_day_missing': True,
                    'latest_data_missing': True
                },
                'imputation_method': 'forward_fill',
                'audit_log': {'enabled': True}
            }

    def should_impute(self, symbol: str, trade_date: str, missing_fields: List[str]) -> Tuple[bool, str]:
        """
        判断是否应该补全因子

        Returns:
            (是否补全, 原因说明)
        """
        reason_parts = []

        # 规则1：检查是否是最新数据（必须补全）
        latest_trade_date = self.trade_manager.get_last_trade_date_str()
        if trade_date.strftime('%Y%m%d') >= latest_trade_date:
            reason_parts.append("最新交易日数据")

        # 规则2：检查是否单日缺失
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as consecutive_missing
                    FROM stock_daily_data
                    WHERE symbol = %s
                      AND trade_date >= DATE_SUB(%s, INTERVAL 5 DAY)
                      AND (pb IS NULL OR pe_ttm IS NULL)
                    ORDER BY trade_date DESC
                    LIMIT 5
                """, (symbol, trade_date))
                result = cursor.fetchone()

                if result and result[0] <= 1:
                    reason_parts.append("单日缺失")

        # 规则3：检查是否少数股票缺失（<5只）
        cursor.execute("""
            SELECT COUNT(DISTINCT symbol) as missing_stocks
            FROM stock_daily_data
            WHERE trade_date = %s
              AND (pb IS NULL OR pe_ttm IS NULL)
        """, (trade_date,))
        result = cursor.fetchone()

        if result and result[0] < 5:
            reason_parts.append(f"少数股票缺失({result[0]}只)")

        # 综合判断
        if reason_parts:
            return True, " ∧ ".join(reason_parts)

        return False, "不符合补全条件"

    def impute_factors(self, symbol: str, trade_date: str, fields: List[str] = None) -> Dict[str, float]:
        """
        执行因子补全

        方法：
        1. 前值填充（优先）
        2. 行业均值填充（备选）
        """
        if fields is None:
            fields = ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm']

        imputed_values = {}

        with self.db.get_connection() as conn:
            for field in fields:
                # 前值填充
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT {field}
                    FROM stock_daily_data
                    WHERE symbol = %s
                      AND trade_date < %s
                      AND {field} IS NOT NULL AND {field} != 0
                    ORDER BY trade_date DESC
                    LIMIT 1
                """, (symbol, trade_date))
                result = cursor.fetchone()

                if result and result[0] is not None:
                    imputed_values[field] = float(result[0])
                    self._log_imputation(symbol, trade_date, field, 'forward_fill', result[0])
                else:
                    # 行业均值填充（备选）
                    industry_avg = self._get_industry_average(symbol, trade_date, field)
                    if industry_avg is not None:
                        imputed_values[field] = industry_avg
                        self._log_imputation(symbol, trade_date, field, 'industry_avg', industry_avg)
                    else:
                        logger.warning(f"无法补全 {symbol} {trade_date} {field}")
                        imputed_values[field] = None

        return imputed_values

    def _get_industry_average(self, symbol: str, trade_date: str, field: str) -> float:
        """获取行业均值（简化版）"""
        # 实际项目中应实现行业分类查询
        return None

    def _log_imputation(self, symbol: str, trade_date: str, field: str, method: str, value: float):
        """记录补全日志"""
        if not self.config['audit_log']['enabled']:
            return

        log_msg = f"因子补全 | {symbol} | {trade_date} | {field} | {method} | {value:.4f}"
        logger.info(log_msg)

        # 可选：写入审计表
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO factor_imputation_audit 
                    (symbol, trade_date, factor_field, imputation_method, imputed_value, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (symbol, trade_date, field, method, value))
                conn.commit()
        except Exception as e:
            logger.debug(f"审计日志写入失败: {e}")


def batch_impute_latest_data():
    """
    批量补全最新数据
    仅补全最新交易日的缺失因子
    """
    print("\n🔧 批量补全最新数据因子")
    print("=" * 60)

    engine = FactorImputationEngine()
    db = DatabaseConnector()

    # 查询最新交易日需要补全的股票
    with db.get_connection() as conn:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT symbol, trade_date, 
                       CASE WHEN pb IS NULL THEN 'pb' END as missing_pb,
                       CASE WHEN pe_ttm IS NULL THEN 'pe_ttm' END as missing_pe,
                       CASE WHEN ps_ttm IS NULL THEN 'ps_ttm' END as missing_ps,
                       CASE WHEN pcf_ttm IS NULL THEN 'pcf_ttm' END as missing_pcf
                FROM stock_daily_data
                WHERE trade_date = (SELECT MAX(trade_date) FROM stock_daily_data)
                  AND (pb IS NULL OR pe_ttm IS NULL OR ps_ttm IS NULL OR pcf_ttm IS NULL)
            """)

            rows = cursor.fetchall()

    if not rows:
        print("✅ 最新数据无需补全")
        return

    print(f"发现 {len(rows)} 只股票需要补全因子")

    # 执行补全
    updated_count = 0
    for row in rows:
        symbol = row['symbol']
        trade_date = row['trade_date']
        missing_fields = [f for f in ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm'] if row[f'missing_{f}'] is not None]

        if not missing_fields:
            continue

        should_impute, reason = engine.should_impute(symbol, trade_date, missing_fields)

        if should_impute:
            print(f"\n{symbol} {trade_date} - 补全原因: {reason}")
            imputed_values = engine.impute_factors(symbol, trade_date, missing_fields)

            # 更新数据库
            with db.get_connection() as conn:
                cursor = conn.cursor()
                for field, value in imputed_values.items():
                    if value is not None:
                        cursor.execute(f"""
                            UPDATE stock_daily_data 
                            SET {field} = %s, updated_time = NOW()
                            WHERE symbol = %s AND trade_date = %s
                        """, (value, symbol, trade_date))
                conn.commit()

            updated_count += 1
            print(f"✅ 补全 {len([v for v in imputed_values.values() if v is not None])} 个因子")
        else:
            print(f"⏭️  {symbol} {trade_date} - 跳过补全")

    print(f"\n" + "=" * 60)
    print(f"🎉 补全完成: {updated_count} 只股票因子已补全")
    print("=" * 60)


if __name__ == "__main__":
    batch_impute_latest_data()