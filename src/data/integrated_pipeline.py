# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\integrated_pipeline_fixed.py
# --修改为：File Path: E:/MyFile/stock_database_v1/src/data\integrated_pipeline.py
# File Name: integrated_pipeline
# @ Author: mango-gh22
# @ Date：2025/12/10 21:13
"""
desc 修复版数据管道 - 直接解决存储问题
integrated_pipeline_fixed.py-->integrated_pipeline.py
"""
# 完整替换为无污染版本

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import time
import sys
import importlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.logger import get_logger
from src.data.baostock_collector import BaostockCollector
from src.data.adaptive_storage import AdaptiveDataStorage
from src.data.enhanced_processor import EnhancedDataProcessor
from src.data.storage_tracer import StorageTracer  # v0.6.0 新增追踪器

logger = get_logger(__name__)

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class IntegratedDataPipeline:
    """v0.6.0 纯净版数据管道"""

    def __init__(self, config_path: Optional[str] = None):
        logger.info("🚀 初始化 v0.6.0 数据管道")

        # v0.6.0 增强：明确默认路径
        if config_path is None:
            from pathlib import Path
            config_path = str(Path(__file__).parent.parent.parent / "config" / "database.yaml")
            logger.info(f"使用默认配置路径: {config_path}")

        # 明确类型实例化
        self.collector = BaostockCollector()
        self.processor = EnhancedDataProcessor(config_path)
        self.storage = AdaptiveDataStorage(config_path)
        self.tracer = StorageTracer()  # v0.6.0 存储追踪器

        # 验证组件
        self._validate_components()

        logger.info("✅ 数据管道初始化完成")

    def _validate_components(self):
        """验证组件完整性"""
        checks = {
            'collector': hasattr(self.collector, 'fetch_daily_data'),
            'processor': hasattr(self.processor, 'process_stock_data'),
            'storage': hasattr(self.storage, 'store_daily_data'),
            'tracer': hasattr(self.tracer, 'trace_store_daily_data')
        }

        logger.info(f"组件验证: {checks}")

        if not all(checks.values()):
            missing = [k for k, v in checks.items() if not v]
            raise RuntimeError(f"组件缺失方法: {missing}")

    def process_single_stock(self, symbol: str, start_date: str, end_date: str, adjust: str = 'qfq') -> Dict[str, Any]:
        """处理单只股票 - v0.6.0 追踪版"""
        start_time = time.time()
        trace_id = f"{symbol}_{datetime.now().strftime('%H%M%S')}"

        try:
            logger.info(f"[{trace_id}] 📊 开始处理: {symbol}")

            # 1. 采集
            raw_data = self.collector.fetch_daily_data(
                # symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust
                symbol=symbol, start_date=start_date, end_date=end_date
            )

            if raw_data.empty:
                logger.warning(f"[{trace_id}] ⚠️ 无数据: {symbol}")
                return {'symbol': symbol, 'status': 'no_data', 'trace_id': trace_id}

            logger.info(f"[{trace_id}] 📥 采集完成: {len(raw_data)}条")

            # 2. 处理
            processed_data, quality_report = self.processor.process_stock_data(
                raw_data, symbol, 'baostock'
            )

            if processed_data.empty:
                logger.warning(f"[{trace_id}] ⚠️ 处理为空: {symbol}")
                return {'symbol': symbol, 'status': 'processed_empty', 'trace_id': trace_id}

            logger.info(f"[{trace_id}] 🔧 处理完成: {len(processed_data)}条")

            # 3. 存储（v0.6.0 追踪）
            affected_rows, storage_report = self.tracer.trace_store_daily_data(
                self.storage, processed_data
            )

            # 4. 验证一致性
            validation = storage_report.get('validation', {})
            if not validation.get('consistent', False):
                raise RuntimeError(f"存储验证失败: {validation}")

            execution_time = time.time() - start_time

            # 5. 记录日志
            self.storage.log_data_update(
                data_type='daily',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                rows_affected=affected_rows,
                status='success',
                execution_time=execution_time
            )

            logger.info(f"[{trace_id}] ✅ 完成: {symbol}, {affected_rows}行, {execution_time:.2f}s")

            return {
                'symbol': symbol,
                'status': 'success',
                'records': len(processed_data),
                'affected': affected_rows,
                'quality_score': quality_report.get('total_score', 0),
                'execution_time': execution_time,
                'trace_id': trace_id
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"[{trace_id}] ❌ 失败: {symbol}, {e}", exc_info=True)

            # 错误日志
            self.storage.log_data_update(
                data_type='daily',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                rows_affected=0,
                status='error',
                error_message=str(e),
                execution_time=execution_time
            )

            return {
                'symbol': symbol,
                'status': 'error',
                'reason': str(e),
                'trace_id': trace_id,
                'execution_time': execution_time
            }

    def batch_process(self, symbols: List[str], start_date: str, end_date: str, max_concurrent: int = 3) -> Dict[
        str, Any]:
        """批量处理 - v0.6.0"""
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"[{batch_id}] 批量处理 {len(symbols)} 只股票，并发数: {max_concurrent}")

        results = {
            'batch_id': batch_id,
            'total': len(symbols),
            'success': 0,
            'failed': 0,
            'total_rows': 0,
            'details': [],
            'start_time': datetime.now().isoformat()
        }

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {
                executor.submit(self.process_single_stock, sym, start_date, end_date): sym
                for sym in symbols
            }

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    results['details'].append(result)

                    if result['status'] == 'success':
                        results['success'] += 1
                        results['total_rows'] += result['affected']
                    else:
                        results['failed'] += 1

                except Exception as e:
                    results['failed'] += 1
                    results['details'].append({
                        'symbol': symbol,
                        'status': 'error',
                        'reason': str(e)
                    })

                processed = results['success'] + results['failed']
                if processed % 5 == 0:
                    logger.info(f"[{batch_id}] 进度: {processed}/{results['total']}")

        results['end_time'] = datetime.now().isoformat()
        results['duration'] = (datetime.fromisoformat(results['end_time']) -
                               datetime.fromisoformat(results['start_time'])).total_seconds()

        logger.info(f"[{batch_id}] ✅ 批量完成: 成功{results['success']}/{results['total']}, "
                    f"失败{results['failed']}, 总行数{results['total_rows']}, "
                    f"耗时{results['duration']:.2f}s")

        return results


# 测试函数
def test_pipeline():
    """测试管道"""
    print("\n🧪 测试 IntegratedDataPipeline (v0.6.0)")
    print("=" * 50)

    pipeline = IntegratedDataPipeline()
    result = pipeline.process_single_stock('sh600519', '20240101', '20240105')

    print(f"测试结果:")
    for key, value in result.items():
        print(f"  {key}: {value}")

    success = result['status'] == 'success'
    print(f"\n{'✅ 测试通过' if success else '❌ 测试失败'}")

    return success


if __name__ == "__main__":
    success = test_pipeline()
    import sys

    sys.exit(0 if success else 1)