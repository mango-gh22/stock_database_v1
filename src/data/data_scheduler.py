# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\data_scheduler.py
# @ Author: mango-gh22
# @ Date：2025/12/10 22:15
"""
desc 完整替换为 Baostock 专用版本调度器

Scheduler‌（调度程序）是计算机和管理领域中负责规划任务或资源的程序或角色，核心功能是优化执行顺序和资源分配。具体来说：
1. ‌计算机领域‌
‌操作系统调度程序‌：管理进程优先级和处理器时间分配，例如任务调度器。
‌分布式系统调度器‌：如YARN调度器，负责为应用分配集群资源。
‌React调度器‌：处理任务优先级和中断，确保浏览器空闲时继续执行。
2. ‌企业管理领域‌
‌生产调度员‌：制定生产计划，统筹资源配置和时间安排。
3. ‌其他场景‌
‌事件调度器‌：定义通用接口处理外部事件。
"""

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 完整替换为 Baostock 专用版本
from src.config.logging_config import setup_logging
# from src.data.akshare_collector import AKShareCollector  # 改为AKShare
from src.data.baostock_collector import BaostockCollector  # ✅ 改用 Baostock
from src.data.adaptive_storage import AdaptiveDataStorage  # ✅ 明确使用 Adaptive
# from src.data.data_storage import DataStorage
from src.data.import_csi_a50 import CSI_A50_Importer

logger = setup_logging()

class DataScheduler:
    """Baostock 数据采集调度器（v0.6.0）"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        self.collector = BaostockCollector()  # ✅ 实例化 Baostock
        self.storage = AdaptiveDataStorage(config_path)  # ✅ 明确 Adaptive

        # 中证A50 成分股（硬编码，避免依赖外部文件）
        self.csi_a50_symbols = [
            {'symbol': 'sh600519', 'name': '贵州茅台'},
            {'symbol': 'sh600036', 'name': '招商银行'},
            {'symbol': 'sh600309', 'name': '万华化学'},
            {'symbol': 'sh600900', 'name': '长江电力'},
            {'symbol': 'sh601318', 'name': '中国平安'},
            {'symbol': 'sh601899', 'name': '紫金矿业'},
            {'symbol': 'sh603288', 'name': '海天味业'},
            {'symbol': 'sh603501', 'name': '韦尔股份'},
            {'symbol': 'sh601166', 'name': '兴业银行'},
            {'symbol': 'sz000333', 'name': '美的集团'},
            {'symbol': 'sz000858', 'name': '五粮液'},
            {'symbol': 'sz002415', 'name': '海康威视'},
            {'symbol': 'sz002594', 'name': '比亚迪'},
            {'symbol': 'sz300059', 'name': '东方财富'},
            {'symbol': 'sz300750', 'name': '宁德时代'},
        ]

        logger.info(f"v0.6.0 调度器初始化: {len(self.csi_a50_symbols)} 只A50成分股")

    def collect_daily_data_for_symbol(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """采集单只股票日线数据（v0.6.0 追踪版）"""
        result = {
            'symbol': symbol,
            'success': False,
            'rows_affected': 0,
            'error': None,
            'trace_id': f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }

        try:
            logger.info(f"[{result['trace_id']}] 开始采集: {symbol} [{start_date} - {end_date}]")

            # 1. 获取数据
            df = self.collector.fetch_daily_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                # 2. v0.6.0 使用存储追踪器
                from src.data.storage_tracer import StorageTracer
                tracer = StorageTracer()

                affected_rows, storage_report = tracer.trace_store_daily_data(
                    self.storage, df
                )

                # 3. 记录日志（v0.6.0 兼容接口）
                self.storage.log_data_update(
                    data_type='daily',
                    symbol=symbol,
                    rows_affected=affected_rows,
                    status='success' if affected_rows > 0 else 'partial',
                    execution_time=storage_report.get('execution_time', 0)
                )

                result['success'] = True
                result['rows_affected'] = affected_rows
                result['storage_report'] = storage_report

                logger.info(f"[{result['trace_id']}] ✅ 完成: {symbol}, {affected_rows}行")
            else:
                result['error'] = '未获取到数据'
                logger.warning(f"[{result['trace_id']}] ⚠️ 无数据: {symbol}")

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"[{result['trace_id']}] ❌ 失败: {symbol}, {e}", exc_info=True)

            # 记录失败日志
            try:
                self.storage.log_data_update(
                    data_type='daily',
                    symbol=symbol,
                    rows_affected=0,
                    status='error',
                    error_message=str(e)
                )
            except:
                pass

        return result

    def collect_all_daily_data(self, symbols: List[str] = None, max_workers: int = 3) -> Dict[str, Any]:
        """批量采集所有股票日线数据（v0.6.0）"""
        if not symbols:
            symbols = [stock['symbol'] for stock in self.csi_a50_symbols]

        logger.info(f"批量采集 {len(symbols)} 只股票，并发数: {max_workers}")

        results = {
            'total': len(symbols),
            'success': 0,
            'failed': 0,
            'total_rows': 0,
            'failed_symbols': [],
            'trace_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for symbol in symbols:
                # 默认采集最近30天
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

                future = executor.submit(
                    self.collect_daily_data_for_symbol,
                    symbol, start_date, end_date
                )
                futures[future] = symbol

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result['success']:
                        results['success'] += 1
                        results['total_rows'] += result['rows_affected']
                    else:
                        results['failed'] += 1
                        results['failed_symbols'].append({
                            'symbol': symbol,
                            'error': result['error']
                        })
                except Exception as e:
                    results['failed'] += 1
                    results['failed_symbols'].append({
                        'symbol': symbol,
                        'error': str(e)
                    })

                # 进度日志
                processed = results['success'] + results['failed']
                if processed % 5 == 0:
                    logger.info(f"进度: {processed}/{results['total']}")

        results['duration'] = time.time() - start_time

        logger.info(
            f"批量完成: 成功{results['success']}/{results['total']}, "
            f"失败{results['failed']}, 总行数{results['total_rows']}, "
            f"耗时{results['duration']:.2f}s"
        )

        return results

    def run_demo_collection(self):
        """运行演示采集（v0.6.0）"""
        print("\n🚀 P3阶段演示：Baostock数据采集")
        print("=" * 50)

        # 只采集前3只测试
        test_symbols = [stock['symbol'] for stock in self.csi_a50_symbols[:3]]

        print(f"测试采集 {len(test_symbols)} 只股票:")
        for symbol in test_symbols:
            print(f"  - {symbol}")

        results = self.collect_all_daily_data(test_symbols, max_workers=2)

        print(f"\n📊 采集结果:")
        print(f"  成功: {results['success']} 只")
        print(f"  失败: {results['failed']} 只")
        print(f"  总数据行: {results['total_rows']}")

        if results['failed_symbols']:
            print(f"\n❌ 失败股票:")
            for fail in results['failed_symbols']:
                print(f"    {fail['symbol']}: {fail['error']}")

        # 验证数据库
        print("\n🔍 数据库验证...")
        try:
            with self.storage.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) as cnt FROM stock_daily_data")
                    total_records = cursor.fetchone()[0]
                    print(f"  数据库日线数据总量: {total_records} 条")

                    # 验证刚插入的数据
                    for symbol in test_symbols:
                        cursor.execute(
                            "SELECT COUNT(*) as cnt FROM stock_daily_data WHERE symbol = %s",
                            (symbol,)
                        )
                        symbol_count = cursor.fetchone()[0]
                        print(f"  {symbol}: {symbol_count} 条")
        except Exception as e:
            print(f"  ⚠️  查询失败: {e}")

        print("\n✅ P3阶段演示完成！")


# 测试函数
def test_scheduler():
    """测试调度器"""
    print("🧪 测试 DataScheduler (v0.6.0)")

    scheduler = DataScheduler()
    result = scheduler.collect_daily_data_for_symbol('sh600519', '20240101', '20240105')

    print(f"测试结果: {result}")
    return result['success']


if __name__ == "__main__":
    success = test_scheduler()
    sys.exit(0 if success else 1)
