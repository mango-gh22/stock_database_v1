# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_data_pipeline.py
# File Name: factor_data_pipeline
# @ Author: mango-gh22
# @ Date：2026/1/3 12:42
"""
desc 因子数据完整管道 - 集成下载、存储、增量更新
支持交易日管理，基于真实交易日进行数据下载
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import time
import sys
import os

# 添加项目路径
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.utils.enhanced_trade_date_manager import EnhancedTradeDateManager
from src.config.logging_config import setup_logging

logger = setup_logging()


class FactorDataPipeline:
    """
    因子数据完整管道 - 集成下载、存储、增量更新
    支持交易日管理，确保基于真实交易日进行数据操作
    """

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化因子数据管道

        Args:
            config_path: 配置文件路径
        """
        # 初始化组件
        self.downloader = BaostockPBFactorDownloader()
        self.storage = FactorStorageManager(config_path)
        self.trade_date_manager = EnhancedTradeDateManager()

        # 统计信息
        self.stats = {
            'total_symbols': 0,
            'successful': 0,
            'failed': 0,
            'no_data': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0
        }

        logger.info("✅ 因子数据管道初始化完成")

    def _adjust_date_range(self, start_date: str, end_date: str) -> Tuple[str, str]:
        """
        调整日期范围为真实的交易日范围

        Args:
            start_date: 原始开始日期 (YYYYMMDD)
            end_date: 原始结束日期 (YYYYMMDD)

        Returns:
            调整后的(开始日期, 结束日期)
        """
        try:
            # 如果交易日管理器没有相应方法，使用下载器自带的调整
            if hasattr(self.trade_date_manager, 'adjust_to_trade_date'):
                # 调整开始日期为最近的交易日（向后调整）
                adjusted_start = self.trade_date_manager.adjust_to_trade_date(start_date, direction='backward')
                if adjusted_start != start_date:
                    logger.info(f"调整开始日期: {start_date} -> {adjusted_start}")
                    start_date = adjusted_start

                # 调整结束日期为最近的交易日（向前调整）
                adjusted_end = self.trade_date_manager.adjust_to_trade_date(end_date, direction='backward')
                if adjusted_end != end_date:
                    logger.info(f"调整结束日期: {end_date} -> {adjusted_end}")
                    end_date = adjusted_end
            else:
                # 使用下载器的交易日检查
                logger.debug("使用下载器的交易日检查")
                # 这里依赖下载器内部的交易日调整

            # 验证日期范围
            try:
                start_dt = datetime.strptime(start_date, '%Y%m%d')
                end_dt = datetime.strptime(end_date, '%Y%m%d')

                if start_dt > end_dt:
                    logger.warning(f"开始日期晚于结束日期，交换: {start_date} <-> {end_date}")
                    start_date, end_date = end_date, start_date

            except ValueError as e:
                logger.warning(f"日期格式验证失败: {e}")

            return start_date, end_date

        except Exception as e:
            logger.warning(f"日期范围调整失败: {e}, 使用原始范围")
            return start_date, end_date

    def update_single_symbol(self, symbol: str, mode: str = 'incremental',
                             start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """
        更新单只股票的因子数据

        Args:
            symbol: 股票代码
            mode: 更新模式 ('incremental', 'full', 'specific')
            start_date: 特定开始日期 (仅mode='specific'时使用)
            end_date: 特定结束日期 (仅mode='specific'时使用)

        Returns:
            更新结果字典
        """
        result = {
            'symbol': symbol,
            'mode': mode,
            'status': 'pending',
            'records_downloaded': 0,
            'records_stored': 0,
            'error': None,
            'execution_time': 0
        }

        start_time = time.time()

        try:
            logger.info(f"📊 开始处理: {symbol} ({mode}模式)")

            # 1. 确定下载范围
            if mode == 'incremental':
                # 增量模式：基于数据库已有数据
                download_range = self.storage.calculate_incremental_range(symbol)
                if not download_range or not download_range[0]:
                    result['status'] = 'no_update_needed'
                    result['reason'] = '数据已最新'
                    logger.info(f"  {symbol}: 数据已最新，跳过")
                    return result

                start_date, end_date = download_range

            elif mode == 'full':
                # 全量模式：默认下载最近2年数据
                if not end_date:
                    end_date = datetime.now().strftime('%Y%m%d')
                if not start_date:
                    # 默认下载最近2年
                    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y%m%d')

            elif mode == 'specific' and start_date and end_date:
                # 特定范围模式
                pass
            else:
                raise ValueError(f"无效的模式或日期参数: mode={mode}, start_date={start_date}, end_date={end_date}")

            # 2. 调整日期范围为交易日
            start_date, end_date = self._adjust_date_range(start_date, end_date)
            logger.info(f"  下载范围: {start_date} - {end_date}")

            # 3. 下载因子数据
            logger.info(f"  下载因子数据...")
            factor_data = self.downloader.fetch_factor_data(symbol, start_date, end_date)

            if factor_data.empty:
                result['status'] = 'no_data'
                logger.warning(f"  {symbol}: 无数据")
                return result

            result['records_downloaded'] = len(factor_data)
            logger.info(f"  下载成功: {len(factor_data)} 条记录")

            # 4. 存储数据
            logger.info(f"  存储因子数据...")
            affected_rows, storage_report = self.storage.store_factor_data(factor_data)

            result['records_stored'] = affected_rows
            result['storage_report'] = storage_report

            if affected_rows > 0:
                result['status'] = 'success'
                logger.info(f"  ✅ 存储成功: {affected_rows} 条记录")
            else:
                result['status'] = 'skipped'
                result['reason'] = '数据已存在或无更新'
                logger.info(f"  ⚠️  无新记录: {symbol}")

            # 5. 清理缓存
            self.storage.clear_cache(symbol)

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            logger.error(f"  ❌ 处理失败 {symbol}: {e}")

        finally:
            # 计算执行时间
            result['execution_time'] = time.time() - start_time
            logger.info(f"  处理完成: {symbol}, 耗时: {result['execution_time']:.2f}秒")

        return result

    def update_batch_symbols(self, symbols: List[str], mode: str = 'incremental',
                             start_date: str = None, end_date: str = None,
                             max_workers: int = 1) -> Dict[str, Any]:
        """
        批量更新多只股票的因子数据

        注意：由于Baostock平台限制，这里保持单线程
        但接口设计为支持未来多线程扩展

        Args:
            symbols: 股票代码列表
            mode: 更新模式
            start_date: 特定开始日期
            end_date: 特定结束日期
            max_workers: 最大工作线程数（当前固定为1）

        Returns:
            批量处理结果
        """
        # 强制单线程（Baostock限制）
        max_workers = 1

        logger.info(f"🚀 开始批量更新: {len(symbols)} 只股票")
        logger.info(f"⚙️  模式: {mode}, 线程数: {max_workers}")

        self.stats = {
            'total_symbols': len(symbols),
            'successful': 0,
            'failed': 0,
            'no_data': 0,
            'total_records': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'duration_seconds': 0
        }

        detailed_results = []

        # 单线程顺序处理（遵守平台限制）
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{len(symbols)}] 处理 {symbol}")

                # 更新单只股票
                result = self.update_single_symbol(symbol, mode, start_date, end_date)
                detailed_results.append(result)

                # 更新统计
                if result['status'] == 'success':
                    self.stats['successful'] += 1
                    self.stats['total_records'] += result.get('records_stored', 0)
                elif result['status'] == 'no_data':
                    self.stats['no_data'] += 1
                elif result['status'] == 'error':
                    self.stats['failed'] += 1

                # 进度显示
                progress = (i / len(symbols)) * 100
                logger.info(f"  进度: {progress:.1f}%")

            except Exception as e:
                logger.error(f"处理股票失败 {symbol}: {e}")
                self.stats['failed'] += 1
                detailed_results.append({
                    'symbol': symbol,
                    'status': 'error',
                    'error': str(e)
                })

        # 完成统计
        self.stats['end_time'] = datetime.now()
        self.stats['duration_seconds'] = (
                self.stats['end_time'] - self.stats['start_time']
        ).total_seconds()

        # 生成报告
        report = self._generate_batch_report(detailed_results)

        logger.info(f"✅ 批量更新完成")
        logger.info(
            f"   成功: {self.stats['successful']}, 失败: {self.stats['failed']}, 无数据: {self.stats['no_data']}")
        logger.info(f"   总记录: {self.stats['total_records']}, 耗时: {self.stats['duration_seconds']:.2f}秒")

        return report

    def _generate_batch_report(self, detailed_results: List[Dict]) -> Dict[str, Any]:
        """
        生成批量处理报告
        """
        report = {
            'summary': {
                'total_symbols': len(detailed_results),
                'successful': sum(1 for r in detailed_results if r['status'] == 'success'),
                'failed': sum(1 for r in detailed_results if r['status'] == 'error'),
                'no_data': sum(1 for r in detailed_results if r['status'] == 'no_data'),
                'skipped': sum(1 for r in detailed_results if r['status'] == 'skipped'),
                'total_records': sum(r.get('records_stored', 0) for r in detailed_results)
            },
            'performance': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
                'duration_seconds': self.stats['duration_seconds']
            },
            'detailed_results': detailed_results
        }

        return report

    def get_update_status(self, symbol: str = None) -> Dict[str, Any]:
        """
        获取更新状态

        Args:
            symbol: 股票代码，如果为None则返回整体状态

        Returns:
            状态字典
        """
        if symbol:
            # 获取特定股票的最后更新日期
            last_date = self.storage.get_last_factor_date(symbol)
            return {
                'symbol': symbol,
                'last_update_date': last_date,
                'has_data': last_date is not None,
                'update_needed': self._check_update_needed(symbol) if last_date else True
            }
        else:
            # 返回整体统计
            return {
                'total_symbols': self.stats['total_symbols'],
                'successful': self.stats['successful'],
                'failed': self.stats['failed'],
                'no_data': self.stats['no_data'],
                'total_records': self.stats['total_records'],
                'last_run': {
                    'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
                    'duration_seconds': self.stats['duration_seconds']
                }
            }

    def _check_update_needed(self, symbol: str) -> bool:
        """
        检查是否需要更新
        """
        try:
            start_date, end_date = self.storage.calculate_incremental_range(symbol)
            return start_date is not None and end_date is not None
        except Exception:
            return True


# 测试函数
def test_factor_data_pipeline():
    """测试因子数据管道"""
    print("\n🧪 测试因子数据管道")
    print("=" * 50)

    try:
        # 1. 初始化
        print("初始化FactorDataPipeline...")
        pipeline = FactorDataPipeline()
        print("✅ 初始化成功")

        # 2. 测试单只股票增量更新
        print("\n测试单只股票增量更新...")
        test_symbol = '600519'  # 贵州茅台

        result = pipeline.update_single_symbol(test_symbol, mode='incremental')

        print(f"更新结果:")
        print(f"  状态: {result['status']}")
        print(f"  下载记录: {result.get('records_downloaded', 0)}")
        print(f"  存储记录: {result.get('records_stored', 0)}")
        print(f"  耗时: {result.get('execution_time', 0):.2f}秒")

        if result['status'] == 'error':
            print(f"  错误: {result.get('error', 'Unknown')}")

        # 3. 测试状态查询
        print("\n测试状态查询...")
        status = pipeline.get_update_status(test_symbol)
        print(f"状态信息:")
        for key, value in status.items():
            print(f"  {key}: {value}")

        # 4. 测试日期范围调整
        print("\n测试日期范围调整...")
        test_start = '20260101'  # 非交易日（元旦）
        test_end = '20260103'  # 非交易日（周末）

        adjusted_start, adjusted_end = pipeline._adjust_date_range(test_start, test_end)
        print(f"原始范围: {test_start} - {test_end}")
        print(f"调整后: {adjusted_start} - {adjusted_end}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_factor_data_pipeline()

    if success:
        print("\n✅ 因子数据管道测试通过！")
    else:
        print("\n❌ 因子数据管道测试失败")

    exit(0 if success else 1)