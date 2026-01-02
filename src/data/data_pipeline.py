# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\data_pipeline.py
# File Name: data_pipeline
# @ Author: mango-gh22
# @ Date：2025/12/7 19:48
"""
desc 构建一个完整的数据处理管道，整合数据采集、清洗、处理和存储功能
stock_database_v1/src/data/data_pipeline.py
数据管道 - 整合采集、清洗、处理和存储
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import json
import time

# 导入现有模块
from src.data.data_collector import BaseDataCollector

# from src.data.data_storage import DataStorage
# 关键修复：使用 AdaptiveDataStorage 而不是 DataStorage
from src.data.adaptive_storage import AdaptiveDataStorage

from src.utils.code_converter import normalize_stock_code
from src.config.logging_config import setup_logging

# 为了保持兼容性，可以给 AdaptiveDataStorage 起个别名
# DataStorage = AdaptiveDataStorage  # 这样代码中已有的 DataStorage 引用仍然有效

logger = setup_logging()


class DataPipeline:
    """数据管道 - 整合完整的数据处理流程"""

    def __init__(self,
                 collector: BaseDataCollector,
                 storage: AdaptiveDataStorage,  # 这里应该是 AdaptiveDataStorage
                 config_path: str = 'config/database.yaml'):
        """
        初始化数据管道

        Args:
            collector: 数据采集器实例
            storage: 数据存储器实例
            config_path: 配置文件路径
        """
        self.collector = collector
        self.storage = storage  # 这应该是 AdaptiveDataStorage 的实例,类型明确为 AdaptiveDataStorage
        self.config_path = config_path

        # 初始化数据处理器
        self.data_processor = DataProcessor()

        # 缓存目录
        self.cache_dir = Path('data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 报告目录
        self.report_dir = Path('data/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)


    def fetch_and_store_daily_data(self,
                                   symbol: str,
                                   start_date: str,
                                   end_date: str,
                                   auto_adjust: bool = True) -> Dict[str, Any]:
        """
        获取并存储日线数据（完整流程）

        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            auto_adjust: 是否自动调整日期范围

        Returns:
            处理结果字典
        """
        result = {
            'symbol': symbol,
            'start_date': start_date,
            'end_date': end_date,
            'status': 'pending',
            'records_fetched': 0,
            'records_stored': 0,
            'processing_time': 0,
            'errors': []
        }

        start_time = time.time()

        try:
            # 1. 标准化股票代码
            normalized_symbol = normalize_stock_code(symbol)
            logger.info(f"开始处理股票: {symbol} -> {normalized_symbol}")

            # 2. 自动调整日期范围（如果需要）
            if auto_adjust:
                last_update = self.storage.get_last_update_date(normalized_symbol, self.storage.supported_tables.get('daily', 'stock_daily_data'))
                if last_update:
                    # 从最后更新日期的下一天开始
                    last_date = datetime.strptime(last_update, '%Y%m%d')
                    next_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
                    if next_date <= end_date:
                        start_date = next_date
                        logger.info(f"调整开始日期为: {start_date} (基于最后更新日期)")

            # 3. 获取数据
            logger.info(f"获取数据: {normalized_symbol} [{start_date} - {end_date}]")
            df_raw = self.collector.fetch_daily_data(normalized_symbol, start_date, end_date)

            if df_raw.empty:
                result['status'] = 'no_data'
                result['processing_time'] = time.time() - start_time
                logger.warning(f"未获取到数据: {normalized_symbol}")
                return result

            result['records_fetched'] = len(df_raw)

            # 4. 数据清洗
            logger.info(f"清洗数据: {normalized_symbol} ({len(df_raw)} 条记录)")
            df_clean = self.data_processor.clean_daily_data(df_raw, normalized_symbol)

            # 5. 计算技术指标
            logger.info(f"计算技术指标: {normalized_symbol}")
            df_with_indicators = self.data_processor.calculate_technical_indicators(df_clean)

            # 6. 数据验证
            logger.info(f"验证数据质量: {normalized_symbol}")
            quality_report = self.data_processor.validate_data_quality(df_with_indicators)

            if quality_report['status'] == 'poor':
                logger.warning(f"数据质量较差: {normalized_symbol}, 质量评分: {quality_report['quality_score']}")
                result['warnings'].append(f"数据质量评分较低: {quality_report['quality_score']}")

            # 7. 存储数据 - 关键修复：提取元组中的整数
            logger.info(f"存储数据: {normalized_symbol}")
            storage_result = self.storage.store_daily_data(df_with_indicators)  # 返回元组

            # ========== 关键修复：提取行数 ==========
            if isinstance(storage_result, tuple) and len(storage_result) >= 2:
                rows_affected = storage_result[0]  # 元组第一个元素是行数
                storage_status = storage_result[1]  # 第二个元素是状态信息
            else:
                rows_affected = storage_result if isinstance(storage_result, int) else 0
                storage_status = {}

            # 确保 rows_affected 是整数
            rows_affected = int(rows_affected) if rows_affected else 0
            result['records_stored'] = rows_affected  # 存储整数，不是元组！

            # 8. 记录更新日志 - 传递整数
            execution_time = time.time() - start_time

            # 根据存储状态确定日志状态
            log_status = 'success'
            log_error = None

            if isinstance(storage_status, dict):
                status_info = storage_status.get('status', '')
                if status_info == 'skipped':
                    log_status = 'skipped'
                    log_error = storage_status.get('reason', '')
                elif status_info == 'error':
                    log_status = 'error'
                    log_error = storage_status.get('error', '')

            self.storage.log_data_update(
                data_type=self.storage.supported_tables.get('daily', 'stock_daily_data'),
                symbol=normalized_symbol,
                start_date=start_date,
                end_date=end_date,
                rows_affected=rows_affected,  # 传递整数
                status=log_status,
                error_message=log_error,
                execution_time=execution_time
            )

            result['status'] = 'success'
            result['processing_time'] = execution_time
            result['quality_score'] = quality_report['quality_score']

            logger.info(f"处理完成: {normalized_symbol}, 存储 {rows_affected} 条记录")

        except Exception as e:
            result['status'] = 'error'
            result['errors'].append(str(e))
            result['processing_time'] = time.time() - start_time

            # 记录错误日志
            self.storage.log_data_update(
                data_type=self.storage.supported_tables.get('daily', 'stock_daily_data'),
                symbol=normalized_symbol if 'normalized_symbol' in locals() else symbol,
                start_date=start_date,
                end_date=end_date,
                rows_affected=0,
                status='error',
                error_message=str(e),
                execution_time=time.time() - start_time
            )

            logger.error(f"处理失败: {symbol}, 错误: {e}")

        return result

    # 在 DataPipeline 类中添加以下方法
    def run_incremental_update(self, market: str = "上证", days_back: int = 7,
                               limit: int = 20, max_concurrent: int = 3) -> Dict[str, Any]:
        """
        增量更新 - 高层业务接口（门面模式）

        Args:
            market: 市场类型 (上证/深证/科创板等)
            days_back: 回溯天数
            limit: 股票数量限制
            max_concurrent: 最大并发数

        Returns:
            更新结果字典
        """
        from datetime import datetime, timedelta
        import logging
        logger = logging.getLogger(__name__)

        logger.info(f"🔄 开始增量更新: {market}，回溯 {days_back} 天，限制 {limit} 只股票")

        try:
            # 1. 获取指定市场的股票列表
            logger.info(f"📋 获取{market}股票列表...")
            stock_list = self.collector.fetch_stock_list(market)

            if stock_list.empty:
                logger.warning(f"⚠️ 未获取到{market}股票列表")
                return {
                    "status": "error",
                    "success": False,
                    "message": f"未获取到{market}股票列表"
                }

            # 2. 提取股票代码（限制数量）
            symbols = stock_list['symbol'].head(limit).tolist()
            logger.info(f"📊 处理 {len(symbols)} 只股票: {symbols[:3]}...")

            # 3. 计算增量日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            logger.info(f"📅 日期范围: {start_date} 至 {end_date}")

            # 4. 调用底层的批量处理方法
            logger.info(f"⚡ 开始批量处理，并发数: {max_concurrent}")
            batch_result = self.batch_process_stocks(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                max_concurrent=max_concurrent
            )

            # 5. 整理结果，匹配 run.py 中的期望格式
            result = {
                "status": "success",
                "success": batch_result['success'] > 0,  # 只要有成功就为True
                "market": market,
                "days_back": days_back,
                "total_symbols": batch_result['total_symbols'],
                "success_count": batch_result['success'],
                "failed": batch_result['failed'],
                "no_data": batch_result['no_data'],
                "total_records": batch_result['total_records'],
                "new_records": batch_result['total_records'],  # 假设所有记录都是新的
                "updated_records": batch_result['success'],  # 成功处理的股票数
                "duration": batch_result['processing_time'],
                "batch_id": batch_result.get('batch_id', '')
            }

            logger.info(f"✅ 增量更新完成: 成功 {result['success_count']} 只，失败 {result['failed']} 只")

            return result

        except Exception as e:
            logger.error(f"❌ 增量更新失败: {e}")
            return {
                "status": "error",
                "success": False,
                "error": str(e)
            }



    def batch_process_stocks(self,
                             symbols: List[str],
                             start_date: str,
                             end_date: str,
                             max_concurrent: int = 3) -> Dict[str, Any]:
        """
        批量处理多只股票

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_concurrent: 最大并发数

        Returns:
            批量处理结果
        """
        batch_result = {
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': datetime.now().isoformat(),
            'total_symbols': len(symbols),
            'processed': 0,
            'success': 0,
            'failed': 0,
            'no_data': 0,
            'total_records': 0,
            'symbol_results': [],
            'processing_time': 0
        }

        batch_start_time = time.time()

        logger.info(f"开始批量处理 {len(symbols)} 只股票")

        # 限制并发数，避免API限制
        import concurrent.futures

        def process_single(symbol: str) -> Dict[str, Any]:
            """处理单只股票"""
            try:
                result = self.fetch_and_store_daily_data(symbol, start_date, end_date)
                return result
            except Exception as e:
                return {
                    'symbol': symbol,
                    'status': 'error',
                    'errors': [str(e)],
                    'processing_time': 0
                }

        # 使用线程池处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            future_to_symbol = {
                executor.submit(process_single, symbol): symbol
                for symbol in symbols
            }

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    batch_result['symbol_results'].append(result)

                    if result['status'] == 'success':
                        batch_result['success'] += 1
                        # ========== 关键修复：确保是整数 ==========
                        records = result.get('records_stored', 0)
                        if isinstance(records, tuple):
                            records = records[0] if records else 0
                        batch_result['total_records'] += int(records)  # 转换为整数
                    elif result['status'] == 'no_data':
                        batch_result['no_data'] += 1
                    else:
                        batch_result['failed'] += 1

                    batch_result['processed'] += 1

                    # 进度日志
                    if batch_result['processed'] % 10 == 0:
                        logger.info(f"处理进度: {batch_result['processed']}/{batch_result['total_symbols']}")

                except Exception as e:
                    logger.error(f"处理股票时发生异常 {symbol}: {e}")
                    batch_result['failed'] += 1
                    batch_result['processed'] += 1

        # 完成批量处理
        batch_result['end_time'] = datetime.now().isoformat()
        batch_result['processing_time'] = time.time() - batch_start_time

        # 生成报告
        self._generate_batch_report(batch_result)

        logger.info(
            f"批量处理完成: 成功 {batch_result['success']}, 失败 {batch_result['failed']}, 无数据 {batch_result['no_data']}")

        return batch_result

    def _generate_batch_report(self, batch_result: Dict[str, Any]):
        """生成批量处理报告"""
        report = {
            'batch_id': batch_result['batch_id'],
            'start_time': batch_result['start_time'],
            'end_time': batch_result['end_time'],
            'total_processing_time': round(batch_result['processing_time'], 2),

            'summary': {
                'total_symbols': batch_result['total_symbols'],
                'success': batch_result['success'],
                'failed': batch_result['failed'],
                'no_data': batch_result['no_data'],
                'total_records': batch_result['total_records']
            },

            'performance': {
                'symbols_per_second': round(batch_result['total_symbols'] / batch_result['processing_time'], 2) if
                batch_result['processing_time'] > 0 else 0,
                'records_per_second': round(batch_result['total_records'] / batch_result['processing_time'], 2) if
                batch_result['processing_time'] > 0 else 0
            },

            'detailed_results': []
        }

        # 添加详细结果
        for result in batch_result['symbol_results']:
            detailed = {
                'symbol': result.get('symbol'),
                'status': result.get('status'),
                'records_fetched': result.get('records_fetched', 0),
                'records_stored': result.get('records_stored', 0),
                'processing_time': result.get('processing_time', 0),
                'quality_score': result.get('quality_score', 0),
                'errors': result.get('errors', [])
            }
            report['detailed_results'].append(detailed)

        # 保存报告
        report_file = self.report_dir / f"{batch_result['batch_id']}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"批量处理报告已保存: {report_file}")

        # 生成简要文本报告
        text_report = f"""
批量处理报告
============
批次ID: {batch_result['batch_id']}
处理时间: {batch_result['start_time']} - {batch_result['end_time']}
总耗时: {round(batch_result['processing_time'], 2)} 秒

汇总统计:
--------
总股票数: {batch_result['total_symbols']}
成功: {batch_result['success']}
失败: {batch_result['failed']}
无数据: {batch_result['no_data']}
总记录数: {batch_result['total_records']}

性能指标:
--------
股票/秒: {report['performance']['symbols_per_second']}
记录/秒: {report['performance']['records_per_second']}

详细报告请查看: {report_file}
        """

        print(text_report)



class DataProcessor:
    """数据处理类 - 负责数据清洗和计算"""

    def __init__(self):
        self.required_columns = [
            'trade_date', 'open', 'close', 'high', 'low',
            'volume', 'amount', 'pre_close'
        ]

    def clean_daily_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        清洗日线数据

        Args:
            df: 原始数据
            symbol: 股票代码

        Returns:
            清洗后的数据
        """
        if df.empty:
            return df

        df_clean = df.copy()

        # 1. 添加股票代码
        if 'symbol' not in df_clean.columns:
            df_clean['symbol'] = symbol

        # 2. 确保日期格式正确
        if 'trade_date' in df_clean.columns:
            df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date']).dt.strftime('%Y%m%d')

        # 3. 处理缺失值
        for col in ['open', 'high', 'low', 'close']:
            if col in df_clean.columns:
                # 价格数据使用前向填充
                df_clean[col] = df_clean[col].fillna(method='ffill').fillna(method='bfill')

        if 'volume' in df_clean.columns:
            df_clean['volume'] = df_clean['volume'].fillna(0)

        if 'amount' in df_clean.columns:
            df_clean['amount'] = df_clean['amount'].fillna(0)

        # 4. 验证价格数据
        df_clean = self._validate_price_data(df_clean)

        # 5. 去除重复数据
        df_clean = df_clean.drop_duplicates(subset=['symbol', 'trade_date'], keep='last')

        # 6. 排序
        df_clean = df_clean.sort_values('trade_date')
        df_clean = df_clean.reset_index(drop=True)

        # 7. 添加处理标记
        df_clean['processed_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_clean['data_source'] = 'processed'

        return df_clean

    def _validate_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """验证价格数据合理性"""
        if df.empty:
            return df

        # 基本验证规则
        valid_mask = pd.Series(True, index=df.index)

        # 1. 价格必须为正
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                valid_mask &= (df[col] > 0)

        # 2. high >= low
        if all(col in df.columns for col in ['high', 'low']):
            valid_mask &= (df['high'] >= df['low'])

        # 3. 价格在高低范围内
        if all(col in df.columns for col in ['open', 'high', 'low']):
            valid_mask &= (df['open'] >= df['low']) & (df['open'] <= df['high'])

        if all(col in df.columns for col in ['close', 'high', 'low']):
            valid_mask &= (df['close'] >= df['low']) & (df['close'] <= df['high'])

        # 移除无效数据
        df_valid = df[valid_mask].copy()

        if len(df) != len(df_valid):
            logger.warning(f"移除了 {len(df) - len(df_valid)} 条无效价格数据")

        return df_valid

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        Args:
            df: 清洗后的数据

        Returns:
            包含技术指标的数据
        """
        if df.empty or 'close' not in df.columns:
            return df

        df_indicators = df.copy()

        # 确保数据已排序
        df_indicators = df_indicators.sort_values('trade_date')

        # 计算涨跌幅
        if 'pre_close' in df_indicators.columns:
            df_indicators['change'] = df_indicators['close'] - df_indicators['pre_close']
            df_indicators['pct_change'] = (df_indicators['change'] / df_indicators['pre_close'] * 100).round(4)

        # 计算振幅
        if all(col in df_indicators.columns for col in ['high', 'low', 'pre_close']):
            df_indicators['amplitude'] = (
                        (df_indicators['high'] - df_indicators['low']) / df_indicators['pre_close'] * 100).round(4)

        # 计算移动平均线
        close_prices = df_indicators['close'].astype(float)

        ma_periods = [5, 10, 20, 30, 60, 120, 250]
        for period in ma_periods:
            df_indicators[f'ma{period}'] = close_prices.rolling(window=period).mean().round(4)

        # 计算成交量均线
        if 'volume' in df_indicators.columns:
            volume_series = df_indicators['volume'].astype(float)
            for period in [5, 10, 20]:
                df_indicators[f'volume_ma{period}'] = volume_series.rolling(window=period).mean().round(2)

        # 计算成交量比
        if 'volume_ma5' in df_indicators.columns and 'volume' in df_indicators.columns:
            df_indicators['volume_ratio'] = (df_indicators['volume'] / df_indicators['volume_ma5']).round(2)

        # 计算换手率（需要流通股本数据，这里使用简化计算）
        if 'amount' in df_indicators.columns and 'volume' in df_indicators.columns:
            # 假设平均价格
            avg_price = (df_indicators['high'] + df_indicators['low']) / 2
            turnover = df_indicators['volume'] * avg_price
            df_indicators['turnover_rate'] = (turnover / df_indicators['amount']).round(4)

        return df_indicators

    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证数据质量

        Args:
            df: 数据

        Returns:
            质量报告
        """
        report = {
            'total_records': len(df),
            'missing_values': {},
            'duplicates': 0,
            'price_issues': 0,
            'volume_issues': 0,
            'quality_score': 100,
            'status': 'excellent'
        }

        if df.empty:
            report['status'] = 'empty'
            report['quality_score'] = 0
            return report

        # 1. 检查缺失值
        for col in self.required_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    report['missing_values'][col] = int(missing_count)

        # 2. 检查重复数据
        if 'symbol' in df.columns and 'trade_date' in df.columns:
            duplicates = df.duplicated(subset=['symbol', 'trade_date']).sum()
            report['duplicates'] = int(duplicates)

        # 3. 检查价格问题
        price_cols = ['open', 'high', 'low', 'close']
        price_issues = 0

        for col in price_cols:
            if col in df.columns:
                # 检查负值
                negative = (df[col] <= 0).sum()
                price_issues += negative

        # 检查价格关系
        if all(col in df.columns for col in ['high', 'low']):
            invalid_high_low = (df['high'] < df['low']).sum()
            price_issues += invalid_high_low

        report['price_issues'] = int(price_issues)

        # 4. 检查成交量问题
        if 'volume' in df.columns:
            negative_volume = (df['volume'] < 0).sum()
            report['volume_issues'] = int(negative_volume)

        # 5. 计算质量评分
        penalty = 0

        # 缺失值惩罚
        for col, count in report['missing_values'].items():
            penalty += (count / len(df)) * 20

        # 重复数据惩罚
        if report['duplicates'] > 0:
            penalty += (report['duplicates'] / len(df)) * 30

        # 价格问题惩罚
        if report['price_issues'] > 0:
            penalty += (report['price_issues'] / len(df)) * 50

        # 成交量问题惩罚
        if report['volume_issues'] > 0:
            penalty += min(report['volume_issues'] * 10, 100)

        quality_score = max(0, 100 - penalty)
        report['quality_score'] = round(quality_score, 1)

        # 6. 确定状态
        if quality_score >= 90:
            report['status'] = 'excellent'
        elif quality_score >= 70:
            report['status'] = 'good'
        elif quality_score >= 50:
            report['status'] = 'fair'
        elif quality_score >= 30:
            report['status'] = 'poor'
        else:
            report['status'] = 'very_poor'

        return report

    def calculate_advanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算高级技术指标

        Args:
            df: 包含基础指标的数据

        Returns:
            包含高级指标的数据
        """
        if df.empty or 'close' not in df.columns:
            return df

        df_advanced = df.copy()

        # 确保数据排序
        df_advanced = df_advanced.sort_values('trade_date')
        close_prices = df_advanced['close'].astype(float)

        # 1. RSI (相对强弱指数)
        delta = close_prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df_advanced['rsi'] = (100 - (100 / (1 + rs))).round(2)

        # 2. MACD
        ema12 = close_prices.ewm(span=12, adjust=False).mean()
        ema26 = close_prices.ewm(span=26, adjust=False).mean()
        df_advanced['macd'] = (ema12 - ema26).round(4)
        df_advanced['macd_signal'] = df_advanced['macd'].ewm(span=9, adjust=False).mean().round(4)
        df_advanced['macd_hist'] = (df_advanced['macd'] - df_advanced['macd_signal']).round(4)

        # 3. 布林带
        window = 20
        df_advanced['bb_middle'] = close_prices.rolling(window=window).mean()
        bb_std = close_prices.rolling(window=window).std()
        df_advanced['bb_upper'] = df_advanced['bb_middle'] + 2 * bb_std
        df_advanced['bb_lower'] = df_advanced['bb_middle'] - 2 * bb_std
        df_advanced['bb_width'] = (
                    (df_advanced['bb_upper'] - df_advanced['bb_lower']) / df_advanced['bb_middle'] * 100).round(2)

        # 4. ATR (平均真实波幅)
        high_low = df_advanced['high'] - df_advanced['low']
        high_close = abs(df_advanced['high'] - df_advanced['close'].shift())
        low_close = abs(df_advanced['low'] - df_advanced['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df_advanced['atr'] = true_range.rolling(window=14).mean().round(4)

        # 5. 波动率
        if 'pct_change' in df_advanced.columns:
            df_advanced['volatility_20d'] = df_advanced['pct_change'].rolling(window=20).std() * np.sqrt(252)

        return df_advanced


class TushareDataCollector(BaseDataCollector):
    """Tushare数据采集器实现"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        super().__init__(config_path)
        self._init_tushare()

    def _init_tushare(self):
        """初始化Tushare"""
        try:
            import tushare as ts
            from src.config.config_loader import load_tushare_config

            config = load_tushare_config()
            token = config.get('token')

            if token:
                ts.set_token(token)
                self.pro = ts.pro_api()
                logger.info("Tushare API初始化成功")
            else:
                logger.warning("未配置Tushare token")
                self.pro = None
        except ImportError:
            logger.error("未安装tushare库")
            self.pro = None
        except Exception as e:
            logger.error(f"初始化Tushare失败: {e}")
            self.pro = None

    def fetch_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        if not self.pro:
            logger.error("Tushare未初始化")
            return pd.DataFrame()

        try:
            # 执行速率限制
            self.enforce_rate_limit()

            # 转换代码格式
            ts_code = self._convert_to_ts_code(symbol)

            # 获取数据
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                # 重命名列以匹配我们的格式
                column_mapping = {
                    'ts_code': 'symbol',
                    'trade_date': 'trade_date',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'pre_close': 'pre_close',
                    'change': 'change',
                    'pct_chg': 'pct_change',
                    'vol': 'volume',
                    'amount': 'amount'
                }

                df = df.rename(columns=column_mapping)
                df['symbol'] = symbol  # 使用标准化代码

                return df
            else:
                logger.warning(f"未获取到数据: {symbol}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取日线数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def fetch_minute_data(self, symbol: str, trade_date: str, freq: str = '1min') -> pd.DataFrame:
        """获取分钟线数据"""
        if not self.pro:
            logger.error("Tushare未初始化")
            return pd.DataFrame()

        try:
            self.enforce_rate_limit()

            ts_code = self._convert_to_ts_code(symbol)

            df = self.pro.ft_mins(
                ts_code=ts_code,
                freq=freq,
                start_date=trade_date,
                end_date=trade_date
            )

            if df is not None and not df.empty:
                df = df.rename(columns={
                    'ts_code': 'symbol',
                    'trade_time': 'trade_time',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'volume',
                    'amount': 'amount'
                })

                df['symbol'] = symbol
                df['trade_date'] = trade_date

                return df
            else:
                logger.warning(f"未获取到分钟数据: {symbol} {trade_date}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取分钟数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def fetch_basic_info(self, symbol: str) -> Dict[str, Any]:
        """获取股票基本信息"""
        if not self.pro:
            return {}

        try:
            self.enforce_rate_limit()

            ts_code = self._convert_to_ts_code(symbol)

            df = self.pro.stock_basic(
                ts_code=ts_code,
                fields='ts_code,symbol,name,area,industry,market,list_date,is_hs'
            )

            if df is not None and not df.empty:
                return df.iloc[0].to_dict()
            else:
                return {}

        except Exception as e:
            logger.error(f"获取基本信息失败 {symbol}: {e}")
            return {}

    def _convert_to_ts_code(self, normalized_code: str) -> str:
        """将标准化代码转换为Tushare格式"""
        if normalized_code.startswith('sh'):
            return f"{normalized_code[2:]}.SH"
        elif normalized_code.startswith('sz'):
            return f"{normalized_code[2:]}.SZ"
        else:
            return normalized_code


# 使用示例
def main():
    """数据管道使用示例"""
    print("股票数据管道示例")
    print("=" * 50)

    # 1. 创建采集器
    print("初始化数据采集器...")
    collector = TushareDataCollector()

    # 2. 创建存储器
    print("初始化数据存储器...")
    storage = DataStorage()

    # 3. 创建数据管道
    print("创建数据管道...")
    pipeline = DataPipeline(collector, storage)

    # 4. 示例股票代码
    sample_symbols = [
        '600519',  # 贵州茅台
        '000001',  # 平安银行
        '000858'  # 五粮液
    ]

    # 标准化股票代码
    normalized_symbols = []
    for symbol in sample_symbols:
        try:
            normalized = normalize_stock_code(symbol)
            normalized_symbols.append(normalized)
            print(f"原始代码: {symbol} -> 标准化: {normalized}")
        except Exception as e:
            print(f"代码转换失败 {symbol}: {e}")

    if not normalized_symbols:
        print("没有有效的股票代码，使用示例代码")
        normalized_symbols = ['sh600519', 'sz000001']

    # 5. 批量处理数据
    print(f"\n批量处理 {len(normalized_symbols)} 只股票数据...")
    batch_result = pipeline.batch_process_stocks(
        symbols=normalized_symbols,
        start_date='20240101',
        end_date='20241231',
        max_concurrent=2
    )

    # 6. 显示结果
    print(f"\n批量处理完成:")
    print(f"总股票数: {batch_result['total_symbols']}")
    print(f"成功: {batch_result['success']}")
    print(f"失败: {batch_result['failed']}")
    print(f"无数据: {batch_result['no_data']}")
    print(f"总记录数: {batch_result['total_records']}")
    print(f"总耗时: {round(batch_result['processing_time'], 2)} 秒")

    # 7. 数据处理示例
    print(f"\n数据处理示例:")
    processor = DataProcessor()

    # 获取并处理单只股票数据
    for symbol in normalized_symbols[:1]:  # 只处理第一只股票作为示例
        try:
            print(f"\n处理股票: {symbol}")

            # 从数据库获取数据（假设已经存储）
            last_update = storage.get_last_update_date(symbol, self.storage.supported_tables.get('daily', 'stock_daily_data'))
            if last_update:
                # 获取最近30天数据
                end_date = last_update
                start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')

                # 使用采集器获取数据
                df_raw = collector.fetch_daily_data(symbol, start_date, end_date)

                if not df_raw.empty:
                    # 清洗数据
                    df_clean = processor.clean_daily_data(df_raw, symbol)

                    # 计算技术指标
                    df_indicators = processor.calculate_technical_indicators(df_clean)

                    # 计算高级指标
                    df_advanced = processor.calculate_advanced_indicators(df_indicators)

                    # 验证数据质量
                    quality_report = processor.validate_data_quality(df_advanced)

                    print(f"原始数据: {len(df_raw)} 条")
                    print(f"清洗后数据: {len(df_clean)} 条")
                    print(f"数据质量评分: {quality_report['quality_score']} - {quality_report['status']}")
                    print(f"数据列: {list(df_advanced.columns)}")

                    # 显示前几行数据
                    print(f"\n数据示例:")
                    print(df_advanced[['trade_date', 'open', 'close', 'volume', 'ma5', 'ma10']].head())
                else:
                    print(f"未获取到数据: {symbol}")
            else:
                print(f"未找到数据: {symbol}")

        except Exception as e:
            print(f"处理股票数据失败 {symbol}: {e}")

    print("\n数据管道示例完成!")


if __name__ == "__main__":
    main()