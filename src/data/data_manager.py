# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\data_manager.py
# File Name: data_manager
# @ Author: mango-gh22
# @ Date：2025/12/7 19:56
"""
desc P5-集成数据管理器
"""

"""
stock_database_v1/src/data/data_manager.py
数据管理器 - 整合所有数据处理功能
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import json
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入项目模块
from src.config.logging_config import setup_logging
from src.utils.code_converter import normalize_stock_code
from src.database.db_connector import DatabaseConnector
from src.data.data_collector import BaseDataCollector
from src.data.data_storage import DataStorage
from src.data.data_pipeline import DataPipeline, TushareDataCollector

logger = setup_logging()


class DataManager:
    """数据管理器 - 统一管理所有数据操作"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据管理器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path

        # 初始化组件
        self._init_components()

        # 缓存管理
        self.cache_manager = CacheManager()

        # 质量监控
        self.quality_monitor = DataQualityMonitor()

        # 统计信息
        self.stats = {
            'total_fetched': 0,
            'total_stored': 0,
            'total_failed': 0,
            'last_update': None
        }

        logger.info("数据管理器初始化完成")

    def _init_components(self):
        """初始化所有组件"""
        # 数据库连接
        self.db_connector = DatabaseConnector(self.config_path)

        # 数据采集器
        self.collector = TushareDataCollector(self.config_path)

        # 数据存储器
        self.storage = DataStorage(self.config_path)

        # 数据管道
        self.pipeline = DataPipeline(self.collector, self.storage, self.config_path)

    def get_stock_basic_info(self,
                             symbols: Optional[List[str]] = None,
                             market: str = "A股",
                             force_update: bool = False) -> pd.DataFrame:
        """
        获取股票基本信息

        Args:
            symbols: 股票代码列表，None表示获取所有
            market: 市场类型
            force_update: 是否强制更新

        Returns:
            股票基本信息DataFrame
        """
        cache_key = f"basic_info_{market}_{hashlib.md5(str(symbols).encode()).hexdigest()[:16]}"

        # 检查缓存
        if not force_update:
            cached_data = self.cache_manager.get(cache_key)
            if cached_data is not None:
                logger.info(f"使用缓存数据: 股票基本信息")
                return cached_data

        try:
            # 从数据库获取
            if not force_update:
                df = self._get_basic_info_from_db(symbols, market)
                if not df.empty:
                    # 缓存结果
                    self.cache_manager.set(cache_key, df, ttl_hours=24)
                    return df

            # 需要更新数据
            if symbols:
                updated_data = self._update_basic_info_batch(symbols)
            else:
                updated_data = self._update_all_basic_info(market)

            # 缓存更新后的数据
            if not updated_data.empty:
                self.cache_manager.set(cache_key, updated_data, ttl_hours=24)

            return updated_data

        except Exception as e:
            logger.error(f"获取股票基本信息失败: {e}")
            return pd.DataFrame()

    def _get_basic_info_from_db(self,
                                symbols: Optional[List[str]],
                                market: str) -> pd.DataFrame:
        """从数据库获取股票基本信息"""
        try:
            connection = self.db_connector.get_connection()

            query = "SELECT * FROM stock_basic_info WHERE 1=1"
            params = []

            if symbols:
                # 标准化代码
                normalized_symbols = [normalize_stock_code(s) for s in symbols]
                placeholders = ','.join(['%s'] * len(normalized_symbols))
                query += f" AND normalized_code IN ({placeholders})"
                params.extend(normalized_symbols)

            if market == "A股":
                query += " AND exchange IN ('SH', 'SZ', 'BJ')"
            elif market == "港股":
                query += " AND exchange = 'HK'"
            elif market == "美股":
                query += " AND exchange IN ('NYSE', 'NASDAQ')"

            df = pd.read_sql_query(query, connection, params=params)
            connection.close()

            return df

        except Exception as e:
            logger.error(f"从数据库获取基本信息失败: {e}")
            return pd.DataFrame()

    def _update_basic_info_batch(self, symbols: List[str]) -> pd.DataFrame:
        """批量更新股票基本信息"""
        logger.info(f"批量更新股票基本信息: {len(symbols)} 只股票")

        all_data = []

        for symbol in symbols:
            try:
                normalized_code = normalize_stock_code(symbol)
                info = self.collector.fetch_basic_info(normalized_code)

                if info:
                    # 转换为DataFrame格式
                    df_info = pd.DataFrame([info])
                    all_data.append(df_info)

                    # 更新到数据库
                    self._save_basic_info_to_db(df_info)

                    logger.info(f"更新基本信息成功: {normalized_code}")
                else:
                    logger.warning(f"未获取到基本信息: {symbol}")

            except Exception as e:
                logger.error(f"更新股票基本信息失败 {symbol}: {e}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

    def _update_all_basic_info(self, market: str) -> pd.DataFrame:
        """更新所有股票基本信息"""
        logger.info(f"更新所有{market}股票基本信息")

        # 这里需要调用Tushare的stock_basic接口获取所有股票
        # 由于API限制，这里简化为返回空DataFrame
        logger.warning("批量获取所有股票基本信息功能需要特殊API权限")
        return pd.DataFrame()

    def _save_basic_info_to_db(self, df: pd.DataFrame):
        """保存股票基本信息到数据库"""
        if df.empty:
            return

        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            # 准备插入语句
            insert_sql = """
                INSERT INTO stock_basic_info (
                    symbol, normalized_code, ts_code, name, fullname, enname, 
                    cnspell, exchange, market, industry, area, list_date, 
                    list_status, is_hs, created_time, updated_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    fullname = VALUES(fullname),
                    enname = VALUES(enname),
                    cnspell = VALUES(cnspell),
                    market = VALUES(market),
                    industry = VALUES(industry),
                    area = VALUES(area),
                    list_status = VALUES(list_status),
                    is_hs = VALUES(is_hs),
                    updated_time = VALUES(updated_time)
            """

            # 准备数据
            records = []
            for _, row in df.iterrows():
                # 标准化代码
                normalized_code = normalize_stock_code(row.get('symbol', ''))

                record = (
                    row.get('symbol', ''),
                    normalized_code,
                    row.get('ts_code', ''),
                    row.get('name', ''),
                    row.get('fullname', ''),
                    row.get('enname', ''),
                    row.get('cnspell', ''),
                    row.get('exchange', ''),
                    row.get('market', ''),
                    row.get('industry', ''),
                    row.get('area', ''),
                    row.get('list_date', None),
                    row.get('list_status', 'L'),
                    row.get('is_hs', 'N'),
                    datetime.now(),
                    datetime.now()
                )
                records.append(record)

            # 批量插入
            cursor.executemany(insert_sql, records)
            connection.commit()

            logger.info(f"保存股票基本信息成功: {len(records)} 条记录")

            cursor.close()
            connection.close()

        except Exception as e:
            logger.error(f"保存股票基本信息失败: {e}")

    def get_daily_data(self,
                       symbol: str,
                       start_date: str,
                       end_date: str,
                       adjust: str = "qfq",
                       force_update: bool = False) -> pd.DataFrame:
        """
        获取日线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            adjust: 复权类型 ('qfq': 前复权, 'hfq': 后复权, '': 不复权)
            force_update: 是否强制更新

        Returns:
            日线数据DataFrame
        """
        normalized_code = normalize_stock_code(symbol)
        cache_key = f"daily_{normalized_code}_{start_date}_{end_date}_{adjust}"

        # 检查缓存
        if not force_update:
            cached_data = self.cache_manager.get(cache_key)
            if cached_data is not None:
                logger.info(f"使用缓存数据: {normalized_code}")
                return cached_data

        try:
            # 从数据库获取
            if not force_update:
                df = self._get_daily_data_from_db(normalized_code, start_date, end_date)
                if not df.empty:
                    # 应用复权（如果数据库中的数据是原始数据）
                    if adjust != '':
                        df = self._adjust_prices(df, adjust, normalized_code)

                    # 缓存结果
                    self.cache_manager.set(cache_key, df, ttl_hours=1)
                    return df

            # 需要更新数据
            result = self.pipeline.fetch_and_store_daily_data(
                symbol=normalized_code,
                start_date=start_date,
                end_date=end_date,
                auto_adjust=True
            )

            if result['status'] == 'success' and result['records_stored'] > 0:
                # 重新从数据库获取
                df = self._get_daily_data_from_db(normalized_code, start_date, end_date)

                # 应用复权
                if adjust != '':
                    df = self._adjust_prices(df, adjust, normalized_code)

                # 缓存结果
                self.cache_manager.set(cache_key, df, ttl_hours=1)

                return df
            else:
                logger.warning(f"获取日线数据失败: {normalized_code}, 结果: {result}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取日线数据失败 {normalized_code}: {e}")
            return pd.DataFrame()

    def _get_daily_data_from_db(self,
                                normalized_code: str,
                                start_date: str,
                                end_date: str) -> pd.DataFrame:
        """从数据库获取日线数据"""
        try:
            connection = self.db_connector.get_connection()

            query = """
                SELECT * FROM stock_daily_data 
                WHERE normalized_code = %s 
                AND trade_date >= %s 
                AND trade_date <= %s
                ORDER BY trade_date
            """

            df = pd.read_sql_query(query, connection,
                                   params=[normalized_code, start_date, end_date])
            connection.close()

            return df

        except Exception as e:
            logger.error(f"从数据库获取日线数据失败: {e}")
            return pd.DataFrame()

    def _adjust_prices(self, df: pd.DataFrame, adjust_type: str, symbol: str) -> pd.DataFrame:
        """应用复权因子调整价格"""
        if df.empty or adjust_type == '':
            return df

        try:
            # 获取复权因子
            adjust_factors = self._get_adjust_factors(symbol)

            if adjust_factors.empty:
                logger.warning(f"未找到复权因子: {symbol}")
                return df

            # 合并复权因子
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            adjust_factors['trade_date'] = pd.to_datetime(adjust_factors['trade_date'])

            df_merged = pd.merge(df, adjust_factors, on='trade_date', how='left')

            # 应用复权
            if adjust_type == 'qfq':
                # 前复权：以最新价格为基准
                latest_factor = adjust_factors['adj_factor'].iloc[-1] if not adjust_factors.empty else 1.0
                price_cols = ['open_price', 'close_price', 'high_price', 'low_price', 'pre_close_price']

                for col in price_cols:
                    if col in df_merged.columns:
                        df_merged[col] = df_merged[col] * df_merged['adj_factor'] / latest_factor

            elif adjust_type == 'hfq':
                # 后复权：以历史价格为基准
                earliest_factor = adjust_factors['adj_factor'].iloc[0] if not adjust_factors.empty else 1.0
                price_cols = ['open_price', 'close_price', 'high_price', 'low_price', 'pre_close_price']

                for col in price_cols:
                    if col in df_merged.columns:
                        df_merged[col] = df_merged[col] * earliest_factor / df_merged['adj_factor']

            # 移除复权因子列
            df_adjusted = df_merged.drop(columns=['adj_factor'], errors='ignore')

            return df_adjusted

        except Exception as e:
            logger.error(f"应用复权失败 {symbol}: {e}")
            return df

    def _get_adjust_factors(self, symbol: str) -> pd.DataFrame:
        """获取复权因子"""
        try:
            # 这里需要调用Tushare的复权因子接口
            # 简化实现，返回空DataFrame
            logger.warning(f"复权因子功能需要特殊API权限: {symbol}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取复权因子失败 {symbol}: {e}")
            return pd.DataFrame()

    def batch_update_daily_data(self,
                                symbols: List[str],
                                start_date: str,
                                end_date: str,
                                max_concurrent: int = 3,
                                progress_callback=None) -> Dict[str, Any]:
        """
        批量更新日线数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_concurrent: 最大并发数
            progress_callback: 进度回调函数

        Returns:
            批量处理结果
        """
        logger.info(f"开始批量更新日线数据: {len(symbols)} 只股票")

        # 标准化所有代码
        normalized_symbols = []
        invalid_symbols = []

        for symbol in symbols:
            try:
                normalized = normalize_stock_code(symbol)
                normalized_symbols.append(normalized)
            except Exception as e:
                logger.error(f"股票代码标准化失败 {symbol}: {e}")
                invalid_symbols.append(symbol)

        if invalid_symbols:
            logger.warning(f"无效的股票代码: {invalid_symbols}")

        # 执行批量处理
        batch_result = self.pipeline.batch_process_stocks(
            symbols=normalized_symbols,
            start_date=start_date,
            end_date=end_date,
            max_concurrent=max_concurrent
        )

        # 更新统计信息
        self.stats['total_fetched'] += batch_result.get('total_records', 0)
        self.stats['total_stored'] += sum(r.get('records_stored', 0) for r in batch_result.get('symbol_results', []))
        self.stats['total_failed'] += batch_result.get('failed', 0)
        self.stats['last_update'] = datetime.now()

        # 触发质量监控
        self._trigger_quality_monitor(batch_result)

        # 触发进度回调
        if progress_callback:
            progress_callback(batch_result)

        return batch_result

    def _trigger_quality_monitor(self, batch_result: Dict[str, Any]):
        """触发数据质量监控"""
        try:
            for result in batch_result.get('symbol_results', []):
                if result.get('status') == 'success' and result.get('symbol'):
                    symbol = result.get('symbol')

                    # 从数据库获取最新数据进行检查
                    df = self._get_daily_data_from_db(
                        symbol,
                        batch_result.get('start_date', '20240101'),
                        batch_result.get('end_date', datetime.now().strftime('%Y%m%d'))
                    )

                    if not df.empty:
                        quality_report = self.quality_monitor.check_daily_data(df)
                        self.quality_monitor.save_report(quality_report)

                        if quality_report.get('overall_score', 0) < 70:
                            logger.warning(f"数据质量警告: {symbol}, 评分: {quality_report['overall_score']}")

        except Exception as e:
            logger.error(f"触发质量监控失败: {e}")

    def get_market_data(self,
                        market: str = "A股",
                        index_code: str = "000001",
                        start_date: str = None,
                        end_date: str = None) -> pd.DataFrame:
        """
        获取市场指数数据

        Args:
            market: 市场类型
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            市场数据DataFrame
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        try:
            # 标准化指数代码
            if market == "A股":
                if index_code == "000001":
                    normalized_code = "sh000001"  # 上证指数
                elif index_code == "399001":
                    normalized_code = "sz399001"  # 深证成指
                elif index_code == "399006":
                    normalized_code = "sz399006"  # 创业板指
                else:
                    normalized_code = normalize_stock_code(index_code)

            # 获取数据
            cache_key = f"market_{normalized_code}_{start_date}_{end_date}"
            cached_data = self.cache_manager.get(cache_key)

            if cached_data is not None:
                logger.info(f"使用缓存市场数据: {normalized_code}")
                return cached_data

            # 从数据库获取
            connection = self.db_connector.get_connection()

            query = """
                SELECT * FROM index_daily_data 
                WHERE normalized_code = %s 
                AND trade_date >= %s 
                AND trade_date <= %s
                ORDER BY trade_date
            """

            df = pd.read_sql_query(query, connection,
                                   params=[normalized_code, start_date, end_date])
            connection.close()

            if not df.empty:
                self.cache_manager.set(cache_key, df, ttl_hours=6)
                return df
            else:
                logger.warning(f"未找到市场数据: {normalized_code}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取市场数据失败: {e}")
            return pd.DataFrame()

    def get_financial_data(self,
                           symbol: str,
                           report_type: str = "Annual",
                           years: int = 5) -> pd.DataFrame:
        """
        获取财务数据

        Args:
            symbol: 股票代码
            report_type: 报告类型 (Annual/Q1/H1/Q3)
            years: 获取最近N年的数据

        Returns:
            财务数据DataFrame
        """
        normalized_code = normalize_stock_code(symbol)

        try:
            connection = self.db_connector.get_connection()

            query = """
                SELECT * FROM stock_financial_indicators 
                WHERE normalized_code = %s 
                AND report_type = %s
                ORDER BY report_date DESC
                LIMIT %s
            """

            df = pd.read_sql_query(query, connection,
                                   params=[normalized_code, report_type, years * 4])
            connection.close()

            return df

        except Exception as e:
            logger.error(f"获取财务数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def cleanup_cache(self, older_than_days: int = 7):
        """清理过期缓存"""
        try:
            deleted_count = self.cache_manager.cleanup(older_than_days)
            logger.info(f"清理缓存完成，删除 {deleted_count} 条记录")

            # 清理数据库缓存表
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            cursor.execute("""
                DELETE FROM data_cache 
                WHERE expires_at < NOW() 
                AND last_accessed_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (older_than_days,))

            deleted_db_count = cursor.rowcount
            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"清理数据库缓存完成，删除 {deleted_db_count} 条记录")

            return deleted_count + deleted_db_count

        except Exception as e:
            logger.error(f"清理缓存失败: {e}")
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """获取数据统计信息"""
        try:
            connection = self.db_connector.get_connection()

            stats = {
                'basic_info_count': 0,
                'daily_data_count': 0,
                'minute_data_count': 0,
                'financial_data_count': 0,
                'last_update': None
            }

            # 查询各表记录数
            tables = [
                ('stock_basic_info', 'basic_info_count'),
                ('stock_daily_data', 'daily_data_count'),
                ('stock_minute_data', 'minute_data_count'),
                ('stock_financial_indicators', 'financial_data_count')
            ]

            for table_name, stat_key in tables:
                cursor = connection.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                stats[stat_key] = count
                cursor.close()

            connection.close()

            # 合并内存统计
            stats.update(self.stats)

            return stats

        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return self.stats


class CacheManager:
    """缓存管理器"""

    def __init__(self, cache_dir: str = './data/cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 内存缓存（LRU）
        self.memory_cache = {}
        self.max_memory_cache_size = 100  # 最大缓存条目数

        logger.info(f"缓存管理器初始化，缓存目录: {cache_dir}")

    def get(self, key: str) -> Optional[pd.DataFrame]:
        """获取缓存数据"""
        try:
            # 1. 检查内存缓存
            if key in self.memory_cache:
                data, expiry_time = self.memory_cache[key]
                if datetime.now() < expiry_time:
                    logger.debug(f"内存缓存命中: {key}")
                    return data
                else:
                    del self.memory_cache[key]

            # 2. 检查文件缓存
            cache_file = self.cache_dir / f"{key}.parquet"
            if cache_file.exists():
                # 检查文件是否过期
                file_mtime = cache_file.stat().st_mtime
                file_age = time.time() - file_mtime

                # 默认缓存1小时
                if file_age < 3600:
                    try:
                        df = pd.read_parquet(cache_file)

                        # 添加到内存缓存
                        self._add_to_memory_cache(key, df)

                        logger.debug(f"文件缓存命中: {key}")
                        return df
                    except Exception as e:
                        logger.warning(f"读取缓存文件失败 {key}: {e}")
                        cache_file.unlink(missing_ok=True)

            return None

        except Exception as e:
            logger.error(f"获取缓存失败 {key}: {e}")
            return None

    def set(self, key: str, data: pd.DataFrame, ttl_hours: int = 1):
        """设置缓存数据"""
        try:
            # 添加到内存缓存
            self._add_to_memory_cache(key, data, ttl_hours)

            # 保存到文件
            cache_file = self.cache_dir / f"{key}.parquet"
            data.to_parquet(cache_file)

            logger.debug(f"缓存已设置: {key}")

        except Exception as e:
            logger.error(f"设置缓存失败 {key}: {e}")

    def _add_to_memory_cache(self, key: str, data: pd.DataFrame, ttl_hours: int = 1):
        """添加到内存缓存"""
        expiry_time = datetime.now() + timedelta(hours=ttl_hours)
        self.memory_cache[key] = (data, expiry_time)

        # 清理过期的缓存条目
        self._cleanup_memory_cache()

        # 如果超出大小限制，移除最旧的条目
        if len(self.memory_cache) > self.max_memory_cache_size:
            oldest_key = next(iter(self.memory_cache))
            del self.memory_cache[oldest_key]

    def _cleanup_memory_cache(self):
        """清理内存缓存"""
        current_time = datetime.now()
        expired_keys = [
            key for key, (_, expiry_time) in self.memory_cache.items()
            if current_time >= expiry_time
        ]

        for key in expired_keys:
            del self.memory_cache[key]

    def cleanup(self, older_than_days: int = 7) -> int:
        """清理过期缓存文件"""
        try:
            deleted_count = 0
            cutoff_time = time.time() - (older_than_days * 24 * 3600)

            for cache_file in self.cache_dir.glob("*.parquet"):
                if cache_file.stat().st_mtime < cutoff_time:
                    cache_file.unlink()
                    deleted_count += 1

            # 清空内存缓存
            self.memory_cache.clear()

            logger.info(f"清理缓存文件完成，删除 {deleted_count} 个文件")
            return deleted_count

        except Exception as e:
            logger.error(f"清理缓存文件失败: {e}")
            return 0


class DataQualityMonitor:
    """数据质量监控器"""

    def __init__(self):
        self.quality_thresholds = {
            'excellent': 90,
            'good': 70,
            'fair': 50,
            'poor': 30
        }

    def check_daily_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查日线数据质量

        Args:
            df: 日线数据

        Returns:
            质量报告
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'data_type': 'daily',
            'total_records': len(df),
            'checks': {},
            'issues': [],
            'suggestions': []
        }

        if df.empty:
            report['overall_score'] = 0
            report['quality_level'] = 'empty'
            return report

        check_results = []

        # 1. 完整性检查
        completeness_score = self._check_completeness(df)
        check_results.append(('completeness', completeness_score))

        # 2. 准确性检查
        accuracy_score = self._check_accuracy(df)
        check_results.append(('accuracy', accuracy_score))

        # 3. 一致性检查
        consistency_score = self._check_consistency(df)
        check_results.append(('consistency', consistency_score))

        # 4. 及时性检查
        timeliness_score = self._check_timeliness(df)
        check_results.append(('timeliness', timeliness_score))

        # 计算综合评分
        weights = {
            'completeness': 0.3,
            'accuracy': 0.4,
            'consistency': 0.2,
            'timeliness': 0.1
        }

        overall_score = sum(
            score * weights.get(check_type, 0.25)
            for check_type, score in check_results
        )

        report['overall_score'] = round(overall_score, 2)
        report['quality_level'] = self._get_quality_level(overall_score)

        # 详细检查结果
        for check_type, score in check_results:
            report['checks'][check_type] = {
                'score': round(score, 2),
                'weight': weights.get(check_type, 0.25),
                'weighted_score': round(score * weights.get(check_type, 0.25), 2)
            }

        # 生成问题和建议
        report['issues'] = self._generate_issues(df, check_results)
        report['suggestions'] = self._generate_suggestions(report['issues'])

        return report

    def _check_completeness(self, df: pd.DataFrame) -> float:
        """检查完整性"""
        if df.empty:
            return 0.0

        required_cols = [
            'trade_date', 'open_price', 'close_price',
            'high_price', 'low_price', 'volume'
        ]

        # 检查必需列是否存在
        existing_cols = [col for col in required_cols if col in df.columns]
        completeness_col = len(existing_cols) / len(required_cols) * 100

        # 检查缺失值
        missing_score = 100
        for col in existing_cols:
            missing_pct = df[col].isnull().sum() / len(df) * 100
            missing_score -= missing_pct * 0.5

        return max(0, min(100, completeness_col * 0.6 + missing_score * 0.4))

    def _check_accuracy(self, df: pd.DataFrame) -> float:
        """检查准确性"""
        if df.empty:
            return 0.0

        accuracy_score = 100

        # 价格有效性检查
        price_cols = ['open_price', 'close_price', 'high_price', 'low_price']
        for col in price_cols:
            if col in df.columns:
                # 检查负值
                negative_count = (df[col] <= 0).sum()
                if negative_count > 0:
                    accuracy_score -= (negative_count / len(df)) * 50

        # 高低价关系检查
        if all(col in df.columns for col in ['high_price', 'low_price']):
            invalid_high_low = (df['high_price'] < df['low_price']).sum()
            if invalid_high_low > 0:
                accuracy_score -= (invalid_high_low / len(df)) * 100

        # 涨跌幅合理性检查
        if 'pct_change' in df.columns:
            extreme_changes = (df['pct_change'].abs() > 20).sum()
            if extreme_changes > 0:
                accuracy_score -= (extreme_changes / len(df)) * 30

        return max(0, min(100, accuracy_score))

    def _check_consistency(self, df: pd.DataFrame) -> float:
        """检查一致性"""
        if len(df) < 2:
            return 100.0

        consistency_score = 100

        # 检查时间序列连续性
        if 'trade_date' in df.columns:
            df_sorted = df.sort_values('trade_date')
            df_sorted['date_dt'] = pd.to_datetime(df_sorted['trade_date'])
            date_gaps = (df_sorted['date_dt'].diff().dt.days > 1).sum()

            if date_gaps > 0:
                consistency_score -= (date_gaps / len(df)) * 40

        # 检查成交量一致性
        if 'volume' in df.columns:
            zero_volume = (df['volume'] == 0).sum()
            if zero_volume > 0:
                consistency_score -= (zero_volume / len(df)) * 30

        return max(0, min(100, consistency_score))

    def _check_timeliness(self, df: pd.DataFrame) -> float:
        """检查及时性"""
        if df.empty or 'trade_date' not in df.columns:
            return 0.0

        # 获取最新数据日期
        latest_date = pd.to_datetime(df['trade_date']).max()
        days_diff = (datetime.now() - latest_date).days

        if days_diff <= 1:
            return 100
        elif days_diff <= 3:
            return 80
        elif days_diff <= 7:
            return 60
        elif days_diff <= 30:
            return 40
        else:
            return 20

    def _get_quality_level(self, score: float) -> str:
        """获取质量等级"""
        for level, threshold in self.quality_thresholds.items():
            if score >= threshold:
                return level
        return 'very_poor'

    def _generate_issues(self, df: pd.DataFrame, check_results: List[Tuple[str, float]]) -> List[str]:
        """生成问题列表"""
        issues = []

        for check_type, score in check_results:
            if score < 80:
                if check_type == 'completeness':
                    issues.append(f"数据完整性不足，得分: {score:.1f}")
                elif check_type == 'accuracy':
                    issues.append(f"数据准确性存在问题，得分: {score:.1f}")
                elif check_type == 'consistency':
                    issues.append(f"数据一致性不足，得分: {score:.1f}")
                elif check_type == 'timeliness':
                    issues.append(f"数据及时性不足，得分: {score:.1f}")

        # 具体问题检查
        if 'trade_date' in df.columns:
            latest_date = pd.to_datetime(df['trade_date']).max()
            days_diff = (datetime.now() - latest_date).days
            if days_diff > 7:
                issues.append(f"数据更新延迟 {days_diff} 天")

        if 'volume' in df.columns:
            zero_volume = (df['volume'] == 0).sum()
            if zero_volume > 0:
                issues.append(f"发现 {zero_volume} 条零成交量记录")

        return issues

    def _generate_suggestions(self, issues: List[str]) -> List[str]:
        """生成改进建议"""
        suggestions = []

        if any('完整性' in issue for issue in issues):
            suggestions.append("建议检查数据采集完整性，确保所有必需字段都被获取")

        if any('准确性' in issue for issue in issues):
            suggestions.append("建议验证价格数据的逻辑关系，修复高低价不一致问题")

        if any('一致性' in issue for issue in issues):
            suggestions.append("建议检查时间序列的连续性，补充缺失的交易日数据")

        if any('及时性' in issue for issue in issues):
            suggestions.append("建议更新数据源，获取最新的交易日数据")

        if any('零成交量' in issue for issue in issues):
            suggestions.append("建议检查停牌日期或数据质量问题")

        if not issues:
            suggestions.append("数据质量良好，继续保持当前数据更新策略")

        return suggestions

    def save_report(self, report: Dict[str, Any]):
        """保存质量报告"""
        try:
            report_dir = Path('data/quality_reports')
            report_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = report_dir / f"quality_report_{timestamp}.json"

            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            logger.info(f"质量报告已保存: {report_file}")

        except Exception as e:
            logger.error(f"保存质量报告失败: {e}")


# 使用示例
def main():
    """数据管理器使用示例"""
    print("股票数据管理器示例")
    print("=" * 50)

    # 1. 创建数据管理器
    print("初始化数据管理器...")
    data_manager = DataManager()

    # 2. 获取股票基本信息
    print("\n获取股票基本信息...")
    symbols = ['600519', '000001', '000858']

    basic_info = data_manager.get_stock_basic_info(symbols=symbols)
    if not basic_info.empty:
        print(f"获取到 {len(basic_info)} 只股票基本信息")
        print(basic_info[['symbol', 'name', 'industry', 'list_date']].head())
    else:
        print("未获取到股票基本信息")

    # 3. 获取单只股票日线数据
    print("\n获取单只股票日线数据...")
    df_daily = data_manager.get_daily_data(
        symbol='600519',
        start_date='20240101',
        end_date='20240131',
        adjust='qfq'
    )

    if not df_daily.empty:
        print(f"获取到 {len(df_daily)} 条日线数据")
        print(df_daily[['trade_date', 'open_price', 'close_price', 'volume', 'pct_change']].head())

        # 数据质量检查
        quality_monitor = DataQualityMonitor()
        quality_report = quality_monitor.check_daily_data(df_daily)

        print(f"\n数据质量报告:")
        print(f"  综合评分: {quality_report.get('overall_score', 0)}")
        print(f"  质量等级: {quality_report.get('quality_level', 'unknown')}")
        print(f"  总记录数: {quality_report.get('total_records', 0)}")
    else:
        print("未获取到日线数据")

    # 4. 获取市场指数数据
    print("\n获取市场指数数据...")
    df_market = data_manager.get_market_data(
        market="A股",
        index_code="000001",
        start_date="20240101",
        end_date="20240131"
    )

    if not df_market.empty:
        print(f"获取到 {len(df_market)} 条市场指数数据")
        print(df_market[['trade_date', 'close_point', 'pct_change']].head())
    else:
        print("未获取到市场指数数据")

    # 5. 获取统计信息
    print("\n获取数据统计信息...")
    stats = data_manager.get_stats()

    print(f"数据统计:")
    print(f"  股票基本信息: {stats.get('basic_info_count', 0)} 条")
    print(f"  日线数据: {stats.get('daily_data_count', 0)} 条")
    print(f"  财务数据: {stats.get('financial_data_count', 0)} 条")
    print(f"  总获取记录: {stats.get('total_fetched', 0)} 条")
    print(f"  总存储记录: {stats.get('total_stored', 0)} 条")
    print(f"  最后更新: {stats.get('last_update')}")

    # 6. 清理缓存
    print("\n清理过期缓存...")
    deleted_count = data_manager.cleanup_cache(older_than_days=1)
    print(f"清理缓存完成，删除 {deleted_count} 条记录")

    print("\n数据管理器示例完成!")


if __name__ == "__main__":
    main()