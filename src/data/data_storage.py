# -*- coding: utf-8 -*-
# File Path: E:/MyFile/stock_database_v1/src/data\data_storage.py
# @ Author: m_mango
# @ Date：2025/12/5 18:46
"""
数据存储管理器 - 增强优化版
在原版基础上优化：动态列映射、批量性能、错误处理、兼容性
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import logging

import os
import sys
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(project_root))

from src.config.logging_config import setup_logging
from src.database.db_connector import DatabaseConnector
from src.utils.code_converter import normalize_stock_code  # ✅ 强制添加此行

logger = setup_logging()


class DataStorage:
    """数据存储管理器 - 增强优化版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据存储器

        Args:
            config_path: 数据库配置文件路径
        """
        # 关键修复：添加 logger
        import logging
        self.logger = logging.getLogger(__name__)

        # 初始化数据库连接器
        self.db_connector = DatabaseConnector(config_path)

        # 缓存初始化
        self._table_columns_cache = {}  # 缓存表结构，避免重复查
        self._column_order_cache = {}

        # 设置映射关系
        self._setup_column_mappings()

        self.logger.info("数据存储器初始化完成")

        # 支持的表映射
        self.supported_tables = {
            'daily': 'stock_daily_data',
            'basic': 'stock_basic_info',
            'index_constituent': 'stock_index_constituent',
            'financial': 'stock_financial_indicators',
            'minute': 'stock_minute_data'
        }
        self.logger.info(f"数据库连接器: {self.db_connector.config.get('host')}:{self.db_connector.config.get('port')}")

    def _setup_column_mappings(self):
        """设置列名映射关系 - 修复版-修正版：量比"""
        # 正确的映射关系（根据表结构）
        self.column_mapping = {
            # 价格字段
            'open': 'open_price',
            'high': 'high_price', 
            'low': 'low_price',
            'close': 'close_price',
            
            # 成交字段 - 关键修复！
            'volume': 'volume',
            'amount': 'amount',      # Baostock的amount → 数据库的amount

            # 涨跌幅
            'pctChg': 'change_percent',
            'pct_change': 'change_percent',
            'change': 'change_amount',
            'pcfNcfTTM': 'pcf_ttm',


            # 技术指标
            'turnoverrate': 'turnover_rate',  # ✅ 修正：换手率--总股本
            'turn': 'turnover_rate_f',  # ✅ 修正:Baostock的turn → 数据库的turnover_rate_f流通换手率

            # ✅ 新增：量比（从外部计算）
            'volume_ratio': 'volume_ratio',

            # 其他字段
            'preclose': 'pre_close_price',
            'pre_close': 'pre_close_price',
            'pctChg': 'change_percent',
            'amplitude': 'amplitude',
            'turnover': 'turnover_rate',
            'adjustflag': 'adjust_flag',
            'tradestatus': 'trade_status',
            
            # 财务指标
            'pe': 'pe',
            'pe_ttm': 'pe_ttm',
            'pb': 'pb',
            'ps': 'ps',
            'ps_ttm': 'ps_ttm',
            'dv_ratio': 'dv_ratio',
            'dv_ttm': 'dv_ttm',
            
            # 股票基本信息
            'totalShare': 'total_share',
            'floatShare': 'float_share',
            'freeShare': 'free_share',
            'totalMv': 'total_mv',
            'circMv': 'circ_mv'
        }
        
        # 可选：添加验证日志
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"字段映射已设置: {len(self.column_mapping)} 个映射关系")
    def _get_table_columns(self, table_name: str) -> set:
        """获取数据库表的实际字段名集合"""
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                    cols = {row[0] for row in cursor.fetchall()}
                    self._table_columns_cache[table_name] = cols
                    logger.debug(f"缓存表 `{table_name}` 字段: {sorted(cols)}")
                    return cols
        except Exception as e:
            logger.error(f"无法获取表 `{table_name}` 结构: {e}")
            # 根据你提供的表结构，返回默认字段
            default_cols = {
                'symbol', 'trade_date', 'open_price', 'high_price', 'low_price',
                'close_price', 'pre_close_price', 'volume', 'amount', 'turnover_rate',
                'turnover_rate_f',
                'change_percent', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
                'amplitude', 'data_source', 'processed_time', 'quality_grade',
                'created_time', 'updated_time', 'volume_ma5', 'volume_ma10', 'volume_ma20',
                'rsi', 'bb_middle', 'bb_upper', 'bb_lower', 'volatility_20d',
                'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
                'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv'
            }
            self._table_columns_cache[table_name] = default_cols
            logger.warning(f"使用默认字段集合: {len(default_cols)} 个字段")
            return default_cols

    def _preprocess_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """核心优化：因子字段防误删 + 代码结构简化
        预处理数据 - v0.8.0 因子强制保留版
        """
        if df.empty:
            logger.warning("输入数据为空")
            return df

        original_columns = list(df.columns)
        logger.debug(f"原始字段: {original_columns}")

        df_processed = df.copy()

        # === 1. ✅ 股票代码字段强制标准化（关键修复） ===
        symbol_created = False

        if 'code' in df_processed.columns:
            df_processed['symbol'] = df_processed['code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            symbol_created = True
            logger.debug(f"从 'code' 字段生成标准化 symbol")

        elif 'bs_code' in df_processed.columns:
            df_processed['symbol'] = df_processed['bs_code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            symbol_created = True
            logger.debug(f"从 'bs_code' 字段生成标准化 symbol")

        elif 'symbol' in df_processed.columns:
            df_processed['symbol'] = df_processed['symbol'].apply(
                lambda x: normalize_stock_code(str(x)) if pd.notna(x) else None
            )
            symbol_created = True
            logger.debug(f"标准化现有 symbol 字段")

        if not symbol_created:
            logger.error(f"❌ 预处理失败：找不到股票代码字段")
            return pd.DataFrame()

        # === 2. 日期字段处理 ===
        date_created = False

        if 'trade_date' in df_processed.columns:
            df_processed['trade_date'] = pd.to_datetime(df_processed['trade_date'], errors='coerce')
            date_created = True
            logger.debug(f"使用现有 'trade_date' 字段")

        elif 'date' in df_processed.columns:
            df_processed['trade_date'] = pd.to_datetime(df_processed['date'], errors='coerce')
            date_created = True
            logger.debug(f"从 'date' 字段生成 trade_date")

        if not date_created:
            logger.error(f"❌ 预处理失败：找不到日期字段")
            return pd.DataFrame()

        # === 3. 字段映射（Baostock → 数据库）===
        field_mapping = {
            # 价格字段
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'preclose': 'pre_close_price',
            'pre_close': 'pre_close_price',

            # 成交量金额
            'volume': 'volume',
            'amount': 'amount',

            # 涨跌幅
            'pctChg': 'change_percent',
            'pct_change': 'change_percent',
            'change': 'change_amount',
            'pcfNcfTTM': 'pcf_ttm',

            # 技术指标
            'turn': 'turnover_rate_f',
            'turnoverrate': 'turnover_rate',
            'amplitude': 'amplitude',

            # 其他字段
            'adjustflag': 'adjust_flag',
            'tradestatus': 'trade_status'
        }

        # 应用字段映射
        rename_map = {}
        for src_field, target_field in field_mapping.items():
            if src_field in df_processed.columns and target_field not in df_processed.columns:
                rename_map[src_field] = target_field

        if rename_map:
            df_processed = df_processed.rename(columns=rename_map)
            logger.debug(f"字段映射: {rename_map}")

        # === 4. ❌ 移除 Baostock 原始因子字段（避免命名冲突）===
        # Baostock 返回的原始字段在映射后应删除
        baostock_raw_fields = ['peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM']
        for field in baostock_raw_fields:
            if field in df_processed.columns:
                df_processed = df_processed.drop(columns=[field])
                logger.debug(f"删除 Baostock 原始字段: {field}")


        # === 5. 数据类型转换 ===
        # ✅ 统一定义所有数值字段（包含因子）
        numeric_fields = [
            'open_price', 'high_price', 'low_price', 'close_price',
            'pre_close_price', 'volume', 'amount', 'change_percent',
            'turnover_rate', 'turnover_rate_f', 'amplitude',
            'change_amount', 'volume_ratio',
            # ✅ 核心因子字段
            'pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm'
        ]

        # ✅ 动态添加其他可能存在的因子字段
        additional_factors = ['pe', 'ps', 'dv_ratio', 'dv_ttm',
                              'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
        for field in additional_factors:
            if field in df_processed.columns:
                numeric_fields.append(field)

        # 执行数值转换
        for field in numeric_fields:
            if field in df_processed.columns:
                df_processed[field] = pd.to_numeric(df_processed[field], errors='coerce')
                logger.debug(f"数值转换 {field}: {df_processed[field].notna().sum()} 条有效")


        # === 6. ✅ 智能清理原始字段（明确保留因子）===
        columns_to_remove = [
            'code', 'bs_code', 'date', 'open', 'high', 'low', 'close',
            'preclose', 'pctChg', 'turn', 'adjustflag', 'tradestatus'
        ]

        # ✅ 因子保护列表（必须保留）
        protected_fields = ['pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm'] + additional_factors

        # 执行清理
        for col in columns_to_remove:
            if col in df_processed.columns and col not in protected_fields:
                df_processed = df_processed.drop(columns=[col])
                logger.debug(f"删除原始字段: {col}")


        # === 6. 数据过滤（只删除关键字段为空的行）===
        before_filter = len(df_processed)
        df_processed = df_processed.dropna(subset=['symbol', 'trade_date'], how='any')

        # 确保 symbol 不为空字符串
        if 'symbol' in df_processed.columns:
            df_processed = df_processed[df_processed['symbol'].notna() & (df_processed['symbol'] != '')]

        after_filter = len(df_processed)
        if before_filter > after_filter:
            logger.info(f"过滤掉 {before_filter - after_filter} 条无效行")

        # === 7. 日期格式标准化 ===
        if 'trade_date' in df_processed.columns:
            # 转换为 MySQL 标准格式
            df_processed['trade_date'] = pd.to_datetime(
                df_processed['trade_date'],
                errors='coerce'
            ).dt.strftime('%Y-%m-%d')
            logger.debug("trade_date 已格式化为 YYYY-MM-DD")

        # === 8. 添加元数据字段 ===
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'created_time' in self._get_table_columns(table_name):
            df_processed['created_time'] = current_time

        if 'updated_time' in self._get_table_columns(table_name):
            df_processed['updated_time'] = current_time

        # === 9. 计算衍生字段（量比、振幅）===
        # 计算量比（volume_ratio）
        if 'volume' in df_processed.columns and len(df_processed) > 5:
            df_processed['volume_ratio'] = (
                    df_processed['volume'] /
                    df_processed['volume'].shift(1).rolling(5, min_periods=1).mean()
            ).fillna(1.0).clip(0, 50)
            logger.debug(f"计算量比: 均值={df_processed['volume_ratio'].mean():.2f}")

        # 计算振幅（amplitude）
        if all(col in df_processed.columns for col in ['high_price', 'low_price', 'pre_close_price']):
            df_processed['amplitude'] = (
                    (df_processed['high_price'] - df_processed['low_price']) /
                    df_processed['pre_close_price'] * 100
            ).round(4)
            logger.debug(f"计算振幅: 均值={df_processed['amplitude'].mean():.2f}%")

        # === 10. 最终验证 ===
        factor_cols = [col for col in ['pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm'] if col in df_processed.columns]
        logger.info(f"✅ 因子字段保留验证: {len(factor_cols)}个 -> {factor_cols}")

        logger.info(f"✅ 预处理完成: {len(df_processed)} 条记录, {len(df_processed.columns)} 个字段")
        logger.debug(f"最终字段: {list(df_processed.columns)}")

        return df_processed

    def _get_table_column_order(self, table_name: str) -> List[str]:
        """
        获取数据库表的字段定义顺序
        """
        cache_key = f"{table_name}_order"

        if hasattr(self, '_column_order_cache') and cache_key in self._column_order_cache:
            return self._column_order_cache[cache_key]

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                    columns = [row[0] for row in cursor.fetchall()]

                    if not hasattr(self, '_column_order_cache'):
                        self._column_order_cache = {}
                    self._column_order_cache[cache_key] = columns

                    logger.debug(f"获取表 {table_name} 字段顺序: {len(columns)} 个字段")
                    return columns

        except Exception as e:
            logger.error(f"无法获取表 `{table_name}` 字段顺序: {e}")

            default_order = [
                'symbol', 'trade_date', 'open_price', 'high_price', 'low_price',
                'close_price', 'pre_close_price', 'volume', 'amount', 'turnover_rate',
                'turnover_rate_f', 'volume_ratio', 'ma5', 'ma10', 'ma20', 'ma30',
                'ma60', 'ma120', 'ma250', 'amplitude', 'data_source', 'processed_time',
                'quality_grade', 'created_time', 'updated_time', 'change_percent',
                'volume_ma5', 'volume_ma10', 'volume_ma20', 'rsi', 'bb_middle',
                'bb_upper', 'bb_lower', 'volatility_20d', 'pe', 'pe_ttm', 'pb',
                'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share', 'float_share',
                'free_share', 'total_mv', 'circ_mv'
            ]

            if not hasattr(self, '_column_order_cache'):
                self._column_order_cache = {}
            self._column_order_cache[cache_key] = default_order

            return default_order

    def _build_dynamic_sql(self, df: pd.DataFrame, table_name: str) -> Tuple[str, str, List[str]]:
        """
        构建动态SQL语句 - 确保使用数据库字段顺序
        """
        # 获取数据库表的字段顺序
        db_column_order = self._get_table_column_order(table_name)

        # 只选择数据库表中存在且DataFrame中有的字段
        valid_columns = [col for col in db_column_order if col in df.columns]

        if not valid_columns:
            raise ValueError(f"没有有效的列可以插入到表 {table_name}")

        # 检查必需字段是否存在
        required_columns = ['symbol', 'trade_date']
        for req_col in required_columns:
            if req_col not in valid_columns:
                raise ValueError(f"必需字段 '{req_col}' 不存在")

        columns_str = ', '.join([f'`{col}`' for col in valid_columns])
        placeholders = ', '.join(['%s'] * len(valid_columns))
        insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        # 假设唯一键为 (symbol, trade_date)
        unique_columns = ['symbol', 'trade_date']
        update_columns = [col for col in valid_columns if col not in unique_columns]

        if update_columns:
            update_set = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
            update_sql = f"ON DUPLICATE KEY UPDATE {update_set}"
        else:
            update_sql = ""

        logger.debug(f"构建SQL - 表: {table_name}, 插入列: {len(valid_columns)}")
        logger.debug(f"字段顺序: {valid_columns}")

        return insert_sql, update_sql, valid_columns

    def _prepare_records(self, df: pd.DataFrame, valid_columns: List[str] = None) -> List[Tuple]:
        """
        准备插入记录 - 修复：使用指定的字段顺序
        """
        if valid_columns is None:
            valid_columns = list(df.columns)

        records = []

        for _, row in df.iterrows():
            record = []
            for col in valid_columns:
                value = row[col] if col in row.index else None

                if pd.isna(value):
                    record.append(None)
                elif isinstance(value, (np.integer, np.int64)):
                    record.append(int(value))
                elif isinstance(value, (np.floating, np.float64)):
                    record.append(float(value))
                elif isinstance(value, (datetime, pd.Timestamp)):
                    # 确保时间戳格式正确
                    if isinstance(value, pd.Timestamp):
                        record.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        record.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(value, str) and col in ['trade_date']:
                    # 如果是日期字符串，确保格式正确
                    try:
                        pd_date = pd.to_datetime(value)
                        record.append(pd_date.strftime('%Y-%m-%d'))
                    except:
                        record.append(value)
                else:
                    record.append(value)

            records.append(tuple(record))

        return records

    def store_daily_data(self, data, table_name: str = None) -> Tuple[int, Dict]:
        """
        存储日线数据 - 增强版：支持多种输入类型并确保数据一致性

        Args:
            data: 输入数据，可以是 pd.DataFrame、list of dicts 或 dict
            table_name: 目标表名，默认使用 stock_daily_data

        Returns:
            (影响行数, 详细信息字典)
        """
        try:
            # 1. 转换输入为 DataFrame
            if isinstance(data, pd.DataFrame):
                df = data.copy()
            elif isinstance(data, list):
                if not data:
                    logger.warning("日线数据为空列表，跳过存储")
                    return 0, {'status': 'skipped', 'reason': 'empty_list'}
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                error_msg = f"不支持的输入类型: {type(data)}"
                logger.error(error_msg)
                return 0, {'status': 'error', 'reason': error_msg}

            if df.empty:
                logger.warning("日线数据为空，跳过存储")
                return 0, {'status': 'skipped', 'reason': 'empty_data'}

            logger.info(f"接收到数据: {type(data).__name__} -> 转换后 {len(df)} 行 {len(df.columns)} 列")

            # 2. 确定表名
            if table_name is None:
                table_name = self.supported_tables.get('daily', 'stock_daily_data')

            # 3. 数据验证和日志
            logger.info(f"开始处理数据，目标表: {table_name}")
            logger.debug(f"数据列名: {list(df.columns)}")
            logger.debug(f"前2行示例:\n{df.head(2).to_string()}")

            # 检查必要字段
            required_columns = ['symbol', 'trade_date']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                error_msg = f"缺少必要字段: {missing_columns}"
                logger.error(error_msg)
                return 0, {'status': 'error', 'reason': error_msg, 'missing_columns': missing_columns}

            # 4. 数据预处理
            logger.info(f"开始预处理数据，原始数据 {len(df)} 条")
            df_processed = self._preprocess_data(df, table_name)

            if df_processed.empty:
                logger.error("预处理后数据为空")
                return 0, {'status': 'skipped', 'reason': 'preprocess_failed'}

            logger.info(f"✅ 预处理完成: {len(df_processed)} 条记录")
            logger.debug(f"预处理后列名: {list(df_processed.columns)}")

            # 5. 检查字段一致性
            with self.db_connector.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    # 获取表结构
                    cursor.execute(f"DESCRIBE {table_name}")
                    table_columns = [row['Field'] for row in cursor.fetchall()]

                    # 获取数据列
                    data_columns = list(df_processed.columns)

                    # 找出匹配的列
                    matching_columns = [col for col in data_columns if col in table_columns]
                    missing_in_table = [col for col in data_columns if col not in table_columns]
                    missing_in_data = [col for col in table_columns if
                                       col not in data_columns and col not in ['id', 'created_time', 'updated_time']]

                    logger.info(f"字段匹配: {len(matching_columns)}/{len(data_columns)} 个字段可插入")
                    if missing_in_table:
                        logger.warning(f"数据中的字段在表中不存在: {missing_in_table}")
                    if missing_in_data:
                        logger.debug(f"表中的字段在数据中不存在: {missing_in_data}")

            # 6. 构建动态SQL - 只使用匹配的字段
            insert_sql, update_sql, valid_columns = self._build_dynamic_sql(df_processed, table_name)
            full_sql = insert_sql + (" " + update_sql if update_sql else "")

            logger.debug(f"SQL语句: {full_sql}")
            logger.info(f"将插入 {len(valid_columns)} 个字段: {valid_columns}")

            # 7. 准备记录
            records = self._prepare_records(df_processed, valid_columns)

            if not records:
                logger.error("没有有效的记录可以插入")
                return 0, {'status': 'skipped', 'reason': 'no_valid_records'}

            # 8. 批量插入 - 使用事务确保数据一致性
            logger.info(f"开始插入 {len(records)} 条记录到表 {table_name}")
            logger.debug(f"第一条记录示例: {records[0] if records else 'None'}")

            affected_rows = 0
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        # 批量插入
                        cursor.executemany(full_sql, records)
                        affected_rows = cursor.rowcount

                        # 显式提交事务
                        conn.commit()

                        logger.info(f"✅ 数据库提交成功，影响行数: {affected_rows}")

                        # 验证实际插入的记录
                        symbol = df_processed['symbol'].iloc[0] if 'symbol' in df_processed.columns else 'unknown'
                        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name} WHERE symbol = %s", (symbol,))
                        actual_count = cursor.fetchone()[0]
                        logger.info(f"✅ 验证成功: 表中有 {actual_count} 条 {symbol} 的记录")

                    except Exception as e:
                        # 出错时回滚
                        conn.rollback()
                        logger.error(f"❌ 数据库插入失败，已回滚: {e}")

                        # 尝试单条插入以找出问题记录
                        logger.info("尝试单条插入以调试问题...")
                        for i, record in enumerate(records[:3]):  # 只测试前3条
                            try:
                                cursor.execute(full_sql, record)
                                logger.info(f"  记录 {i + 1} 单独插入成功")
                            except Exception as single_error:
                                logger.error(f"  记录 {i + 1} 失败: {single_error}")
                                logger.error(f"    记录内容: {record}")

                        raise

            # 9. 返回成功结果
            symbol = df_processed['symbol'].iloc[0] if 'symbol' in df_processed.columns else 'unknown'
            logger.info(
                f"存储日线数据完成: {symbol}, "
                f"表: {table_name}, "
                f"记录: {len(records)}条, "
                f"影响: {affected_rows}行"
            )

            # 10. 记录数据更新日志
            self._log_data_update(
                data_type='daily',
                symbol=symbol,
                table_name=table_name,
                records_processed=len(records),
                records_affected=affected_rows,
                status='success'
            )

            return affected_rows, {
                'status': 'success',
                'table': table_name,
                'records_processed': len(records),
                'records_affected': affected_rows,
                'symbol': symbol,
                'input_type': type(data).__name__,
                'input_shape': f"{len(df)}x{len(df.columns)}",
                'processed_shape': f"{len(df_processed)}x{len(df_processed.columns)}",
                'matching_columns': len(matching_columns),
                'total_columns': len(data_columns),
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"存储日线数据失败: {e}", exc_info=True)

            # 记录失败日志
            symbol = 'unknown'
            if 'df' in locals() and 'symbol' in df.columns:
                symbol = df['symbol'].iloc[0] if not df.empty else 'unknown'

            self._log_data_update(
                data_type='daily',
                symbol=symbol,
                table_name=table_name or 'stock_daily_data',
                records_processed=0,
                records_affected=0,
                status='error',
                error_message=str(e)
            )

            return 0, {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__,
                'symbol': symbol,
                'table': table_name,
                'timestamp': datetime.now().isoformat()
            }


    def _build_dynamic_sql(self, df: pd.DataFrame, table_name: str) -> Tuple[str, str, list]:
        """
        构建动态SQL语句

        Returns:
            (INSERT语句, ON DUPLICATE KEY UPDATE语句, 有效字段列表)
        """
        try:
            # 获取表的实际字段
            with self.db_connector.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(f"DESCRIBE {table_name}")
                    table_fields = [row['Field'] for row in cursor.fetchall()]

            # 找出数据中存在的表字段
            data_columns = list(df.columns)
            valid_columns = [col for col in data_columns if col in table_fields]

            if not valid_columns:
                raise ValueError(f"没有匹配的字段可以插入到表 {table_name}")

            # 构建INSERT部分
            columns_str = ', '.join(valid_columns)
            placeholders = ', '.join(['%s'] * len(valid_columns))
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # 构建ON DUPLICATE KEY UPDATE部分
            # 确定唯一键：假设symbol和trade_date是唯一键
            unique_keys = ['symbol', 'trade_date']
            update_columns = [col for col in valid_columns if
                              col not in unique_keys and col not in ['id', 'created_time']]

            if update_columns:
                update_set = ', '.join([f"{col} = VALUES({col})" for col in update_columns])
                update_sql = f"ON DUPLICATE KEY UPDATE {update_set}"
            else:
                update_sql = ""

            logger.debug(f"构建SQL: {len(valid_columns)}个字段，{len(update_columns)}个更新字段")

            return insert_sql, update_sql, valid_columns

        except Exception as e:
            logger.error(f"构建SQL语句失败: {e}", exc_info=True)
            raise

    def _prepare_records(self, df: pd.DataFrame, valid_columns: list) -> list:
        """
        准备要插入的记录
        """
        try:
            # 确保所有字段都存在
            for col in valid_columns:
                if col not in df.columns:
                    df[col] = None

            # 转换DataFrame为记录列表
            records = []
            for _, row in df.iterrows():
                record = tuple(row[col] if pd.notna(row[col]) else None for col in valid_columns)
                records.append(record)

            logger.debug(f"准备了 {len(records)} 条记录，每条 {len(valid_columns)} 个字段")

            return records

        except Exception as e:
            logger.error(f"准备记录失败: {e}", exc_info=True)
            return []

    def _log_data_update(self, data_type: str, symbol: str, table_name: str,
                         records_processed: int, records_affected: int,
                         status: str, error_message: str = None):
        """
        记录数据更新日志
        """
        try:
            log_entry = {
                'data_type': data_type,
                'symbol': symbol,
                'table_name': table_name,
                'records_processed': records_processed,
                'records_affected': records_affected,
                'status': status,
                'error_message': error_message,
                'created_time': datetime.now()
            }

            # 记录到日志文件
            logger.info(f"📝 数据更新日志: {data_type} {symbol} 行数: {records_processed} 状态: {status}")

            # 可以在这里将日志存入数据库
            # self._save_update_log_to_db(log_entry)

        except Exception as e:
            logger.error(f"记录数据更新日志失败: {e}")

    def get_last_update_date(self, symbol: str = None, table_name: str = None) -> Optional[str]:
        """
        获取指定股票或全表的最后更新日期

        Args:
            symbol: 股票代码（可带点，如 'sh.600519'）
            table_name: 表名

        Returns:
            最后更新日期字符串，如 '2025-12-31'，如果不存在则返回 None
        """
        try:
            if table_name is None:
                table_name = self.supported_tables.get('daily', 'stock_daily_data')

            with self.db_connector.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    if symbol:
                        clean_symbol = str(symbol).replace('.', '')
                        query = f"SELECT MAX(trade_date) as last_date FROM `{table_name}` WHERE symbol = %s"
                        cursor.execute(query, (clean_symbol,))
                    else:
                        query = f"SELECT MAX(trade_date) as last_date FROM `{table_name}`"
                        cursor.execute(query)

                    result = cursor.fetchone()
                    if result and result['last_date']:
                        if isinstance(result['last_date'], str):
                            return result['last_date']
                        else:
                            return result['last_date'].strftime('%Y-%m-%d')
                    return None

        except Exception as e:
            logger.warning(f"获取最后更新日期失败: {e}")
            return None


    def log_data_update(self, data_type: str, symbol: str, *args, **kwargs):
        """
        记录数据更新日志 - 简化稳定版
        先保证不报错，让主流程能运行
        """
        try:
            # 简单的日志记录
            log_msg = f"数据更新日志: {data_type} {symbol}"

            # 尝试解析基本参数
            if len(args) >= 2:
                # 格式: (rows_affected, status)
                rows = args[0]
                status = args[1] if len(args) > 1 else 'unknown'
                log_msg += f" 行数: {rows} 状态: {status}"
            elif 'rows_affected' in kwargs:
                log_msg += f" 行数: {kwargs['rows_affected']} 状态: {kwargs.get('status', 'unknown')}"

            # 记录日志（使用 print 确保总是能输出）
            print(f"📝 {log_msg}")
            if hasattr(self, 'logger'):
                self.logger.info(log_msg)

            return {'success': True}

        except Exception as e:
            # 即使出错也不影响主流程
            error_msg = f"日志记录简化处理失败: {e}"
            print(f"⚠️ {error_msg}")
            if hasattr(self, 'logger'):
                self.logger.warning(error_msg)
            return {'success': False, 'error': str(e)}

def test_fixed_storage():
    """测试修复后的数据存储器"""
    import pandas as pd
    from datetime import datetime

    print("🧪 测试修复版数据存储器")
    print("=" * 50)

    try:
        # 1. 初始化
        print("初始化 DataStorage...")
        storage = DataStorage()
        print("✅ DataStorage 初始化成功")

        # 2. 创建测试数据
        print("创建测试数据...")
        test_symbol = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        test_date = datetime.now().strftime('%Y-%m-%d')

        test_data = pd.DataFrame({
            'symbol': [test_symbol],
            'trade_date': [test_date],
            'open_price': [100.0],
            'high_price': [105.0],
            'low_price': [95.0],
            'close_price': [102.0],
            'volume': [1000000],
            'amount': [102000000],
            'data_source': ['test'],
            'processed_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })

        print(f"测试数据: {test_symbol} {test_date}")

        # 3. 存储数据
        print("存储数据...")
        affected_rows, report = storage.store_daily_data(test_data)

        print(f"存储结果:")
        print(f"  影响行数: {affected_rows}")
        print(f"  状态: {report['status']}")

        # 4. 验证数据是否插入
        if affected_rows > 0:
            time.sleep(1)
            last_date = storage.get_last_update_date(symbol=test_symbol)
            if last_date == test_date:
                print("✅ 数据验证成功：最后更新日期匹配")
            else:
                print(f"❌ 验证失败：期望 {test_date}, 实际 {last_date}")

            # 清理测试数据
            print("清理测试数据...")
            try:
                with storage.db_connector.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
                        conn.commit()
                        print(f"清理完成，删除 {cursor.rowcount} 条记录")
            except Exception as e:
                print(f"⚠️ 清理失败: {e}")

        return affected_rows > 0

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_fixed_storage()

    if success:
        print("\n✅ 修复版数据存储器测试通过！")
    else:
        print("\n❌ 修复版数据存储器测试失败")

    exit(0 if success else 1)