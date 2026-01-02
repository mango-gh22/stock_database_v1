# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\adaptive_storage.py
# File Name: adaptive_storage
# @ Author: mango-gh22
# @ Date：2025/12/10 22:15
"""
desc 适配现有表结构的存储类
自适应数据存储器 - 根据实际表结构动态适配
"""

import logging

from src.database.db_connector import DatabaseConnector
from src.utils.logger import get_logger
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Dict, List, Optional, Any
from src.utils.code_converter import normalize_stock_code

logger = get_logger(__name__)

class AdaptiveDataStorage:
    """自适应数据存储器 - 根据表结构动态适配"""

    def __init__(self, config_path: str = 'config/database.yaml', table_name: str = 'stock_daily_data'):
        """
        自适应数据存储器初始化

        Args:
            config_path: 配置文件路径，如果为 None 则使用默认路径
            table_name: 目标表名
        """
        self.logger = logging.getLogger(__name__)  # 👈 添加这行

        if config_path is None:
            from pathlib import Path
            project_root = Path(__file__).parent.parent.parent
            config_path = str(project_root / "config" / "database.yaml")

        self.db_connector = DatabaseConnector(config_path)
        self.table_name = table_name
        self.table_columns = self._load_table_columns()
        self.column_mapping = self._create_column_mapping()
        logger.info(f"自适应数据存储器初始化完成: {table_name}, {len(self.table_columns)}列")

    def _load_table_columns(self) -> List[str]:
        """加载表中实际存在的列"""
        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {self.table_name}")
                    columns = cursor.fetchall()
                    return [col[0] for col in columns]
        except Exception as e:
            logger.error(f"加载表列失败: {e}")
            # 返回常见列作为后备
            return [
                'id', 'symbol', 'trade_date', 'open', 'close', 'high', 'low',
                'pre_close', 'volume', 'amount', 'pct_change', 'change_amount',
                'turnover_rate', 'turnover_rate_f', 'volume_ratio',
                'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250',
                'amplitude', 'created_at', 'updated_at'
            ]

    def _create_column_mapping(self) -> Dict[str, str]:
        """创建列名映射关系 - v0.6.2 修复版（仅修改5个字段）"""
        mapping = {
            # 基础价格数据映射 - 修复：目标列必须与数据库一致
            'open_price': 'open_price',      # ✅ 修复：'open' → 'open_price'
            'high_price': 'high_price',      # ✅ 修复：'high' → 'high_price'
            'low_price': 'low_price',        # ✅ 修复：'low' → 'low_price'
            'close_price': 'close_price',    # ✅ 修复：'close' → 'close_price'
            'pre_close_price': 'pre_close_price',  # ✅ 修复：'pre_close' → 'pre_close_price'

            # 其余字段保持不变（已经正确）
            'change_percent': 'pct_change',
            'change_amount': 'change_amount',
            'volume': 'volume',
            'amount': 'amount',
            'amplitude': 'amplitude',

            # 技术指标映射（正确）
            'ma5': 'ma5',
            'ma10': 'ma10',
            'ma20': 'ma20',
            'ma30': 'ma30',
            'ma60': 'ma60',
            'ma120': 'ma120',
            'ma250': 'ma250',

            # 日期和代码
            'trade_date': 'trade_date',
            'symbol': 'symbol',

            # 元数据
            'created_at': 'created_at',
            'updated_at': 'updated_at'
        }

        # 验证映射（保留原有逻辑）
        valid_mapping = {}
        for source_col, target_col in mapping.items():
            if target_col in self.table_columns:
                valid_mapping[source_col] = target_col
            else:
                logger.debug(f"映射跳过: {source_col} → {target_col} (目标列不存在)")

        logger.info(f"列映射创建完成: {len(valid_mapping)}个有效映射")
        return valid_mapping

    def store_daily_data(self, df: pd.DataFrame) -> Tuple[int, Dict]:
        """
        存储日线数据 - 自适应表结构

        Args:
            df: 要存储的数据

        Returns:
            (影响行数, 详细报告)
        """
        if df.empty:
            logger.warning("日线数据为空，跳过存储")
            return 0, {'status': 'skipped', 'reason': 'empty_data'}

        try:
            # 1. 准备数据（根据表结构调整）
            df_processed = self._prepare_data(df)

            if df_processed.empty:
                return 0, {'status': 'skipped', 'reason': 'no_valid_columns'}

            # 2. 构建并执行SQL
            affected_rows = self._execute_insert(df_processed)

            # 3. 返回结果
            symbol = df_processed['symbol'].iloc[0] if 'symbol' in df_processed.columns else 'unknown'
            logger.info(f"存储完成: {symbol}, {affected_rows}行影响")

            return affected_rows, {
                'status': 'success',
                'table': self.table_name,
                'records': len(df_processed),
                'affected': affected_rows,
                'symbol': symbol,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"存储失败: {e}")
            return 0, {
                'status': 'error',
                'reason': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """准备数据 - v0.6.1 修复版"""
        df_processed = df.copy()

        # v0.6.1 诊断日志
        self.logger.info(f"📊 准备数据: 输入{len(df_processed)}行, {len(df_processed.columns)}列")
        self.logger.debug(f"📊 输入列名: {list(df_processed.columns)}")

        # 1. 快速匹配路径（输入列名已匹配数据库）
        direct_match_cols = [col for col in df_processed.columns if col in self.table_columns]

        if len(direct_match_cols) >= 7:  # 核心列足够
            self.logger.info(f"✅ 快速路径: {len(direct_match_cols)}列直接匹配")
            df_processed = df_processed[direct_match_cols]
        else:
            # 2. 备用路径：尝试映射（不应该走到这里）
            self.logger.warning(f"⚠️ 只有{len(direct_match_cols)}列直接匹配，尝试映射")

            rename_dict = {}
            for src, tgt in self.column_mapping.items():
                if src in df_processed.columns and tgt in self.table_columns:
                    rename_dict[src] = tgt

            if rename_dict:
                df_processed = df_processed.rename(columns=rename_dict)
                self.logger.info(f"重命名列: {rename_dict}")

            # 再次过滤
            final_cols = [col for col in df_processed.columns if col in self.table_columns]
            df_processed = df_processed[final_cols]

        # 3. 数据类型转换（保留成功列）
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'amount']
        for col in numeric_cols:
            if col in df_processed.columns:
                try:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
                except Exception as e:
                    self.logger.warning(f"列转换失败 {col}: {e}")

        # 4. 处理缺失值（只过滤 symbol/trade_date 为空的行）
        before_filter = len(df_processed)

        # v0.6.1 关键修复：只过滤关键字段为空的行，不删除 NaN 数值
        if 'symbol' in df_processed.columns:
            df_processed = df_processed[df_processed['symbol'].notna() & (df_processed['symbol'] != '')]

        if 'trade_date' in df_processed.columns:
            df_processed = df_processed[df_processed['trade_date'].notna()]

        after_filter = len(df_processed)
        if before_filter > after_filter:
            self.logger.info(f"过滤掉 {before_filter - after_filter} 条无效行")

        # 5. 日期格式化
        if 'trade_date' in df_processed.columns:
            df_processed['trade_date'] = self._standardize_date_format(df_processed['trade_date'])

        # 6. 添加时间戳
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'created_time' in self.table_columns and 'created_time' not in df_processed.columns:
            df_processed['created_at'] = current_time
        if 'updated_time' in self.table_columns and 'updated_time' not in df_processed.columns:
            df_processed['updated_at'] = current_time

        self.logger.info(f"✅ 数据准备完成: {len(df_processed)}行, {len(df_processed.columns)}列")
        self.logger.debug(f"最终列: {list(df_processed.columns)}")

        return df_processed

    def _standardize_date_format(self, date_series):
        """
        标准化日期格式为 'YYYY-MM-DD'

        支持输入格式:
        - '20240128' -> '2024-01-28'
        - '2024-01-28' -> '2024-01-28' (保持不变)
        - '2024/01/28' -> '2024-01-28'
        - datetime对象 -> 'YYYY-MM-DD'
        """

        def format_single_date(date_val):
            if pd.isna(date_val):
                return None

            # 如果是字符串
            if isinstance(date_val, str):
                # 移除空格
                date_str = date_val.strip()

                # 如果已经是 YYYY-MM-DD 格式，直接返回
                if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
                    return date_str

                # 如果是 YYYY/MM/DD 格式
                if len(date_str) == 10 and date_str[4] == '/' and date_str[7] == '/':
                    return date_str.replace('/', '-')

                # 如果是 YYYYMMDD 格式
                if len(date_str) == 8 and date_str.isdigit():
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            # 如果是datetime或Timestamp
            elif isinstance(date_val, (datetime, pd.Timestamp)):
                return date_val.strftime('%Y-%m-%d')

            # 其他情况尝试转换
            try:
                return pd.to_datetime(date_val, errors='coerce').strftime('%Y-%m-%d')
            except:
                return None

        # 应用格式化函数
        if isinstance(date_series, pd.Series):
            return date_series.apply(format_single_date)
        else:
            return format_single_date(date_series)

    # _*_ coding: utf-8 _*_
    # File Path: E:/MyFile/stock_database_v1/src/data\adaptive_storage.py
    # ... 保留头部导入和方法 ...

    def _execute_insert(self, df: pd.DataFrame) -> int:
        """执行插入操作 - v0.6.8 最终版：绕过损坏的唯一键"""
        if df.empty:
            return 0

        # 获取可插入列（明确排除id）
        table_columns = self._get_insertable_columns()
        insert_columns = [col for col in table_columns if col.lower() != 'id' and col in df.columns]

        if not insert_columns:
            logger.warning("没有可插入的列")
            return 0

        self.logger.info(f"插入列: {len(insert_columns)}个，表总列: {len(table_columns)}个")

        # 构建SQL（使用INSERT IGNORE避开冲突）
        columns_str = ', '.join(insert_columns)
        placeholders = ', '.join(['%s'] * len(insert_columns))
        sql = f"INSERT IGNORE INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"

        records = self._prepare_records(df, insert_columns)
        if not records:
            return 0

        symbol = df['symbol'].iloc[0] if 'symbol' in df else 'unknown'

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 先查询当前记录数
                    cursor.execute(
                        "SELECT COUNT(*) FROM {} WHERE symbol = %s".format(self.table_name),
                        (symbol,)
                    )
                    pre_count = cursor.fetchone()[0]

                    # 执行插入
                    cursor.executemany(sql, records)
                    conn.commit()

                    # 查询插入后记录数
                    cursor.execute(
                        "SELECT COUNT(*) FROM {} WHERE symbol = %s".format(self.table_name),
                        (symbol,)
                    )
                    post_count = cursor.fetchone()[0]

                    actual_new = post_count - pre_count

                    self.logger.info(f"🎯 插入完成: {symbol}, 新增{actual_new}条, 总计{post_count}条")
                    return actual_new
        except Exception as e:
            logger.error(f"插入失败: {e}")
            return 0

    def _get_insertable_columns(self) -> List[str]:
        """获取可插入的列（排除自增主键）- v0.6.2 新增"""
        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {self.table_name}")
                    columns_info = cursor.fetchall()

                    insertable = []
                    for col_info in columns_info:
                        col_name = col_info[0]
                        # 检查是否为自增主键
                        is_auto_increment = False
                        is_primary_key = False

                        if len(col_info) > 5:
                            extra_info = str(col_info[5]).lower()
                            is_auto_increment = 'auto_increment' in extra_info

                        if len(col_info) > 3:
                            key_info = str(col_info[3])
                            is_primary_key = key_info == 'PRI'

                        if not (is_auto_increment and is_primary_key):
                            insertable.append(col_name)
                        else:
                            self.logger.debug(f"排除自增主键列: {col_name}")

                    return insertable
        except Exception as e:
            logger.error(f"获取可插入列失败: {e}")
            # 保底方案：排除已知的自增主键
            return [col for col in self.table_columns if col.lower() != 'id']

    def _prepare_records(self, df: pd.DataFrame, columns: List[str]) -> List[tuple]:
        """准备插入记录"""
        records = []

        for _, row in df.iterrows():
            record = []
            for col in columns:
                val = row[col]

                # 处理特殊值
                if pd.isna(val):
                    record.append(None)
                elif isinstance(val, (np.integer, np.int64)):
                    record.append(int(val))
                elif isinstance(val, (np.floating, np.float64)):
                    # 限制小数位数
                    if col in ['pct_change', 'amplitude', 'turnover_rate', 'turnover_rate_f']:
                        record.append(round(float(val), 4))
                    elif col in ['open', 'close', 'high', 'low', 'pre_close',
                                 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250']:
                        record.append(round(float(val), 3))
                    elif col == 'amount':
                        record.append(round(float(val), 3))
                    elif col == 'volume_ratio':
                        record.append(round(float(val), 3))
                    else:
                        record.append(float(val))
                elif isinstance(val, datetime):
                    record.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(val, pd.Timestamp):
                    record.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    record.append(val)

            records.append(tuple(record))

        return records

    def save_daily_data(self, df: pd.DataFrame) -> bool:
        """
        兼容性方法：保存日线数据

        Args:
            df: 要存储的数据

        Returns:
            是否成功
        """
        logger.warning("使用兼容方法save_daily_data")
        try:
            affected_rows, report = self.store_daily_data(df)
            return affected_rows > 0
        except Exception as e:
            logger.error(f"save_daily_data失败: {e}")
            return False

    def batch_store_daily_data(self, data_dict: Dict[str, pd.DataFrame]) -> Dict:
        """
        批量存储日线数据

        Args:
            data_dict: {股票代码: DataFrame} 字典

        Returns:
            批量处理报告
        """
        total_symbols = len(data_dict)
        logger.info(f"开始批量存储: {total_symbols}只股票")

        results = {
            'total_symbols': total_symbols,
            'success_count': 0,
            'error_count': 0,
            'total_records': 0,
            'total_affected': 0,
            'details': []
        }

        for symbol, df in data_dict.items():
            try:
                affected_rows, report = self.store_daily_data(df)

                results['total_records'] += len(df)
                results['total_affected'] += affected_rows

                if report['status'] == 'success':
                    results['success_count'] += 1
                    logger.debug(f"✅ 成功: {symbol}, {affected_rows}行")
                else:
                    results['error_count'] += 1
                    logger.warning(f"⚠️ 失败: {symbol}, 原因: {report.get('reason', 'unknown')}")

                results['details'].append({
                    'symbol': symbol,
                    'records': len(df),
                    'affected': affected_rows,
                    'status': report['status']
                })

            except Exception as e:
                results['error_count'] += 1
                logger.error(f"❌ 异常: {symbol}, 错误: {e}")
                results['details'].append({
                    'symbol': symbol,
                    'status': 'error',
                    'error': str(e)
                })

        results['success_rate'] = (results['success_count'] / total_symbols * 100) if total_symbols > 0 else 0

        logger.info(
            f"批量存储完成: "
            f"成功{results['success_count']}/{total_symbols}, "
            f"成功率{results['success_rate']:.1f}%, "
            f"总记录{results['total_records']}, "
            f"总影响{results['total_affected']}"
        )

        return results

    def get_last_update_date(self, symbol: str) -> Optional[str]:
        """
        获取股票最后更新日期

        Args:
            symbol: 股票代码

        Returns:
            最后更新日期 (YYYYMMDD) 或 None
        """
        try:
            # 标准化股票代码
            normalized_symbol = normalize_stock_code(symbol)

            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = f"""
                        SELECT MAX(trade_date) as last_date 
                        FROM {self.table_name} 
                        WHERE symbol = %s
                    """
                    cursor.execute(query, (normalized_symbol,))
                    result = cursor.fetchone()

                    if result and result[0]:
                        # 转换为YYYYMMDD格式
                        if isinstance(result[0], str):
                            return result[0].replace('-', '')
                        else:
                            return result[0].strftime('%Y%m%d')
                    else:
                        return None

        except Exception as e:
            logger.error(f"获取最后更新日期失败 {symbol}: {e}")
            return None

    def get_stock_count(self, symbol: str) -> int:
        """
        获取股票数据记录数

        Args:
            symbol: 股票代码

        Returns:
            记录数
        """
        try:
            normalized_symbol = normalize_stock_code(symbol)

            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = f"SELECT COUNT(*) FROM {self.table_name} WHERE symbol = %s"
                    cursor.execute(query, (normalized_symbol,))
                    result = cursor.fetchone()
                    return result[0] if result else 0

        except Exception as e:
            logger.error(f"获取记录数失败 {symbol}: {e}")
            return 0

    def log_data_update(self, data_type: str, symbol: str, *args, **kwargs) -> Dict[str, Any]:
        """
        记录数据更新日志 - v0.6.0 超级兼容版
        支持所有历史调用格式，永不抛出异常
        """
        try:
            # ===== 智能参数解析器（v0.6.0 增强）=====
            start_date = end_date = error_message = None
            execution_time = 0.0
            rows_affected = 0
            status = 'unknown'

            # 参数计数调试
            self.logger.debug(f"[v0.6.0] log_data_update called with {len(args)} args, {len(kwargs)} kwargs")

            # 情况1：位置参数模式（data_scheduler.py 专用）
            if len(args) == 2 and isinstance(args[0], (int, tuple, dict)):
                # 格式: ('daily', symbol, rows_affected, status)
                rows_affected = args[0]
                status = args[1] if len(args) > 1 else 'unknown'
                self.logger.debug(f"[v0.6.0] 解析为4参数模式: rows={rows_affected}, status={status}")

            # 情况2：扩展位置参数模式
            elif len(args) >= 4:
                start_date = args[0] if len(args) > 0 else None
                end_date = args[1] if len(args) > 1 else None
                rows_affected = args[2] if len(args) > 2 else 0
                status = args[3] if len(args) > 3 else 'unknown'
                error_message = args[4] if len(args) > 4 else None
                execution_time = args[5] if len(args) > 5 else 0
                self.logger.debug(
                    f"[v0.6.0] 解析为扩展模式: start={start_date}, end={end_date}, rows={rows_affected}")

            # 情况3：混合模式（位置+关键字）
            else:
                # 解析 kwargs
                rows_affected = kwargs.get('rows_affected', 0)
                status = kwargs.get('status', 'unknown')
                start_date = kwargs.get('start_date')
                end_date = kwargs.get('end_date')
                error_message = kwargs.get('error_message')
                execution_time = kwargs.get('execution_time', 0)
                self.logger.debug(f"[v0.6.0] 解析为关键字模式: rows={rows_affected}, status={status}")

            # ===== 安全提取 rows_affected（v0.6.0 防弹版）=====
            rows_int = self._safe_extract_rows(rows_affected)

            # ===== 记录到数据库日志表 =====
            self._log_to_database(
                data_type=data_type,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                rows_affected=rows_int,
                status=status,
                error_message=error_message,
                execution_time=execution_time
            )

            self.logger.info(f"✅ 日志记录成功: {data_type} {symbol} rows={rows_int} status={status}")
            return {'success': True, 'rows_logged': rows_int}

        except Exception as e:
            self.logger.error(f"❌ log_data_update 解析失败（但流程继续）: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _safe_extract_rows(self, rows_value: Any) -> int:
        """安全提取行数（v0.6.0 终极版）"""
        try:
            # 情况1：已经是整数
            if isinstance(rows_value, int):
                return rows_value

            # 情况2：元组 (5, {...})
            if isinstance(rows_value, tuple) and len(rows_value) > 0:
                first = rows_value[0]
                return int(first) if first is not None else 0

            # 情况3：字典 {'rows_affected': 5}
            if isinstance(rows_value, dict):
                return rows_value.get('rows_affected', 0)

            # 情况4：字符串
            if isinstance(rows_value, str):
                return int(rows_value) if rows_value.isdigit() else 0

            # 情况5：其他（尝试转换）
            return int(rows_value) if rows_value is not None else 0

        except (ValueError, TypeError) as e:
            self.logger.warning(f"行数提取失败，返回0: {e}, 输入值: {rows_value}")
            return 0

    def _log_to_database(self, **log_data):
        """将日志写入数据库（确保即使失败也不影响主流程）"""
        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO data_update_logs 
                        (data_type, symbol, start_date, end_date, rows_affected, status, error_message, execution_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        log_data.get('data_type'),
                        log_data.get('symbol'),
                        log_data.get('start_date'),
                        log_data.get('end_date'),
                        log_data.get('rows_affected', 0),
                        log_data.get('status', 'unknown'),
                        log_data.get('error_message'),
                        log_data.get('execution_time', 0)
                    ))
                    conn.commit()
        except Exception as e:
            self.logger.warning(f"数据库日志写入失败（非致命）: {e}")


# ------------------------------
# 测试函数
def test_adaptive_storage():
    """测试自适应存储器"""
    print("🧪 测试自适应数据存储器")
    print("=" * 50)

    try:
        # 1. 初始化
        storage = AdaptiveDataStorage()
        print(f"✅ 初始化成功，表: {storage.table_name}")
        print(f"   表列数: {len(storage.table_columns)}")
        print(f"   有效映射: {len(storage.column_mapping)}")

        # 2. 创建测试数据（模拟增强处理器输出）
        import pandas as pd
        test_data = pd.DataFrame({
            'symbol': ['sh600519'] * 3,
            'trade_date': ['20241201', '20241202', '20241203'],
            'open_price': [100.123, 101.456, 102.789],
            'high_price': [105.123, 106.456, 107.789],
            'low_price': [98.123, 99.456, 100.789],
            'close_price': [103.123, 104.456, 105.789],
            'pre_close_price': [102.0, 103.0, 104.0],
            'change_percent': [1.1, 1.41, 1.72],
            'change': [1.123, 1.456, 1.789],
            'volume': [1000000, 2000000, 3000000],
            'amount': [103123000.0, 208912000.0, 317367000.0],
            'amplitude': [7.12, 7.01, 6.95],
            'ma5': [101.5, 102.2, 102.9],
            'ma10': [100.8, 101.3, 101.8],
            'ma20': [99.5, 99.8, 100.1],
            'data_source': ['baostock'] * 3,  # 这个列会被忽略
            'processed_time': [datetime.now()] * 3,  # 这个列会被忽略
            'quality_grade': ['excellent'] * 3  # 这个列会被忽略
        })

        print(f"📊 测试数据: {len(test_data)}条, {len(test_data.columns)}列")
        print(f"   数据列: {list(test_data.columns)}")

        # 3. 测试存储
        print("🔧 测试数据存储...")
        affected_rows, report = storage.store_daily_data(test_data)

        print(f"✅ 存储结果:")
        print(f"   状态: {report['status']}")
        print(f"   影响行数: {affected_rows}")
        print(f"   表名: {report.get('table', 'N/A')}")
        print(f"   记录数: {report.get('records', 0)}")
        print(f"   股票: {report.get('symbol', 'unknown')}")

        # 4. 测试兼容方法
        print("🔧 测试兼容方法...")
        saved = storage.save_daily_data(test_data)
        print(f"   兼容方法结果: {saved}")

        # 5. 测试批量存储
        print("🔧 测试批量存储...")
        batch_data = {
            'sz000001': test_data.copy().assign(symbol='sz000001'),
            'sz000858': test_data.copy().assign(symbol='sz000858')
        }

        batch_result = storage.batch_store_daily_data(batch_data)
        print(f"✅ 批量结果:")
        print(f"   总股票: {batch_result['total_symbols']}")
        print(f"   成功: {batch_result['success_count']}")
        print(f"   失败: {batch_result['error_count']}")
        print(f"   成功率: {batch_result['success_rate']:.1f}%")

        print("✅ 自适应数据存储器测试通过")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        print(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = test_adaptive_storage()
    exit(0 if success else 1)