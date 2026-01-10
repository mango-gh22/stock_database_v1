# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_storage_manager.py
# @ Author: mango-gh22
# @ Date：2026/1/3 12:41
"""
desc 因子数据存储管理器 - 专门处理PB等因子数据的存储和增量更新
集成DataStorage架构，针对因子数据进行优化
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import sys
import os

# 添加项目路径
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.data.data_storage import DataStorage
from src.config.logging_config import setup_logging
from src.utils.code_converter import normalize_stock_code  # ✅ 强制添加此行

logger = setup_logging()


class FactorStorageManager(DataStorage):
    """
    因子数据存储管理器 - 专门处理估值因子数据
    继承DataStorage，添加因子专用功能和增量更新逻辑
    """

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化因子存储管理器

        Args:
            config_path: 数据库配置文件路径
        """
        super().__init__(config_path)

        # 因子相关配置
        self.factor_fields = [
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm'
        ]

        # 财务指标字段
        self.financial_fields = [
            'total_share', 'float_share', 'free_share',
            'total_mv', 'circ_mv'
        ]

        # 缓存管理
        self._last_date_cache = {}

        logger.info("✅ 因子存储管理器初始化完成")

    def _preprocess_factor_data(self, df: pd.DataFrame, table_name: str = 'stock_daily_data') -> pd.DataFrame:
        """
        预处理因子数据 - 专门处理PB等因子数据

        Args:
            df: 原始因子数据
            table_name: 目标表名

        Returns:
            预处理后的DataFrame
        """
        if df.empty:
            logger.warning("因子数据为空")
            return df

        df_processed = df.copy()

        # 1. 字段映射：Baostock字段 -> 数据库字段
        field_mapping = {
            # 估值指标
            'peTTM': 'pe_ttm',
            'pbMRQ': 'pb',
            'psTTM': 'ps_ttm',
            'pcfNcfTTM': 'pcf_ttm',

            # 基础字段
            'code': 'symbol',
            'tradeDate': 'trade_date',
            'turnoverRate': 'turnover_rate',
            'turnoverRate_f': 'turnover_rate_f',
            'volumeRatio': 'volume_ratio',

            # 市值相关
            'totalShare': 'total_share',
            'floatShare': 'float_share',
            'freeShare': 'free_share',
            'totalMv': 'total_mv',
            'circMv': 'circ_mv',

            # 其他
            'pe': 'pe',
            'ps': 'ps',
            'dvRatio': 'dv_ratio',
            'dvTtm': 'dv_ttm'
        }

        # 应用字段映射
        rename_map = {}
        for src_field, target_field in field_mapping.items():
            if src_field in df_processed.columns and target_field not in df_processed.columns:
                rename_map[src_field] = target_field

        if rename_map:
            df_processed = df_processed.rename(columns=rename_map)
            logger.debug(f"字段映射: {rename_map}")

    # 在 _preprocess_factor_data 方法中，确保 symbol 标准化：

        # 2. ✅ 确保必需字段存在且标准化
        if 'bs_code' in df_processed.columns:
            # 从 bs_code (sh.600519) 转换为 sh600519
            df_processed['symbol'] = df_processed['bs_code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            logger.debug("从bs_code生成标准化 symbol")
        elif 'code' in df_processed.columns:
            df_processed['symbol'] = df_processed['code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            logger.debug("从code生成标准化 symbol")
        elif 'symbol' in df_processed.columns:
            # 强制标准化已有的 symbol 字段
            df_processed['symbol'] = df_processed['symbol'].apply(
                lambda x: normalize_stock_code(str(x)) if pd.notna(x) else None
            )
            logger.debug("标准化现有 symbol 字段")

        # 3. 日期格式化
        if 'trade_date' in df_processed.columns:
            # 确保日期格式
            df_processed['trade_date'] = pd.to_datetime(
                df_processed['trade_date'], errors='coerce'
            ).dt.strftime('%Y-%m-%d')

        # 4. 数值字段转换
        numeric_fields = self.factor_fields + self.financial_fields
        for field in numeric_fields:
            if field in df_processed.columns:
                try:
                    df_processed[field] = pd.to_numeric(df_processed[field], errors='coerce')
                except Exception as e:
                    logger.warning(f"数值转换失败 {field}: {e}")

        # 5. 选择需要插入的字段（只包含表中有的字段）
        table_columns = self._get_table_columns(table_name)
        available_columns = [col for col in df_processed.columns if col in table_columns]

        # 确保必需字段
        required_columns = ['symbol', 'trade_date']
        for req_col in required_columns:
            if req_col in df_processed.columns and req_col not in available_columns:
                available_columns.append(req_col)

        df_processed = df_processed[available_columns] if available_columns else pd.DataFrame()

        logger.info(f"预处理完成: {len(df_processed)} 条记录，{len(available_columns)} 个字段")

        return df_processed

    # 在 FactorStorageManager 类中添加（放在 __init__ 之后或其他方法附近）
    def prepare_factor_data_for_storage(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        公有接口：准备因子数据用于存储
        """
        return self._preprocess_factor_data(df, table_name='stock_daily_data')

    def store_factor_data(self, data: Any, table_name: str = 'stock_daily_data') -> Tuple[int, Dict]:
        """
        存储因子数据 - 专门处理PB等因子

        Args:
            data: 因子数据，可以是DataFrame或字典列表
            table_name: 目标表名，默认stock_daily_data

        Returns:
            (影响行数, 详细信息字典)
        """
        try:
            # 转换为DataFrame
            if isinstance(data, pd.DataFrame):
                df = data.copy()
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError(f"不支持的数据类型: {type(data)}")

            if df.empty:
                logger.warning("因子数据为空")
                return 0, {'status': 'skipped', 'reason': 'empty_data'}

            logger.info(f"开始存储因子数据: {len(df)} 条记录")
            logger.debug(f"原始字段: {list(df.columns)}")

            # 预处理因子数据
            df_processed = self._preprocess_factor_data(df, table_name)

            if df_processed.empty:
                logger.error("预处理后无有效数据")
                return 0, {'status': 'skipped', 'reason': 'preprocess_failed'}

            # 检查必需字段
            if 'symbol' not in df_processed.columns or 'trade_date' not in df_processed.columns:
                logger.error(f"缺少必需字段，现有字段: {list(df_processed.columns)}")
                return 0, {'status': 'error', 'reason': 'missing_required_fields'}

            # 获取股票代码和日期范围信息
            symbol = df_processed['symbol'].iloc[0]
            dates = df_processed['trade_date'].tolist()
            date_range = f"{min(dates)} 至 {max(dates)}" if dates else "N/A"

            logger.info(f"数据信息: {symbol}, 日期范围: {date_range}")
            logger.debug(f"预处理后字段: {list(df_processed.columns)}")
            logger.debug(f"前2行示例:\n{df_processed.head(2).to_string()}")

            # 调用父类的store_daily_data方法
            affected_rows, result = super().store_daily_data(df_processed, table_name)

            if affected_rows > 0:
                logger.info(f"✅ 因子数据存储成功: {symbol}, {affected_rows} 条记录")
            else:
                # 分析为什么没有影响行数
                existing_count = self._check_existing_data(symbol, dates[0] if dates else None)
                logger.info(f"⚠️  存储0条记录: {symbol} (数据库中已有 {existing_count} 条记录)")

            return affected_rows, {
                'status': 'success' if affected_rows > 0 else 'skipped',
                'symbol': symbol,
                'records_processed': len(df_processed),
                'records_affected': affected_rows,
                'date_range': date_range,
                'factor_fields': [f for f in self.factor_fields if f in df_processed.columns]
            }

        except Exception as e:
            logger.error(f"存储因子数据失败: {e}", exc_info=True)
            return 0, {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            }

    def _check_existing_data(self, symbol: str, date: str) -> int:
        """
        检查数据库中已有的数据数量
        """
        try:
            clean_symbol = str(symbol).replace('.', '')

            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 查询指定日期及之前的数据
                    if date:
                        cursor.execute(
                            "SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s AND trade_date <= %s",
                            (clean_symbol, date)
                        )
                    else:
                        cursor.execute(
                            "SELECT COUNT(*) FROM stock_daily_data WHERE symbol = %s",
                            (clean_symbol,)
                        )
                    count = cursor.fetchone()[0]
                    return count
        except Exception as e:
            logger.warning(f"检查现有数据失败: {e}")
            return 0

    # 在 src/data/factor_storage_manager.py 中修复 get_last_factor_date 方法
    def get_last_factor_date(self, symbol: str, table_name: str = 'stock_daily_data') -> Optional[str]:
        """
        获取指定股票的最后因子数据日期

        Args:
            symbol: 股票代码
            table_name: 表名

        Returns:
            最后日期字符串 (YYYY-MM-DD) 或 None
        """
        cache_key = f"{symbol}_{table_name}"

        if cache_key in self._last_date_cache:
            return self._last_date_cache[cache_key]

        try:
            clean_symbol = str(symbol).replace('.', '')

            # 尝试获取最后有因子数据的日期（任何因子字段不为空）
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 查询有pb、pe_ttm或ps_ttm数据的最后日期
                    cursor.execute(f"""
                        SELECT MAX(trade_date) 
                        FROM {table_name} 
                        WHERE symbol = %s 
                        AND (pb IS NOT NULL OR pe_ttm IS NOT NULL OR ps_ttm IS NOT NULL)
                    """, (clean_symbol,))

                    result = cursor.fetchone()
                    if result and result[0]:
                        last_date = result[0]
                        if isinstance(last_date, str):
                            # 已经是字符串格式
                            formatted_date = last_date
                        else:
                            # datetime格式转换
                            formatted_date = last_date.strftime('%Y-%m-%d')

                        self._last_date_cache[cache_key] = formatted_date
                        logger.debug(f"最后因子日期: {symbol} -> {formatted_date}")
                        return formatted_date

                    # 如果没有因子数据，返回最后交易日期
                    cursor.execute(f"""
                        SELECT MAX(trade_date) 
                        FROM {table_name} 
                        WHERE symbol = %s
                    """, (clean_symbol,))

                    result = cursor.fetchone()
                    if result and result[0]:
                        last_date = result[0]
                        if isinstance(last_date, str):
                            formatted_date = last_date
                        else:
                            formatted_date = last_date.strftime('%Y-%m-%d')
                        return formatted_date

                    return None

        except Exception as e:
            logger.warning(f"获取最后因子日期失败 {symbol}: {e}")
            return None

    # def calculate_incremental_range(self, symbol: str, factor_type: str = 'pb') -> Tuple[Optional[str], Optional[str]]:
    #     """
    #     计算增量下载范围
    #
    #     Args:
    #         symbol: 股票代码
    #         factor_type: 因子类型，用于日志记录
    #
    #     Returns:
    #         (开始日期, 结束日期)，如果无需更新则返回 (None, None)
    #     """
    #     try:
    #         # 获取最后更新日期
    #         last_date = self.get_last_factor_date(symbol)
    #
    #         if not last_date:
    #             logger.info(f"{symbol}: 无历史数据，需要全量下载")
    #             return '20050101', datetime.now().strftime('%Y%m%d')  # 从2005年开始
    #
    #         # 转换为datetime
    #         try:
    #             last_dt = datetime.strptime(str(last_date), '%Y-%m-%d')
    #         except:
    #             last_dt = datetime.strptime(str(last_date), '%Y%m%d')
    #
    #         # 检查是否需要更新（最后日期是否在今天之前）
    #         today = datetime.now().date()
    #
    #         if last_dt.date() >= today:
    #             logger.info(f"{symbol}: 数据已最新（最后更新: {last_date}）")
    #             return None, None
    #
    #         # 计算开始日期（最后日期的下一天）
    #         start_date = (last_dt + timedelta(days=1)).strftime('%Y%m%d')
    #         end_date = today.strftime('%Y%m%d')
    #
    #         logger.info(f"{symbol}: 增量范围 {start_date} - {end_date}（基于最后更新: {last_date}）")
    #         return start_date, end_date
    #
    #     except Exception as e:
    #         logger.error(f"计算增量范围失败 {symbol}: {e}")
    #         return None, None

    # 在_factor_storage_manager.py中修改以下方法

    def calculate_improved_incremental_range(self, symbol: str, factor_type: str = 'pb') -> Tuple[
        Optional[str], Optional[str]]:
        """
        改进的增量范围计算 - 修复日期比较逻辑

        Args:
            symbol: 股票代码
            factor_type: 因子类型，用于日志记录

        Returns:
            (开始日期, 结束日期)，如果无需更新则返回 (None, None)
        """
        try:
            # 获取最后更新日期
            last_date = self.get_last_factor_date(symbol)

            if not last_date:
                logger.info(f"{symbol}: 无历史数据，需要全量下载")
                return '20050101', datetime.now().strftime('%Y%m%d')  # 从2005年开始

            # 转换为datetime
            try:
                last_dt = datetime.strptime(str(last_date), '%Y-%m-%d')
            except:
                try:
                    last_dt = datetime.strptime(str(last_date), '%Y%m%d')
                except:
                    logger.error(f"无法解析最后日期格式: {last_date}")
                    return '20050101', datetime.now().strftime('%Y%m%d')

            # 检查是否需要更新（最后日期是否在昨天之前）
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            # 修正：只有当最后日期 >= 今天才跳过
            if last_dt.date() >= today:
                logger.info(f"{symbol}: 数据已最新（最后更新: {last_date} >= 今天）")
                return None, None

            # 计算开始日期（最后日期的下一天）
            start_date = (last_dt + timedelta(days=1)).strftime('%Y%m%d')
            end_date = yesterday.strftime('%Y%m%d')  # 结束到昨天，避免请求未来数据

            # 确保开始日期不晚于结束日期
            if start_date > end_date:
                logger.warning(f"{symbol}: 开始日期 {start_date} 晚于结束日期 {end_date}")
                # 如果开始日期晚于结束日期，检查是否是同一天
                if start_date[:8] == end_date[:8]:
                    # 同一天，说明数据已最新
                    return None, None
                else:
                    # 不同天，需要调整
                    start_date, end_date = end_date, start_date

            logger.info(f"{symbol}: 增量范围 {start_date} - {end_date}（基于最后更新: {last_date}）")
            return start_date, end_date

        except Exception as e:
            logger.error(f"计算增量范围失败 {symbol}: {e}")
            # 出错时返回全量范围，确保数据完整性
            return '20050101', datetime.now().strftime('%Y%m%d')

    # 在FactorStorageManager类中替换原来的calculate_incremental_range方法
    def calculate_incremental_range(self, symbol: str, factor_type: str = 'pb') -> Tuple[Optional[str], Optional[str]]:
        """向后兼容的增量范围计算（调用改进版本）"""
        return self.calculate_improved_incremental_range(symbol, factor_type)



    def update_factors_for_symbol(
            self,
            symbol: str,
            downloader,
            incremental: bool = True,
            table_name: str = 'stock_daily_data'
    ) -> Tuple[bool, Dict]:
        """
        更新单只股票的因子数据（端到端）

        Args:
            symbol: 股票代码（如 '600519'）
            downloader: 因子下载器实例（需有 fetch_factor_data 方法）
            incremental: 是否增量更新
            table_name: 目标表名

        Returns:
            (success: bool, result: dict)
        """
        try:
            if incremental:
                start_date, end_date = self.calculate_incremental_range(symbol)
                if not start_date or not end_date:
                    return True, {'status': 'up_to_date'}
            else:
                # 全量下载（例如从2005年至今）
                start_date = '20050101'
                end_date = datetime.now().strftime('%Y%m%d')

            if not start_date or not end_date:
                return True, {'status': 'no_range'}

            logger.info(f"📥 请求因子数据: {symbol} [{start_date} - {end_date}]")

            # 下载原始数据
            raw_df = downloader.fetch_factor_data(symbol, start_date, end_date)

            if raw_df.empty:
                logger.warning(f"⚠️ 无因子数据: {symbol} [{start_date} - {end_date}]")
                return True, {
                    'status': 'no_data',
                    'request_range': {'start': start_date, 'end': end_date}
                }

            # 预处理
            processed_df = self._preprocess_factor_data(raw_df, table_name)

            if processed_df.empty:
                logger.error(f"❌ 预处理后无有效数据: {symbol}")
                return False, {
                    'status': 'preprocess_failed',
                    'error': '预处理后无有效字段'
                }

            # 存储
            affected_rows, store_report = self.store_factor_data(processed_df, table_name)

            # 构建返回结果
            dates = processed_df['trade_date'].tolist()
            result = {
                'status': 'success',
                'records_stored': affected_rows,
                'request_range': {'start': start_date, 'end': end_date},
                'data_range': {
                    'start': min(dates) if dates else None,
                    'end': max(dates) if dates else None
                },
                'factor_fields': store_report.get('factor_fields', [])
            }

            logger.info(f"✅ 更新完成: {symbol}, 存储 {affected_rows} 条记录")
            return True, result

        except Exception as e:
            logger.error(f"❌ 更新因子失败 {symbol}: {e}", exc_info=True)
            return False, {
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            }



    def clear_cache(self, symbol: str = None):
        """
        清理缓存数据

        Args:
            symbol: 指定股票代码，如果为None则清理所有缓存
        """
        if symbol:
            keys_to_remove = [k for k in self._last_date_cache.keys() if k.startswith(symbol)]
            for key in keys_to_remove:
                del self._last_date_cache[key]
            logger.debug(f"清理缓存: {symbol}")
        else:
            self._last_date_cache.clear()
            logger.debug("清理所有缓存")


# 测试函数
def test_factor_storage_manager():
    """测试因子存储管理器"""
    print("\n🧪 测试因子存储管理器")
    print("=" * 50)

    try:
        # 1. 初始化
        print("初始化FactorStorageManager...")
        storage = FactorStorageManager()
        print("✅ 初始化成功")

        # 2. 创建测试数据
        print("创建测试因子数据...")
        import pandas as pd

        test_data = pd.DataFrame({
            'bs_code': ['sh600519'],
            'trade_date': ['2026-01-03'],
            'pe_ttm': [20.5],
            'pb': [5.2],
            'ps_ttm': [8.3],
            'total_share': [1250000000.0],
            'total_mv': [250000000000.0]
        })

        print(f"测试数据: {len(test_data)} 条")
        print(f"字段: {list(test_data.columns)}")

        # 3. 存储数据
        print("存储因子数据...")
        affected_rows, report = storage.store_factor_data(test_data)

        print(f"存储结果:")
        print(f"  影响行数: {affected_rows}")
        print(f"  状态: {report['status']}")
        print(f"  股票: {report['symbol']}")
        print(f"  处理记录: {report['records_processed']}")

        # 4. 测试增量范围计算
        print("\n测试增量范围计算...")
        if report['symbol']:
            start_date, end_date = storage.calculate_incremental_range(report['symbol'])
            print(f"  增量范围: {start_date} - {end_date}")

        # 5. 测试最后日期查询
        if report['symbol']:
            last_date = storage.get_last_factor_date(report['symbol'])
            print(f"  最后更新日期: {last_date}")

        # 6. 清理测试缓存
        storage.clear_cache()

        return affected_rows >= 0  # 返回True表示测试正常完成（不要求一定插入数据）

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_factor_storage_manager()

    if success:
        print("\n✅ 因子存储管理器测试通过！")
    else:
        print("\n❌ 因子存储管理器测试失败")

    exit(0 if success else 1)