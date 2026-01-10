# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_pb_factor_downloader.py
# File Name: baostock_pb_factor_downloader
# @ Author: mango-gh22
# @ Date：2026/1/3 11:20
"""
desc PB等因子数据下载器 - 单线程实现（修正版）
下载：peTTM(滚动市盈率), pbMRQ(市净率), psTTM(滚动市销率)等估值指标
"""

import baostock as bs
import pandas as pd
import numpy as np
import time
import random
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import threading
from pathlib import Path
import sys

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.enhanced_trade_date_manager import get_enhanced_trade_date_manager
from src.utils.code_converter import normalize_stock_code
from src.data.baostock_factor_config import get_config_loader
from src.data.baostock_factor_base import BaseFactorDownloader

logger = logging.getLogger(__name__)


class BaostockPBFactorDownloader(BaseFactorDownloader):
    """PB等因子数据下载器 - 估值指标专用（修正版）"""

    def __init__(self, config_path: str = 'config/factor_config.yaml'):
        super().__init__(config_path)

        # 因子字段映射（Baostock -> 数据库）- 修正版
        self.field_mapping = {
            # 估值指标
            'peTTM': 'pe_ttm',  # 滚动市盈率
            'pbMRQ': 'pb',  # 市净率（最新季报）
            'psTTM': 'ps_ttm',  # 滚动市销率
            'pcfNcfTTM': 'pcf_ttm',  # 滚动市现率（可选）

            # 基础字段
            'date': 'trade_date',
            'code': 'bs_code',

            # 其他可能需要的字段 - 修正字段名
            'turn': 'turnover_rate_f',  # 流通换手率（正确字段名）
            'tradestatus': 'trade_status',  # 交易状态
        }

        # 数据库需要的所有因子字段（根据您提供的表结构）
        self.target_factor_fields = [
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm', 'total_share', 'float_share',
            'free_share', 'total_mv', 'circ_mv'
        ]

        # 本次下载的因子字段（估值指标）
        self.download_factor_fields = ['pe_ttm', 'pb', 'ps_ttm']

        logger.info(f"初始化PB因子下载器，下载字段: {self.download_factor_fields}")

    def get_baostock_fields_string(self) -> str:
        """获取Baostock字段字符串（包含所有需要的字段）"""
        # 从配置获取字段，如果配置中有则使用，否则使用默认
        config_fields = self.config.get('baostock_fields.daily_fields')
        if config_fields:
            logger.debug(f"使用配置中的Baostock字段: {config_fields}")
            return config_fields

        # 如果没有配置，则构建默认字段
        base_fields = ['date', 'code']

        # 反转映射，查找数据库字段对应的Baostock字段
        reverse_mapping = {v: k for k, v in self.field_mapping.items()}

        # 添加所有需要下载的因子字段
        for db_field in self.download_factor_fields:
            if db_field in reverse_mapping:
                baostock_field = reverse_mapping[db_field]
                if baostock_field not in base_fields:
                    base_fields.append(baostock_field)

        # 添加其他可能需要的字段 - 修正字段名
        additional_fields = ['turn', 'tradestatus', 'adjustflag']
        for field in additional_fields:
            if field not in base_fields:
                base_fields.append(field)

        fields_str = ','.join(base_fields)
        logger.debug(f"构建的Baostock字段: {fields_str}")
        return fields_str

    # 在 baostock_pb_factor_downloader.py 中添加日期格式转换函数

    def _convert_date_format(self, date_str: str) -> str:
        """
        转换日期格式为Baostock需要的格式 (YYYY-MM-DD -> YYYYMMDD)

        Args:
            date_str: 日期字符串，支持多种格式

        Returns:
            YYYYMMDD格式的日期字符串
        """
        if not date_str:
            return date_str

        try:
            # 移除分隔符
            date_str = str(date_str).strip()

            # 如果已经是8位数字，直接返回
            if date_str.isdigit() and len(date_str) == 8:
                return date_str

            # 尝试解析常见格式
            from datetime import datetime

            formats_to_try = [
                '%Y-%m-%d',  # 2025-12-01
                '%Y/%m/%d',  # 2025/12/01
                '%Y%m%d',  # 20251201
                '%Y-%m-%d %H:%M:%S',  # 2025-12-01 00:00:00
            ]

            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y%m%d')
                except:
                    continue

            # 如果都失败，尝试简单处理
            date_str = date_str.replace('-', '').replace('/', '').replace(' ', '')
            if len(date_str) >= 8:
                return date_str[:8]

            raise ValueError(f"无法解析日期格式: {date_str}")

        except Exception as e:
            logger.warning(f"日期格式转换失败 {date_str}: {e}")
            # 返回原始值
            return date_str

    # 修改 fetch_factor_data 方法，在开始处添加日期转换：
    def fetch_factor_data(self, symbol: str, start_date: str,
                          end_date: str) -> Optional[pd.DataFrame]:
        """
        获取单只股票的因子数据（使用交易日验证）
        """
        # 转换日期格式
        start_date = self._convert_date_format(start_date)
        end_date = self._convert_date_format(end_date)

        # 获取交易日管理器
        trade_manager = get_enhanced_trade_date_manager()

        # 继续原有逻辑...

        # 验证日期是否为交易日
        start_valid, start_reason = trade_manager.validate_trade_date(start_date)
        end_valid, end_reason = trade_manager.validate_trade_date(end_date)

        if not start_valid:
            logger.warning(f"开始日期 {start_date} 不是交易日: {start_reason}")
            # 自动调整为最近的交易日
            adjusted_start = trade_manager.get_last_trade_date_str(start_date)
            logger.info(f"自动调整为: {adjusted_start}")
            start_date = adjusted_start

        if not end_valid:
            logger.warning(f"结束日期 {end_date} 不是交易日: {end_reason}")
            adjusted_end = trade_manager.get_last_trade_date_str(end_date)
            logger.info(f"自动调整为: {adjusted_end}")
            end_date = adjusted_end

        # 确保开始日期不晚于结束日期
        if start_date > end_date:
            logger.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}")
            # 交换日期
            start_date, end_date = end_date, start_date

        # 继续原有的下载逻辑
        with self._download_lock:
            return self._fetch_factor_data_sync(symbol, start_date, end_date)

    def _fetch_factor_data_sync(self, symbol: str, start_date: str,
                                end_date: str) -> Optional[pd.DataFrame]:
        """同步获取因子数据（内部实现）"""
        self._ensure_logged_in()

        bs_code = self._convert_to_bs_code(symbol)
        if not self._is_valid_stock(bs_code):
            logger.warning(f"⚠️ 跳过非股票代码: {bs_code}")
            return pd.DataFrame()

        # 格式化日期
        formatted_start = self._format_date_for_baostock(start_date)
        formatted_end = self._format_date_for_baostock(end_date)

        logger.info(f"📥 获取因子数据: {bs_code} [{formatted_start} - {formatted_end}]")

        max_retries = self.config.get('execution.max_retries', 3)
        retry_delay_base = self.config.get('execution.retry_delay_base', 3)

        # 构建请求字段
        fields_str = self.get_baostock_fields_string()

        for attempt in range(max_retries):
            try:
                self._enforce_rate_limit()

                if attempt > 0:
                    logger.info(f"重试登录 (尝试 {attempt + 1}/{max_retries})")
                    self._login_baostock()

                # 获取因子数据
                logger.debug(f"请求字段: {fields_str}")
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=fields_str,
                    start_date=formatted_start,
                    end_date=formatted_end,
                    frequency="d",
                    adjustflag="3"  # 不复权
                )

                if rs.error_code != '0':
                    error_msg = rs.error_msg
                    # 如果是数据不存在错误，返回空DataFrame
                    if "不存在" in error_msg or "未找到" in error_msg:
                        logger.info(f"无因子数据: {bs_code}")
                        return pd.DataFrame()
                    raise RuntimeError(f"Baostock error: {error_msg}")

                # 安全获取数据
                data_list = self._safe_fetch_data(rs)

                if not data_list:
                    logger.info(f"无因子数据: {bs_code}")
                    return pd.DataFrame()

                # 转换为DataFrame
                df = pd.DataFrame(data_list, columns=rs.fields)

                # 处理数据
                df_processed = self._process_factor_data(df, symbol)

                # 更新统计
                self._update_stats(success=True, records=len(df_processed))

                logger.info(f"✅ 获取因子数据成功: {bs_code}, {len(df_processed)} 条记录")
                return df_processed

            except Exception as e:
                err_str = str(e).lower()
                wait_sec = retry_delay_base + attempt * 2 + random.uniform(0, 1)

                # 对特定错误类型增加等待时间
                if any(kw in err_str for kw in ['utf', 'codec', 'decompress', 'invalid', 'timeout']):
                    wait_sec *= 2

                logger.warning(f"⚠️ 尝试 {attempt + 1}/{max_retries} 失败: {str(e)[:100]} → 等待 {wait_sec:.1f}s")

                if attempt < max_retries - 1:
                    time.sleep(wait_sec)
                else:
                    logger.error(f"❌ 所有重试均失败: {symbol}")
                    self._update_stats(success=False)
                    return pd.DataFrame()

        return pd.DataFrame()

    # 在 _process_factor_data 方法中，修改 symbol 处理逻辑：

    def _process_factor_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """处理因子数据，转换为数据库格式"""
        if df.empty:
            return df

        df_processed = df.copy()

        # 1. 重命名列（Baostock -> 数据库）
        rename_dict = {}
        for baostock_field, db_field in self.field_mapping.items():
            if baostock_field in df_processed.columns and db_field not in df_processed.columns:
                rename_dict[baostock_field] = db_field

        if rename_dict:
            df_processed = df_processed.rename(columns=rename_dict)

        # 2. ✅ 确保 symbol 字段标准化（关键修复）
        if 'bs_code' in df_processed.columns:
            # 从 bs_code (sh.600519) 转换为 sh600519
            df_processed['symbol'] = df_processed['bs_code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            logger.debug(f"从 bs_code 生成标准化 symbol")
        elif 'code' in df_processed.columns:
            # 从 code 字段转换
            df_processed['symbol'] = df_processed['code'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )
            logger.debug(f"从 code 生成标准化 symbol")
        else:
            # 如果都没有，使用传入的 symbol 参数
            df_processed['symbol'] = normalize_stock_code(symbol)
            logger.debug(f"使用参数 symbol: {symbol}")

        # 3. 处理日期字段
        if 'trade_date' in df_processed.columns:
            # 转换为datetime
            df_processed['trade_date'] = pd.to_datetime(df_processed['trade_date'], errors='coerce')
            # 移除无效日期
            df_processed = df_processed[df_processed['trade_date'].notna()]
            # 转换为YYYYMMDD格式
            df_processed['trade_date'] = df_processed['trade_date'].dt.strftime('%Y%m%d')

        # 4. 转换数值类型
        numeric_columns = []
        for field in self.download_factor_fields:
            if field in df_processed.columns:
                numeric_columns.append(field)

        # 添加其他可能的数值列
        other_numeric = ['turnover_rate']
        for col in other_numeric:
            if col in df_processed.columns:
                numeric_columns.append(col)

        for col in numeric_columns:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
                # 处理异常值
                if col in ['pe_ttm', 'pb', 'ps_ttm']:
                    # 估值指标应该为正数，负值或极大值设为NaN
                    df_processed[col] = df_processed[col].apply(
                        lambda x: x if pd.notna(x) and 0 < x < 1e6 else np.nan
                    )

        # 5. 排序
        if 'trade_date' in df_processed.columns:
            df_processed = df_processed.sort_values('trade_date', ascending=False)

        # 6. 重置索引
        df_processed = df_processed.reset_index(drop=True)

        return df_processed

    def download_batch_factors(self, symbols: List[str],
                               start_date: str = None,
                               end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        批量下载因子数据（智能交易日处理）- 使用增强版交易日管理器
        """
        # 获取增强版交易日管理器
        trade_manager = get_enhanced_trade_date_manager()

        # --- 智能日期范围处理 ---
        # 1. 处理结束日期
        if not end_date:
            # 使用最后一个交易日
            end_date = trade_manager.get_last_trade_date_str()
            logger.debug(f"未指定结束日期，使用最后交易日: {end_date}")

        # 2. 验证并调整结束日期
        end_valid, end_reason = trade_manager.validate_trade_date(end_date)
        if not end_valid:
            logger.warning(f"结束日期 {end_date} 不是交易日: {end_reason}")
            adjusted_end = trade_manager.get_last_trade_date_str(end_date)
            logger.info(f"自动调整为: {adjusted_end}")
            end_date = adjusted_end

        # 3. 处理开始日期
        if not start_date:
            # 默认回溯30个交易日（而不是固定天数）
            default_trade_days = self.config.get('date_range.default_trade_days_back', 30)

            # 使用交易日管理器计算准确的交易日范围
            start_date, _ = trade_manager.get_trade_date_range_str(
                days_back=default_trade_days,
                end_date_str=end_date
            )
            logger.debug(f"未指定开始日期，自动回溯{default_trade_days}个交易日: {start_date}")
        else:
            # 验证并调整开始日期
            start_valid, start_reason = trade_manager.validate_trade_date(start_date)
            if not start_valid:
                logger.warning(f"开始日期 {start_date} 不是交易日: {start_reason}")
                adjusted_start = trade_manager.get_last_trade_date_str(start_date)
                logger.info(f"自动调整为: {adjusted_start}")
                start_date = adjusted_start

        # 4. 确保日期范围有效性
        if start_date > end_date:
            logger.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}，自动交换日期")
            start_date, end_date = end_date, start_date

        # 5. 获取完整交易日列表（用于日志和验证）
        trade_dates = trade_manager.get_trade_dates_in_range(start_date, end_date)
        if not trade_dates:
            logger.error(f"日期范围 {start_date} - {end_date} 内没有交易日")
            return {}

        logger.info(f"📅 交易日范围: {start_date} - {end_date}")
        logger.info(f"📈 实际交易日数: {len(trade_dates)}个")

        # --- 开始批量下载 ---
        self.download_stats['start_time'] = datetime.now()
        self.reset_stats()

        logger.info(f"🚀 开始批量下载因子数据")
        logger.info(f"📊 股票数量: {len(symbols)}")
        logger.info(f"⚙️  单线程模式")

        results = {}
        failed_symbols = []

        # 单线程顺序处理
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{len(symbols)}] 处理 {symbol}")

                # 使用已经验证调整过的日期
                df = self.fetch_factor_data(symbol, start_date, end_date)

                if df is not None and not df.empty:
                    results[symbol] = df

                    # 检查实际获取的交易日数
                    actual_dates = len(df)
                    expected_dates = len(trade_dates)
                    if actual_dates < expected_dates:
                        logger.warning(f"  ⚠️  数据不完整: {actual_dates}/{expected_dates}个交易日")
                    else:
                        logger.info(f"  ✅ 成功: {actual_dates} 条记录")

                    # 显示样本数据
                    if i <= 3:  # 只显示前3只股票的样本
                        self._log_sample_data(symbol, df)
                else:
                    failed_symbols.append(symbol)
                    logger.warning(f"  ⚠️  无数据")

                # 进度报告
                if i % self.config.get('batch.progress_report_interval', 10) == 0:
                    self._report_progress(i, len(symbols))

                # 请求间隔（除了最后一个）
                if i < len(symbols):
                    sleep_time = self.request_interval + random.uniform(0, 0.5)
                    time.sleep(sleep_time)

            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"  ❌ 处理失败: {e}")
                logger.debug(f"失败详情:", exc_info=True)

        # 结束统计
        self.download_stats['end_time'] = datetime.now()

        # 生成报告
        self._generate_batch_report(results, failed_symbols, start_date, end_date)

        # 额外统计交易日覆盖情况
        if results:
            self._analyze_trade_date_coverage(results, trade_dates)

        return results

    def _analyze_trade_date_coverage(self, results: Dict[str, pd.DataFrame],
                                     expected_dates: List[str]) -> None:
        """
        分析交易日覆盖情况
        """
        if not results:
            return

        logger.info("📊 交易日覆盖分析:")

        # 统计每只股票的交易日覆盖
        coverage_stats = {}
        for symbol, df in results.items():
            if 'date' in df.columns:
                actual_dates = set(df['date'].astype(str).tolist())
                expected_set = set(expected_dates)
                missing_dates = expected_set - actual_dates
                coverage = len(actual_dates) / len(expected_set) * 100
                coverage_stats[symbol] = {
                    'coverage_pct': coverage,
                    'missing_count': len(missing_dates)
                }

        if coverage_stats:
            avg_coverage = sum(stats['coverage_pct'] for stats in coverage_stats.values()) / len(coverage_stats)
            logger.info(f"  平均交易日覆盖率: {avg_coverage:.1f}%")

            # 找出覆盖率最低的股票
            worst_symbol = min(coverage_stats.items(),
                               key=lambda x: x[1]['coverage_pct'])
            if worst_symbol[1]['coverage_pct'] < 95:
                logger.warning(f"  最低覆盖率: {worst_symbol[0]} ({worst_symbol[1]['coverage_pct']:.1f}%)")

    def _log_sample_data(self, symbol: str, df: pd.DataFrame, num_rows: int = 3):
        """记录样本数据"""
        if df.empty:
            return

        sample = df.head(num_rows).copy()

        # 选择显示的列
        display_cols = ['trade_date']
        for field in self.download_factor_fields:
            if field in sample.columns:
                display_cols.append(field)

        # 添加换手率
        if 'turnover_rate' in sample.columns:
            display_cols.append('turnover_rate')

        # 只取存在的列
        display_cols = [col for col in display_cols if col in sample.columns]

        if display_cols:
            sample_display = sample[display_cols]
            logger.debug(f"  📊 {symbol} 样本数据:\n{sample_display.to_string()}")

    def _report_progress(self, current: int, total: int):
        """报告进度"""
        progress_pct = (current / total) * 100

        # 确保start_time已设置
        if self.download_stats.get('start_time'):
            elapsed = (datetime.now() - self.download_stats['start_time']).total_seconds()

            if elapsed > 0:
                speed = current / elapsed  # 股票/秒
                eta = (total - current) / speed if speed > 0 else 0
            else:
                speed = 0
                eta = 0

            logger.info(
                f"📈 进度: {current}/{total} ({progress_pct:.1f}%) | "
                f"速度: {speed:.2f} 股票/秒 | "
                f"预计剩余: {eta / 60:.1f}分钟"
            )
        else:
            logger.info(f"📈 进度: {current}/{total} ({progress_pct:.1f}%)")

    def _generate_batch_report(self, results: Dict[str, pd.DataFrame],
                               failed_symbols: List[str],
                               start_date: str, end_date: str):
        """生成批量处理报告"""
        total_symbols = len(results) + len(failed_symbols)
        success_count = len(results)
        fail_count = len(failed_symbols)

        total_records = sum(len(df) for df in results.values())

        # 安全计算持续时间
        start_time = self.download_stats.get('start_time')
        end_time = self.download_stats.get('end_time')

        if start_time and end_time:
            duration = (end_time - start_time).total_seconds()
        else:
            duration = 0
            logger.warning("无法计算持续时间，开始或结束时间为空")

        report = {
            'batch_id': f"factor_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'timestamp': datetime.now().isoformat(),
            'date_range': {'start': start_date, 'end': end_date},
            'statistics': {
                'total_symbols': total_symbols,
                'successful': success_count,
                'failed': fail_count,
                'success_rate': (success_count / total_symbols * 100) if total_symbols > 0 else 0,
                'total_records': total_records,
                'duration_seconds': duration,
                'speed_symbols_per_second': success_count / duration if duration > 0 else 0
            },
            'success_symbols': list(results.keys()),
            'failed_symbols': failed_symbols,
            'factors_downloaded': self.download_factor_fields
        }

        # 记录报告
        logger.info(f"📊 批量处理完成:")
        logger.info(f"  成功: {success_count}/{total_symbols} ({report['statistics']['success_rate']:.1f}%)")
        logger.info(f"  失败: {fail_count}")
        logger.info(f"  总记录: {total_records}")
        logger.info(f"  耗时: {duration:.1f}秒")

        if duration > 0:
            logger.info(f"  速度: {report['statistics']['speed_symbols_per_second']:.2f} 股票/秒")

        # 保存报告到文件
        if self.config.get('monitoring.save_report', True):
            self._save_report_to_file(report)

        return report

    def _save_report_to_file(self, report: Dict[str, Any]):
        """保存报告到文件"""
        try:
            report_dir_str = self.config.get('monitoring.report_dir', 'data/reports/factors')
            report_dir = Path(report_dir_str)
            report_dir.mkdir(parents=True, exist_ok=True)

            report_file = report_dir / f"{report['batch_id']}.json"

            import json
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            logger.info(f"📄 报告已保存: {report_file}")

        except Exception as e:
            logger.error(f"保存报告失败: {e}")

    def validate_factor_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        验证因子数据质量

        Args:
            df: 要验证的DataFrame

        Returns:
            (是否有效, 问题列表)
        """
        if df.empty:
            return False, ["数据为空"]

        problems = []

        # 1. 检查必需字段
        required_fields = ['symbol', 'trade_date']
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            problems.append(f"缺少必需字段: {missing_fields}")

        # 2. 检查至少有一个因子字段
        factor_fields_present = [field for field in self.download_factor_fields if field in df.columns]
        if not factor_fields_present:
            problems.append(f"无有效因子字段，期望: {self.download_factor_fields}")

        # 3. 检查数据完整性
        if 'trade_date' in df.columns:
            date_count = df['trade_date'].notna().sum()
            if date_count < len(df):
                problems.append(f"日期字段缺失: {len(df) - date_count}条")

        # 4. 检查数值范围
        for factor_field in factor_fields_present:
            if factor_field in df.columns:
                valid_count = df[factor_field].apply(
                    lambda x: pd.notna(x) and 0 < x < 1e6
                ).sum()

                invalid_count = len(df) - valid_count
                if invalid_count > 0:
                    problems.append(f"{factor_field}: {invalid_count}个无效值")

        return len(problems) == 0, problems


def test_pb_factor_downloader():
    """测试PB因子下载器"""
    import sys
    import logging as log

    # 配置详细日志
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试PB因子下载器")
    print("=" * 50)

    try:
        # 创建下载器
        downloader = BaostockPBFactorDownloader()

        # 测试登录
        downloader._ensure_logged_in()
        if not hasattr(downloader, 'lg') or not downloader.lg:
            print("❌ Baostock登录失败")
            return False

        print("✅ Baostock登录成功")

        # 测试字段映射
        print("\n🔍 测试字段映射:")
        print(f"  下载的因子字段: {downloader.download_factor_fields}")
        fields_str = downloader.get_baostock_fields_string()
        print(f"  Baostock字段字符串: {fields_str}")

        # 测试单只股票下载
        print("\n📥 测试单只股票下载:")
        test_symbol = '600519'  # 贵州茅台

        # 设置日期范围（最近7天，避免数据过多）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

        print(f"  股票: {test_symbol}")
        print(f"  日期范围: {start_date} - {end_date}")

        df = downloader.fetch_factor_data(test_symbol, start_date, end_date)

        if df is not None and not df.empty:
            print(f"  ✅ 下载成功: {len(df)} 条记录")

            # 显示列信息
            print(f"  数据列: {list(df.columns)}")

            # 显示样本数据
            if not df.empty:
                sample = df.head(3)
                display_cols = ['trade_date']
                for field in downloader.download_factor_fields:
                    if field in sample.columns:
                        display_cols.append(field)

                if display_cols:
                    sample_display = sample[display_cols]
                    print(f"  样本数据:")
                    print(sample_display.to_string())

            # 验证数据
            is_valid, problems = downloader.validate_factor_data(df)
            if is_valid:
                print("  ✅ 数据验证通过")
            else:
                print(f"  ⚠️  数据验证问题: {problems}")
        else:
            print(f"  ⚠️  无数据")

        # 测试批量下载（少量股票）
        print("\n🚀 测试批量下载（少量股票）:")
        test_symbols = ['600519', '000001', '000858']  # 茅台、平安、五粮液

        results = downloader.download_batch_factors(
            symbols=test_symbols,
            start_date=start_date,
            end_date=end_date
        )

        print(f"  批量处理结果:")
        print(f"    成功: {len(results)} 只")
        print(f"    总记录: {sum(len(df) for df in results.values())}")

        # 显示统计
        stats = downloader.get_download_stats()
        print(f"\n📊 下载统计:")
        print(f"    总请求: {stats['total_requests']}")
        print(f"    成功: {stats['successful']}")
        print(f"    失败: {stats['failed']}")
        if 'success_rate' in stats:
            print(f"    成功率: {stats['success_rate']:.1f}%")
        print(f"    总记录: {stats['total_records']}")

        # 退出登录
        downloader.logout()
        print("\n✅ PB因子下载器测试完成")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_pb_factor_downloader()
    sys.exit(0 if success else 1)