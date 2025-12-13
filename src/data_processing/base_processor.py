# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data_processing\base_processor.py
# File Name: base_processor
# @ Author: mango-gh22
# @ Date：2025/12/7 19:36

"""
stock_database_v1/src/data_processing/base_processor.py
数据处理基础模块 - 集成到现有架构
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
import logging
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# 导入项目中的现有模块
from src.database.db_connector import DatabaseConnector
from src.utils.code_converter import normalize_stock_code
from src.config.config_loader import load_tushare_config

# 配置日志
logger = logging.getLogger(__name__)


class BaseDataProcessor:
    """基础数据处理类 - 集成到现有架构"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据处理器

        Args:
            config_path: 数据库配置文件路径
        """
        self.db_connector = DatabaseConnector(config_path)
        self.config_path = config_path

        # 加载Tushare配置
        self.tushare_config = load_tushare_config()

        # 初始化数据源API（这里使用Tushare作为示例）
        self._init_data_source()

        # 缓存配置
        self.cache_dir = Path('data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _init_data_source(self):
        """初始化数据源API"""
        try:
            import tushare as ts
            # 从配置读取token
            token = self.tushare_config.get('token')
            if token:
                ts.set_token(token)
                self.pro = ts.pro_api()
                logger.info("Tushare API初始化成功")
            else:
                logger.warning("未配置Tushare token，部分功能可能受限")
                self.pro = None
        except ImportError:
            logger.warning("未安装tushare，使用备用数据源")
            self.pro = None
        except Exception as e:
            logger.error(f"初始化数据源失败: {e}")
            self.pro = None

    def get_stock_basic_info(self, market: str = "A股") -> pd.DataFrame:
        """
        获取股票基本信息

        Args:
            market: 市场类型 ('A股', '港股', '美股')

        Returns:
            股票基本信息DataFrame
        """
        try:
            if self.pro:
                # 使用Tushare获取股票列表
                if market == "A股":
                    df = self.pro.stock_basic(
                        exchange='',
                        list_status='L',
                        fields='ts_code,symbol,name,area,industry,market,list_date'
                    )
                    # 转换代码格式
                    df['normalized_code'] = df['ts_code'].apply(lambda x: normalize_stock_code(x))
                    return df
                else:
                    logger.warning(f"暂不支持的市场类型: {market}")
                    return pd.DataFrame()
            else:
                # 备用方案：从数据库获取
                return self._get_stock_info_from_db(market)

        except Exception as e:
            logger.error(f"获取股票基本信息失败: {e}")
            return pd.DataFrame()

    def _get_stock_info_from_db(self, market: str) -> pd.DataFrame:
        """从数据库获取股票信息"""
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor(dictionary=True)

            cursor.execute("SELECT * FROM stock_basic_info WHERE status = 'L'")
            results = cursor.fetchall()

            df = pd.DataFrame(results)
            cursor.close()
            connection.close()

            return df
        except Exception as e:
            logger.error(f"从数据库获取股票信息失败: {e}")
            return pd.DataFrame()

    def fetch_daily_data(self,
                         stock_code: str,
                         start_date: str = "19900101",
                         end_date: str = None,
                         adjust: str = "qfq") -> pd.DataFrame:
        """
        获取日线数据

        Args:
            stock_code: 标准化股票代码（如 'sh600519'）
            start_date: 开始日期 'YYYYMMDD'
            end_date: 结束日期 'YYYYMMDD'
            adjust: 复权类型 ('qfq': 前复权, 'hfq': 后复权, None: 不复权)

        Returns:
            日线数据DataFrame
        """
        # 标准化股票代码
        normalized_code = normalize_stock_code(stock_code)

        # 检查缓存
        cache_key = f"daily_{normalized_code}_{start_date}_{end_date}_{adjust}"
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            logger.info(f"使用缓存数据: {normalized_code}")
            return cached_data

        try:
            df = self._fetch_daily_from_tushare(normalized_code, start_date, end_date, adjust)

            if not df.empty:
                # 数据清洗
                df = self._clean_daily_data(df, normalized_code)
                # 缓存数据
                self._save_to_cache(cache_key, df)

            return df

        except Exception as e:
            logger.error(f"获取日线数据失败 {normalized_code}: {e}")
            # 尝试从数据库获取
            return self._get_daily_from_db(normalized_code, start_date, end_date)

    def _fetch_daily_from_tushare(self,
                                  normalized_code: str,
                                  start_date: str,
                                  end_date: str,
                                  adjust: str) -> pd.DataFrame:
        """从Tushare获取日线数据"""
        if not self.pro:
            return pd.DataFrame()

        # 转换代码格式为Tushare格式（如 'sh600519' -> '600519.SH'）
        if normalized_code.startswith('sh'):
            ts_code = f"{normalized_code[2:]}.SH"
        elif normalized_code.startswith('sz'):
            ts_code = f"{normalized_code[2:]}.SZ"
        else:
            ts_code = normalized_code

        try:
            # 调整参数映射
            if adjust == 'qfq':
                adj = 'qfq'
            elif adjust == 'hfq':
                adj = 'hfq'
            else:
                adj = None

            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adj=adj
            )

            if not df.empty:
                # 重命名列以匹配数据库
                column_mapping = {
                    'ts_code': 'code',
                    'trade_date': 'date',
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
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['code'] = normalized_code

                # 计算技术指标
                df = self._calculate_basic_indicators(df)

            return df

        except Exception as e:
            logger.error(f"Tushare API调用失败: {e}")
            return pd.DataFrame()

    def _get_daily_from_db(self,
                           normalized_code: str,
                           start_date: str,
                           end_date: str) -> pd.DataFrame:
        """从数据库获取日线数据"""
        try:
            connection = self.db_connector.get_connection()

            query = """
                SELECT * FROM stock_daily_data 
                WHERE code = %s 
                AND date >= %s 
                AND date <= %s
                ORDER BY date
            """

            df = pd.read_sql_query(query, connection,
                                   params=[normalized_code, start_date, end_date])

            connection.close()
            return df

        except Exception as e:
            logger.error(f"从数据库获取日线数据失败: {e}")
            return pd.DataFrame()

    def _clean_daily_data(self, df: pd.DataFrame, code: str) -> pd.DataFrame:
        """
        清洗日线数据

        Args:
            df: 原始数据
            code: 股票代码

        Returns:
            清洗后的数据
        """
        if df.empty:
            return df

        df_clean = df.copy()

        # 1. 处理缺失值
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_cols:
            if col in df_clean.columns:
                if col in ['open', 'high', 'low', 'close']:
                    # 价格数据使用前向填充
                    df_clean[col] = df_clean[col].fillna(method='ffill').fillna(method='bfill')
                elif col == 'volume':
                    # 成交量填充为0
                    df_clean[col] = df_clean[col].fillna(0)
                else:
                    df_clean[col] = df_clean[col].fillna(0)

        # 2. 验证数据有效性
        df_clean = self._validate_price_data(df_clean)

        # 3. 去除重复数据
        df_clean = df_clean.drop_duplicates(subset=['date', 'code'], keep='last')

        # 4. 排序
        df_clean = df_clean.sort_values('date')
        df_clean = df_clean.reset_index(drop=True)

        # 5. 添加处理标记
        df_clean['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_clean['data_source'] = 'tushare_processed'

        return df_clean

    def _validate_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """验证价格数据合理性"""
        if df.empty:
            return df

        # 基本价格验证
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                # 移除负值
                df = df[df[col] > 0]
                # 移除异常大值
                df = df[df[col] < 1e6]

        # 验证 high >= low
        if all(col in df.columns for col in ['high', 'low']):
            df = df[df['high'] >= df['low']]

        # 验证价格在高低范围内
        if all(col in df.columns for col in ['open', 'high', 'low']):
            df = df[(df['open'] >= df['low']) & (df['open'] <= df['high'])]

        if all(col in df.columns for col in ['close', 'high', 'low']):
            df = df[(df['close'] >= df['low']) & (df['close'] <= df['high'])]

        return df

    def _calculate_basic_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算基础技术指标"""
        if df.empty or 'close' not in df.columns:
            return df

        df_indicators = df.copy()

        # 移动平均线
        periods = [5, 10, 20, 30, 60]
        for period in periods:
            df_indicators[f'ma{period}'] = df_indicators['close'].rolling(window=period).mean()

        # 成交量均线
        if 'volume' in df_indicators.columns:
            df_indicators['volume_ma5'] = df_indicators['volume'].rolling(window=5).mean()
            df_indicators['volume_ma10'] = df_indicators['volume'].rolling(window=10).mean()

        # 涨跌幅计算（如果不存在）
        if 'pct_change' not in df_indicators.columns and 'pre_close' in df_indicators.columns:
            df_indicators['pct_change'] = (df_indicators['close'] - df_indicators['pre_close']) / df_indicators[
                'pre_close'] * 100

        return df_indicators

    def save_daily_data(self, df: pd.DataFrame) -> bool:
        """
        保存日线数据到数据库

        Args:
            df: 日线数据

        Returns:
            是否成功
        """
        if df.empty:
            logger.warning("空DataFrame，不保存")
            return False

        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            # 准备插入语句
            insert_query = """
                INSERT INTO stock_daily_data 
                (code, date, open, high, low, close, volume, amount, 
                 pre_close, change, pct_change, ma5, ma10, ma20, ma30, ma60,
                 volume_ma5, volume_ma10, processed_at, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                amount = VALUES(amount),
                ma5 = VALUES(ma5),
                ma10 = VALUES(ma10),
                ma20 = VALUES(ma20),
                ma30 = VALUES(ma30),
                ma60 = VALUES(ma60),
                volume_ma5 = VALUES(volume_ma5),
                volume_ma10 = VALUES(volume_ma10),
                processed_at = VALUES(processed_at),
                data_source = VALUES(data_source)
            """

            # 准备数据
            records = []
            for _, row in df.iterrows():
                record = (
                    row.get('code'),
                    row.get('date'),
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('volume', 0),
                    row.get('amount', 0),
                    row.get('pre_close'),
                    row.get('change', 0),
                    row.get('pct_change', 0),
                    row.get('ma5'),
                    row.get('ma10'),
                    row.get('ma20'),
                    row.get('ma30'),
                    row.get('ma60'),
                    row.get('volume_ma5'),
                    row.get('volume_ma10'),
                    row.get('processed_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    row.get('data_source', 'base_processor')
                )
                records.append(record)

            # 批量插入
            cursor.executemany(insert_query, records)
            connection.commit()

            logger.info(f"保存日线数据成功: {len(records)} 条记录")

            cursor.close()
            connection.close()

            return True

        except Exception as e:
            logger.error(f"保存日线数据失败: {e}")
            return False

    def batch_update_daily_data(self,
                                stock_list: List[str],
                                start_date: str = "20230101",
                                end_date: str = None,
                                max_workers: int = 3) -> Dict:
        """
        批量更新日线数据

        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            max_workers: 最大并发数

        Returns:
            更新结果统计
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"开始批量更新日线数据，股票数: {len(stock_list)}")

        results = {
            'total': len(stock_list),
            'success': 0,
            'failed': 0,
            'failed_codes': []
        }

        def update_single_stock(code: str) -> Tuple[str, bool]:
            """更新单只股票数据"""
            try:
                # 标准化代码
                normalized_code = normalize_stock_code(code)

                # 获取数据
                df = self.fetch_daily_data(normalized_code, start_date, end_date)

                if not df.empty:
                    # 保存数据
                    success = self.save_daily_data(df)
                    return code, success
                else:
                    logger.warning(f"获取到空数据: {code}")
                    return code, False

            except Exception as e:
                logger.error(f"更新股票数据失败 {code}: {e}")
                return code, False

        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {
                executor.submit(update_single_stock, code): code
                for code in stock_list
            }

            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    stock_code, success = future.result()
                    if success:
                        results['success'] += 1
                        logger.info(f"更新成功: {stock_code}")
                    else:
                        results['failed'] += 1
                        results['failed_codes'].append(stock_code)
                        logger.warning(f"更新失败: {stock_code}")
                except Exception as e:
                    results['failed'] += 1
                    results['failed_codes'].append(code)
                    logger.error(f"更新异常 {code}: {e}")

        # 生成报告
        self._generate_batch_report(results, start_date, end_date)

        logger.info(f"批量更新完成: 成功 {results['success']}, 失败 {results['failed']}")
        return results

    def _generate_batch_report(self, results: Dict, start_date: str, end_date: str):
        """生成批量更新报告"""
        report = {
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'date_range': f"{start_date} - {end_date}",
            'summary': {
                'total_stocks': results['total'],
                'successful': results['success'],
                'failed': results['failed'],
                'success_rate': round(results['success'] / results['total'] * 100, 2)
            },
            'failed_codes': results['failed_codes']
        }

        # 保存报告
        report_dir = Path('data/reports')
        report_dir.mkdir(parents=True, exist_ok=True)

        report_file = report_dir / f"batch_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"更新报告已保存: {report_file}")

    def _get_from_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """从缓存获取数据"""
        cache_file = self.cache_dir / f"{cache_key}.parquet"

        if cache_file.exists():
            try:
                # 检查缓存时间（缓存有效期为1小时）
                file_mtime = cache_file.stat().st_mtime
                if time.time() - file_mtime < 3600:
                    df = pd.read_parquet(cache_file)
                    return df
            except Exception as e:
                logger.warning(f"读取缓存失败 {cache_key}: {e}")

        return None

    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        """保存数据到缓存"""
        try:
            cache_file = self.cache_dir / f"{cache_key}.parquet"
            df.to_parquet(cache_file)
        except Exception as e:
            logger.warning(f"保存缓存失败 {cache_key}: {e}")

    def calculate_advanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算高级技术指标

        Args:
            df: 包含基础指标的日线数据

        Returns:
            添加高级指标的数据
        """
        if df.empty or 'close' not in df.columns:
            return df

        df_advanced = df.copy()

        # 计算收益率
        df_advanced['returns'] = df_advanced['close'].pct_change()

        # 计算波动率 (20日滚动)
        df_advanced['volatility_20d'] = df_advanced['returns'].rolling(window=20).std() * np.sqrt(252)

        # 计算RSI
        delta = df_advanced['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df_advanced['rsi'] = 100 - (100 / (1 + rs))

        # 计算布林带
        df_advanced['bb_middle'] = df_advanced['close'].rolling(window=20).mean()
        bb_std = df_advanced['close'].rolling(window=20).std()
        df_advanced['bb_upper'] = df_advanced['bb_middle'] + 2 * bb_std
        df_advanced['bb_lower'] = df_advanced['bb_middle'] - 2 * bb_std
        df_advanced['bb_width'] = (df_advanced['bb_upper'] - df_advanced['bb_lower']) / df_advanced['bb_middle']

        # 计算ATR (Average True Range)
        high_low = df_advanced['high'] - df_advanced['low']
        high_close = abs(df_advanced['high'] - df_advanced['close'].shift())
        low_close = abs(df_advanced['low'] - df_advanced['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df_advanced['atr'] = true_range.rolling(window=14).mean()

        return df_advanced

    def get_market_data(self, market: str = "A股", start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取市场整体数据（指数等）

        Args:
            market: 市场类型
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            市场数据
        """
        try:
            if market == "A股":
                # 获取上证指数
                index_code = "sh000001"
                df = self.fetch_daily_data(index_code, start_date, end_date)
                df['market'] = 'A股'
                df['index_name'] = '上证指数'
                return df
            else:
                logger.warning(f"暂不支持的市场: {market}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取市场数据失败: {e}")
            return pd.DataFrame()


class DataQualityChecker:
    """数据质量检查器"""

    @staticmethod
    def check_daily_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查日线数据质量

        Args:
            df: 日线数据

        Returns:
            质量检查报告
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'checks': {}
        }

        if df.empty:
            report['status'] = 'EMPTY'
            return report

        # 1. 检查缺失值
        missing_stats = {}
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            missing_pct = round(missing_count / len(df) * 100, 2)
            if missing_count > 0:
                missing_stats[col] = {
                    'missing_count': int(missing_count),
                    'missing_pct': missing_pct
                }

        report['checks']['missing_values'] = {
            'stats': missing_stats,
            'pass': len(missing_stats) == 0
        }

        # 2. 检查重复数据
        duplicates = df.duplicated(subset=['date', 'code'], keep=False).sum()
        report['checks']['duplicates'] = {
            'count': int(duplicates),
            'pass': duplicates == 0
        }

        # 3. 检查价格数据有效性
        price_checks = {}
        price_cols = ['open', 'high', 'low', 'close']

        for col in price_cols:
            if col in df.columns:
                # 检查负值
                negative_count = (df[col] <= 0).sum()
                # 检查异常值（假设超过10000为异常）
                outlier_count = (df[col] > 10000).sum()

                price_checks[col] = {
                    'negative_count': int(negative_count),
                    'outlier_count': int(outlier_count),
                    'valid': negative_count == 0 and outlier_count == 0
                }

        report['checks']['price_validity'] = {
            'details': price_checks,
            'pass': all(check['valid'] for check in price_checks.values())
        }

        # 4. 检查时间序列连续性
        if 'date' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('date').copy()
            df_sorted['date_dt'] = pd.to_datetime(df_sorted['date'])
            gaps = (df_sorted['date_dt'].diff().dt.days > 1).sum()
            report['checks']['time_continuity'] = {
                'gaps': int(gaps),
                'pass': gaps == 0
            }

        # 5. 计算整体评分
        passed_checks = sum(1 for check in report['checks'].values() if check.get('pass', False))
        total_checks = len(report['checks'])

        if total_checks > 0:
            quality_score = round(passed_checks / total_checks * 100, 1)
        else:
            quality_score = 0

        report['quality_score'] = quality_score

        if quality_score >= 80:
            report['status'] = 'GOOD'
        elif quality_score >= 60:
            report['status'] = 'FAIR'
        else:
            report['status'] = 'POOR'

        return report

    @staticmethod
    def generate_quality_report(df: pd.DataFrame, output_path: str = None) -> str:
        """
        生成详细的质量报告

        Args:
            df: 数据
            output_path: 报告输出路径

        Returns:
            报告内容
        """
        report = DataQualityChecker.check_daily_data_quality(df)

        # 生成文本报告
        report_text = f"""
数据质量检查报告
================
检查时间: {report['timestamp']}
数据记录数: {report['total_records']}
整体质量评分: {report['quality_score']}/100
整体状态: {report['status']}

详细检查结果:
"""

        for check_name, check_result in report['checks'].items():
            report_text += f"\n{check_name.upper()}:\n"
            if 'pass' in check_result:
                status = "✓ 通过" if check_result['pass'] else "✗ 未通过"
                report_text += f"  状态: {status}\n"

            if 'stats' in check_result:
                for col, stats in check_result['stats'].items():
                    report_text += f"  {col}: 缺失{stats['missing_count']}条({stats['missing_pct']}%)\n"

            if 'count' in check_result:
                report_text += f"  重复记录: {check_result['count']}条\n"

        # 保存报告
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_text)

        return report_text


# 使用示例
def main():
    """主函数示例"""
    print("股票数据库数据处理基础模块")
    print("=" * 50)

    # 1. 初始化处理器
    processor = BaseDataProcessor()

    # 2. 获取股票列表
    print("获取股票列表...")
    stock_list_df = processor.get_stock_basic_info()

    if not stock_list_df.empty:
        print(f"获取到 {len(stock_list_df)} 只股票")
        print(f"列名: {stock_list_df.columns.tolist()}")
        print(stock_list_df.head())
    else:
        print("获取股票列表失败，使用示例股票")
        stock_list_df = pd.DataFrame({
            'ts_code': ['600519.SH', '000001.SZ'],
            'name': ['贵州茅台', '平安银行']
        })

    # 3. 示例股票代码（转换为标准化格式）
    sample_codes = []
    for _, row in stock_list_df.head(3).iterrows():
        try:
            code = normalize_stock_code(row['ts_code'])
            sample_codes.append(code)
            print(f"原始代码: {row['ts_code']} -> 标准化: {code}")
        except Exception as e:
            print(f"代码转换失败 {row['ts_code']}: {e}")

    if not sample_codes:
        sample_codes = ['sh600519', 'sz000001']

    # 4. 批量更新数据
    print(f"\n批量更新 {len(sample_codes)} 只股票数据...")
    results = processor.batch_update_daily_data(
        stock_list=sample_codes,
        start_date="20240101",
        end_date="20241231",
        max_workers=2
    )

    print(f"\n批量更新结果:")
    print(f"  成功: {results['success']}")
    print(f"  失败: {results['failed']}")

    # 5. 数据质量检查
    if results['success'] > 0:
        print("\n数据质量检查...")
        for code in sample_codes:
            try:
                # 从数据库加载最近30天的数据
                df = processor._get_daily_from_db(code, "20241201", "20241231")

                if not df.empty:
                    checker = DataQualityChecker()
                    report = checker.generate_quality_report(df)
                    print(f"\n{code} 数据质量报告:")
                    print(f"  记录数: {len(df)}")
                    print(f"  日期范围: {df['date'].min()} 到 {df['date'].max()}")

                    # 检查报告状态
                    quality_report = checker.check_daily_data_quality(df)
                    print(f"  质量评分: {quality_report['quality_score']} - {quality_report['status']}")
            except Exception as e:
                print(f"检查数据质量失败 {code}: {e}")

    print("\n数据处理模块初始化完成！")


if __name__ == "__main__":
    main()