# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\enhanced_processor.py
# File Name: enhanced_processor
# @ Author: mango-gh22
# @ Date：2025/12/10 18:55
"""
desc 创建 EnhancedDataProcessor

增强版数据处理器 - 完全集成现有架构
集成 db_connector, data_manager, config_loader, code_converter
修复已知问题：fillna弃用方法、MA计算问题
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    # 导入现有架构模块
    from src.database.db_connector import DatabaseConnector
    from src.data.data_manager import DataManager
    from src.config.config_loader import load_tushare_config
    from src.utils.code_converter import normalize_stock_code
    from src.utils.logger import get_logger  # 使用现有的get_logger
    from src.config.secret_loader import get_db_password
    from src.config.config_loader import load_database_config
except ImportError as e:
    print(f"导入错误: {e}")
    print("尝试创建缺失的模块...")
    # 创建简化的模块供测试
    pass


# 创建简化的模块结构供测试（如果导入失败）
class MockCodeConverter:
    @staticmethod
    def to_database_format(code: str) -> str:
        """简化版本"""
        if '.' in code:
            parts = code.split('.')
            if len(parts) == 2:
                return f"{parts[1].lower()}{parts[0]}"
        return code


class MockLogger:
    def __init__(self, name):
        self.name = name

    def info(self, msg):
        print(f"[INFO] {msg}")

    def warning(self, msg):
        print(f"[WARNING] {msg}")

    def error(self, msg):
        print(f"[ERROR] {msg}")

    def debug(self, msg):
        print(f"[DEBUG] {msg}")


def get_logger(name):
    return MockLogger(name)


class EnhancedDataProcessor:
    """增强版数据处理器 - 与现有架构完全集成"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化处理器

        Args:
            config_path: 配置文件路径，默认使用项目配置
        """
        # 1. 初始化日志（使用现有的get_logger）
        self.logger = get_logger(__name__)

        # 2. 加载配置（使用现有架构）
        self.config = self._load_configurations(config_path)

        # 3. 初始化数据库连接（可选）
        self.db_connector = None
        try:
            self.db_connector = DatabaseConnector()
            self.logger.info("数据库连接器初始化成功")
        except Exception as e:
            self.logger.warning(f"数据库连接器初始化失败: {e}")

        # 4. 质量阈值（与data_manager保持一致）
        self.quality_thresholds = {
            'excellent': 90,
            'good': 70,
            'fair': 50,
            'poor': 30
        }

        # 5. 技术指标配置
        self.indicators_config = self._load_indicators_config()

        self.logger.info("增强版数据处理器初始化完成")

    def _load_configurations(self, config_path: Optional[str] = None) -> Dict:
        """加载所有配置 - 集成现有配置系统"""
        config = {
            'database': {},
            'tushare': {},
            'processor': {}
        }

        try:
            # 尝试加载数据库配置
            try:
                db_config = load_database_config()
                config['database'] = db_config
            except:
                config['database'] = self._get_default_db_config()

            # 尝试加载Tushare配置
            try:
                tushare_config = load_tushare_config()
                config['tushare'] = tushare_config
            except:
                config['tushare'] = {}

            # 处理器特定配置
            config['processor'] = {
                'clean_rules': {
                    'min_price': 0.01,
                    'max_price': 1000000,
                    'min_volume': 0
                },
                'indicators': {
                    'ma_periods': [5, 10, 20, 30, 60, 120, 250],
                    'volume_ma_periods': [5, 10, 20],
                    'min_data_points': 5
                }
            }

        except Exception as e:
            self.logger.warning(f"配置加载失败，使用默认配置: {e}")
            config = self._get_default_config()

        return config

    def _get_default_db_config(self) -> Dict:
        """获取默认数据库配置"""
        return {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'database': 'stock_database',
            'charset': 'utf8mb4',
            'pool_size': 5
        }

    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'database': self._get_default_db_config(),
            'processor': {
                'clean_rules': {
                    'min_price': 0.01,
                    'max_price': 1000000,
                    'min_volume': 0
                },
                'indicators': {
                    'ma_periods': [5, 10, 20, 30, 60, 120, 250],
                    'volume_ma_periods': [5, 10, 20],
                    'min_data_points': 5
                }
            }
        }

    def _load_indicators_config(self) -> Dict:
        """加载技术指标配置"""
        return {
            'ma_periods': [5, 10, 20, 30, 60, 120, 250],
            'volume_ma_periods': [5, 10, 20],
            'advanced_indicators': {
                'rsi_period': 14,
                'bb_period': 20,
                'bb_std': 2,
                'atr_period': 14
            }
        }

    def clean_data(self, df: pd.DataFrame, symbol: Optional[str] = None) -> pd.DataFrame:
        """
        数据清洗 - 修复弃用方法，集成现有规则

        Args:
            df: 原始DataFrame
            symbol: 股票代码（用于日志）

        Returns:
            清洗后的DataFrame
        """
        if df.empty:
            self.logger.warning(f"空数据: {symbol or 'unknown'}")
            return df

        df_clean = df.copy()
        original_count = len(df_clean)

        self.logger.info(f"开始数据清洗: {symbol}, 原始数据{original_count}条")

        # 1. 修复弃用的fillna方法 - 使用ffill().bfill()替换
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            if col in df_clean.columns:
                # 修复：使用ffill().bfill()替换弃用的fillna(method='...')
                try:
                    # 先尝试前向填充，再后向填充
                    df_clean[col] = df_clean[col].ffill().bfill()
                except Exception as e:
                    self.logger.warning(f"列{col}填充失败: {e}")

        # 2. 价格有效性检查
        price_cols = ['open', 'high', 'low', 'close', 'pre_close']
        available_price_cols = [col for col in price_cols if col in df_clean.columns]

        if available_price_cols:
            min_price = self.config['processor']['clean_rules']['min_price']
            max_price = self.config['processor']['clean_rules']['max_price']

            # 创建有效价格掩码
            valid_mask = pd.Series(True, index=df_clean.index)
            for col in available_price_cols:
                col_mask = (df_clean[col] >= min_price) & (df_clean[col] <= max_price)
                invalid_count = (~col_mask).sum()
                if invalid_count > 0:
                    self.logger.warning(
                        f"移除{invalid_count}条无效{col}数据: {symbol}"
                    )
                valid_mask = valid_mask & col_mask

            df_clean = df_clean[valid_mask]

        # 3. 逻辑一致性检查（high >= low, high >= close, low <= close）
        if all(col in df_clean.columns for col in ['high', 'low', 'close']):
            logic_mask = (
                    (df_clean['high'] >= df_clean['low']) &
                    (df_clean['high'] >= df_clean['close']) &
                    (df_clean['low'] <= df_clean['close'])
            )
            invalid_logic = (~logic_mask).sum()
            if invalid_logic > 0:
                self.logger.warning(
                    f"移除{invalid_logic}条逻辑不一致数据: {symbol}"
                )
                df_clean = df_clean[logic_mask]

        # 4. 成交量检查
        if 'volume' in df_clean.columns:
            min_volume = self.config['processor']['clean_rules']['min_volume']
            volume_mask = df_clean['volume'] >= min_volume
            invalid_volume = (~volume_mask).sum()
            if invalid_volume > 0:
                self.logger.warning(
                    f"移除{invalid_volume}条无效成交量数据: {symbol}"
                )
                df_clean = df_clean[volume_mask]

        # 5. 移除重复日期
        if 'trade_date' in df_clean.columns:
            duplicates = df_clean.duplicated(subset=['trade_date'], keep='last')
            if duplicates.any():
                dup_count = duplicates.sum()
                self.logger.warning(f"移除{dup_count}条重复日期数据: {symbol}")
                df_clean = df_clean[~duplicates]

        cleaned_count = len(df_clean)
        removed_count = original_count - cleaned_count

        if removed_count > 0:
            self.logger.info(
                f"数据清洗完成: {symbol}, "
                f"移除{removed_count}条, 保留{cleaned_count}条"
            )
        else:
            self.logger.info(f"数据清洗完成: {symbol}, 所有数据有效")

        return df_clean

    def calculate_technical_indicators(
            self,
            df: pd.DataFrame,
            symbol: Optional[str] = None,
            min_data_points: Optional[int] = None
    ) -> pd.DataFrame:
        """
        计算技术指标 - 修复列名问题
        """
        if df.empty:
            self.logger.warning(f"空数据，跳过指标计算: {symbol or 'unknown'}")
            return df

        if min_data_points is None:
            min_data_points = self.config['processor']['indicators']['min_data_points']

        df_calc = df.copy()
        self.logger.info(f"开始计算技术指标: {symbol}, {len(df_calc)}条数据")

        # 确保按日期排序
        if 'trade_date' in df_calc.columns:
            df_calc = df_calc.sort_values('trade_date').reset_index(drop=True)

        # 检查数据是否足够
        if len(df_calc) < min_data_points:
            self.logger.warning(
                f"数据不足{min_data_points}条，跳过技术指标计算: {symbol}"
            )
            return df_calc

        # 修复：检查列名，支持多种命名格式
        close_col = None
        for col_name in ['close', 'close_price', 'close_price']:
            if col_name in df_calc.columns:
                close_col = col_name
                break

        if close_col is None:
            self.logger.warning(f"找不到收盘价列，跳过技术指标计算: {symbol}")
            return df_calc

        # 修复：使用找到的列名
        # 计算涨跌幅（如果基础数据存在）
        pre_close_col = None
        for col_name in ['pre_close', 'pre_close_price', 'preclose']:
            if col_name in df_calc.columns:
                pre_close_col = col_name
                break

        if pre_close_col:
            df_calc['pct_change'] = (
                    (df_calc[close_col] - df_calc[pre_close_col]) /
                    df_calc[pre_close_col].replace(0, np.nan) * 100
            ).round(2)
            # 处理除零错误
            df_calc['pct_change'] = df_calc['pct_change'].replace([np.inf, -np.inf], np.nan)

        # 计算振幅
        high_col = None
        low_col = None
        for col_name in ['high', 'high_price']:
            if col_name in df_calc.columns:
                high_col = col_name
                break

        for col_name in ['low', 'low_price']:
            if col_name in df_calc.columns:
                low_col = col_name
                break

        if high_col and low_col and pre_close_col:
            df_calc['amplitude'] = (
                    (df_calc[high_col] - df_calc[low_col]) /
                    df_calc[pre_close_col].replace(0, np.nan) * 100
            ).round(2)
            df_calc['amplitude'] = df_calc['amplitude'].replace([np.inf, -np.inf], np.nan)

        # 修复：MA计算 - 使用正确的列名
        ma_periods = self.indicators_config['ma_periods']

        for period in ma_periods:
            col_name = f'ma{period}'
            if len(df_calc) >= period:
                # 充足数据，计算完整MA
                df_calc[col_name] = df_calc[close_col].rolling(
                    window=period,
                    min_periods=1
                ).mean().round(2)
            else:
                # 数据不足，计算可用的最大值
                available_period = min(period, len(df_calc))
                df_calc[col_name] = df_calc[close_col].rolling(
                    window=available_period,
                    min_periods=1
                ).mean().round(2)
                if period > 5:  # 只对长周期记录警告
                    self.logger.debug(
                        f"MA{period}使用{available_period}条数据计算: {symbol}"
                    )

        # 成交量均线
        volume_periods = self.indicators_config['volume_ma_periods']
        volume_col = None
        for col_name in ['volume', 'vol']:
            if col_name in df_calc.columns:
                volume_col = col_name
                break

        if volume_col:
            for period in volume_periods:
                col_name = f'volume_ma{period}'
                if len(df_calc) >= period:
                    df_calc[col_name] = df_calc[volume_col].rolling(
                        window=period,
                        min_periods=1
                    ).mean().round(0)
                else:
                    available_period = min(period, len(df_calc))
                    df_calc[col_name] = df_calc[volume_col].rolling(
                        window=available_period,
                        min_periods=1
                    ).mean().round(0)

        # 高级指标（可选）
        if len(df_calc) >= 14:  # RSI需要至少14个周期
            try:
                self._calculate_advanced_indicators(df_calc, close_col)
                self.logger.debug(f"高级指标计算完成: {symbol}")
            except Exception as e:
                self.logger.warning(f"高级指标计算失败: {symbol}, 错误: {e}")

        self.logger.info(
            f"技术指标计算完成: {symbol}, {len(df_calc)}条记录"
        )

        return df_calc

    def _calculate_advanced_indicators(self, df: pd.DataFrame, close_col: str = 'close'):
        """计算高级技术指标"""
        # RSI (14天)
        delta = df[close_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi'] = df['rsi'].clip(0, 100).round(2)

        # 布林带 (20天，2个标准差)
        df['bb_middle'] = df[close_col].rolling(window=20).mean()
        bb_std = df[close_col].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + 2 * bb_std
        df['bb_lower'] = df['bb_middle'] - 2 * bb_std

        # 20日波动率
        df['returns'] = df[close_col].pct_change()
        df['volatility_20d'] = df['returns'].rolling(window=20).std() * np.sqrt(252)
        df['volatility_20d'] = df['volatility_20d'].round(4)

        # 清理临时列
        if 'returns' in df.columns:
            df.drop('returns', axis=1, inplace=True)

    def assess_data_quality(self, df: pd.DataFrame, symbol: str) -> Dict:
        """
        数据质量评估 - 与data_manager质量标准一致

        Args:
            df: 待评估数据
            symbol: 股票代码

        Returns:
            质量评估报告
        """
        if df.empty:
            return {
                'symbol': symbol,
                'total_score': 0,
                'grade': 'poor',
                'status': 'EMPTY_DATA',
                'issues': ['空数据'],
                'record_count': 0,
                'assessment_time': datetime.now().isoformat()
            }

        scores = {}

        # 1. 完整性 (30%)
        completeness = self._calculate_completeness_score(df)
        scores['completeness'] = completeness

        # 2. 准确性 (40%)
        accuracy = self._calculate_accuracy_score(df)
        scores['accuracy'] = accuracy

        # 3. 一致性 (20%)
        consistency = self._calculate_consistency_score(df)
        scores['consistency'] = consistency

        # 4. 及时性 (10%)
        timeliness = self._calculate_timeliness_score(df)
        scores['timeliness'] = timeliness

        # 加权总分
        weights = {'completeness': 0.3, 'accuracy': 0.4,
                   'consistency': 0.2, 'timeliness': 0.1}
        total_score = sum(scores[k] * weights[k] for k in weights)
        total_score = round(total_score, 2)

        # 确定质量等级
        grade = self._score_to_grade(total_score)

        # 识别问题
        issues = self._identify_quality_issues(df, scores)

        return {
            'symbol': symbol,
            'total_score': total_score,
            'grade': grade,
            'scores': scores,
            'issues': issues,
            'record_count': len(df),
            'assessment_time': datetime.now().isoformat()
        }

    def _calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """计算完整性分数"""
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        available_cols = [col for col in required_cols if col in df.columns]

        if not available_cols:
            return 0.0

        # 字段存在率
        field_score = len(available_cols) / len(required_cols) * 100

        # 缺失值率
        missing_rates = []
        for col in available_cols:
            if col in df.columns:
                missing_rate = df[col].isnull().mean()
                missing_rates.append(missing_rate)

        missing_score = 100 - (sum(missing_rates) / len(missing_rates) * 100) if missing_rates else 100

        # 综合分数
        completeness_score = (field_score * 0.5 + missing_score * 0.5)
        return round(completeness_score, 2)

    def _calculate_accuracy_score(self, df: pd.DataFrame) -> float:
        """计算准确性分数"""
        accuracy_items = []

        # 1. 价格有效性
        price_cols = [col for col in ['open', 'high', 'low', 'close'] if col in df.columns]
        if price_cols:
            valid_prices = []
            for col in price_cols:
                valid_ratio = ((df[col] > 0) & (df[col] < 1e6)).mean()
                valid_prices.append(valid_ratio * 100)
            price_score = sum(valid_prices) / len(valid_prices) if valid_prices else 100
            accuracy_items.append(price_score)

        # 2. 逻辑一致性
        if all(col in df.columns for col in ['high', 'low']):
            logic_ratio = (df['high'] >= df['low']).mean() * 100
            accuracy_items.append(logic_ratio)

        # 3. 成交量有效性
        if 'volume' in df.columns:
            volume_ratio = (df['volume'] >= 0).mean() * 100
            accuracy_items.append(volume_ratio)

        accuracy_score = sum(accuracy_items) / len(accuracy_items) if accuracy_items else 100
        return round(accuracy_score, 2)

    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """计算一致性分数"""
        consistency_items = []

        # 1. 日期连续性
        if 'trade_date' in df.columns:
            try:
                dates = pd.to_datetime(df['trade_date'])
                date_diff = dates.diff().dt.days
                # 检查是否连续（允许周末间隔）
                is_weekday = dates.dt.weekday < 5
                expected_diff = 1  # 期望交易日间隔为1天
                # 计算连续性分数
                if len(date_diff) > 1:
                    continuity_ratio = (date_diff[1:] <= 3).mean() * 100  # 允许最多3天间隔
                    consistency_items.append(continuity_ratio)
            except:
                pass

        # 2. 数据一致性
        if 'pct_change' in df.columns and 'amplitude' in df.columns:
            # 涨跌幅和振幅应该在合理范围内
            pct_change_valid = ((df['pct_change'].abs() <= 20) | df['pct_change'].isna()).mean() * 100
            amplitude_valid = ((df['amplitude'] <= 30) | df['amplitude'].isna()).mean() * 100
            consistency_items.extend([pct_change_valid, amplitude_valid])

        consistency_score = sum(consistency_items) / len(consistency_items) if consistency_items else 100
        return round(consistency_score, 2)

    def _calculate_timeliness_score(self, df: pd.DataFrame) -> float:
        """计算及时性分数"""
        if 'trade_date' not in df.columns:
            return 100.0

        try:
            dates = pd.to_datetime(df['trade_date'])
            latest_date = dates.max()
            days_diff = (datetime.now() - latest_date).days

            if days_diff <= 1:
                return 100.0
            elif days_diff <= 3:
                return 80.0
            elif days_diff <= 7:
                return 60.0
            elif days_diff <= 30:
                return 40.0
            else:
                return 20.0
        except:
            return 100.0

    def _score_to_grade(self, score: float) -> str:
        """分数转等级 - 与data_manager一致"""
        if score >= self.quality_thresholds['excellent']:
            return 'excellent'
        elif score >= self.quality_thresholds['good']:
            return 'good'
        elif score >= self.quality_thresholds['fair']:
            return 'fair'
        else:
            return 'poor'

    def _identify_quality_issues(self, df: pd.DataFrame, scores: Dict) -> List[str]:
        """识别质量问题"""
        issues = []

        if scores.get('completeness', 100) < 80:
            issues.append('数据完整性不足')

        if scores.get('accuracy', 100) < 80:
            issues.append('数据准确性可疑')

        if scores.get('consistency', 100) < 80:
            issues.append('数据一致性差')

        if scores.get('timeliness', 100) < 60:
            issues.append('数据不够及时')

        # 检查具体问题
        if 'close' in df.columns:
            if df['close'].isnull().any():
                issues.append('存在缺失的收盘价')

            if (df['close'] <= 0).any():
                issues.append('存在非正收盘价')

        return issues

    def prepare_for_storage(
            self,
            df: pd.DataFrame,
            symbol: str,
            data_source: str = 'baostock'
    ) -> pd.DataFrame:
        """
        准备数据存储 - 标准化列名和格式

        Args:
            df: 处理后的数据
            symbol: 股票代码
            data_source: 数据源

        Returns:
            适合数据库存储的DataFrame
        """
        if df.empty:
            self.logger.warning(f"空数据，跳过存储准备: {symbol}")
            return df

        df_storage = df.copy()

        # 标准化股票代码
        try:
            standardized_symbol = normalize_stock_code(symbol)
        except:
            standardized_symbol = symbol
            self.logger.warning(f"代码标准化失败，使用原始代码: {symbol}")

        # 添加元数据
        df_storage['symbol'] = standardized_symbol
        df_storage['processed_time'] = datetime.now()
        df_storage['data_source'] = data_source
        df_storage['quality_grade'] = 'pending'  # 将在质检后更新

        # 标准化列名（匹配数据库表结构）
        column_mapping = {
            'trade_date': 'trade_date',
            'ts_code': 'symbol',  # 如果已有ts_code
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'pre_close': 'pre_close_price',
            'change': 'change',  # 涨跌额
            'pct_change': 'change_percent',
            'volume': 'volume',
            'amount': 'amount',
            'amplitude': 'amplitude',
            'turnover_rate': 'turnover_rate',
            'turnover_rate_f': 'turnover_rate_f',
            'volume_ratio': 'volume_ratio',
            'pe': 'pe',
            'pe_ttm': 'pe_ttm',
            'pb': 'pb',
            'ps': 'ps',
            'ps_ttm': 'ps_ttm',
            'dv_ratio': 'dv_ratio',
            'dv_ttm': 'dv_ttm',
            'total_share': 'total_share',
            'float_share': 'float_share',
            'free_share': 'free_share',
            'total_mv': 'total_mv',
            'circ_mv': 'circ_mv'
        }

        # 重命名列
        rename_dict = {}
        for old_col, new_col in column_mapping.items():
            if old_col in df_storage.columns and old_col != new_col:
                rename_dict[old_col] = new_col

        if rename_dict:
            df_storage = df_storage.rename(columns=rename_dict)

        # 确保日期格式
        if 'trade_date' in df_storage.columns:
            try:
                # 尝试转换为YYYYMMDD格式
                df_storage['trade_date'] = pd.to_datetime(
                    df_storage['trade_date']
                ).dt.strftime('%Y%m%d')
            except:
                # 如果转换失败，保持原样
                pass

        # 确保数值列类型
        numeric_cols = [
            'open_price', 'high_price', 'low_price', 'close_price',
            'pre_close_price', 'volume', 'amount', 'change_percent',
            'amplitude', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120', 'ma250'
        ]

        for col in numeric_cols:
            if col in df_storage.columns:
                df_storage[col] = pd.to_numeric(df_storage[col], errors='coerce')

        self.logger.info(
            f"存储准备完成: {symbol}, {len(df_storage)}条记录, {len(df_storage.columns)}列"
        )

        return df_storage

    def process_stock_data(
            self,
            raw_df: pd.DataFrame,
            symbol: str,
            data_source: str = 'baostock'
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        完整的数据处理流程

        Args:
            raw_df: 原始数据
            symbol: 股票代码
            data_source: 数据源

        Returns:
            (处理后的DataFrame, 质量报告)
        """
        try:
            self.logger.info(f"开始处理股票数据: {symbol}")

            # 1. 清洗数据
            df_clean = self.clean_data(raw_df, symbol)

            if df_clean.empty:
                self.logger.warning(f"清洗后数据为空: {symbol}")
                return pd.DataFrame(), {
                    'symbol': symbol,
                    'total_score': 0,
                    'grade': 'poor',
                    'status': 'CLEANED_EMPTY',
                    'issues': ['清洗后无数据']
                }

            # 2. 计算技术指标
            df_with_indicators = self.calculate_technical_indicators(
                df_clean, symbol
            )

            # 3. 质量评估
            quality_report = self.assess_data_quality(
                df_with_indicators, symbol
            )

            # 4. 准备存储
            df_final = self.prepare_for_storage(
                df_with_indicators, symbol, data_source
            )

            # 更新质量等级
            if not df_final.empty and 'quality_grade' in df_final.columns:
                df_final['quality_grade'] = quality_report['grade']

            self.logger.info(
                f"处理完成: {symbol}, "
                f"质量: {quality_report['grade']} ({quality_report['total_score']}分), "
                f"记录: {len(df_final)}条"
            )

            return df_final, quality_report

        except Exception as e:
            self.logger.error(f"处理失败: {symbol}, 错误: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise


def create_test_data() -> pd.DataFrame:
    """创建测试数据"""
    dates = pd.date_range('2024-01-01', periods=31, freq='D')

    # 创建基本价格数据
    base_price = 100.0
    returns = np.random.normal(0.001, 0.02, 31)  # 日收益率
    prices = base_price * (1 + np.cumsum(returns))

    df = pd.DataFrame({
        'trade_date': dates.strftime('%Y%m%d'),
        'open': prices * (1 + np.random.uniform(-0.01, 0.01, 31)),
        'high': prices * (1 + np.random.uniform(0, 0.03, 31)),
        'low': prices * (1 + np.random.uniform(-0.03, 0, 31)),
        'close': prices,
        'pre_close': np.roll(prices, 1),
        'volume': np.random.randint(1000000, 10000000, 31),
        'amount': prices * np.random.randint(1000000, 10000000, 31) / 10000
    })

    # 设置第一条的pre_close为close
    df.loc[0, 'pre_close'] = df.loc[0, 'close'] * 0.99

    return df


def test_enhanced_processor():
    """测试增强版处理器"""
    print("🧪 测试增强版数据处理器")
    print("=" * 50)

    try:
        # 1. 初始化处理器
        processor = EnhancedDataProcessor()
        print("✅ 处理器初始化成功")

        # 2. 创建测试数据
        test_data = create_test_data()
        print(f"📊 创建测试数据: {len(test_data)} 条记录")
        print(f"   日期范围: {test_data['trade_date'].iloc[0]} 到 {test_data['trade_date'].iloc[-1]}")

        # 3. 测试完整流程
        symbol = 'sh600519'
        print(f"🔧 开始处理: {symbol}")

        df_processed, quality_report = processor.process_stock_data(
            test_data, symbol, 'test'
        )

        print(f"✅ 处理完成: {len(df_processed)} 条记录")
        print(f"📈 质量报告:")
        print(f"   等级: {quality_report['grade']}")
        print(f"   分数: {quality_report['total_score']}")
        print(f"   各维度分数: {quality_report['scores']}")
        if quality_report['issues']:
            print(f"   问题: {', '.join(quality_report['issues'])}")

        # 4. 显示数据示例
        if not df_processed.empty:
            print("📋 数据示例 (前3条):")
            sample_cols = ['trade_date', 'close_price', 'change_percent', 'ma5', 'volume']
            available_cols = [col for col in sample_cols if col in df_processed.columns]

            for i in range(min(3, len(df_processed))):
                row = df_processed.iloc[i]
                info_parts = []
                for col in available_cols:
                    val = row[col]
                    if isinstance(val, (int, np.integer)):
                        info_parts.append(f"{col}: {val:,}")
                    elif isinstance(val, float):
                        if col == 'change_percent':
                            sign = '+' if val >= 0 else ''
                            info_parts.append(f"{col}: {sign}{val:.2f}%")
                        else:
                            info_parts.append(f"{col}: {val:.2f}")
                    else:
                        info_parts.append(f"{col}: {val}")

                print(f"   第{i + 1}条: {', '.join(info_parts)}")

            # 显示列信息
            print(f"📊 最终数据形状: {len(df_processed)} 行 × {len(df_processed.columns)} 列")
            print(
                f"   技术指标列: {[col for col in df_processed.columns if 'ma' in col or 'bb' in col or 'rsi' in col]}")

        print("✅ 增强版数据处理器测试通过")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        print(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = test_enhanced_processor()
    sys.exit(0 if success else 1)