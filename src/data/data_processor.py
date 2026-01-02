# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\data_processor.py
# File Name: data_processor
# @ Author: mango-gh22
# @ Date：2025/12/7 23:18
"""
desc 创建数据处理器
"""

# src/data/data_processor.py
"""
数据处理器 - 负责数据清洗和技术指标计算
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
from pathlib import Path

from src.utils.code_converter import normalize_stock_code

logger = logging.getLogger(__name__)


class DataProcessor:
    """数据处理器 - 清洗和计算技术指标"""

    def __init__(self):
        # 技术指标配置
        self.ma_periods = [5, 10, 20, 30, 60, 120, 250]
        self.volume_ma_periods = [5, 10, 20]

        logger.info("数据处理器初始化完成")

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
            logger.warning(f"空DataFrame，无法清洗: {symbol}")
            return df

        df_clean = df.copy()

        # 1. 确保有正确的股票代码
        if 'symbol' not in df_clean.columns:
            df_clean['symbol'] = symbol

        # 2. 确保日期格式正确
        if 'trade_date' in df_clean.columns:
            # 转换为datetime，然后格式化
            df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date'], errors='coerce')
            df_clean = df_clean.dropna(subset=['trade_date'])
            # df_clean['trade_date'] = df_clean['trade_date'].dt.strftime('%Y%m%d')
            df_clean['trade_date'] = self._clean_date_column(df_clean['trade_date'])


        # 3. 处理缺失值
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price',
                        'volume', 'amount', 'pre_close_price']

        for col in numeric_cols:
            if col in df_clean.columns:
                if col in ['open_price', 'high_price', 'low_price', 'close_price', 'pre_close_price']:
                    # 价格数据使用前向填充
                    df_clean[col] = df_clean[col].fillna(method='ffill').fillna(method='bfill')
                elif col == 'volume':
                    # 成交量填充为0
                    df_clean[col] = df_clean[col].fillna(0)
                else:
                    df_clean[col] = df_clean[col].fillna(0)

        # 4. 验证价格数据有效性
        df_clean = self._validate_price_data(df_clean)

        # 5. 去除重复数据
        df_clean = df_clean.drop_duplicates(subset=['trade_date'], keep='last')

        # 6. 按日期排序
        df_clean = df_clean.sort_values('trade_date')
        df_clean = df_clean.reset_index(drop=True)

        # 7. 添加处理标记
        df_clean['processed_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_clean['data_source'] = 'baostock_processed'

        logger.info(f"数据清洗完成: {symbol}, {len(df_clean)} 条记录")
        return df_clean

    def _clean_date_column(self, date_series):
        """
        清洗日期列 - 统一格式并处理异常

        Args:
            date_series: 日期列Series

        Returns:
            清洗后的日期Series
        """

        def format_date(date_val):
            """格式化单个日期"""
            if pd.isna(date_val):
                return None

            # 处理字符串日期
            if isinstance(date_val, str):
                date_str = str(date_val).strip()

                # 移除中文日期中的年月日字符
                date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')

                # 处理多种分隔符
                date_str = date_str.replace('/', '-').replace('.', '-')

                # 尝试解析日期
                try:
                    # 尝试常见格式
                    for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d', '%Y.%m.%d']:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            return dt.strftime('%Y-%m-%d')
                        except:
                            continue

                    # 如果都失败，尝试pandas解析
                    dt = pd.to_datetime(date_str, errors='coerce')
                    if pd.notna(dt):
                        return dt.strftime('%Y-%m-%d')

                except Exception as e:
                    logger.debug(f"日期解析失败 {date_val}: {e}")

            # 处理datetime对象
            elif isinstance(date_val, (datetime, pd.Timestamp)):
                return date_val.strftime('%Y-%m-%d')

            return None

        # 应用格式化
        cleaned_series = date_series.apply(format_date)

        # 统计处理情况
        original_count = len(date_series)
        cleaned_count = cleaned_series.notna().sum()

        if original_count != cleaned_count:
            logger.warning(
                f"日期清洗: 原始{original_count}条, 成功{cleaned_count}条, 失败{original_count - cleaned_count}条")

        return cleaned_series


    def _validate_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """验证价格数据合理性"""
        if df.empty:
            return df

        valid_mask = pd.Series(True, index=df.index)

        # 1. 价格必须为正
        price_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'pre_close_price']
        for col in price_cols:
            if col in df.columns:
                valid_mask &= (df[col] > 0)

        # 2. high >= low
        if all(col in df.columns for col in ['high_price', 'low_price']):
            valid_mask &= (df['high_price'] >= df['low_price'])

        # 3. 价格在高低范围内
        if all(col in df.columns for col in ['open_price', 'high_price', 'low_price']):
            valid_mask &= (df['open_price'] >= df['low_price']) & (df['open_price'] <= df['high_price'])

        if all(col in df.columns for col in ['close_price', 'high_price', 'low_price']):
            valid_mask &= (df['close_price'] >= df['low_price']) & (df['close_price'] <= df['high_price'])

        # 移除无效数据
        df_valid = df[valid_mask].copy()

        if len(df) != len(df_valid):
            removed_count = len(df) - len(df_valid)
            logger.warning(f"移除 {removed_count} 条无效价格数据")

        return df_valid

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        Args:
            df: 清洗后的数据，需要包含 close_price 列

        Returns:
            包含技术指标的数据
        """
        if df.empty or 'close_price' not in df.columns:
            logger.warning("数据为空或缺少收盘价列，无法计算技术指标")
            return df

        df_indicators = df.copy()

        # 确保数据已按日期排序
        df_indicators = df_indicators.sort_values('trade_date')
        df_indicators = df_indicators.reset_index(drop=True)

        # 获取价格序列
        close_prices = df_indicators['close_price'].astype(float)

        # 1. 计算移动平均线
        for period in self.ma_periods:
            if len(close_prices) >= period:
                df_indicators[f'ma{period}'] = close_prices.rolling(window=period).mean().round(4)
            else:
                df_indicators[f'ma{period}'] = np.nan

        # 2. 计算成交量均线
        if 'volume' in df_indicators.columns:
            volume_series = df_indicators['volume'].astype(float)
            for period in self.volume_ma_periods:
                if len(volume_series) >= period:
                    df_indicators[f'volume_ma{period}'] = volume_series.rolling(window=period).mean().round(2)
                else:
                    df_indicators[f'volume_ma{period}'] = np.nan

            # 计算量比（相对于5日均量）
            if 'volume_ma5' in df_indicators.columns:
                df_indicators['volume_ratio'] = (df_indicators['volume'] / df_indicators['volume_ma5']).round(2)

        # 3. 计算涨跌幅（如果不存在）
        if 'pct_change' not in df_indicators.columns and 'pre_close_price' in df_indicators.columns:
            df_indicators['pct_change'] = ((df_indicators['close_price'] - df_indicators['pre_close_price']) /
                                           df_indicators['pre_close_price'] * 100).round(4)

        # 4. 计算涨跌额（如果不存在）
        if 'change_amount' not in df_indicators.columns and 'pre_close_price' in df_indicators.columns:
            df_indicators['change_amount'] = (df_indicators['close_price'] - df_indicators['pre_close_price']).round(4)

        # 5. 计算振幅（如果不存在）
        if 'amplitude' not in df_indicators.columns and all(
                col in df_indicators.columns for col in ['high_price', 'low_price', 'pre_close_price']):
            df_indicators['amplitude'] = ((df_indicators['high_price'] - df_indicators['low_price']) /
                                          df_indicators['pre_close_price'] * 100).round(4)

        # 6. 计算换手率（如果不存在）
        if 'turnover_rate' not in df_indicators.columns and 'volume' in df_indicators.columns:
            # 简化的换手率计算（实际需要流通股本）
            # 这里使用成交量/1000000作为近似值
            df_indicators['turnover_rate'] = (df_indicators['volume'] / 1000000).round(4)

        logger.info(f"技术指标计算完成: {len(df_indicators)} 条记录")
        return df_indicators

    def calculate_advanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算高级技术指标

        Args:
            df: 包含基础指标的数据

        Returns:
            包含高级指标的数据
        """
        if df.empty or 'close_price' not in df.columns:
            return df

        df_advanced = df.copy()

        # 确保数据已排序
        df_advanced = df_advanced.sort_values('trade_date')
        df_advanced = df_advanced.reset_index(drop=True)

        close_prices = df_advanced['close_price'].astype(float)

        # 1. RSI（相对强弱指数）
        if len(close_prices) >= 14:
            delta = close_prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df_advanced['rsi'] = (100 - (100 / (1 + rs))).round(2)

        # 2. MACD
        if len(close_prices) >= 26:
            ema12 = close_prices.ewm(span=12, adjust=False).mean()
            ema26 = close_prices.ewm(span=26, adjust=False).mean()
            df_advanced['macd'] = (ema12 - ema26).round(4)
            df_advanced['macd_signal'] = df_advanced['macd'].ewm(span=9, adjust=False).mean().round(4)
            df_advanced['macd_hist'] = (df_advanced['macd'] - df_advanced['macd_signal']).round(4)

        # 3. 布林带
        if len(close_prices) >= 20:
            window = 20
            df_advanced['bb_middle'] = close_prices.rolling(window=window).mean()
            bb_std = close_prices.rolling(window=window).std()
            df_advanced['bb_upper'] = df_advanced['bb_middle'] + 2 * bb_std
            df_advanced['bb_lower'] = df_advanced['bb_middle'] - 2 * bb_std
            df_advanced['bb_width'] = ((df_advanced['bb_upper'] - df_advanced['bb_lower']) /
                                       df_advanced['bb_middle'] * 100).round(2)

        # 4. 波动率（20日年化）
        if 'pct_change' in df_advanced.columns:
            if len(df_advanced) >= 20:
                df_advanced['volatility_20d'] = (df_advanced['pct_change'].rolling(window=20).std() *
                                                 np.sqrt(252)).round(4)

        logger.info(f"高级技术指标计算完成")
        return df_advanced

    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证数据质量

        Args:
            df: 要验证的数据

        Returns:
            质量报告
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'missing_values': {},
            'price_issues': 0,
            'volume_issues': 0,
            'quality_score': 100,
            'status': 'excellent'
        }

        if df.empty:
            report['quality_score'] = 0
            report['status'] = 'empty'
            return report

        # 1. 检查缺失值
        required_cols = ['trade_date', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']
        for col in required_cols:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    report['missing_values'][col] = int(missing_count)

        # 2. 检查价格问题
        price_cols = ['open_price', 'high_price', 'low_price', 'close_price']
        price_issues = 0

        for col in price_cols:
            if col in df.columns:
                # 检查负值
                negative = (df[col] <= 0).sum()
                price_issues += negative

        # 检查价格关系
        if all(col in df.columns for col in ['high_price', 'low_price']):
            invalid_high_low = (df['high_price'] < df['low_price']).sum()
            price_issues += invalid_high_low

        report['price_issues'] = int(price_issues)

        # 3. 检查成交量问题
        if 'volume' in df.columns:
            negative_volume = (df['volume'] < 0).sum()
            report['volume_issues'] = int(negative_volume)

        # 4. 计算质量评分
        penalty = 0

        # 缺失值惩罚
        for col, count in report['missing_values'].items():
            penalty += (count / len(df)) * 20

        # 价格问题惩罚
        if report['price_issues'] > 0:
            penalty += (report['price_issues'] / len(df)) * 50

        # 成交量问题惩罚
        if report['volume_issues'] > 0:
            penalty += min(report['volume_issues'] * 10, 100)

        quality_score = max(0, 100 - penalty)
        report['quality_score'] = round(quality_score, 1)

        # 5. 确定状态
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

    def prepare_for_storage(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备数据存储格式

        Args:
            df: 处理后的数据

        Returns:
            适合存储的数据格式
        """
        if df.empty:
            return df

        df_storage = df.copy()

        # 确保所有必需的列都存在
        required_columns = {
            'symbol': '',
            'trade_date': '',
            'open_price': 0.0,
            'close_price': 0.0,
            'high_price': 0.0,
            'low_price': 0.0,
            'volume': 0,
            'amount': 0.0,
            'pre_close_price': 0.0,
            'change_amount': 0.0,
            'pct_change': 0.0,
            'turnover_rate': 0.0,
            'amplitude': 0.0,
            'ma5': 0.0,
            'ma10': 0.0,
            'ma20': 0.0,
            'ma30': 0.0,
            'ma60': 0.0,
            'volume_ma5': 0,
            'volume_ma10': 0,
            'volume_ratio': 0.0,
            'processed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_source': 'processed'
        }

        # 添加缺失的列
        for col, default_value in required_columns.items():
            if col not in df_storage.columns:
                df_storage[col] = default_value

        # 确保数据类型正确
        float_cols = ['open_price', 'close_price', 'high_price', 'low_price',
                      'amount', 'pre_close_price', 'change_amount', 'pct_change',
                      'turnover_rate', 'amplitude', 'ma5', 'ma10', 'ma20',
                      'ma30', 'ma60', 'volume_ratio']

        int_cols = ['volume', 'volume_ma5', 'volume_ma10']

        for col in float_cols:
            if col in df_storage.columns:
                df_storage[col] = pd.to_numeric(df_storage[col], errors='coerce').fillna(0.0)

        for col in int_cols:
            if col in df_storage.columns:
                df_storage[col] = pd.to_numeric(df_storage[col], errors='coerce').fillna(0).astype(int)

        # 确保日期格式正确
        if 'trade_date' in df_storage.columns:
            df_storage['trade_date'] = pd.to_datetime(df_storage['trade_date'], errors='coerce').dt.strftime('%Y%m%d')

        logger.info(f"数据存储准备完成: {len(df_storage)} 条记录")
        return df_storage


def test_data_processor():
    """测试数据处理器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试数据处理器")
    print("=" * 50)

    processor = DataProcessor()

    try:
        # 1. 创建测试数据
        print("\n📊 1. 创建测试数据")

        dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
        np.random.seed(42)

        # 生成随机价格序列
        base_price = 100.0
        returns = np.random.normal(0.001, 0.02, len(dates))
        prices = base_price * np.exp(np.cumsum(returns))

        test_data = pd.DataFrame({
            'trade_date': dates.strftime('%Y%m%d'),
            'open_price': prices * (1 + np.random.uniform(-0.01, 0.01, len(dates))),
            'high_price': prices * (1 + np.random.uniform(0, 0.02, len(dates))),
            'low_price': prices * (1 + np.random.uniform(-0.02, 0, len(dates))),
            'close_price': prices,
            'pre_close_price': prices * (1 + np.random.uniform(-0.01, 0.01, len(dates))),
            'volume': np.random.randint(1000000, 10000000, len(dates)),
            'amount': prices * np.random.randint(1000000, 10000000, len(dates)),
        })

        test_data['symbol'] = 'sh600519'

        print(f"   创建测试数据: {len(test_data)} 条记录")
        print(f"   日期范围: {test_data['trade_date'].min()} 到 {test_data['trade_date'].max()}")

        # 2. 测试数据清洗
        print("\n🧹 2. 测试数据清洗")
        cleaned_data = processor.clean_daily_data(test_data, 'sh600519')
        print(f"   清洗后数据: {len(cleaned_data)} 条记录")

        # 3. 测试质量验证
        print("\n🔍 3. 测试数据质量验证")
        quality_report = processor.validate_data_quality(cleaned_data)
        print(f"   质量评分: {quality_report['quality_score']}")
        print(f"   质量状态: {quality_report['status']}")
        print(f"   价格问题: {quality_report['price_issues']} 条")

        # 4. 测试技术指标计算
        print("\n📈 4. 测试技术指标计算")
        with_indicators = processor.calculate_technical_indicators(cleaned_data)

        print(f"   基础指标计算完成")
        ma_columns = [col for col in with_indicators.columns if col.startswith('ma')]
        print(f"   移动平均线: {ma_columns}")

        if 'ma5' in with_indicators.columns:
            print(f"   MA5示例值: {with_indicators['ma5'].iloc[10]:.2f}")

        # 5. 测试高级指标计算
        print("\n🚀 5. 测试高级指标计算")
        with_advanced = processor.calculate_advanced_indicators(with_indicators)

        advanced_cols = ['rsi', 'macd', 'bb_middle', 'volatility_20d']
        available_cols = [col for col in advanced_cols if col in with_advanced.columns]
        print(f"   高级指标: {available_cols}")

        # 6. 测试存储准备
        print("\n💾 6. 测试存储准备")
        storage_ready = processor.prepare_for_storage(with_advanced)

        print(f"   存储准备完成")
        print(f"   列数: {len(storage_ready.columns)}")
        print(f"   示例列: {list(storage_ready.columns)[:10]}...")

        # 7. 显示数据示例
        print("\n📋 7. 数据示例")
        sample = storage_ready.head(3)
        for i, (_, row) in enumerate(sample.iterrows()):
            print(f"   第{i + 1}条数据:")
            print(f"     日期: {row['trade_date']}")
            print(f"     收盘价: {row.get('close_price', 'N/A'):.2f}")
            print(f"     涨跌幅: {row.get('pct_change', 0):+.2f}%")
            print(f"     MA5: {row.get('ma5', 'N/A'):.2f}")
            print(f"     成交量: {row.get('volume', 0):,.0f}")

        print("\n✅ 数据处理器测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_data_processor()