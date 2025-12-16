# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/processors\adjustor.py
# File Name: adjustor
# @ Author: mango-gh22
# @ Date：2025/12/14 15:36
"""
desc 复权计算模块 - 实现前复权、后复权价格计算
修复版本: v0.5.1-fix - 修正数据库连接器初始化顺序
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
import logging
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum

from src.query.query_engine import QueryEngine
from src.database.db_connector import DatabaseConnector

try:
    from src.data.adjustment_factor_manager import AdjustmentFactorManager
except ImportError:
    # 如果文件不存在，在 adjustor.py 内创建简化版
    class AdjustmentFactorManager:
        """简化的复权因子管理器"""

        def __init__(self, config_path: str):
            pass

        def fetch_factors_from_baostock(self, symbol: str, **kwargs):
            return pd.DataFrame()

        def save_factors_to_db(self, factors_df):
            return True

logger = logging.getLogger(__name__)


class AdjustType(Enum):
    """复权类型枚举"""
    NONE = "none"  # 不复权
    FORWARD = "forward"  # 前复权
    BACKWARD = "backward"  # 后复权


class AdjustMethod(Enum):
    """复权计算方法枚举"""
    FACTOR = "factor"  # 因子法
    PRICE = "price"  # 价格法


class DividendEvent:
    """分红送股事件"""

    def __init__(self, symbol: str, ex_date: str,
                 cash_div: float = 0.0,  # 现金分红
                 shares_div: float = 0.0,  # 送股比例
                 allotment_ratio: float = 0.0,  # 配股比例
                 allotment_price: float = 0.0,  # 配股价
                 split_ratio: float = 1.0):  # 拆股比例
        self.symbol = symbol
        self.ex_date = datetime.strptime(ex_date, '%Y-%m-%d').date() if isinstance(ex_date, str) else ex_date
        self.cash_div = float(cash_div) if cash_div else 0.0
        self.shares_div = float(shares_div) if shares_div else 0.0
        self.allotment_ratio = float(allotment_ratio) if allotment_ratio else 0.0
        self.allotment_price = float(allotment_price) if allotment_price else 0.0
        self.split_ratio = float(split_ratio) if split_ratio else 1.0

        # 计算复权因子
        self.forward_factor = self._calculate_forward_factor()
        self.backward_factor = self._calculate_backward_factor()

    def _calculate_forward_factor(self) -> float:
        """计算前复权因子"""
        # 前复权因子 = 除权前价格 / 除权后价格
        # 考虑现金分红、送股、配股、拆股

        if self.split_ratio != 1.0:
            # 拆股
            return 1.0 / self.split_ratio

        # 计算除权参考价
        # 除权参考价 = (前收盘价 - 现金分红 + 配股价 * 配股比例) / (1 + 送股比例 + 配股比例)

        # 假设前收盘价为1（用于计算因子）
        pre_close = 1.0

        numerator = pre_close - self.cash_div + self.allotment_price * self.allotment_ratio
        denominator = 1.0 + self.shares_div + self.allotment_ratio

        if denominator == 0:
            return 1.0

        ex_ref_price = numerator / denominator

        if ex_ref_price == 0:
            return 1.0

        return pre_close / ex_ref_price

    def _calculate_backward_factor(self) -> float:
        """计算后复权因子"""
        # 后复权因子 = 除权后价格 / 除权前价格
        # 是前复权因子的倒数
        forward_factor = self._calculate_forward_factor()
        if forward_factor == 0:
            return 1.0
        return 1.0 / forward_factor

    def __str__(self):
        return (f"DividendEvent(symbol={self.symbol}, ex_date={self.ex_date}, "
                f"cash={self.cash_div}, shares={self.shares_div}, "
                f"allotment={self.allotment_ratio}@{self.allotment_price})")


class StockAdjustor:
    """股票复权计算器 - 修复版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化复权计算器 - 修复初始化顺序

        Args:
            config_path: 数据库配置文件路径
        """
        self.config_path = config_path
        self.factor_cache = {}  # 缓存复权因子

        # 修复：先初始化数据库连接器
        try:
            self.db_connector = DatabaseConnector(config_path)
            logger.info("数据库连接器初始化成功")
        except Exception as e:
            logger.error(f"数据库连接器初始化失败: {e}")
            self.db_connector = None

        # 修复：然后创建表（如果连接器存在）
        if self.db_connector:
            try:
                self._create_adjustment_table()
            except Exception as e:
                logger.error(f"创建复权因子表失败: {e}")

        # 初始化其他组件
        try:
            self.query_engine = QueryEngine(config_path)
            self.adjustment_manager = AdjustmentFactorManager(config_path)
            logger.info("股票复权计算器初始化完成")
        except Exception as e:
            logger.error(f"复权计算器初始化失败: {e}")
            self.query_engine = None
            self.adjustment_manager = None

    def _create_adjustment_table(self):
        """创建复权因子表 - 修复版"""
        if not self.db_connector:
            logger.error("数据库连接器不可用，无法创建表")
            return

        try:
            # 检查表是否已存在
            check_sql = "SHOW TABLES LIKE 'adjust_factors'"
            result = self.db_connector.execute_query(check_sql)

            if result:
                logger.info("复权因子表已存在")
                return

            sql = """
            CREATE TABLE IF NOT EXISTS adjust_factors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                ex_date DATE NOT NULL,
                cash_div DECIMAL(10, 4),
                shares_div DECIMAL(10, 4),
                allotment_ratio DECIMAL(10, 4),
                allotment_price DECIMAL(10, 4),
                split_ratio DECIMAL(10, 4),
                forward_factor DECIMAL(12, 6),
                backward_factor DECIMAL(12, 6),
                total_factor DECIMAL(12, 6),
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_symbol_ex_date (symbol, ex_date)
            )
            """
            self.db_connector.execute_query(sql)
            logger.info("复权因子表创建成功")

        except Exception as e:
            logger.error(f"创建复权因子表失败: {e}")

    def load_dividend_events(self, symbol: str) -> List[DividendEvent]:
        """
        加载分红送股事件

        Args:
            symbol: 股票代码

        Returns:
            分红事件列表
        """
        events = []

        try:
            # 这里应该是从数据库或API获取分红数据
            # 暂时使用模拟数据
            # TODO: 集成实际的分红数据源

            # 示例事件
            sample_events = [
                {
                    'ex_date': '2023-06-15',
                    'cash_div': 0.5,  # 每股分红0.5元
                    'shares_div': 0.3,  # 10送3股
                    'allotment_ratio': 0.0,
                    'allotment_price': 0.0,
                    'split_ratio': 1.0
                },
                {
                    'ex_date': '2022-06-10',
                    'cash_div': 0.3,
                    'shares_div': 0.0,
                    'allotment_ratio': 0.2,  # 10配2股
                    'allotment_price': 5.0,  # 配股价5元
                    'split_ratio': 1.0
                }
            ]

            for event_data in sample_events:
                event = DividendEvent(
                    symbol=symbol,
                    ex_date=event_data['ex_date'],
                    cash_div=event_data['cash_div'],
                    shares_div=event_data['shares_div'],
                    allotment_ratio=event_data['allotment_ratio'],
                    allotment_price=event_data['allotment_price'],
                    split_ratio=event_data['split_ratio']
                )
                events.append(event)

            logger.info(f"加载 {len(events)} 个分红事件: {symbol}")

        except Exception as e:
            logger.error(f"加载分红事件失败: {symbol}, {e}")

        return events

    def calculate_adjust_factors(self, symbol: str,
                                 events: List[DividendEvent]) -> pd.DataFrame:
        """
        计算复权因子

        Args:
            symbol: 股票代码
            events: 分红事件列表

        Returns:
            复权因子DataFrame
        """

        if not events:
            logger.warning(f"没有分红事件: {symbol}")
            return pd.DataFrame()

        return self.adjustment_manager.fetch_factors(symbol)  # 放在这里对吗

        # 按除权日排序
        events.sort(key=lambda x: x.ex_date, reverse=True)

        factors = []
        cumulative_forward = 1.0
        cumulative_backward = 1.0

        for event in events:
            cumulative_forward *= event.forward_factor
            cumulative_backward *= event.backward_factor

            factor_record = {
                'symbol': symbol,
                'ex_date': event.ex_date,
                'cash_div': event.cash_div,
                'shares_div': event.shares_div,
                'allotment_ratio': event.allotment_ratio,
                'allotment_price': event.allotment_price,
                'split_ratio': event.split_ratio,
                'forward_factor': cumulative_forward,
                'backward_factor': cumulative_backward,
                'total_factor': cumulative_forward  # 总因子用于前复权
            }

            factors.append(factor_record)

        # 缓存因子
        cache_key = f"{symbol}_factors"
        self.factor_cache[cache_key] = factors

        # 保存到数据库
        self._save_factors_to_db(factors)

        df = pd.DataFrame(factors)
        logger.info(f"计算复权因子完成: {symbol}, 共{len(factors)}个因子")

        return df

    def _save_factors_to_db(self, factors: List[Dict]):
        """保存复权因子到数据库"""
        try:
            for factor in factors:
                query = """
                    INSERT INTO adjust_factors 
                    (symbol, ex_date, cash_div, shares_div, allotment_ratio, 
                     allotment_price, split_ratio, forward_factor, backward_factor, total_factor)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    cash_div = VALUES(cash_div),
                    shares_div = VALUES(shares_div),
                    allotment_ratio = VALUES(allotment_ratio),
                    allotment_price = VALUES(allotment_price),
                    split_ratio = VALUES(split_ratio),
                    forward_factor = VALUES(forward_factor),
                    backward_factor = VALUES(backward_factor),
                    total_factor = VALUES(total_factor),
                    updated_time = CURRENT_TIMESTAMP
                """

                params = (
                    factor['symbol'],
                    factor['ex_date'],
                    factor['cash_div'],
                    factor['shares_div'],
                    factor['allotment_ratio'],
                    factor['allotment_price'],
                    factor['split_ratio'],
                    factor['forward_factor'],
                    factor['backward_factor'],
                    factor['total_factor']
                )

                self.db_connector.execute_query(query, params)

            logger.info(f"保存复权因子到数据库: {len(factors)}条")

        except Exception as e:
            logger.error(f"保存复权因子失败: {e}")

    def get_adjust_factors(self, symbol: str,
                           ex_date: str = None) -> pd.DataFrame:
        """
        获取复权因子

        Args:
            symbol: 股票代码
            ex_date: 除权除息日（可选）

        Returns:
            复权因子DataFrame
        """
        try:
            query = """
                SELECT * FROM adjust_factors 
                WHERE symbol = %s
            """
            params = [symbol]

            if ex_date:
                query += " AND ex_date = %s"
                params.append(ex_date)

            query += " ORDER BY ex_date DESC"

            result = self.db_connector.execute_query(query, tuple(params))
            df = pd.DataFrame(result) if result else pd.DataFrame()

            return df

        except Exception as e:
            logger.error(f"获取复权因子失败: {e}")
            return pd.DataFrame()

    def adjust_price(self, df: pd.DataFrame, symbol: str,
                     adjust_type: AdjustType = AdjustType.FORWARD,
                     adjust_method: AdjustMethod = AdjustMethod.FACTOR) -> pd.DataFrame:
        """
        复权价格计算

        Args:
            df: 原始价格DataFrame
            symbol: 股票代码
            adjust_type: 复权类型
            adjust_method: 复权方法

        Returns:
            复权后的DataFrame
        """
        if df.empty:
            logger.warning("空数据框，无法进行复权")
            return df

        if adjust_type == AdjustType.NONE:
            logger.info("不复权处理")
            return df

        # 获取复权因子
        factors_df = self.get_adjust_factors(symbol)
        if factors_df.empty:
            logger.warning(f"没有复权因子: {symbol}")
            return df

        # 创建数据副本
        adjusted_df = df.copy()

        # 确保trade_date是日期类型
        if 'trade_date' in adjusted_df.columns:
            adjusted_df['trade_date'] = pd.to_datetime(adjusted_df['trade_date']).dt.date

        # 根据复权类型处理
        if adjust_type == AdjustType.FORWARD:
            # 前复权：最近的价格不变，历史价格调整
            self._apply_forward_adjustment(adjusted_df, factors_df, adjust_method)
        elif adjust_type == AdjustType.BACKWARD:
            # 后复权：历史价格不变，最近价格调整
            self._apply_backward_adjustment(adjusted_df, factors_df, adjust_method)

        # 添加复权类型标记
        adjusted_df['adjust_type'] = adjust_type.value
        adjusted_df['adjust_method'] = adjust_method.value

        logger.info(f"复权计算完成: {symbol}, {adjust_type.value}, {len(adjusted_df)}条记录")

        return adjusted_df

    def _apply_forward_adjustment(self, df: pd.DataFrame,
                                  factors_df: pd.DataFrame,
                                  method: AdjustMethod):
        """应用前复权"""
        if method == AdjustMethod.FACTOR:
            # 因子法
            for _, factor_row in factors_df.iterrows():
                ex_date = factor_row['ex_date']
                total_factor = factor_row['total_factor']

                if pd.isna(total_factor) or total_factor == 0:
                    continue

                # 对除权日之前的数据应用因子
                mask = df['trade_date'] < ex_date
                price_columns = ['open', 'high', 'low', 'close', 'pre_close']

                for col in price_columns:
                    if col in df.columns:
                        df.loc[mask, col] = df.loc[mask, col] / total_factor

        # 价格法等其他方法可以在这里扩展

    def _apply_backward_adjustment(self, df: pd.DataFrame,
                                   factors_df: pd.DataFrame,
                                   method: AdjustMethod):
        """应用后复权"""
        if method == AdjustMethod.FACTOR:
            # 因子法
            for _, factor_row in factors_df.iterrows():
                ex_date = factor_row['ex_date']
                total_factor = factor_row['total_factor']

                if pd.isna(total_factor) or total_factor == 0:
                    continue

                # 对除权日及之后的数据应用因子
                mask = df['trade_date'] >= ex_date
                price_columns = ['open', 'high', 'low', 'close', 'pre_close']

                for col in price_columns:
                    if col in df.columns:
                        df.loc[mask, col] = df.loc[mask, col] * total_factor

    def adjust_batch(self, symbols: List[str],
                     adjust_type: AdjustType = AdjustType.FORWARD,
                     start_date: str = None,
                     end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        批量复权计算

        Args:
            symbols: 股票代码列表
            adjust_type: 复权类型
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            各股票的复权数据字典
        """
        results = {}

        logger.info(f"开始批量复权: {len(symbols)}只股票, {adjust_type.value}")

        for i, symbol in enumerate(symbols, 1):
            try:
                # 查询原始数据
                df = self.query_engine.query_daily_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    limit=5000  # 限制数量
                )

                if df.empty:
                    logger.warning(f"无数据: {symbol}")
                    continue

                # 计算复权
                adjusted_df = self.adjust_price(df, symbol, adjust_type)

                results[symbol] = adjusted_df

                if i % 10 == 0:
                    logger.info(f"进度: {i}/{len(symbols)}")

            except Exception as e:
                logger.error(f"复权失败: {symbol}, {e}")

        logger.info(f"批量复权完成: 成功{len(results)}/{len(symbols)}")

        return results

    def generate_adjusted_series(self, symbol: str,
                                 start_date: str = '2020-01-01',
                                 end_date: str = None) -> Dict[str, pd.DataFrame]:
        """
        生成各种复权价格序列

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            包含不同复权类型的字典
        """
        # 查询原始数据
        df = self.query_engine.query_daily_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            limit=10000
        )

        if df.empty:
            logger.warning(f"无数据: {symbol}")
            return {}

        results = {
            'none': df.copy(),
            'forward': None,
            'backward': None
        }

        # 计算前复权
        forward_df = self.adjust_price(df.copy(), symbol, AdjustType.FORWARD)
        results['forward'] = forward_df

        # 计算后复权
        backward_df = self.adjust_price(df.copy(), symbol, AdjustType.BACKWARD)
        results['backward'] = backward_df

        # 对比分析
        comparison = self._compare_adjustments(results)
        results['comparison'] = comparison

        return results

    def _compare_adjustments(self, results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """对比不同复权方式"""
        comparison_data = []

        for adj_type, df in results.items():
            if df is not None and not df.empty and 'close' in df.columns:
                latest = df.iloc[0] if not df.empty else None
                earliest = df.iloc[-1] if not df.empty else None

                if latest is not None and earliest is not None:
                    comparison_data.append({
                        'adjust_type': adj_type,
                        'latest_price': latest['close'],
                        'earliest_price': earliest['close'],
                        'total_return': (latest['close'] - earliest['close']) / earliest['close'] * 100,
                        'records_count': len(df)
                    })

        return pd.DataFrame(comparison_data)

    def validate_adjustment(self, symbol: str) -> Dict:
        """验证复权计算的正确性"""
        validation_results = {
            'symbol': symbol,
            'has_factors': False,
            'factor_count': 0,
            'adjustment_test': {},
            'errors': []
        }

        try:
            # 检查复权因子
            factors_df = self.get_adjust_factors(symbol)
            validation_results['has_factors'] = not factors_df.empty
            validation_results['factor_count'] = len(factors_df)

            if not factors_df.empty:
                # 测试复权计算
                test_dates = ['2023-12-01', '2023-06-01', '2023-01-01']

                for test_date in test_dates:
                    test_df = self.query_engine.query_daily_data(
                        symbol=symbol,
                        start_date=test_date,
                        end_date=test_date,
                        limit=1
                    )

                    if not test_df.empty:
                        # 计算不同复权价格
                        forward_df = self.adjust_price(test_df.copy(), symbol, AdjustType.FORWARD)
                        backward_df = self.adjust_price(test_df.copy(), symbol, AdjustType.BACKWARD)

                        validation_results['adjustment_test'][test_date] = {
                            'original': test_df.iloc[0]['close'] if 'close' in test_df.columns else None,
                            'forward': forward_df.iloc[0][
                                'close'] if not forward_df.empty and 'close' in forward_df.columns else None,
                            'backward': backward_df.iloc[0][
                                'close'] if not backward_df.empty and 'close' in backward_df.columns else None
                        }

            logger.info(f"复权验证完成: {symbol}")

        except Exception as e:
            validation_results['errors'].append(str(e))
            logger.error(f"复权验证失败: {symbol}, {e}")

        return validation_results

    def close(self):
        """关闭连接"""
        if self.db_connector:
            self.db_connector.close_all_connections()
            logger.info("复权计算器连接已关闭")


def test_adjustor():
    """测试复权计算器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试复权计算器")
    print("=" * 50)

    adjustor = StockAdjustor()

    try:
        # 1. 获取股票列表
        print("\n📋 1. 获取股票列表")
        stock_df = adjustor.query_engine.get_stock_list()
        if not stock_df.empty:
            test_symbol = stock_df.iloc[0]['symbol']
            test_name = stock_df.iloc[0]['name']
            print(f"   测试股票: {test_symbol} ({test_name})")

            # 2. 加载分红事件
            print("\n💰 2. 加载分红事件")
            events = adjustor.load_dividend_events(test_symbol)
            print(f"   加载到 {len(events)} 个分红事件")
            for event in events:
                print(f"   {event}")

            # 3. 计算复权因子
            print("\n🔢 3. 计算复权因子")
            factors_df = adjustor.calculate_adjust_factors(test_symbol, events)
            if not factors_df.empty:
                print(f"   计算 {len(factors_df)} 个复权因子")
                for _, row in factors_df.iterrows():
                    print(f"   {row['ex_date']}: 前复权因子={row['forward_factor']:.6f}, "
                          f"后复权因子={row['backward_factor']:.6f}")

            # 4. 获取历史数据
            print("\n📈 4. 获取历史数据")
            df = adjustor.query_engine.query_daily_data(
                symbol=test_symbol,
                start_date='2023-01-01',
                end_date='2023-12-31',
                limit=50
            )

            if not df.empty:
                print(f"   获取到 {len(df)} 条历史数据")
                print(f"   日期范围: {df.iloc[-1]['trade_date']} 到 {df.iloc[0]['trade_date']}")

                # 5. 前复权计算
                print("\n⬇️  5. 前复权计算")
                forward_df = adjustor.adjust_price(
                    df.copy(), test_symbol, AdjustType.FORWARD
                )
                if not forward_df.empty:
                    print(f"   前复权完成: {len(forward_df)}条记录")
                    print(f"   前复权价格示例:")
                    for i in range(min(3, len(forward_df))):
                        row = forward_df.iloc[i]
                        print(f"   {row['trade_date']}: 收盘价 {row['close']:.2f}")

                # 6. 后复权计算
                print("\n⬆️  6. 后复权计算")
                backward_df = adjustor.adjust_price(
                    df.copy(), test_symbol, AdjustType.BACKWARD
                )
                if not backward_df.empty:
                    print(f"   后复权完成: {len(backward_df)}条记录")
                    print(f"   后复权价格示例:")
                    for i in range(min(3, len(backward_df))):
                        row = backward_df.iloc[i]
                        print(f"   {row['trade_date']}: 收盘价 {row['close']:.2f}")

                # 7. 对比分析
                print("\n📊 7. 对比分析")
                all_series = adjustor.generate_adjusted_series(
                    test_symbol, start_date='2023-01-01'
                )

                if 'comparison' in all_series and not all_series['comparison'].empty:
                    print("   不同复权方式对比:")
                    for _, row in all_series['comparison'].iterrows():
                        print(f"   {row['adjust_type']}: "
                              f"最新价={row['latest_price']:.2f}, "
                              f"总收益={row['total_return']:.2f}%")

            # 8. 验证复权
            print("\n✅ 8. 验证复权计算")
            validation = adjustor.validate_adjustment(test_symbol)
            print(f"   验证结果: 有因子={validation['has_factors']}, "
                  f"数量={validation['factor_count']}")

            if validation['adjustment_test']:
                for date, prices in validation['adjustment_test'].items():
                    print(f"   {date}: 原始={prices['original']:.2f}, "
                          f"前复权={prices['forward']:.2f if prices['forward'] else 'N/A'}, "
                          f"后复权={prices['backward']:.2f if prices['backward'] else 'N/A'}")

        print("\n🎉 复权计算器测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        adjustor.close()


if __name__ == "__main__":
    test_adjustor()