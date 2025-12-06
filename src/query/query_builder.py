# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\query_builder.py
# File Name: query_builder
# @ Author: mango-gh22
# @ Date：2025/12/6 16:29

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询条件构建器 - v0.4.0
作者：stock_database_v1项目
日期：2024-12-06
功能：构建复杂的SQL查询条件
"""

from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger(__name__)


class QueryBuilder:
    """查询条件构建器"""

    def __init__(self, table_name: str = None):
        """
        初始化查询构建器

        Args:
            table_name: 表名
        """
        self.table_name = table_name
        self.select_fields = []
        self.conditions = []
        self.params = []
        self.joins = []
        self.group_by = []
        self.order_by = []
        self.limit_value = None
        self.offset_value = None
        self.distinct = False
        self.having_conditions = []
        self.having_params = []

    def reset(self) -> 'QueryBuilder':
        """重置构建器状态"""
        self.select_fields = []
        self.conditions = []
        self.params = []
        self.joins = []
        self.group_by = []
        self.order_by = []
        self.limit_value = None
        self.offset_value = None
        self.distinct = False
        self.having_conditions = []
        self.having_params = []
        return self

    def select(self, fields: Union[str, List[str]]) -> 'QueryBuilder':
        """
        设置查询字段

        Args:
            fields: 字段名或字段列表

        Returns:
            self
        """
        if isinstance(fields, str):
            self.select_fields = [fields]
        elif isinstance(fields, list):
            self.select_fields = fields
        else:
            self.select_fields = ['*']
        return self

    def add_symbol_filter(self, symbol: Union[str, List[str]],
                          symbol_col: str = 'symbol') -> 'QueryBuilder':
        """
        添加股票代码过滤条件

        Args:
            symbol: 单个股票代码或列表
            symbol_col: 股票代码列名

        Returns:
            self
        """
        if not symbol:
            return self

        if isinstance(symbol, str) and symbol.strip():
            self.conditions.append(f"{symbol_col} = %s")
            self.params.append(symbol.strip())
        elif isinstance(symbol, list) and symbol:
            valid_symbols = [s.strip() for s in symbol if s and str(s).strip()]
            if valid_symbols:
                placeholders = ', '.join(['%s'] * len(valid_symbols))
                self.conditions.append(f"{symbol_col} IN ({placeholders})")
                self.params.extend(valid_symbols)

        logger.debug(f"添加股票代码过滤: {symbol}, 条件数: {len(self.conditions)}")
        return self

    def add_date_filter(self,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        date_col: str = 'trade_date') -> 'QueryBuilder':
        """
        添加日期范围过滤条件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            date_col: 日期列名

        Returns:
            self
        """
        if start_date:
            self.conditions.append(f"{date_col} >= %s")
            self.params.append(start_date)
            logger.debug(f"添加开始日期过滤: {start_date}")

        if end_date:
            self.conditions.append(f"{date_col} <= %s")
            self.params.append(end_date)
            logger.debug(f"添加结束日期过滤: {end_date}")

        return self

    def add_exchange_filter(self, exchange: str) -> 'QueryBuilder':
        """
        添加交易所过滤条件

        Args:
            exchange: 交易所代码 (SZ/SH)

        Returns:
            self
        """
        if exchange and exchange.strip():
            self.conditions.append("exchange = %s")
            self.params.append(exchange.strip().upper())
            logger.debug(f"添加交易所过滤: {exchange}")

        return self

    def add_industry_filter(self, industry: str,
                            exact_match: bool = False) -> 'QueryBuilder':
        """
        添加行业过滤条件

        Args:
            industry: 行业关键词
            exact_match: 是否精确匹配

        Returns:
            self
        """
        if not industry or not industry.strip():
            return self

        industry = industry.strip()

        if exact_match:
            self.conditions.append("industry = %s")
            self.params.append(industry)
            logger.debug(f"添加精确行业过滤: {industry}")
        else:
            self.conditions.append("industry LIKE %s")
            self.params.append(f"%{industry}%")
            logger.debug(f"添加模糊行业过滤: {industry}")

        return self

    def add_price_filter(self,
                         min_price: Optional[float] = None,
                         max_price: Optional[float] = None,
                         price_col: str = 'close') -> 'QueryBuilder':
        """
        添加价格过滤条件

        Args:
            min_price: 最低价格
            max_price: 最高价格
            price_col: 价格列名

        Returns:
            self
        """
        if min_price is not None:
            self.conditions.append(f"{price_col} >= %s")
            self.params.append(min_price)
            logger.debug(f"添加最低价格过滤: {min_price}")

        if max_price is not None:
            self.conditions.append(f"{price_col} <= %s")
            self.params.append(max_price)
            logger.debug(f"添加最高价格过滤: {max_price}")

        return self

    def add_volume_filter(self,
                          min_volume: Optional[float] = None,
                          max_volume: Optional[float] = None,
                          volume_col: str = 'volume') -> 'QueryBuilder':
        """
        添加成交量过滤条件

        Args:
            min_volume: 最低成交量
            max_volume: 最高成交量
            volume_col: 成交量列名

        Returns:
            self
        """
        if min_volume is not None:
            self.conditions.append(f"{volume_col} >= %s")
            self.params.append(min_volume)
            logger.debug(f"添加最低成交量过滤: {min_volume}")

        if max_volume is not None:
            self.conditions.append(f"{volume_col} <= %s")
            self.params.append(max_volume)
            logger.debug(f"添加最高成交量过滤: {max_volume}")

        return self

    def add_change_filter(self,
                          min_change: Optional[float] = None,
                          max_change: Optional[float] = None,
                          pct_change: bool = True) -> 'QueryBuilder':
        """
        添加涨跌幅过滤条件

        Args:
            min_change: 最小涨跌幅
            max_change: 最大涨跌幅
            pct_change: 是否百分比变化（True为pct_change，False为change）

        Returns:
            self
        """
        col_name = 'pct_change' if pct_change else 'change'

        if min_change is not None:
            self.conditions.append(f"{col_name} >= %s")
            self.params.append(min_change)
            logger.debug(f"添加最小涨跌幅过滤: {min_change}")

        if max_change is not None:
            self.conditions.append(f"{col_name} <= %s")
            self.params.append(max_change)
            logger.debug(f"添加最大涨跌幅过滤: {max_change}")

        return self

    def add_custom_condition(self, condition: str, *args) -> 'QueryBuilder':
        """
        添加自定义条件

        Args:
            condition: SQL条件字符串
            *args: 条件参数

        Returns:
            self
        """
        if condition and condition.strip():
            self.conditions.append(condition)
            if args:
                self.params.extend(args)
            logger.debug(f"添加自定义条件: {condition}")

        return self

    def join_table(self,
                   table: str,
                   on_condition: str,
                   join_type: str = 'INNER') -> 'QueryBuilder':
        """
        添加表连接

        Args:
            table: 连接的表名
            on_condition: ON条件
            join_type: 连接类型 (INNER, LEFT, RIGHT, FULL)

        Returns:
            self
        """
        join_str = f"{join_type.upper()} JOIN {table} ON {on_condition}"
        self.joins.append(join_str)
        logger.debug(f"添加表连接: {join_str}")
        return self

    def group_by_field(self, field: str) -> 'QueryBuilder':
        """
        添加分组字段

        Args:
            field: 分组字段名

        Returns:
            self
        """
        if field and field.strip():
            self.group_by.append(field)
            logger.debug(f"添加分组字段: {field}")

        return self

    def add_having_condition(self, condition: str, *args) -> 'QueryBuilder':
        """
        添加HAVING条件

        Args:
            condition: HAVING条件
            *args: 条件参数

        Returns:
            self
        """
        if condition and condition.strip():
            self.having_conditions.append(condition)
            if args:
                self.having_params.extend(args)
            logger.debug(f"添加HAVING条件: {condition}")

        return self

    def order_by_field(self,
                       field: str,
                       ascending: bool = True,
                       nulls_last: bool = False) -> 'QueryBuilder':
        """
        添加排序字段

        Args:
            field: 排序字段名
            ascending: 是否升序
            nulls_last: NULL值是否排在最后

        Returns:
            self
        """
        if not field or not field.strip():
            return self

        direction = "ASC" if ascending else "DESC"
        order_str = f"{field} {direction}"

        if nulls_last:
            order_str = f"{order_str} NULLS LAST"

        self.order_by.append(order_str)
        logger.debug(f"添加排序: {order_str}")
        return self

    def limit(self, limit: int) -> 'QueryBuilder':
        """
        设置返回条数限制

        Args:
            limit: 最大返回条数

        Returns:
            self
        """
        if limit and limit > 0:
            self.limit_value = limit
            logger.debug(f"设置限制条数: {limit}")

        return self

    def offset(self, offset: int) -> 'QueryBuilder':
        """
        设置偏移量

        Args:
            offset: 偏移量

        Returns:
            self
        """
        if offset and offset >= 0:
            self.offset_value = offset
            logger.debug(f"设置偏移量: {offset}")

        return self

    def distinct_results(self) -> 'QueryBuilder':
        """设置去重查询"""
        self.distinct = True
        logger.debug("设置去重查询")
        return self

    def build_select_query(self) -> Tuple[str, List[Any]]:
        """
        构建SELECT查询语句

        Returns:
            Tuple: (SQL语句, 参数列表)
        """
        # 构建SELECT部分
        distinct_str = "DISTINCT " if self.distinct else ""
        if not self.select_fields:
            fields_str = "*"
        else:
            fields_str = ", ".join(self.select_fields)

        # 构建FROM部分
        if not self.table_name:
            raise ValueError("未指定表名")

        from_str = f"FROM {self.table_name}"

        # 构建JOIN部分
        join_str = ""
        if self.joins:
            join_str = " " + " ".join(self.joins)

        # 构建WHERE部分
        where_str = ""
        if self.conditions:
            where_str = "WHERE " + " AND ".join(self.conditions)

        # 构建GROUP BY部分
        group_by_str = ""
        if self.group_by:
            group_by_str = "GROUP BY " + ", ".join(self.group_by)

        # 构建HAVING部分
        having_str = ""
        if self.having_conditions:
            having_str = "HAVING " + " AND ".join(self.having_conditions)
            # 合并HAVING参数到主参数列表
            self.params.extend(self.having_params)

        # 构建ORDER BY部分
        order_by_str = ""
        if self.order_by:
            order_by_str = "ORDER BY " + ", ".join(self.order_by)

        # 构建LIMIT和OFFSET部分
        limit_offset_str = ""
        if self.limit_value is not None:
            limit_offset_str = f"LIMIT {self.limit_value}"
            if self.offset_value is not None:
                limit_offset_str += f" OFFSET {self.offset_value}"
        elif self.offset_value is not None:
            limit_offset_str = f"OFFSET {self.offset_value}"

        # 组合完整的SQL语句
        sql_parts = [
            f"SELECT {distinct_str}{fields_str}",
            from_str,
            join_str,
            where_str,
            group_by_str,
            having_str,
            order_by_str,
            limit_offset_str
        ]

        # 过滤空字符串并组合
        sql = " ".join(part for part in sql_parts if part)

        logger.debug(f"构建的SQL语句: {sql}")
        logger.debug(f"参数列表: {self.params}")

        return sql, self.params

    def build_count_query(self) -> Tuple[str, List[Any]]:
        """
        构建COUNT查询语句

        Returns:
            Tuple: (SQL语句, 参数列表)
        """
        # 保存原始设置
        original_select = self.select_fields.copy()
        original_order = self.order_by.copy()
        original_limit = self.limit_value
        original_offset = self.offset_value
        original_distinct = self.distinct

        # 构建COUNT查询
        self.select_fields = ["COUNT(*)"]
        self.order_by = []
        self.limit_value = None
        self.offset_value = None
        self.distinct = False

        sql, params = self.build_select_query()

        # 恢复原始设置
        self.select_fields = original_select
        self.order_by = original_order
        self.limit_value = original_limit
        self.offset_value = original_offset
        self.distinct = original_distinct

        return sql, params

    def build_exists_query(self) -> Tuple[str, List[Any]]:
        """
        构建EXISTS查询语句

        Returns:
            Tuple: (SQL语句, 参数列表)
        """
        # 构建基本查询
        sql, params = self.build_select_query()

        # 修改为EXISTS查询
        exists_sql = f"SELECT EXISTS({sql})"

        logger.debug(f"构建的EXISTS语句: {exists_sql}")
        return exists_sql, params

    def get_params(self) -> List[Any]:
        """获取参数列表"""
        return self.params.copy()

    def get_conditions(self) -> List[str]:
        """获取条件列表"""
        return self.conditions.copy()


# ==================== 查询构建器工厂函数 ====================

def create_daily_data_query(symbol: Union[str, List[str]] = None,
                            start_date: str = None,
                            end_date: str = None,
                            fields: List[str] = None,
                            limit: int = None,
                            order_by: str = 'trade_date',
                            ascending: bool = False) -> Tuple[str, List]:
    """
    创建日线数据查询的便捷函数

    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        fields: 查询字段
        limit: 限制条数
        order_by: 排序字段
        ascending: 是否升序

    Returns:
        Tuple: (SQL语句, 参数列表)
    """
    builder = QueryBuilder('daily_data')

    # 设置查询字段
    if fields:
        builder.select(fields)
    else:
        builder.select(['*'])

    # 添加过滤条件
    builder.add_symbol_filter(symbol)
    builder.add_date_filter(start_date, end_date)

    # 排序
    if order_by:
        builder.order_by_field(order_by, ascending)

    # 限制
    if limit:
        builder.limit(limit)

    return builder.build_select_query()


def create_stock_basic_query(symbol: Union[str, List[str]] = None,
                             exchange: str = None,
                             industry: str = None,
                             is_active: bool = True) -> Tuple[str, List]:
    """
    创建股票基本信息查询的便捷函数

    Args:
        symbol: 股票代码
        exchange: 交易所
        industry: 行业
        is_active: 是否活跃

    Returns:
        Tuple: (SQL语句, 参数列表)
    """
    builder = QueryBuilder('stock_basic')

    builder.select(['*'])

    # 添加过滤条件
    builder.add_symbol_filter(symbol)
    if exchange:
        builder.add_exchange_filter(exchange)
    if industry:
        builder.add_industry_filter(industry, exact_match=False)

    # 活跃状态过滤
    if is_active is not None:
        builder.add_custom_condition('is_active = %s', is_active)

    # 按代码排序
    builder.order_by_field('symbol', ascending=True)

    return builder.build_select_query()


def create_multi_table_query() -> QueryBuilder:
    """
    创建多表联合查询的构建器

    Returns:
        QueryBuilder: 配置好的查询构建器
    """
    builder = QueryBuilder('daily_data d')

    # 连接股票基本信息表
    builder.join_table('stock_basic s', 'd.symbol = s.symbol', 'INNER')

    # 选择常用字段
    builder.select([
        'd.symbol',
        's.name',
        's.industry',
        'd.trade_date',
        'd.open',
        'd.high',
        'd.low',
        'd.close',
        'd.volume',
        'd.pct_change'
    ])

    return builder


# ==================== 使用示例 ====================

if __name__ == "__main__":
    """测试查询构建器"""

    print("=" * 60)
    print("🔧 查询构建器测试")
    print("=" * 60)

    # 示例1: 简单的日线数据查询
    print("\n1. 简单日线查询:")
    builder1 = QueryBuilder('daily_data')
    builder1.select(['trade_date', 'open', 'high', 'low', 'close', 'volume'])
    builder1.add_symbol_filter('000001.SZ')
    builder1.add_date_filter('2024-01-01', '2024-01-31')
    builder1.order_by_field('trade_date', ascending=False)
    builder1.limit(10)

    sql1, params1 = builder1.build_select_query()
    print(f"SQL: {sql1}")
    print(f"参数: {params1}")

    # 示例2: 使用便捷函数
    print("\n2. 使用便捷函数:")
    sql2, params2 = create_daily_data_query(
        symbol='000001.SZ',
        start_date='2024-01-01',
        end_date='2024-01-31',
        limit=5
    )
    print(f"SQL: {sql2}")
    print(f"参数: {params2}")

    # 示例3: 股票基本信息查询
    print("\n3. 股票基本信息查询:")
    sql3, params3 = create_stock_basic_query(
        exchange='SZ',
        industry='银行'
    )
    print(f"SQL: {sql3}")
    print(f"参数: {params3}")

    # 示例4: 多表联合查询
    print("\n4. 多表联合查询:")
    builder4 = create_multi_table_query()
    builder4.add_symbol_filter(['000001.SZ', '000002.SZ'])
    builder4.add_date_filter('2024-01-01')
    builder4.order_by_field('d.trade_date', ascending=False)
    builder4.limit(5)

    sql4, params4 = builder4.build_select_query()
    print(f"SQL: {sql4}")
    print(f"参数: {params4}")

    # 示例5: COUNT查询
    print("\n5. COUNT查询:")
    builder5 = QueryBuilder('daily_data')
    builder5.add_symbol_filter('000001.SZ')
    builder5.add_date_filter('2024-01-01')

    count_sql, count_params = builder5.build_count_query()
    print(f"COUNT SQL: {count_sql}")
    print(f"参数: {count_params}")

    print("\n✅ 查询构建器测试完成!")