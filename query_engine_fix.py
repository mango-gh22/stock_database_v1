
# query_engine.py 快速修复补丁
# 用于修复MySQL保留关键字'change'的问题

def get_daily_data_fixed(self,
                        symbol: str = None,
                        start_date: str = None,
                        end_date: str = None,
                        limit: int = None) -> pd.DataFrame:
    """
    修复版的日线数据查询（处理保留关键字）
    """
    # 使用安全的列名，避免保留关键字
    query = """
    SELECT 
        trade_date, symbol, 
        open, high, low, close,
        volume, amount,
        pct_change
    FROM stock_daily_data
    WHERE 1=1
    """
    params = {}

    if symbol:
        query += " AND symbol = :symbol"
        params['symbol'] = symbol
    if start_date:
        query += " AND trade_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND trade_date <= :end_date"
        params['end_date'] = end_date

    query += " ORDER BY trade_date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        df = pd.read_sql_query(text(query), self.engine, params=params)
        if not df.empty and 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')
        self.logger.info(f"查询日线数据，返回{len(df)}条记录")
        return df
    except Exception as e:
        self.logger.error(f"查询日线数据失败: {e}")
        return pd.DataFrame()
