-- 方法1: 重命名change列为其他名称
ALTER TABLE stock_daily_data CHANGE COLUMN `change` price_change DECIMAL(10,4);

-- 方法2: 创建视图使用别名
CREATE OR REPLACE VIEW daily_data_view AS
SELECT
    trade_date, symbol,
    open, high, low, close,
    volume, amount, pct_change,
    `change` as price_change,  -- 使用别名
    pre_close, turnover_rate
FROM stock_daily_data;

-- 方法3: 直接查询使用反引号
SELECT trade_date, symbol, `open`, `high`, `low`, `close`, `change` FROM stock_daily_data;