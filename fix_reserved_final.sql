
    USE stock_database;

    -- 1. 先查看表结构
    DESCRIBE stock_daily_data;

    -- 2. 创建智能视图
    DROP VIEW IF EXISTS v_daily_data;

    -- 创建通用视图，使用COALESCE处理不同列名
    CREATE VIEW v_daily_data AS
    SELECT 
        trade_date,
        symbol,
        `open`,
        `high`,
        `low`,
        `close`,
        volume,
        amount,
        pct_change,
        COALESCE(price_change, `change`) as price_change,
        pre_close,
        turnover_rate,
        amplitude
    FROM stock_daily_data;

    -- 3. 测试
    SELECT '视图创建成功' as status;
    SELECT COUNT(*) FROM v_daily_data;

    -- 4. 显示视图结构
    DESCRIBE v_daily_data;
    