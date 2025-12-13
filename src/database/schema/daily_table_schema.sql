-- src/database/schema/daily_table_schema.sql
-- 创建完整的表结构（推荐）
CREATE TABLE IF NOT EXISTS stock_daily_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    trade_date VARCHAR(10) NOT NULL COMMENT '交易日期(YYYYMMDD)',

    -- 基础价格数据
    open_price DECIMAL(10, 4) COMMENT '开盘价',
    high_price DECIMAL(10, 4) COMMENT '最高价',
    low_price DECIMAL(10, 4) COMMENT '最低价',
    close_price DECIMAL(10, 4) COMMENT '收盘价',
    pre_close_price DECIMAL(10, 4) COMMENT '前收盘价',
    change_percent DECIMAL(10, 4) COMMENT '涨跌幅(%)',
    change DECIMAL(10, 4) COMMENT '涨跌额',
    volume BIGINT COMMENT '成交量(手)',
    amount DECIMAL(20, 4) COMMENT '成交额(万元)',
    amplitude DECIMAL(10, 4) COMMENT '振幅(%)',

    -- 基础技术指标 (移动平均线)
    ma5 DECIMAL(10, 4) COMMENT '5日均线',
    ma10 DECIMAL(10, 4) COMMENT '10日均线',
    ma20 DECIMAL(10, 4) COMMENT '20日均线',
    ma30 DECIMAL(10, 4) COMMENT '30日均线',
    ma60 DECIMAL(10, 4) COMMENT '60日均线',
    ma120 DECIMAL(10, 4) COMMENT '120日均线',
    ma250 DECIMAL(10, 4) COMMENT '250日均线',

    -- 成交量均线
    volume_ma5 BIGINT COMMENT '5日成交量均线',
    volume_ma10 BIGINT COMMENT '10日成交量均线',
    volume_ma20 BIGINT COMMENT '20日成交量均线',

    -- 高级技术指标
    rsi DECIMAL(10, 4) COMMENT 'RSI相对强弱指标',
    bb_middle DECIMAL(10, 4) COMMENT '布林带中轨',
    bb_upper DECIMAL(10, 4) COMMENT '布林带上轨',
    bb_lower DECIMAL(10, 4) COMMENT '布林带下轨',
    volatility_20d DECIMAL(10, 4) COMMENT '20日波动率',

    -- 财务比率指标
    turnover_rate DECIMAL(10, 4) COMMENT '换手率(%)',
    turnover_rate_f DECIMAL(10, 4) COMMENT '换手率(自由流通股)',
    volume_ratio DECIMAL(10, 4) COMMENT '量比',
    pe DECIMAL(10, 4) COMMENT '市盈率',
    pe_ttm DECIMAL(10, 4) COMMENT '市盈率(TTM)',
    pb DECIMAL(10, 4) COMMENT '市净率',
    ps DECIMAL(10, 4) COMMENT '市销率',
    ps_ttm DECIMAL(10, 4) COMMENT '市销率(TTM)',
    dv_ratio DECIMAL(10, 4) COMMENT '股息率',
    dv_ttm DECIMAL(10, 4) COMMENT '股息率(TTM)',

    -- 市值数据
    total_share DECIMAL(20, 4) COMMENT '总股本',
    float_share DECIMAL(20, 4) COMMENT '流通股本',
    free_share DECIMAL(20, 4) COMMENT '自由流通股本',
    total_mv DECIMAL(20, 4) COMMENT '总市值',
    circ_mv DECIMAL(20, 4) COMMENT '流通市值',

    -- 元数据
    data_source VARCHAR(50) DEFAULT 'baostock' COMMENT '数据源',
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '处理时间',
    quality_grade VARCHAR(20) DEFAULT 'unknown' COMMENT '质量等级',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    UNIQUE KEY idx_unique_symbol_date (symbol, trade_date),
    KEY idx_trade_date (trade_date),
    KEY idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票日线数据表';