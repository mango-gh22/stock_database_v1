-- 股票数据库表结构
-- 创建时间：2024年
-- 版本：v1.0

-- 切换数据库
USE stock_database;

-- 1. 股票基本信息表
CREATE TABLE IF NOT EXISTS stock_basic_info (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码（如：000001.SZ）',
    ts_code VARCHAR(20) COMMENT 'TS代码',
    name VARCHAR(50) NOT NULL COMMENT '股票名称',
    area VARCHAR(50) COMMENT '地域',
    industry VARCHAR(100) COMMENT '所属行业',
    market VARCHAR(20) COMMENT '市场类型（主板/创业板/科创板）',
    list_date DATE COMMENT '上市日期',
    fullname VARCHAR(100) COMMENT '公司全称',
    enname VARCHAR(200) COMMENT '英文全称',
    cnspell VARCHAR(50) COMMENT '拼音缩写',
    exchange VARCHAR(10) COMMENT '交易所代码（SSE/SZSE）',
    curr_type VARCHAR(10) COMMENT '交易货币',
    list_status VARCHAR(5) COMMENT '上市状态（L上市/D退市）',
    is_hs VARCHAR(5) COMMENT '是否沪深港通标的（N否/H沪股通/S深股通）',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    UNIQUE KEY uk_symbol (symbol),
    INDEX idx_ts_code (ts_code),
    INDEX idx_name (name),
    INDEX idx_industry (industry),
    INDEX idx_list_date (list_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票基本信息表';

-- 2. 股票日线数据表（分区表设计）
CREATE TABLE IF NOT EXISTS stock_daily_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    trade_date DATE NOT NULL COMMENT '交易日期',

    -- 价格数据
    open_price DECIMAL(10, 3) COMMENT '开盘价',
    close_price DECIMAL(10, 3) COMMENT '收盘价',
    high_price DECIMAL(10, 3) COMMENT '最高价',
    low_price DECIMAL(10, 3) COMMENT '最低价',
    pre_close_price DECIMAL(10, 3) COMMENT '昨收价',

    -- 成交量/额
    volume BIGINT COMMENT '成交量（手）',
    amount DECIMAL(20, 3) COMMENT '成交额（千元）',

    -- 涨跌幅
    pct_change DECIMAL(10, 4) COMMENT '涨跌幅（百分比）',
    change_amount DECIMAL(10, 3) COMMENT '涨跌额',

    -- 换手率等指标
    turnover_rate DECIMAL(10, 4) COMMENT '换手率（百分比）',
    turnover_rate_f DECIMAL(10, 4) COMMENT '流通换手率',
    volume_ratio DECIMAL(10, 3) COMMENT '量比',

    -- 均线数据
    ma5 DECIMAL(10, 3) COMMENT '5日均价',
    ma10 DECIMAL(10, 3) COMMENT '10日均价',
    ma20 DECIMAL(10, 3) COMMENT '20日均价',
    ma30 DECIMAL(10, 3) COMMENT '30日均价',
    ma60 DECIMAL(10, 3) COMMENT '60日均价',
    ma120 DECIMAL(10, 3) COMMENT '120日均价',
    ma250 DECIMAL(10, 3) COMMENT '250日均价',

    -- 振幅
    amplitude DECIMAL(10, 4) COMMENT '振幅（百分比）',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 唯一约束和索引
    UNIQUE KEY uk_symbol_date (symbol, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_symbol (symbol),
    INDEX idx_date_symbol (trade_date, symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票日线数据表'
PARTITION BY RANGE (YEAR(trade_date)) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 3. 股票分钟线数据表（分区表设计）
CREATE TABLE IF NOT EXISTS stock_minute_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    trade_time DATETIME NOT NULL COMMENT '交易时间',
    trade_date DATE NOT NULL COMMENT '交易日期（冗余，方便查询）',

    -- 价格数据
    open_price DECIMAL(10, 3) COMMENT '开盘价',
    close_price DECIMAL(10, 3) COMMENT '收盘价',
    high_price DECIMAL(10, 3) COMMENT '最高价',
    low_price DECIMAL(10, 3) COMMENT '最低价',

    -- 成交量/额
    volume BIGINT COMMENT '成交量（手）',
    amount DECIMAL(20, 3) COMMENT '成交额（千元）',

    -- 分钟线类型（1分钟/5分钟/15分钟/30分钟/60分钟）
    freq VARCHAR(10) NOT NULL DEFAULT '1min' COMMENT '数据频率',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',

    -- 唯一约束和索引
    UNIQUE KEY uk_symbol_time_freq (symbol, trade_time, freq),
    INDEX idx_symbol_freq (symbol, freq),
    INDEX idx_trade_date_freq (trade_date, freq),
    INDEX idx_trade_time (trade_time),
    INDEX idx_symbol_date_freq (symbol, trade_date, freq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票分钟线数据表'
PARTITION BY RANGE (TO_DAYS(trade_date)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p202403 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p202404 VALUES LESS THAN (TO_DAYS('2024-05-01')),
    PARTITION p202405 VALUES LESS THAN (TO_DAYS('2024-06-01')),
    PARTITION p202406 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p202407 VALUES LESS THAN (TO_DAYS('2024-08-01')),
    PARTITION p202408 VALUES LESS THAN (TO_DAYS('2024-09-01')),
    PARTITION p202409 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p202410 VALUES LESS THAN (TO_DAYS('2024-11-01')),
    PARTITION p202411 VALUES LESS THAN (TO_DAYS('2024-12-01')),
    PARTITION p202412 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 4. 指数信息表
CREATE TABLE IF NOT EXISTS index_info (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    index_code VARCHAR(20) NOT NULL COMMENT '指数代码',
    index_name VARCHAR(100) NOT NULL COMMENT '指数名称',
    index_name_en VARCHAR(200) COMMENT '指数英文名称',
    publisher VARCHAR(100) COMMENT '发布机构',
    index_type VARCHAR(50) COMMENT '指数类型',
    base_date DATE COMMENT '基日',
    base_point DECIMAL(10, 2) COMMENT '基点',
    website VARCHAR(500) COMMENT '官方网站',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    UNIQUE KEY uk_index_code (index_code),
    INDEX idx_index_name (index_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='指数信息表';

-- 5. 股票-指数成分关联表
CREATE TABLE IF NOT EXISTS stock_index_constituent (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    index_code VARCHAR(20) NOT NULL COMMENT '指数代码',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    weight DECIMAL(10, 6) COMMENT '权重（百分比）',
    start_date DATE NOT NULL COMMENT '纳入日期',
    end_date DATE COMMENT '剔除日期',
    is_current TINYINT(1) DEFAULT 1 COMMENT '是否当前成分股（1是，0否）',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 唯一约束和索引
    UNIQUE KEY uk_index_symbol_date (index_code, symbol, start_date),
    INDEX idx_index_code (index_code),
    INDEX idx_symbol (symbol),
    INDEX idx_is_current (is_current),
    INDEX idx_index_current (index_code, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票-指数成分关联表';

-- 6. 数据更新日志表
CREATE TABLE IF NOT EXISTS data_update_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    data_type VARCHAR(50) NOT NULL COMMENT '数据类型（daily/minute/basic）',
    symbol VARCHAR(20) COMMENT '股票代码（如为全市场更新则为空）',
    start_date DATE COMMENT '开始日期',
    end_date DATE COMMENT '结束日期',
    rows_affected INT COMMENT '影响行数',
    status VARCHAR(20) NOT NULL COMMENT '状态（success/failed）',
    error_message TEXT COMMENT '错误信息',

    -- 执行信息
    execution_time DECIMAL(10, 3) COMMENT '执行时间（秒）',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',

    -- 索引
    INDEX idx_data_type (data_type),
    INDEX idx_symbol (symbol),
    INDEX idx_created_time (created_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='数据更新日志表';

-- 7. 股票财务指标表（基础结构）
CREATE TABLE IF NOT EXISTS stock_financial_indicators (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    report_date DATE NOT NULL COMMENT '报告期',
    report_type VARCHAR(20) COMMENT '报告类型（Q1/Q2/Q3/Annual）',

    -- 基本财务数据
    total_assets DECIMAL(20, 3) COMMENT '总资产',
    total_liabilities DECIMAL(20, 3) COMMENT '总负债',
    net_assets DECIMAL(20, 3) COMMENT '净资产',
    operating_income DECIMAL(20, 3) COMMENT '营业收入',
    net_profit DECIMAL(20, 3) COMMENT '净利润',
    eps DECIMAL(10, 4) COMMENT '每股收益',
    bvps DECIMAL(10, 4) COMMENT '每股净资产',
    roe DECIMAL(10, 4) COMMENT '净资产收益率',
    roa DECIMAL(10, 4) COMMENT '总资产收益率',

    -- 估值指标
    pe_ratio DECIMAL(10, 4) COMMENT '市盈率',
    pb_ratio DECIMAL(10, 4) COMMENT '市净率',
    ps_ratio DECIMAL(10, 4) COMMENT '市销率',

    -- 审计字段
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 唯一约束和索引
    UNIQUE KEY uk_symbol_report_date (symbol, report_date, report_type),
    INDEX idx_symbol (symbol),
    INDEX idx_report_date (report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票财务指标表';