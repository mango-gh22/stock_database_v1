-- scripts/schema/create_tables_fixed.sql
USE stock_database;

-- 1. 股票基本信息表（去掉中文注释避免编码问题）
CREATE TABLE IF NOT EXISTS stock_basic_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    ts_code VARCHAR(20),
    name VARCHAR(50) NOT NULL,
    area VARCHAR(50),
    industry VARCHAR(100),
    market VARCHAR(20),
    list_date DATE,
    fullname VARCHAR(100),
    enname VARCHAR(200),
    cnspell VARCHAR(50),
    exchange VARCHAR(10),
    curr_type VARCHAR(10),
    list_status VARCHAR(5),
    is_hs VARCHAR(5),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol (symbol),
    INDEX idx_ts_code (ts_code),
    INDEX idx_name (name),
    INDEX idx_industry (industry),
    INDEX idx_list_date (list_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 股票日线数据表（简化版，先不分区）
CREATE TABLE IF NOT EXISTS stock_daily_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(10, 3),
    close_price DECIMAL(10, 3),
    high_price DECIMAL(10, 3),
    low_price DECIMAL(10, 3),
    pre_close_price DECIMAL(10, 3),
    volume BIGINT,
    amount DECIMAL(20, 3),
    pct_change DECIMAL(10, 4),
    change_amount DECIMAL(10, 3),
    turnover_rate DECIMAL(10, 4),
    turnover_rate_f DECIMAL(10, 4),
    volume_ratio DECIMAL(10, 3),
    ma5 DECIMAL(10, 3),
    ma10 DECIMAL(10, 3),
    ma20 DECIMAL(10, 3),
    ma30 DECIMAL(10, 3),
    ma60 DECIMAL(10, 3),
    ma120 DECIMAL(10, 3),
    ma250 DECIMAL(10, 3),
    amplitude DECIMAL(10, 4),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_date (symbol, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 股票分钟线数据表（简化版）
CREATE TABLE IF NOT EXISTS stock_minute_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trade_time DATETIME NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(10, 3),
    close_price DECIMAL(10, 3),
    high_price DECIMAL(10, 3),
    low_price DECIMAL(10, 3),
    volume BIGINT,
    amount DECIMAL(20, 3),
    freq VARCHAR(10) NOT NULL DEFAULT '1min',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_time_freq (symbol, trade_time, freq),
    INDEX idx_symbol_freq (symbol, freq),
    INDEX idx_trade_date_freq (trade_date, freq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 指数信息表
CREATE TABLE IF NOT EXISTS index_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    index_code VARCHAR(20) NOT NULL,
    index_name VARCHAR(100) NOT NULL,
    index_name_en VARCHAR(200),
    publisher VARCHAR(100),
    index_type VARCHAR(50),
    base_date DATE,
    base_point DECIMAL(10, 2),
    website VARCHAR(500),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_index_code (index_code),
    INDEX idx_index_name (index_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. 股票-指数成分关联表
CREATE TABLE IF NOT EXISTS stock_index_constituent (
    id INT AUTO_INCREMENT PRIMARY KEY,
    index_code VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    weight DECIMAL(10, 6),
    start_date DATE NOT NULL,
    end_date DATE,
    is_current TINYINT(1) DEFAULT 1,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_index_symbol_date (index_code, symbol, start_date),
    INDEX idx_index_code (index_code),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. 数据更新日志表
CREATE TABLE IF NOT EXISTS data_update_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20),
    start_date DATE,
    end_date DATE,
    rows_affected INT,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    execution_time DECIMAL(10, 3),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_data_type (data_type),
    INDEX idx_symbol (symbol),
    INDEX idx_created_time (created_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. 股票财务指标表
CREATE TABLE IF NOT EXISTS stock_financial_indicators (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    report_type VARCHAR(20),
    total_assets DECIMAL(20, 3),
    total_liabilities DECIMAL(20, 3),
    net_assets DECIMAL(20, 3),
    operating_income DECIMAL(20, 3),
    net_profit DECIMAL(20, 3),
    eps DECIMAL(10, 4),
    bvps DECIMAL(10, 4),
    roe DECIMAL(10, 4),
    roa DECIMAL(10, 4),
    pe_ratio DECIMAL(10, 4),
    pb_ratio DECIMAL(10, 4),
    ps_ratio DECIMAL(10, 4),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_report_date (symbol, report_date, report_type),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 在现有表后添加：
CREATE TABLE IF NOT EXISTS adjust_factors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    ex_date DATE NOT NULL COMMENT '除权除息日',
    cash_div DECIMAL(10, 4) COMMENT '现金分红(每股)',
    shares_div DECIMAL(10, 4) COMMENT '送股比例',
    allotment_ratio DECIMAL(10, 4) COMMENT '配股比例',
    allotment_price DECIMAL(10, 4) COMMENT '配股价',
    split_ratio DECIMAL(10, 4) COMMENT '拆股比例',
    forward_factor DECIMAL(12, 6) COMMENT '前复权因子',
    backward_factor DECIMAL(12, 6) COMMENT '后复权因子',
    total_factor DECIMAL(12, 6) COMMENT '累计复权因子',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_ex_date (symbol, ex_date)
);


-- 显示所有表
SELECT 'Tables created successfully!' as message;