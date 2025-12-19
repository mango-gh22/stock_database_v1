-- scripts/schema/create_tables_complete.sql
-- 完整数据库表结构（兼容原结构和新增功能）

USE stock_database;

-- ==================== 股票核心表 ====================

-- 1. 股票基本信息表（增强版）
CREATE TABLE IF NOT EXISTS stock_basic_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    -- 核心标识
    symbol VARCHAR(20) NOT NULL COMMENT '标准股票代码(如600519)',
    normalized_code VARCHAR(20) NOT NULL COMMENT '标准化代码(如sh600519)',
    ts_code VARCHAR(20) COMMENT 'Tushare代码(如600519.SH)',

    -- 基本信息
    name VARCHAR(100) NOT NULL COMMENT '股票名称',
    fullname VARCHAR(200) COMMENT '公司全称',
    enname VARCHAR(300) COMMENT '英文名称',
    cnspell VARCHAR(50) COMMENT '拼音缩写',

    -- 市场信息
    exchange VARCHAR(10) NOT NULL COMMENT '交易所(SH/SZ/BJ)',
    market VARCHAR(20) COMMENT '市场类型(主板/创业板/科创板)',
    market_cap_group VARCHAR(20) COMMENT '市值分组(大盘/中盘/小盘)',

    -- 行业信息
    industry VARCHAR(100) COMMENT '行业分类',
    industry_code VARCHAR(20) COMMENT '行业代码',
    sub_industry VARCHAR(100) COMMENT '子行业',
    area VARCHAR(50) COMMENT '地域',
    province VARCHAR(50) COMMENT '省份',
    city VARCHAR(50) COMMENT '城市',

    -- 上市信息
    list_date DATE NOT NULL COMMENT '上市日期',
    delist_date DATE COMMENT '退市日期',
    list_status VARCHAR(5) DEFAULT 'L' COMMENT '状态(L上市 D退市 P暂停)',

    -- 财务标识
    curr_type VARCHAR(10) DEFAULT 'CNY' COMMENT '货币类型',
    is_hs VARCHAR(5) COMMENT '是否沪深港通(H沪股通 S深股通 N否)',
    hs_type VARCHAR(5) COMMENT '沪深港通类型',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_sync_time TIMESTAMP NULL COMMENT '最后同步时间',

    -- 索引
    UNIQUE KEY uk_normalized_code (normalized_code),
    UNIQUE KEY uk_ts_code (ts_code),
    UNIQUE KEY uk_symbol_exchange (symbol, exchange),
    INDEX idx_name (name),
    INDEX idx_industry (industry),
    INDEX idx_exchange (exchange),
    INDEX idx_list_date (list_date),
    INDEX idx_list_status (list_status),
    INDEX idx_is_hs (is_hs),
    INDEX idx_market (market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 股票日线数据表（性能优化版）
CREATE TABLE IF NOT EXISTS stock_daily_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 标识信息
    normalized_code VARCHAR(20) NOT NULL COMMENT '标准化股票代码',
    trade_date DATE NOT NULL COMMENT '交易日',
    exchange VARCHAR(10) COMMENT '交易所',

    -- 核心价格数据
    open_price DECIMAL(12, 4) COMMENT '开盘价',
    close_price DECIMAL(12, 4) COMMENT '收盘价',
    high_price DECIMAL(12, 4) COMMENT '最高价',
    low_price DECIMAL(12, 4) COMMENT '最低价',
    pre_close_price DECIMAL(12, 4) COMMENT '前收盘价',
    adjust_factor DECIMAL(12, 6) DEFAULT 1.0 COMMENT '复权因子',

    -- 交易量数据
    volume BIGINT UNSIGNED COMMENT '成交量(股)',
    volume_lot BIGINT UNSIGNED COMMENT '成交量(手)',
    amount DECIMAL(20, 4) COMMENT '成交额(元)',

    -- 涨跌数据
    change_amount DECIMAL(12, 4) COMMENT '涨跌额',
    pct_change DECIMAL(10, 6) COMMENT '涨跌幅(%)',
    amplitude DECIMAL(10, 6) COMMENT '振幅(%)',

    -- 衍生指标
    turnover_rate DECIMAL(10, 6) COMMENT '换手率(%)',
    turnover_rate_f DECIMAL(10, 6) COMMENT '自由流通换手率(%)',
    volume_ratio DECIMAL(10, 4) COMMENT '量比',

    -- 移动平均线
    ma5 DECIMAL(12, 4) COMMENT '5日均线',
    ma10 DECIMAL(12, 4) COMMENT '10日均线',
    ma20 DECIMAL(12, 4) COMMENT '20日均线',
    ma30 DECIMAL(12, 4) COMMENT '30日均线',
    ma60 DECIMAL(12, 4) COMMENT '60日均线',
    ma120 DECIMAL(12, 4) COMMENT '120日均线',
    ma250 DECIMAL(12, 4) COMMENT '250日均线',

    -- 成交量均线
    volume_ma5 BIGINT UNSIGNED COMMENT '5日成交量均线',
    volume_ma10 BIGINT UNSIGNED COMMENT '10日成交量均线',
    volume_ma20 BIGINT UNSIGNED COMMENT '20日成交量均线',

    -- 技术指标
    rsi DECIMAL(8, 4) COMMENT '相对强弱指数',
    macd DECIMAL(10, 4) COMMENT 'MACD',
    macd_signal DECIMAL(10, 4) COMMENT 'MACD信号线',
    macd_hist DECIMAL(10, 4) COMMENT 'MACD柱状图',

    -- 布林带
    bb_upper DECIMAL(12, 4) COMMENT '布林带上轨',
    bb_middle DECIMAL(12, 4) COMMENT '布林带中轨',
    bb_lower DECIMAL(12, 4) COMMENT '布林带下轨',
    bb_width DECIMAL(10, 6) COMMENT '布林带宽度(%)',

    -- 波动率
    volatility_20d DECIMAL(10, 6) COMMENT '20日波动率',
    atr DECIMAL(12, 4) COMMENT '平均真实波幅',

    -- 数据质量
    data_source VARCHAR(50) DEFAULT 'tushare' COMMENT '数据来源',
    data_quality TINYINT DEFAULT 100 COMMENT '数据质量评分(0-100)',
    is_adjusted TINYINT(1) DEFAULT 0 COMMENT '是否已复权(0未复权 1前复权 2后复权)',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    processed_time DATETIME COMMENT '数据处理时间',

    -- 约束和索引
    UNIQUE KEY uk_code_date (normalized_code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_exchange (exchange),
    INDEX idx_date_range (normalized_code, trade_date),
    INDEX idx_price_range (trade_date, close_price),
    INDEX idx_volume_range (trade_date, volume),
    INDEX idx_pct_change (pct_change),
    INDEX idx_data_source (data_source),
    INDEX idx_data_quality (data_quality)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(trade_date))
(
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 3. 股票分钟线数据表（分区优化）
CREATE TABLE IF NOT EXISTS stock_minute_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 标识信息
    normalized_code VARCHAR(20) NOT NULL,
    trade_time DATETIME NOT NULL,
    trade_date DATE NOT NULL,
    exchange VARCHAR(10),

    -- 价格数据
    open_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),

    -- 交易数据
    volume BIGINT UNSIGNED,
    amount DECIMAL(20, 4),

    -- 频率信息
    freq VARCHAR(10) NOT NULL DEFAULT '1min' COMMENT '频率(1min/5min/15min/30min/60min)',

    -- 衍生数据
    avg_price DECIMAL(12, 4) COMMENT '均价',
    vwap DECIMAL(12, 4) COMMENT '成交量加权平均价',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_code_time_freq (normalized_code, trade_time, freq),
    INDEX idx_code_date_freq (normalized_code, trade_date, freq),
    INDEX idx_date_freq (trade_date, freq),
    INDEX idx_time_range (trade_time),
    INDEX idx_code_freq (normalized_code, freq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==================== 指数相关表 ====================

-- 4. 指数信息表（增强版）
CREATE TABLE IF NOT EXISTS index_info (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- 标识信息
    index_code VARCHAR(20) NOT NULL COMMENT '指数代码',
    normalized_code VARCHAR(20) NOT NULL COMMENT '标准化代码',
    index_name VARCHAR(100) NOT NULL COMMENT '指数名称',
    index_name_en VARCHAR(200) COMMENT '英文名称',
    index_name_short VARCHAR(50) COMMENT '简称',

    -- 基本信息
    publisher VARCHAR(100) COMMENT '发布机构',
    index_type VARCHAR(50) COMMENT '指数类型(综合/规模/行业/主题/策略)',
    index_category VARCHAR(50) COMMENT '指数分类',
    base_currency VARCHAR(10) DEFAULT 'CNY' COMMENT '基准货币',

    -- 基准信息
    base_date DATE COMMENT '基日',
    base_point DECIMAL(12, 4) COMMENT '基点',
    current_point DECIMAL(12, 4) COMMENT '当前点位',

    -- 成分信息
    constituent_count INT COMMENT '成分股数量',
    total_market_cap DECIMAL(20, 4) COMMENT '总市值',
    avg_pe DECIMAL(12, 4) COMMENT '平均市盈率',
    avg_pb DECIMAL(12, 4) COMMENT '平均市净率',

    -- 联系信息
    website VARCHAR(500) COMMENT '官方网站',
    contact_email VARCHAR(100) COMMENT '联系邮箱',
    description TEXT COMMENT '指数描述',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_index_code (index_code),
    UNIQUE KEY uk_normalized_code (normalized_code),
    INDEX idx_index_name (index_name),
    INDEX idx_index_type (index_type),
    INDEX idx_publisher (publisher)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. 指数日线数据表
CREATE TABLE IF NOT EXISTS index_daily_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 标识信息
    normalized_code VARCHAR(20) NOT NULL COMMENT '指数标准化代码',
    trade_date DATE NOT NULL COMMENT '交易日',

    -- 价格数据
    open_point DECIMAL(12, 4) COMMENT '开盘点位',
    close_point DECIMAL(12, 4) COMMENT '收盘点位',
    high_point DECIMAL(12, 4) COMMENT '最高点位',
    low_point DECIMAL(12, 4) COMMENT '最低点位',
    pre_close_point DECIMAL(12, 4) COMMENT '前收盘点位',

    -- 交易数据
    volume BIGINT UNSIGNED COMMENT '成交量',
    amount DECIMAL(20, 4) COMMENT '成交额',

    -- 涨跌数据
    change_point DECIMAL(12, 4) COMMENT '涨跌点数',
    pct_change DECIMAL(10, 6) COMMENT '涨跌幅(%)',

    -- 衍生指标
    pe DECIMAL(12, 4) COMMENT '市盈率',
    pb DECIMAL(12, 4) COMMENT '市净率',
    dividend_yield DECIMAL(10, 6) COMMENT '股息率(%)',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_code_date (normalized_code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_normalized_code (normalized_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (YEAR(trade_date))
(
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 6. 股票-指数成分关联表（增强版）
CREATE TABLE IF NOT EXISTS stock_index_constituent (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- 关联信息
    index_code VARCHAR(20) NOT NULL COMMENT '指数代码',
    normalized_code VARCHAR(20) NOT NULL COMMENT '股票标准化代码',

    -- 成分信息
    weight DECIMAL(10, 6) COMMENT '权重(%)',
    shares_held BIGINT UNSIGNED COMMENT '持有股数',
    market_value DECIMAL(20, 4) COMMENT '市值',
    weight_rank INT COMMENT '权重排名',

    -- 时间信息
    start_date DATE NOT NULL COMMENT '纳入日期',
    end_date DATE COMMENT '剔除日期',
    announcement_date DATE COMMENT '公告日期',

    -- 状态
    is_current TINYINT(1) DEFAULT 1 COMMENT '是否当前成分(1是 0否)',
    change_type VARCHAR(20) COMMENT '变动类型(新增/剔除/权重调整)',
    change_reason TEXT COMMENT '变动原因',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_index_stock_date (index_code, normalized_code, start_date),
    INDEX idx_index_code (index_code),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_start_date (start_date),
    INDEX idx_is_current (is_current),
    INDEX idx_index_current (index_code, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==================== 财务数据表 ====================

-- 7. 股票财务指标表（完整版）
CREATE TABLE IF NOT EXISTS stock_financial_indicators (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 标识信息
    normalized_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL COMMENT '报告期',
    report_type VARCHAR(20) NOT NULL COMMENT '报告类型(Q1/H1/Q3/Annual)',
    report_year INT NOT NULL COMMENT '报告年份',
    report_quarter INT COMMENT '报告季度',

    -- 资产负债表核心指标
    total_assets DECIMAL(20, 4) COMMENT '总资产',
    total_liabilities DECIMAL(20, 4) COMMENT '总负债',
    total_equity DECIMAL(20, 4) COMMENT '所有者权益',
    current_assets DECIMAL(20, 4) COMMENT '流动资产',
    current_liabilities DECIMAL(20, 4) COMMENT '流动负债',

    -- 利润表核心指标
    operating_income DECIMAL(20, 4) COMMENT '营业收入',
    operating_profit DECIMAL(20, 4) COMMENT '营业利润',
    net_profit DECIMAL(20, 4) COMMENT '净利润',
    net_profit_attributable DECIMAL(20, 4) COMMENT '归母净利润',
    gross_profit DECIMAL(20, 4) COMMENT '毛利',

    -- 现金流量表核心指标
    net_cash_flow_operating DECIMAL(20, 4) COMMENT '经营活动现金流净额',
    net_cash_flow_investing DECIMAL(20, 4) COMMENT '投资活动现金流净额',
    net_cash_flow_financing DECIMAL(20, 4) COMMENT '筹资活动现金流净额',

    -- 盈利能力指标
    gross_margin DECIMAL(10, 6) COMMENT '毛利率(%)',
    operating_margin DECIMAL(10, 6) COMMENT '营业利润率(%)',
    net_margin DECIMAL(10, 6) COMMENT '净利率(%)',
    roe DECIMAL(10, 6) COMMENT '净资产收益率(%)',
    roa DECIMAL(10, 6) COMMENT '总资产收益率(%)',
    roic DECIMAL(10, 6) COMMENT '投入资本回报率(%)',

    -- 估值指标
    eps DECIMAL(12, 4) COMMENT '每股收益',
    bvps DECIMAL(12, 4) COMMENT '每股净资产',
    dps DECIMAL(10, 4) COMMENT '每股股息',
    pe_ratio DECIMAL(12, 4) COMMENT '市盈率',
    pb_ratio DECIMAL(12, 4) COMMENT '市净率',
    ps_ratio DECIMAL(12, 4) COMMENT '市销率',
    dividend_yield DECIMAL(10, 6) COMMENT '股息率(%)',

    -- 偿债能力指标
    debt_to_equity DECIMAL(10, 6) COMMENT '负债权益比',
    current_ratio DECIMAL(10, 4) COMMENT '流动比率',
    quick_ratio DECIMAL(10, 4) COMMENT '速动比率',
    interest_coverage DECIMAL(10, 4) COMMENT '利息保障倍数',

    -- 运营效率指标
    asset_turnover DECIMAL(10, 4) COMMENT '资产周转率',
    inventory_turnover DECIMAL(10, 4) COMMENT '存货周转率',
    receivables_turnover DECIMAL(10, 4) COMMENT '应收账款周转率',

    -- 成长性指标
    revenue_growth_yoy DECIMAL(10, 6) COMMENT '营收同比增长(%)',
    net_profit_growth_yoy DECIMAL(10, 6) COMMENT '净利润同比增长(%)',
    eps_growth_yoy DECIMAL(10, 6) COMMENT '每股收益同比增长(%)',

    -- 数据质量
    audit_opinion VARCHAR(50) COMMENT '审计意见',
    data_source VARCHAR(50) DEFAULT 'tushare' COMMENT '数据来源',
    data_quality TINYINT DEFAULT 100 COMMENT '数据质量评分',

    -- 时间戳
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_code_report (normalized_code, report_date, report_type),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_report_date (report_date),
    INDEX idx_report_year (report_year),
    INDEX idx_report_type (report_type),
    INDEX idx_roe (roe),
    INDEX idx_pe_ratio (pe_ratio),
    INDEX idx_revenue_growth (revenue_growth_yoy)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==================== 系统管理表 ====================

-- 8. 数据更新日志表（增强版）
CREATE TABLE IF NOT EXISTS data_update_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 任务信息
    task_id VARCHAR(100) NOT NULL COMMENT '任务ID',
    task_type VARCHAR(50) NOT NULL COMMENT '任务类型(daily/minute/financial/basic)',
    task_name VARCHAR(200) COMMENT '任务名称',

    -- 数据范围
    data_type VARCHAR(50) NOT NULL COMMENT '数据类型',
    normalized_code VARCHAR(20) COMMENT '股票代码',
    exchange VARCHAR(10) COMMENT '交易所',
    start_date DATE COMMENT '开始日期',
    end_date DATE COMMENT '结束日期',
    freq VARCHAR(10) COMMENT '频率',

    -- 执行结果
    status VARCHAR(20) NOT NULL COMMENT '状态(running/success/failed/partial)',
    rows_fetched INT DEFAULT 0 COMMENT '获取记录数',
    rows_stored INT DEFAULT 0 COMMENT '存储记录数',
    duplicates_skipped INT DEFAULT 0 COMMENT '跳过重复数',
    invalid_records INT DEFAULT 0 COMMENT '无效记录数',

    -- 性能指标
    fetch_duration DECIMAL(10, 3) COMMENT '获取耗时(秒)',
    process_duration DECIMAL(10, 3) COMMENT '处理耗时(秒)',
    total_duration DECIMAL(10, 3) COMMENT '总耗时(秒)',

    -- 错误信息
    error_code VARCHAR(50) COMMENT '错误代码',
    error_message TEXT COMMENT '错误信息',
    error_details TEXT COMMENT '错误详情',
    stack_trace TEXT COMMENT '堆栈跟踪',

    -- 系统信息
    server_host VARCHAR(100) COMMENT '服务器主机',
    worker_id VARCHAR(50) COMMENT '工作器ID',
    batch_id VARCHAR(100) COMMENT '批次ID',

    -- 时间戳
    start_time TIMESTAMP NULL COMMENT '开始时间',
    end_time TIMESTAMP NULL COMMENT '结束时间',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    INDEX idx_task_type (task_type),
    INDEX idx_data_type (data_type),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_status (status),
    INDEX idx_created_time (created_time),
    INDEX idx_start_date (start_date),
    INDEX idx_end_date (end_date),
    INDEX idx_task_id (task_id),
    INDEX idx_batch_id (batch_id),
    INDEX idx_status_time (status, created_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. 数据缓存表
CREATE TABLE IF NOT EXISTS data_cache (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- 缓存标识
    cache_key VARCHAR(255) NOT NULL COMMENT '缓存键',
    cache_type VARCHAR(50) NOT NULL COMMENT '缓存类型(daily/minute/financial/basic)',
    cache_group VARCHAR(50) COMMENT '缓存分组',

    -- 数据内容
    data_hash VARCHAR(64) NOT NULL COMMENT '数据哈希',
    data_size INT COMMENT '数据大小(字节)',
    data_format VARCHAR(20) DEFAULT 'json' COMMENT '数据格式',

    -- 关联信息
    normalized_code VARCHAR(20) COMMENT '股票代码',
    start_date DATE COMMENT '开始日期',
    end_date DATE COMMENT '结束日期',
    freq VARCHAR(10) COMMENT '频率',

    -- 过期控制
    ttl_seconds INT DEFAULT 3600 COMMENT '存活时间(秒)',
    created_time DATETIME NOT NULL COMMENT '创建时间',
    expires_at DATETIME NOT NULL COMMENT '过期时间',
    last_accessed_at DATETIME COMMENT '最后访问时间',

    -- 访问统计
    access_count INT DEFAULT 0 COMMENT '访问次数',
    hit_count INT DEFAULT 0 COMMENT '命中次数',

    -- 元数据
    metadata JSON COMMENT '元数据',
    tags JSON COMMENT '标签',

    -- 索引
    UNIQUE KEY uk_cache_key (cache_key),
    INDEX idx_cache_type (cache_type),
    INDEX idx_cache_group (cache_group),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_expires_at (expires_at),
    INDEX idx_last_accessed_at (last_accessed_at),
    INDEX idx_created_time (created_time),
    INDEX idx_data_hash (data_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 10. 数据质量监控表
CREATE TABLE IF NOT EXISTS data_quality_monitor (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- 监控对象
    data_type VARCHAR(50) NOT NULL COMMENT '数据类型',
    normalized_code VARCHAR(20) COMMENT '股票代码',
    monitor_date DATE NOT NULL COMMENT '监控日期',

    -- 完整性检查
    total_expected INT COMMENT '预期总记录数',
    total_actual INT COMMENT '实际总记录数',
    missing_count INT COMMENT '缺失记录数',
    completeness_rate DECIMAL(5, 2) COMMENT '完整率(%)',

    -- 准确性检查
    validation_passed INT COMMENT '验证通过数',
    validation_failed INT COMMENT '验证失败数',
    accuracy_rate DECIMAL(5, 2) COMMENT '准确率(%)',

    -- 一致性检查
    consistency_errors INT COMMENT '一致性错误数',
    consistency_rate DECIMAL(5, 2) COMMENT '一致率(%)',

    -- 及时性检查
    update_delay_hours DECIMAL(8, 2) COMMENT '更新延迟(小时)',
    timeliness_score DECIMAL(5, 2) COMMENT '及时性评分',

    -- 综合评分
    overall_score DECIMAL(5, 2) COMMENT '综合评分',
    quality_level VARCHAR(20) COMMENT '质量等级(excellent/good/fair/poor)',

    -- 详细报告
    issues_found JSON COMMENT '发现的问题',
    suggestions JSON COMMENT '改进建议',

    -- 时间戳
    checked_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '检查时间',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 索引
    UNIQUE KEY uk_type_code_date (data_type, normalized_code, monitor_date),
    INDEX idx_data_type (data_type),
    INDEX idx_normalized_code (normalized_code),
    INDEX idx_monitor_date (monitor_date),
    INDEX idx_quality_level (quality_level),
    INDEX idx_overall_score (overall_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==================== 视图 ====================

-- 11. 股票日线数据视图（简化查询）
CREATE OR REPLACE VIEW vw_stock_daily_summary AS
SELECT
    s.normalized_code,
    s.symbol,
    s.name,
    s.exchange,
    s.industry,
    d.trade_date,
    d.open_price,
    d.close_price,
    d.high_price,
    d.low_price,
    d.volume,
    d.amount,
    d.pct_change,
    d.ma5,
    d.ma10,
    d.ma20,
    d.volume_ma5,
    d.rsi,
    d.macd,
    d.data_quality
FROM stock_daily_data d
JOIN stock_basic_info s ON d.normalized_code = s.normalized_code
WHERE d.data_quality >= 80
ORDER BY d.trade_date DESC, s.normalized_code;

-- 12. 财务指标视图
CREATE OR REPLACE VIEW vw_financial_summary AS
SELECT
    s.normalized_code,
    s.symbol,
    s.name,
    f.report_date,
    f.report_type,
    f.operating_income,
    f.net_profit,
    f.roe,
    f.eps,
    f.pe_ratio,
    f.revenue_growth_yoy,
    f.net_profit_growth_yoy
FROM stock_financial_indicators f
JOIN stock_basic_info s ON f.normalized_code = s.normalized_code
WHERE f.report_type = 'Annual'
ORDER BY f.report_date DESC;

-- 13. 数据质量视图
CREATE OR REPLACE VIEW vw_data_quality_overview AS
SELECT
    data_type,
    monitor_date,
    COUNT(*) as stock_count,
    AVG(overall_score) as avg_score,
    SUM(CASE WHEN quality_level = 'excellent' THEN 1 ELSE 0 END) as excellent_count,
    SUM(CASE WHEN quality_level = 'good' THEN 1 ELSE 0 END) as good_count,
    SUM(CASE WHEN quality_level = 'fair' THEN 1 ELSE 0 END) as fair_count,
    SUM(CASE WHEN quality_level = 'poor' THEN 1 ELSE 0 END) as poor_count
FROM data_quality_monitor
GROUP BY data_type, monitor_date
ORDER BY monitor_date DESC, data_type;

-- ==================== 存储过程 ====================

-- 14. 清理过期缓存存储过程
DELIMITER $$
CREATE PROCEDURE sp_cleanup_expired_cache()
BEGIN
    DELETE FROM data_cache
    WHERE expires_at < NOW()
    AND last_accessed_at < DATE_SUB(NOW(), INTERVAL 7 DAY);

    SELECT ROW_COUNT() as deleted_count;
END$$
DELIMITER ;

-- 15. 更新数据质量存储过程
DELIMITER $$
CREATE PROCEDURE sp_update_data_quality()
BEGIN
    -- 更新股票日线数据质量
    UPDATE stock_daily_data d
    SET data_quality = (
        CASE
            WHEN volume <= 0 THEN 0
            WHEN open_price <= 0 OR close_price <= 0 OR high_price <= 0 OR low_price <= 0 THEN 30
            WHEN high_price < low_price THEN 40
            WHEN open_price < low_price OR open_price > high_price THEN 50
            WHEN close_price < low_price OR close_price > high_price THEN 50
            WHEN pct_change IS NULL THEN 70
            WHEN ABS(pct_change) > 20 THEN 80
            ELSE 95
        END
    )
    WHERE data_quality IS NULL OR updated_time > DATE_SUB(NOW(), INTERVAL 1 DAY);

    SELECT 'Data quality updated' as result;
END$$
DELIMITER ;

-- ==================== 创建完成提示 ====================
SELECT 'All tables created successfully!' as message;

-- 显示表结构信息
SELECT
    TABLE_NAME,
    TABLE_ROWS,
    DATA_LENGTH,
    INDEX_LENGTH,
    CREATE_TIME,
    UPDATE_TIME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'stock_database'
ORDER BY TABLE_NAME;