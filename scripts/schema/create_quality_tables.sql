-- scripts/schema/create_quality_tables.sql
USE stock_database;

-- 1. 数据质量日志表
CREATE TABLE IF NOT EXISTS data_quality_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_type VARCHAR(50) NOT NULL COMMENT '检查类型',
    symbol VARCHAR(20) COMMENT '股票代码',
    check_date DATE COMMENT '检查日期',
    rule_name VARCHAR(100) COMMENT '规则名称',
    rule_description TEXT COMMENT '规则描述',
    check_result ENUM('PASS', 'WARNING', 'ERROR', 'CRITICAL') NOT NULL,
    error_message TEXT,
    affected_rows INT DEFAULT 0,
    severity_level VARCHAR(20) COMMENT '严重级别',
    suggestion TEXT COMMENT '修复建议',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_check_type (check_type),
    INDEX idx_symbol_date (symbol, check_date),
    INDEX idx_result (check_result),
    INDEX idx_created_time (created_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 复权因子表
CREATE TABLE IF NOT EXISTS adjust_factors (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    ex_date DATE NOT NULL COMMENT '除权除息日',
    cash_div DECIMAL(10, 4) COMMENT '现金分红',
    shares_div DECIMAL(10, 6) COMMENT '送股比例',
    allotment_ratio DECIMAL(10, 6) COMMENT '配股比例',
    allotment_price DECIMAL(10, 4) COMMENT '配股价',
    split_ratio DECIMAL(10, 6) COMMENT '拆股比例',
    forward_factor DECIMAL(20, 10) COMMENT '前复权因子',
    backward_factor DECIMAL(20, 10) COMMENT '后复权因子',
    total_factor DECIMAL(20, 10) COMMENT '总复权因子',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_ex_date (symbol, ex_date),
    INDEX idx_symbol (symbol),
    INDEX idx_ex_date (ex_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 数据异常记录表
CREATE TABLE IF NOT EXISTS data_anomalies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    anomaly_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    field_name VARCHAR(50) NOT NULL,
    expected_value DECIMAL(20, 4),
    actual_value DECIMAL(20, 4),
    deviation_rate DECIMAL(10, 4),
    algorithm VARCHAR(50) COMMENT '检测算法',
    confidence DECIMAL(5, 4) COMMENT '置信度',
    status ENUM('DETECTED', 'REVIEWED', 'CORRECTED', 'IGNORED') DEFAULT 'DETECTED',
    reviewer VARCHAR(50),
    review_notes TEXT,
    corrected_value DECIMAL(20, 4),
    corrected_time DATETIME,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_date_field (symbol, trade_date, field_name, anomaly_type),
    INDEX idx_symbol_date (symbol, trade_date),
    INDEX idx_anomaly_type (anomaly_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 质量规则配置表（可选，可以从yaml文件加载）
CREATE TABLE IF NOT EXISTS quality_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_expression TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'WARNING',
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_rule_name (rule_name),
    INDEX idx_rule_type (rule_type),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT 'Quality tables created successfully!' as message;