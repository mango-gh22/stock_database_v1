-- Table: adjust_factors
-- Description: 股票复权因子表 - P6阶段
-- Author: mango-gh22
-- Date: 2026-01-02

CREATE TABLE IF NOT EXISTS `adjust_factors` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    `symbol` VARCHAR(20) NOT NULL COMMENT '股票代码 (sh600519)',
    `ex_date` DATE NOT NULL COMMENT '除权除息日',

    -- 分红数据
    `cash_div` DECIMAL(10,4) NOT NULL DEFAULT 0.0000 COMMENT '每股现金分红(税前)',
    `shares_div` DECIMAL(10,6) NOT NULL DEFAULT 0.000000 COMMENT '每股送股比例',

    -- 配股数据
    `allotment_ratio` DECIMAL(10,6) NOT NULL DEFAULT 0.000000 COMMENT '配股比例',
    `allotment_price` DECIMAL(10,4) NOT NULL DEFAULT 0.0000 COMMENT '配股价格',

    -- 拆股数据
    `split_ratio` DECIMAL(10,6) NOT NULL DEFAULT 1.000000 COMMENT '拆股比例(如10送10则为2.0)',

    -- 复权因子
    `forward_factor` DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000 COMMENT '前复权因子',
    `backward_factor` DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000 COMMENT '后复权因子',
    `total_factor` DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000 COMMENT '总复权因子(默认使用前复权)',

    -- 元数据
    `created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_symbol_exdate` (`symbol`, `ex_date`) COMMENT '股票+除权日唯一',
    KEY `idx_symbol` (`symbol`) COMMENT '股票代码索引',
    KEY `idx_ex_date` (`ex_date`) COMMENT '除权日索引',
    KEY `idx_updated` (`updated_time`) COMMENT '更新时间索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票复权因子表';

-- 数据质量检查视图（P8阶段启用）
-- CREATE OR REPLACE VIEW `v_adjust_factors_quality` AS
-- SELECT
--     symbol,
--     COUNT(*) as total_records,
--     MIN(ex_date) as earliest_date,
--     MAX(ex_date) as latest_date,
--     AVG(ABS(forward_factor - 1)) as avg_forward_change,
--     SUM(CASE WHEN forward_factor <= 0 THEN 1 ELSE 0 END) as invalid_factors
-- FROM adjust_factors
-- GROUP BY symbol;

-- 测试数据（贵州茅台2022年分红示例）
-- INSERT INTO `adjust_factors`
-- (`symbol`, `ex_date`, `cash_div`, `shares_div`, `forward_factor`, `backward_factor`)
-- VALUES
-- ('sh600519', '2022-06-30', 21.6750, 0.0, 0.9785, 1.0219);

-- 查询示例：获取某股票所有复权因子
-- SELECT * FROM adjust_factors WHERE symbol = 'sh600519' ORDER BY ex_date DESC;

-- 查询示例：获取某日期前最新的复权因子
-- SELECT * FROM adjust_factors
-- WHERE symbol = 'sh600519' AND ex_date <= '2022-07-01'
-- ORDER BY ex_date DESC LIMIT 1;