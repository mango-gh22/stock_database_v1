# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_all_column_mappings.py
# File Name: fix_all_column_mappings
# @ Author: mango-gh22
# @ Date：2025/12/14 17:47
"""
desc 
"""
# fix_all_column_mappings.py
"""
全面修复列名映射问题
基于实际的表结构
"""

import sys
import os
import yaml
import shutil

sys.path.insert(0, os.path.abspath('.'))

print("🔧 全面修复列名映射问题")
print("=" * 60)

# 实际表列名 -> 查询别名的映射
COLUMN_MAPPINGS = {
    # 基础价格数据
    'open_price': 'open',
    'high_price': 'high',
    'low_price': 'low',
    'close_price': 'close',
    'pre_close_price': 'pre_close',
    'change_percent': 'pct_change',  # 关键修复：change_percent -> pct_change
    'change_percent': 'price_change',  # 同一个列映射到两个别名
    'volume': 'volume',
    'amount': 'amount',
    'turnover_rate': 'turnover_rate',
    'amplitude': 'amplitude',
    'ma5': 'ma5',
    'ma10': 'ma10',
    'ma20': 'ma20',
    # 技术指标
    'volume_ma5': 'volume_ma5',
    'volume_ma10': 'volume_ma10',
    'volume_ma20': 'volume_ma20',
    'rsi': 'rsi',
    'bb_upper': 'bb_upper',
    'bb_middle': 'bb_middle',
    'bb_lower': 'bb_lower',
    'volatility_20d': 'volatility_20d',
    # 估值指标
    'pe': 'pe',
    'pe_ttm': 'pe_ttm',
    'pb': 'pb',
    'ps': 'ps',
    'ps_ttm': 'ps_ttm',
    'dv_ratio': 'dv_ratio',
    'dv_ttm': 'dv_ttm',
    # 市值数据
    'total_mv': 'total_mv',
    'circ_mv': 'circ_mv'
}

# SQL中的列名替换映射（旧 -> 新）
SQL_FIX_MAPPINGS = {
    'pct_change': 'change_percent',
    'change_amount': 'change_percent',
    'open': 'open_price',
    'high': 'high_price',
    'low': 'low_price',
    'close': 'close_price',
    'pre_close': 'pre_close_price'
}


def backup_file(filepath):
    """备份文件"""
    if os.path.exists(filepath):
        backup_path = filepath + '.backup_' + os.path.basename(filepath).replace('.', '_')
        shutil.copy2(filepath, backup_path)
        return backup_path
    return None


def fix_query_engine():
    """修复查询引擎"""
    print("\n1. 修复查询引擎 (src/query/query_engine.py)...")

    query_engine_path = 'src/query/query_engine.py'
    backup = backup_file(query_engine_path)
    if backup:
        print(f"  已备份到: {backup}")

    try:
        with open(query_engine_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 查找并替换 query_daily_data 方法中的查询语句
        old_pattern = """                SELECT 
                    trade_date, 
                    symbol,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume,
                    amount,
                    pct_change,
                    change_amount as price_change,
                    pre_close_price as pre_close,
                    turnover_rate,
                    amplitude,
                    ma5, ma10, ma20"""

        new_pattern = """                SELECT 
                    trade_date, 
                    symbol,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume,
                    amount,
                    change_percent as pct_change,
                    change_percent as price_change,
                    pre_close_price as pre_close,
                    turnover_rate,
                    amplitude,
                    ma5, ma10, ma20"""

        if old_pattern in content:
            content = content.replace(old_pattern, new_pattern)
            print("  ✅ 修复了 query_daily_data 方法")
        else:
            # 检查是否已经是正确的
            if 'change_percent as pct_change' in content:
                print("  ✅ 查询引擎已经是正确的")
            else:
                print("  ⚠️ 未找到需要修复的查询模式")

        # 保存修复后的文件
        with open(query_engine_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print("  ✅ 查询引擎修复完成")
        return True

    except Exception as e:
        print(f"  ❌ 修复查询引擎失败: {e}")
        return False


def fix_quality_rules():
    """修复质量规则配置"""
    print("\n2. 修复质量规则配置 (config/quality_rules.yaml)...")

    config_path = 'config/quality_rules.yaml'
    if not os.path.exists(config_path):
        print(f"  ⚠️ 配置文件不存在: {config_path}")
        print("  创建默认配置...")
        create_default_quality_rules()
        config_path = 'config/quality_rules.yaml'

    backup = backup_file(config_path)
    if backup:
        print(f"  已备份到: {backup}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        fixes_made = 0

        # 修复SQL语句中的列名
        if 'quality_rules' in config:
            for rule_type, rules in config['quality_rules'].items():
                if isinstance(rules, list):
                    for rule in rules:
                        if 'sql' in rule and rule['sql']:
                            sql = rule['sql']
                            original_sql = sql

                            # 应用修复
                            for wrong_col, correct_col in SQL_FIX_MAPPINGS.items():
                                if wrong_col in sql:
                                    sql = sql.replace(wrong_col, correct_col)

                            if sql != original_sql:
                                rule['sql'] = sql
                                fixes_made += 1
                                print(f"    修复规则: {rule.get('name', 'unnamed')}")

        if fixes_made > 0:
            # 保存修复后的配置
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False,
                          allow_unicode=True, sort_keys=False,
                          indent=2, width=1000)

            print(f"  ✅ 修复了 {fixes_made} 条规则的SQL语句")
        else:
            print("  ✅ 质量规则配置无需修复")

        return True

    except Exception as e:
        print(f"  ❌ 修复质量规则配置失败: {e}")
        return False


def create_default_quality_rules():
    """创建默认的质量规则配置"""
    default_config = {
        'quality_rules': {
            'completeness': [
                {
                    'name': 'missing_price_data',
                    'description': '缺失价格数据检查',
                    'severity': 'ERROR',
                    'sql': """SELECT symbol, trade_date FROM stock_daily_data WHERE open_price IS NULL OR close_price IS NULL OR high_price IS NULL OR low_price IS NULL"""
                },
                {
                    'name': 'missing_volume_data',
                    'description': '缺失成交量数据检查',
                    'severity': 'WARNING',
                    'sql': """SELECT symbol, trade_date FROM stock_daily_data WHERE volume IS NULL OR volume = 0"""
                }
            ],
            'business_logic': [
                {
                    'name': 'price_range_check',
                    'description': '价格范围合理性检查',
                    'severity': 'ERROR',
                    'condition': 'low_price <= open_price <= high_price AND low_price <= close_price <= high_price'
                },
                {
                    'name': 'volume_positive',
                    'description': '成交量为正数',
                    'severity': 'ERROR',
                    'condition': 'volume >= 0'
                },
                {
                    'name': 'change_percent_limit',
                    'description': '涨跌幅限制检查（股票涨跌幅应在±20%内）',
                    'severity': 'WARNING',
                    'condition': 'abs(change_percent) <= 20.0'
                }
            ],
            'consistency': [
                {
                    'name': 'date_continuity',
                    'description': '交易日期连续性检查',
                    'severity': 'WARNING',
                    'algorithm': 'date_gap_detection'
                },
                {
                    'name': 'pre_close_consistency',
                    'description': '前收盘价与昨日收盘价一致性',
                    'severity': 'ERROR',
                    'sql': """SELECT t1.symbol, t1.trade_date, t1.pre_close_price, t2.close_price as prev_close FROM stock_daily_data t1 LEFT JOIN stock_daily_data t2 ON t1.symbol = t2.symbol AND t2.trade_date = DATE_SUB(t1.trade_date, INTERVAL 1 DAY) WHERE ABS(t1.pre_close_price - t2.close_price) > 0.01"""
                }
            ],
            'statistical': [
                {
                    'name': 'price_outlier_3sigma',
                    'description': '3σ价格异常检测',
                    'severity': 'WARNING',
                    'algorithm': 'z_score',
                    'threshold': 3.0
                },
                {
                    'name': 'volume_spike',
                    'description': '成交量异常放大检测',
                    'severity': 'WARNING',
                    'algorithm': 'iqr',
                    'threshold': 1.5
                }
            ]
        },
        'validation': {
            'batch_size': 100,
            'parallel_workers': 4,
            'max_memory_gb': 2,
            'timeout_seconds': 300
        },
        'adjustment': {
            'forward_adjust_method': 'factor',
            'backward_adjust_method': 'factor',
            'keep_original_price': True,
            'cache_factors': True
        }
    }

    with open('config/quality_rules.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(default_config, f, default_flow_style=False,
                  allow_unicode=True, sort_keys=False,
                  indent=2, width=1000)

    print("  ✅ 已创建默认质量规则配置")


def fix_adjustor_module():
    """修复复权计算器模块"""
    print("\n3. 检查复权计算器模块...")

    adjustor_path = 'src/processors/adjustor.py'

    try:
        with open(adjustor_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 检查 adjust_price 方法是否使用正确的列名
        issues = []

        # 检查查询部分 - 应该在 adjust_price 方法中
        if 'query_daily_data' in content:
            print("  ✅ 使用 query_daily_data 方法，将自动获得修复")

        # 检查硬编码的列名
        hardcoded_checks = [
            ('pct_change', 'change_percent'),
            ('change_amount', 'change_percent')
        ]

        for wrong, correct in hardcoded_checks:
            if wrong in content and 'query_daily_data' not in content:
                print(f"  ⚠️ 发现可能的硬编码列名: {wrong}")
                issues.append((wrong, correct))

        if not issues:
            print("  ✅ 复权计算器模块无需修复")
            return True
        else:
            print("  ⚠️ 可能需要手动检查硬编码列名")
            return True

    except Exception as e:
        print(f"  ❌ 检查复权计算器失败: {e}")
        return False


def create_test_script():
    """创建测试脚本验证修复"""
    print("\n4. 创建验证测试脚本...")

    test_script = '''# test_column_fix.py
"""
验证列名修复的测试脚本
"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("🔍 验证列名修复")
print("=" * 60)

try:
    # 1. 测试查询引擎
    print("\\n1. 测试查询引擎...")
    from src.query.query_engine import QueryEngine
    query_engine = QueryEngine()

    # 获取测试股票
    stock_df = query_engine.get_stock_list()
    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']
        print(f"   测试股票: {test_symbol}")

        # 查询数据
        data = query_engine.query_daily_data(symbol=test_symbol, limit=3)
        if not data.empty:
            print(f"   ✅ 查询成功: {len(data)} 条记录")

            # 检查关键列
            required_columns = ['open', 'high', 'low', 'close', 'pct_change', 'volume']
            missing = []
            for col in required_columns:
                if col in data.columns:
                    print(f"     ✓ {col}")
                else:
                    missing.append(col)
                    print(f"     ✗ {col} (缺失)")

            if not missing:
                print("   ✅ 所有必要列都存在")
            else:
                print(f"   ⚠️ 缺失列: {missing}")

            # 显示示例数据
            print("\\n   示例数据:")
            for i in range(min(2, len(data))):
                row = data.iloc[i]
                print(f"     {row['trade_date']}: {row['close']:.2f} ({row.get('pct_change', 0):+.2f}%)")
        else:
            print("   ⚠️ 查询返回空数据")
    else:
        print("   ⚠️ 无股票数据")

    # 2. 测试验证器
    print("\\n2. 测试数据验证器...")
    from src.processors.validator import DataValidator
    validator = DataValidator()

    # 检查规则加载
    rule_count = sum(len(rules) for rules in validator.rules.values())
    print(f"   加载规则: {rule_count} 条")

    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']

        # 运行完整性验证
        results = validator.validate_completeness(test_symbol)
        print(f"   完整性验证: {len(results)} 条规则")

        for result in results:
            status = "✓" if result.result.value == "PASS" else "⚠️"
            print(f"     {status} {result.rule_name}: {result.result.value} ({result.affected_rows}条)")

    # 3. 测试复权计算器
    print("\\n3. 测试复权计算器...")
    from src.processors.adjustor import StockAdjustor, AdjustType
    adjustor = StockAdjustor()

    if not stock_df.empty:
        test_symbol = stock_df.iloc[0]['symbol']

        # 获取数据
        data = query_engine.query_daily_data(symbol=test_symbol, limit=5)
        if not data.empty:
            print(f"   获取 {len(data)} 条数据进行复权测试")

            # 测试前复权
            try:
                forward_df = adjustor.adjust_price(data.copy(), test_symbol, AdjustType.FORWARD)
                print(f"   ✅ 前复权完成: {len(forward_df)} 条")
                print(f"     复权类型: {forward_df['adjust_type'].iloc[0]}")
            except Exception as e:
                print(f"   ❌ 前复权失败: {e}")

    print("\\n" + "=" * 60)
    print("🎉 验证完成!")
    print("=" * 60)

    # 清理
    query_engine.close()
    validator.close()
    adjustor.close()

except Exception as e:
    print(f"\\n❌ 验证失败: {e}")
    import traceback
    traceback.print_exc()
'''

    with open('test_column_fix.py', 'w', encoding='utf-8') as f:
        f.write(test_script)

    print("  ✅ 测试脚本已创建: test_column_fix.py")


def create_column_mapping_document():
    """创建列名映射文档"""
    print("\n5. 创建列名映射文档...")

    doc_content = """# 列名映射文档

## 实际数据库表列名 -> 查询别名映射

### stock_daily_data 表重要列映射：

| 数据库列名 | 查询别名 | 说明 |
|-----------|---------|------|
| open_price | open | 开盘价 |
| high_price | high | 最高价 |
| low_price | low | 最低价 |
| close_price | close | 收盘价 |
| pre_close_price | pre_close | 前收盘价 |
| **change_percent** | **pct_change** | **涨跌幅百分比（关键修复）** |
| change_percent | price_change | 涨跌幅（同列不同别名） |
| volume | volume | 成交量 |
| amount | amount | 成交额 |
| turnover_rate | turnover_rate | 换手率 |
| amplitude | amplitude | 振幅 |
| ma5 | ma5 | 5日均线 |
| ma10 | ma10 | 10日均线 |
| ma20 | ma20 | 20日均线 |

### 常见错误列名：

| 错误列名 | 正确列名 | 说明 |
|---------|---------|------|
| pct_change | change_percent | 表中实际列名 |
| change_amount | change_percent | 表中实际列名 |
| open | open_price | 完整列名 |
| high | high_price | 完整列名 |
| low | low_price | 完整列名 |
| close | close_price | 完整列名 |

### 已修复的文件：

1. **src/query/query_engine.py** - 修改了 query_daily_data 方法
2. **config/quality_rules.yaml** - 修复了SQL语句中的列名
3. **验证器模块** - 自动使用修复后的查询引擎

### 验证方法：

运行测试脚本：
```bash
python test_column_fix.py
"""

