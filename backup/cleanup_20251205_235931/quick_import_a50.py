# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\quick_import_a50.py
# File Name: quick_import_a50
# @ File: quick_import_a50.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 23:08
"""
desc 
"""
# quick_import_a50.py
import sys

sys.path.insert(0, '.')

print("=== 快速导入中证A50 ===")

try:
    # 直接从配置文件读取symbols
    import yaml

    print("1. 读取配置文件...")
    with open('config/symbols.yaml', 'r', encoding='utf-8') as f:
        symbols_config = yaml.safe_load(f)

    csi_a50_stocks = symbols_config.get('csi_a50', [])
    print(f"找到 {len(csi_a50_stocks)} 只中证A50成分股")

    if not csi_a50_stocks:
        print("❌ 配置文件中未找到中证A50成分股")
        sys.exit(1)

    # 连接数据库
    print("2. 连接数据库...")
    from src.database.db_connector import DatabaseConnector

    db = DatabaseConnector()

    if not db.test_connection():
        print("❌ 数据库连接失败")
        sys.exit(1)

    print("3. 导入指数信息...")
    # 插入指数信息
    db.execute_query("""
        INSERT INTO index_info (index_code, index_name, index_name_en, publisher, index_type, base_date, base_point, website)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            index_name = VALUES(index_name),
            updated_time = CURRENT_TIMESTAMP
    """, ('CSI_A50', '中证A50指数', 'CSI A50 Index', '中证指数有限公司', '规模指数', '2014-12-31', 1000.00,
          'https://www.csindex.com.cn/'))

    print("4. 导入股票基本信息...")
    # 导入股票信息
    for i, stock in enumerate(csi_a50_stocks, 1):
        symbol = stock.get('symbol', '')
        name = stock.get('name', '')

        db.execute_query("""
            INSERT INTO stock_basic_info (symbol, name, industry, list_date)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                industry = VALUES(industry),
                updated_time = CURRENT_TIMESTAMP
        """, (
            symbol,
            name,
            stock.get('industry', ''),
            stock.get('list_date', None)
        ), fetch=False)

        if i % 10 == 0:
            print(f"  已导入 {i}/{len(csi_a50_stocks)}")

    print("5. 导入成分股关联...")
    # 导入关联信息
    for i, stock in enumerate(csi_a50_stocks, 1):
        db.execute_query("""
            INSERT INTO stock_index_constituent (index_code, symbol, weight, start_date, is_current)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                weight = VALUES(weight),
                updated_time = CURRENT_TIMESTAMP
        """, (
            'CSI_A50',
            stock.get('symbol', ''),
            stock.get('weight', 0.0),
            '2024-01-01',
            1
        ), fetch=False)

    print("6. 验证导入结果...")
    # 统计
    stats = db.execute_query("""
        SELECT 
            (SELECT COUNT(*) FROM stock_basic_info) as stocks,
            (SELECT COUNT(*) FROM index_info) as indexes,
            (SELECT COUNT(*) FROM stock_index_constituent) as constituents
    """)[0]

    print(f"\n📊 导入完成:")
    print(f"   股票基本信息: {stats['stocks']} 条")
    print(f"   指数信息: {stats['indexes']} 条")
    print(f"   成分股关联: {stats['constituents']} 条")

    # 显示前5只股票
    samples = db.execute_query("""
        SELECT symbol, name, industry 
        FROM stock_basic_info 
        ORDER BY symbol 
        LIMIT 5
    """)

    print("\n📋 前5只股票:")
    for sample in samples:
        print(f"   {sample['symbol']} - {sample['name']} ({sample.get('industry', '')})")

    print("\n🎉 P2阶段完成！数据库表创建 + 数据导入成功！")

except Exception as e:
    print(f"❌ 错误: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)