# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\diagnose_query.py
# File Name: diagnose_query
# @ Author: mango-gh22
# @ Date：2025/12/20 18:16
"""
desc 创建诊断脚本
"""
# # 创建诊断脚本
# cat > scripts / diagnose_query.py << 'EOF'
"""
诊断查询问题的根源
"""
import sys

sys.path.append('.')

from src.query.query_engine import QueryEngine
from src.database.db_connector import DatabaseConnector
import pandas as pd


def test_direct_query():
    """直接测试数据库查询"""
    print("🧪 测试直接数据库查询...")

    try:
        # 创建数据库连接器
        connector = DatabaseConnector('config/database.yaml')

        # 直接执行查询
        sql = """
            SELECT 
                trade_date, 
                symbol,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                amount,
                change_percent as pct_change,
                pre_close_price as pre_close
            FROM stock_daily_data
            WHERE symbol = 'sh600519'
            ORDER BY trade_date DESC
            LIMIT 3
        """

        result = connector.execute_query(sql)

        if result:
            print(f"✅ 直接查询成功，返回 {len(result)} 行")
            print(f"第一行数据: {result[0]}")

            # 转换为DataFrame查看列名
            df = pd.DataFrame(result)
            print(f"\nDataFrame列名: {list(df.columns)}")
            print(f"DataFrame形状: {df.shape}")

            # 如果有列名，显示它们
            if hasattr(result[0], 'keys'):
                print(f"结果键名: {list(result[0].keys())}")
        else:
            print("❌ 查询返回空结果")

    except Exception as e:
        print(f"❌ 直接查询失败: {e}")
        import traceback
        traceback.print_exc()


def test_query_engine():
    """测试查询引擎"""
    print("\n🧪 测试查询引擎...")

    try:
        engine = QueryEngine()

        # 1. 首先查看查询引擎的内部状态
        print("检查查询引擎属性...")
        print(f"引擎类型: {type(engine)}")
        print(f"数据库连接器: {engine.db_connector}")

        # 2. 执行查询
        data = engine.query_daily_data('sh600519', '2024-01-01', '2024-01-10')

        print(f"\n查询结果:")
        print(f"数据条数: {len(data)}")
        print(f"列名列表: {list(data.columns)}")

        if not data.empty:
            print("\n前3行数据预览:")
            for i in range(min(3, len(data))):
                print(f"行 {i}: {dict(data.iloc[i])}")

    except Exception as e:
        print(f"❌ 查询引擎测试失败: {e}")
        import traceback
        traceback.print_exc()


def test_raw_sql():
    """测试原始SQL执行"""
    print("\n🧪 测试原始SQL执行...")

    try:
        from src.database.connector import DatabaseConnector

        connector = DatabaseConnector('config/database.yaml')

        # 测试1：使用当前查询语句
        sql1 = """
            SELECT 
                trade_date, 
                symbol,
                open_price,
                high_price,
                low_price,
                close_price
            FROM stock_daily_data
            WHERE symbol = 'sh600519'
            LIMIT 1
        """

        print("测试1 - 使用明确列名:")
        result1 = connector.execute_query(sql1)
        if result1:
            print(f"结果: {result1[0]}")

        # 测试2：使用别名
        sql2 = """
            SELECT 
                trade_date, 
                symbol,
                open_price as open,
                high_price as high,
                low_price as low,
                close_price as close
            FROM stock_daily_data
            WHERE symbol = 'sh600519'
            LIMIT 1
        """

        print("\n测试2 - 使用别名:")
        result2 = connector.execute_query(sql2)
        if result2:
            print(f"结果: {result2[0]}")

    except Exception as e:
        print(f"❌ 原始SQL测试失败: {e}")


if __name__ == "__main__":
    print("🔍 开始查询问题诊断...")
    test_direct_query()
    test_query_engine()
    test_raw_sql()
