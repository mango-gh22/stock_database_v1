# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_structure.py
# File Name: diagnose_structure
# @ Author: mango-gh22
# @ Date：2025/12/6 19:53
"""
desc 创建诊断脚本
"""
"""
诊断数据库表结构 - 修复版本
"""
import sys
sys.path.insert(0, '.')
from src.utils.logger import setup_logger

# 动态导入，避免导入错误
try:
    from src.database.connection import get_connection
    has_connection = True
except ImportError:
    has_connection = False
    print("⚠️ 无法导入get_connection，尝试直接连接数据库")

logger = setup_logger('diagnose')

def check_table_structure():
    """检查表结构"""
    if not has_connection:
        # 尝试直接连接
        import pymysql
        import yaml
        import os

        # 读取数据库配置
        config_path = os.path.join('config', 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                db_config = yaml.safe_load(f)['development']
        else:
            # 默认配置
            db_config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': '您的密码',
                'database': 'stock_database'
            }

        conn = pymysql.connect(**db_config)
    else:
        conn = get_connection()

    cursor = conn.cursor()

    print("🔍 检查stock_daily_data表结构")
    print("=" * 50)

    try:
        # 检查表结构
        cursor.execute("DESCRIBE stock_daily_data")
        columns = cursor.fetchall()

        print("📋 列名和类型:")
        for col in columns:
            print(f"  {col[0]:20} {col[1]:20} {col[2]:5} {col[3]:10}")

        # 检查是否有change列
        column_names = [col[0].lower() for col in columns]
        print(f"\n当前列名: {column_names}")

        has_change = 'change' in column_names
        has_price_change = 'price_change' in column_names
        print(f"❓ 是否存在change列: {has_change}")
        print(f"❓ 是否存在price_change列: {has_price_change}")

        # 检查保留关键字问题
        reserved_keywords = ['change', 'open', 'close', 'date', 'key']
        for col in columns:
            col_name = col[0].lower()
            if col_name in reserved_keywords:
                print(f"⚠️  警告: '{col_name}' 是MySQL保留关键字")

        # 测试直接查询
        print("\n🧪 测试直接查询:")
        try:
            # 尝试不同列名
            if has_price_change:
                test_query = "SELECT symbol, trade_date, price_change, close FROM stock_daily_data LIMIT 1"
                print(f"使用price_change列查询")
            elif has_change:
                test_query = "SELECT symbol, trade_date, `change`, close FROM stock_daily_data LIMIT 1"
                print(f"使用反引号change列查询")
            else:
                test_query = "SELECT * FROM stock_daily_data LIMIT 1"
                print(f"使用*查询")

            cursor.execute(test_query)
            row = cursor.fetchone()
            print("✅ 查询成功")

            # 获取列名
            cursor.execute("SHOW COLUMNS FROM stock_daily_data")
            col_names = [col[0] for col in cursor.fetchall()]
            print(f"列名列表: {col_names}")

        except Exception as e:
            print(f"❌ 查询失败: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_table_structure()