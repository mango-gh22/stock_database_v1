# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\setup_database_complete.py
# File Name: setup_database_complete
# @ Author: mango-gh22
# @ Date：2025/12/6 17:39
"""
desc 创建数据库表和用户
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的数据库设置脚本
"""

import sys
import os
import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔧 完整的数据库设置")
print("=" * 60)

# 从配置文件中读取数据库配置
import yaml


def load_config():
    """加载配置文件"""
    try:
        with open('config/database.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config['database']['mysql']
    except Exception as e:
        print(f"❌ 加载配置文件失败: {e}")
        # 使用默认配置
        return {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',  # 先用root创建用户
            'password': '',
            'database': 'stock_database'
        }


def create_user_and_database():
    """创建用户和数据库"""
    config = load_config()

    try:
        # 使用root连接（或者已有权限的用户）
        print(f"🔗 使用root用户连接到MySQL...")
        root_conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user='root',  # 或者您现有的MySQL用户名
            password=input("请输入MySQL root密码: "),
            charset='utf8mb4'
        )

        with root_conn.cursor() as cursor:
            # 1. 创建数据库
            print(f"📁 创建数据库 {config['database']}...")
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

            # 2. 创建用户（如果不存在）
            print(f"👤 创建用户 {config['user']}...")
            cursor.execute(
                f"CREATE USER IF NOT EXISTS '{config['user']}'@'localhost' IDENTIFIED BY '{config['password']}'")

            # 3. 授予权限
            print(f"🔑 授予权限给 {config['user']}...")
            cursor.execute(f"GRANT ALL PRIVILEGES ON {config['database']}.* TO '{config['user']}'@'localhost'")
            cursor.execute("FLUSH PRIVILEGES")

        root_conn.commit()
        root_conn.close()
        print("✅ 用户和数据库创建成功")
        return True

    except Exception as e:
        print(f"❌ 创建用户和数据库失败: {e}")
        print("\n⚠️  如果root连接失败，请手动执行以下SQL:")
        print(f"    CREATE DATABASE IF NOT EXISTS {config['database']} CHARACTER SET utf8mb4;")
        print(f"    CREATE USER IF NOT EXISTS '{config['user']}'@'localhost' IDENTIFIED BY '{config['password']}';")
        print(f"    GRANT ALL PRIVILEGES ON {config['database']}.* TO '{config['user']}'@'localhost';")
        print("    FLUSH PRIVILEGES;")
        return False


def create_tables():
    """创建所有表"""
    config = load_config()

    try:
        print(f"\n📊 连接到数据库 {config['database']}...")
        conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            charset='utf8mb4'
        )

        with conn.cursor() as cursor:
            # 读取现有的SQL文件
            sql_file = 'scripts/schema/create_tables_fixed.sql'
            if os.path.exists(sql_file):
                print(f"📄 使用现有的SQL文件: {sql_file}")
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
            else:
                print("⚠️  使用内置的SQL语句")
                sql_content = """
-- 股票基本信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    name VARCHAR(100) NOT NULL COMMENT '股票名称',
    exchange VARCHAR(10) NOT NULL COMMENT '交易所',
    industry VARCHAR(100) COMMENT '行业分类',
    listing_date DATE COMMENT '上市日期',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否活跃',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基本信息表';

-- 日线行情数据表
CREATE TABLE IF NOT EXISTS daily_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL COMMENT '交易日期',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    open DECIMAL(10, 4) COMMENT '开盘价',
    high DECIMAL(10, 4) COMMENT '最高价',
    low DECIMAL(10, 4) COMMENT '最低价',
    close DECIMAL(10, 4) COMMENT '收盘价',
    volume BIGINT COMMENT '成交量(股)',
    amount DECIMAL(20, 4) COMMENT '成交额(元)',
    change DECIMAL(10, 4) COMMENT '涨跌额',
    pct_change DECIMAL(10, 4) COMMENT '涨跌幅(%)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_symbol (trade_date, symbol),
    INDEX idx_symbol (symbol),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日线行情数据表';
"""

            # 执行SQL
            sql_statements = sql_content.strip().split(';')

            for i, sql in enumerate(sql_statements):
                sql = sql.strip()
                if sql:
                    print(f"  执行SQL {i + 1}/{len(sql_statements)}...")
                    try:
                        cursor.execute(sql)
                    except Exception as e:
                        print(f"    ⚠️  SQL执行跳过: {e}")

        conn.commit()
        conn.close()
        print("✅ 数据库表创建成功")
        return True

    except Exception as e:
        print(f"❌ 创建表失败: {e}")
        return False


def import_sample_data():
    """导入示例数据"""
    print("\n📥 导入示例数据...")

    config = load_config()

    try:
        conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            charset='utf8mb4'
        )

        with conn.cursor() as cursor:
            # 检查是否已有数据
            cursor.execute("SELECT COUNT(*) FROM stock_basic")
            count = cursor.fetchone()[0]

            if count > 0:
                print(f"⚠️  已有 {count} 条股票数据，跳过导入")
                return True

            # 导入中证A50示例数据
            print("  导入中证A50示例数据...")
            a50_stocks = [
                ("000001.SZ", "平安银行", "SZ", "银行", "1991-04-03", 1),
                ("000858.SZ", "五粮液", "SZ", "食品饮料", "1998-04-27", 1),
                ("000333.SZ", "美的集团", "SZ", "家用电器", "2013-09-18", 1),
                ("002594.SZ", "比亚迪", "SZ", "汽车", "2011-06-30", 1),
                ("600519.SH", "贵州茅台", "SH", "食品饮料", "2001-08-27", 1),
            ]

            insert_sql = """
            INSERT INTO stock_basic (symbol, name, exchange, industry, listing_date, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), industry=VALUES(industry)
            """

            cursor.executemany(insert_sql, a50_stocks)

            # 导入一些示例日线数据
            print("  导入示例日线数据...")
            import datetime
            base_date = datetime.date(2024, 12, 1)

            daily_data = []
            for symbol, name, exchange, industry, listing_date, is_active in a50_stocks:
                for i in range(5):  # 每只股票导入5天数据
                    trade_date = base_date - datetime.timedelta(days=i)
                    close_price = 100.0 + i * 2.5
                    daily_data.append((
                        trade_date.strftime('%Y-%m-%d'),
                        symbol,
                        close_price - 1.0,  # open
                        close_price + 1.0,  # high
                        close_price - 1.0,  # low
                        close_price,  # close
                        1000000 + i * 100000,  # volume
                        100000000 + i * 10000000,  # amount
                        1.5 + i * 0.1,  # change
                        1.5 + i * 0.1,  # pct_change
                    ))

            if daily_data:
                daily_sql = """
                INSERT INTO daily_data 
                (trade_date, symbol, open, high, low, close, volume, amount, change, pct_change)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close)
                """
                cursor.executemany(daily_sql, daily_data)

        conn.commit()
        conn.close()
        print(f"✅ 导入 {len(a50_stocks)} 只股票和 {len(daily_data)} 条日线数据")
        return True

    except Exception as e:
        print(f"❌ 导入数据失败: {e}")
        return False


def verify_setup():
    """验证设置"""
    print("\n🔍 验证设置...")

    config = load_config()

    try:
        conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            charset='utf8mb4'
        )

        with conn.cursor() as cursor:
            # 检查表
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📊 数据库表数量: {len(tables)}")
            print(f"📋 表列表: {tables}")

            # 检查数据
            required_tables = ['stock_basic', 'daily_data']
            for table in required_tables:
                if table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  {table}: {count} 条记录")
                else:
                    print(f"  ❌ {table}: 表不存在")

        conn.close()

        # 测试SQLAlchemy连接
        print("\n🔗 测试SQLAlchemy连接...")
        from src.database.connection import engine
        with engine.connect() as conn:
            result = conn.execute("SELECT DATABASE()")
            db_name = list(result)[0][0]
            print(f"✅ SQLAlchemy连接成功: {db_name}")

        return True

    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False


def main():
    """主函数"""
    print("欢迎使用股票数据库设置工具")
    print("=" * 60)

    print("\n步骤1: 创建用户和数据库")
    if not create_user_and_database():
        choice = input("是否继续尝试创建表？(y/n): ")
        if choice.lower() != 'y':
            return

    print("\n步骤2: 创建数据库表")
    if not create_tables():
        print("⚠️  表创建失败，继续尝试导入数据...")

    print("\n步骤3: 导入示例数据")
    import_sample_data()

    print("\n步骤4: 验证设置")
    verify_setup()

    print("\n🎉 设置完成!")
    print("\n📋 下一步:")
    print("1. 测试查询引擎: python main.py --action p4_query_test")
    print("2. 测试技术指标: python main.py --action p4_indicators_test")
    print("3. 运行完整测试: python main.py --action p4_full_test")


if __name__ == "__main__":
    main()