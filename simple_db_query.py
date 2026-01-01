# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\simple_db_query.py
# File Name: simple_db_query
# @ Author: mango-gh22
# @ Date：2025/12/28 20:16
"""
desc 
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\simple_db_query.py
# File Name: simple_db_query
"""
简单数据库查询 - 直接在命令行查看数据
"""

import mysql.connector
import pandas as pd
from tabulate import tabulate

# 连接到数据库
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="stock_user",
    password="",  # 你的密码
    database="stock_database"
)

cursor = conn.cursor(dictionary=True)

print("=" * 80)
print("📊 股票数据库查询")
print("=" * 80)

# 1. 显示表结构
print("\n1. 📋 表结构")
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print(f"数据库中有 {len(tables)} 个表:")
for table in tables:
    table_name = list(table.values())[0]
    cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
    count = cursor.fetchone()['count']
    print(f"  {table_name:25} - {count:10,} 行")

# 2. 查看 stock_daily_data 表结构
print("\n2. 🔍 stock_daily_data 表结构")
cursor.execute("DESC stock_daily_data")
columns = cursor.fetchall()
print("字段列表:")
for col in columns[:15]:  # 只显示前15个字段
    print(f"  {col['Field']:25} {col['Type']:20} {col['Null']:5}")

if len(columns) > 15:
    print(f"  ... 还有 {len(columns) - 15} 个字段")

# 3. 查看最新数据
print("\n3. 🕒 最新10条数据")
cursor.execute("""
    SELECT 
        id, symbol, trade_date, 
        open_price, close_price, volume,
        created_time
    FROM stock_daily_data 
    ORDER BY created_time DESC 
    LIMIT 10
""")

latest_data = cursor.fetchall()

if latest_data:
    # 使用 pandas 格式化输出
    df = pd.DataFrame(latest_data)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
else:
    print("  没有找到数据")

# 4. 统计数据
print("\n4. 📈 统计数据")
cursor.execute("SELECT COUNT(*) as total FROM stock_daily_data")
total = cursor.fetchone()['total']

cursor.execute("SELECT COUNT(DISTINCT symbol) as symbols FROM stock_daily_data")
symbols = cursor.fetchone()['symbols']

cursor.execute("SELECT MIN(trade_date) as earliest, MAX(trade_date) as latest FROM stock_daily_data")
dates = cursor.fetchone()

print(f"   总记录数: {total:,}")
print(f"   股票数量: {symbols}")
print(f"   日期范围: {dates['earliest']} 到 {dates['latest']}")

# 5. 查看今天的数据
print("\n5. 📅 今天的数据")
today = pd.Timestamp.now().strftime('%Y-%m-%d')
cursor.execute("SELECT COUNT(*) as today_count FROM stock_daily_data WHERE trade_date = %s", (today,))
today_count = cursor.fetchone()['today_count']
print(f"   今天 ({today}) 有 {today_count} 条记录")

if today_count > 0:
    cursor.execute("""
        SELECT symbol, trade_date, open_price, close_price, volume, created_time
        FROM stock_daily_data 
        WHERE trade_date = %s
        ORDER BY created_time DESC
        LIMIT 5
    """, (today,))
    today_data = cursor.fetchall()

    if today_data:
        df_today = pd.DataFrame(today_data)
        print(tabulate(df_today, headers='keys', tablefmt='psql', showindex=False))

conn.close()

print("\n" + "=" * 80)
print("✅ 查询完成")
print("=" * 80)