# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\demo_database_mysql.py
# File Name: demo_database_mysql
# @ Author: mango-gh22
# @ Date：2025/12/7 14:02
"""
desc 
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\demo_database_mysql.py
# File Name: demo_database_mysql
# @ Author: mango-gh22
# @ Date：2025/12/7 14:02
"""
数据库连接测试脚本
"""
import os
from dotenv import load_dotenv
import pymysql
import yaml

# 1. 加载环境变量
load_dotenv()
CONFIG_PWD = os.getenv('DB_PASSWORD')  # 从.env读取的密码
print(f"1. 从 .env 文件读取的密码是: [{CONFIG_PWD}] (长度: {len(CONFIG_PWD)})")

# 2. 从 database.yaml 读取其他连接参数
with open('config/database.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    mysql_config = config['database']['mysql']

HOST = mysql_config['host']
USER = mysql_config['user']
DATABASE = mysql_config['database']
print(f"2. 从YAML读取的连接参数: 用户『{USER}』@『{HOST}』，数据库『{DATABASE}』")

# 3. 尝试使用这个密码进行连接（关键测试）
print("3. 正在尝试使用以上密码连接数据库...")
try:
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=CONFIG_PWD,  # 使用.env中的密码
        database=DATABASE,
        charset='utf8mb4'
    )
    print("   ✅ 连接成功！.env中的密码与数据库密码匹配。")
    connection.close()
except pymysql.err.OperationalError as e:
    # 错误代码 1045 代表权限拒绝，即密码错误
    error_code = e.args[0] if e.args else None
    if error_code == 1045:
        print(f"   ❌ 连接失败：密码错误 (错误 1045)。")
        print(f"   💡 这表明你为用户『{USER}』设置的数据库密码不是『{CONFIG_PWD}』。")
    else:
        print(f"   ❌ 连接失败，MySQL操作错误: {e}")
except Exception as e:
    # 安全地处理所有其他异常
    error_msg = str(e)
    print(f"   ❌ 连接失败，未知错误: {error_msg}")
