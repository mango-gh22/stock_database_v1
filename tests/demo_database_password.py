# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\demo_database_password.py
# File Name: demo_dabase_password
# @ Author: mango-gh22
# @ Date：2025/12/7 14:10
"""
desc 
"""
import os
from dotenv import load_dotenv
import pymysql
import yaml

# 获取项目根目录路径（假设脚本在 tests/ 目录下）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 1. 加载环境变量 - 指定 .env 文件在项目根目录
env_path = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path=env_path)

CONFIG_PWD = os.getenv('DB_PASSWORD')
print(f"1. 从 .env 文件读取的密码是: [{CONFIG_PWD}] (长度: {len(CONFIG_PWD)})")

# 2. 使用绝对路径读取 database.yaml
config_path = os.path.join(PROJECT_ROOT, 'config', 'database.yaml')
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    mysql_config = config['database']['mysql']

HOST = mysql_config['host']
USER = mysql_config['user']
DATABASE = mysql_config['database']
print(f"2. 从YAML读取的连接参数: 用户『{USER}』@『{HOST}』，数据库『{DATABASE}』")

# 3. 尝试连接
print("3. 正在尝试使用以上密码连接数据库...")
try:
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=CONFIG_PWD,
        database=DATABASE,
        charset='utf8mb4',
        port=mysql_config.get('port', 3306)  # 添加端口
    )
    print("   ✅ 连接成功！.env中的密码有效。")

    # 额外验证：执行一个简单查询
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1+1 AS result")
        result = cursor.fetchone()
        # 方法1：使用数字索引（因为SELECT只有一个字段）
        print(f"   ✅ 查询测试成功: 1+1 = {result[0]}")
        # 或者 方法2：指定返回字典游标（如果已设置 cursorclass=pymysql.cursors.DictCursor）

    connection.close()
except pymysql.err.OperationalError as e:
    if e.args[0] == 1045:
        print(f"   ❌ 连接失败：密码错误 (错误 1045)。")
        print(f"   💡 请确认MySQL中用户『{USER}』的密码确实是『{CONFIG_PWD}』。")
        print(f"   💡 在MySQL中执行: ALTER USER '{USER}'@'localhost' IDENTIFIED BY '{CONFIG_PWD}';")
    else:
        print(f"   ❌ 连接失败，其他错误: {e}")
except Exception as e:
    print(f"   ❌ 连接失败，未知错误: {e}")