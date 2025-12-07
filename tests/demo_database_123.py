# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\demo_database_123.py
# File Name: demo_database_123
# @ Author: mango-gh22
# @ Date：2025/12/7 12:24
"""
desc 
"""
import os
from dotenv import load_dotenv

load_dotenv()  # 加载 .env
pwd = os.getenv('DB_PASSWORD')
print(f'密码是: [{pwd}]')  # 应该输出：密码是: [123]
print(f'长度: {len(pwd)}')  # 应该输出：长度: 3