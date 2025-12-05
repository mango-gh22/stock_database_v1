# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\setup.py
# File Name: setup
# @ File: setup.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 20:58
"""
desc 
"""
# setup.py
from setuptools import setup, find_packages

setup(
    name="stock_database",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "tushare>=1.2.89",
        "mysql-connector-python>=8.0.0",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0",
    ],
)