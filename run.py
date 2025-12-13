# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\run.py
# File Name: run
# @ Author: mango-gh22
# @ Date：2025/12/7 20:57
"""
desc 
"""

# run.py - 放置在 E:\MyFile\stock_database_v1\
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data_processing.base_processor import main
main()