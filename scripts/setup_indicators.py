# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\setup_indicators.py
# File Name: setup_indicators
# @ Author: mango-gh22
# @ Date：2025/12/20 19:31
"""
desc 
"""
# 4. 创建设置脚本
# cat > scripts / setup_indicators.py << 'EOF'
"""
设置技术指标模块
"""
import os
import sys


def create_structure():
    """创建目录结构"""
    dirs = [
        'src/indicators',
        'src/indicators/trend',
        'src/indicators/momentum',
        'src/indicators/volatility',
        'src/indicators/volume',
        'tests/indicators',
        'docs/P6',
        'data/cache/indicators',
        'scripts'
    ]

    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
        print(f"创建目录: {dir_path}")

    # 创建__init__.py文件
    init_files = [
        'src/indicators/__init__.py',
        'src/indicators/trend/__init__.py',
        'src/indicators/momentum/__init__.py',
        'src/indicators/volatility/__init__.py',
        'src/indicators/volume/__init__.py',
        'tests/indicators/__init__.py'
    ]

    for file_path in init_files:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('')
        print(f"创建文件: {file_path}")

    print("\n✅ 技术指标模块结构创建完成！")


if __name__ == "__main__":
    create_structure()