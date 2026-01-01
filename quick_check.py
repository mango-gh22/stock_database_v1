# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\quick_check.py
# File Name: quick_check
# @ Author: mango-gh22
# @ Date：2026/1/1 16:46
"""
desc 
"""
# quick_check.py
import sys
import os

sys.path.insert(0, r"E:\MyFile\stock_database_v1")

# 查看 run.py 的参数解析
run_path = os.path.join(r"E:\MyFile\stock_database_v1", "run.py")

if os.path.exists(run_path):
    with open(run_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找 argparse 配置
    import re

    # 查找 action 参数的可选值
    action_pattern = r'choices=\[([^\]]+)\]'
    matches = re.findall(action_pattern, content)

    print("run.py 支持的 action 参数:")
    print("=" * 60)

    for match in matches:
        if 'validate' in match or 'collect' in match or 'test' in match:
            print(f"  可选值: {match}")

    # 查找完整的帮助信息
    if 'def main():' in content:
        lines = content.split('\n')
        in_main = False
        help_lines = []

        for i, line in enumerate(lines):
            if 'def main():' in line:
                in_main = True
            elif in_main and line.strip().startswith('parser.add_argument'):
                help_lines.append(line.strip())
            elif in_main and line.strip() and not line.startswith(' ') and not line.startswith('def'):
                break

        print("\n参数配置:")
        print("-" * 60)
        for line in help_lines[:10]:  # 只显示前10行
            print(f"  {line}")
else:
    print(f"文件不存在: {run_path}")