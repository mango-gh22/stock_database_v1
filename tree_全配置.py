# _*_ coding: utf-8 _*_
# project path >>> E:\MyFile
# @ File: tree_全配置.py
# @ Author: m_mango
# @ PyCharm
# @ date：2025/10/14 16:24
"""
desc 
"""
# tree_全配置.py
from pathlib import Path
import sys

# ----------------- 可自己改的 3 个参数 -----------------
ROOT_DIR      = Path(__file__).resolve().parent   # 默认：脚本所在目录
IGNORE_DIRS   = {'.git', '__pycache__', '.idea', 'node_modules', '.pytest_cache',
                 '.venv', 'notedb', 'backup'
                 }  # 想忽略的文件夹
EXPORT_FILE   = True                              # 是否同时导出到 ice结构图.txt
# -------------------------------------------------------

def build_tree(path: Path, prefix: str = ''):
    """递归生成目录树字符串"""
    items = sorted([p for p in path.iterdir() if p.name not in IGNORE_DIRS],
                   key=lambda p: (p.is_file(), p.name.lower()))
    lines = []
    for idx, item in enumerate(items):
        is_last = idx == len(items) - 1
        lines.append(f"{prefix}{'└── ' if is_last else '├── '}{item.name}")
        if item.is_dir():
            extension = '    ' if is_last else '│   '
            lines.extend(build_tree(item, prefix + extension))
    return lines

if __name__ == '__main__':
    tree_lines = [ROOT_DIR.name + '/'] + build_tree(ROOT_DIR)
    tree_str = '\n'.join(tree_lines)
    print(tree_str)
    if EXPORT_FILE:
        (ROOT_DIR / 'ice结构图.txt').write_text(tree_str, encoding='utf-8')
        print('\n已导出 → ice结构图.txt', file=sys.stderr)