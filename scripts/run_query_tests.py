# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\run_query_tests.py
# File Name: run_query_tests
# @ Author: mango-gh22
# @ Date：2025/12/21 19:46
"""
desc 
"""

"""
File: scripts/run_query_tests.py
Desc: 运行查询测试的便捷脚本
"""
import sys
import subprocess
from pathlib import Path


def run_query_tests():
    """运行查询测试"""
    project_root = Path(__file__).parent.parent

    print("运行查询系统测试...")
    print("=" * 60)

    # 运行测试
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         "tests/query/test_enhanced_query.py", "-v"],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    # 输出结果
    print(result.stdout)

    if result.stderr:
        print("错误输出:")
        print(result.stderr)

    print("=" * 60)
    print(f"退出码: {result.returncode}")

    return result.returncode


if __name__ == "__main__":
    exit_code = run_query_tests()
    sys.exit(exit_code)