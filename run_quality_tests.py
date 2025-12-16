# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\run_quality_tests.py
# File Name: run_quality_tests
# @ Author: mango-gh22
# @ Date：2025/12/14 19:35
"""
desc 
"""

# run_quality_tests.py
"""
运行所有质量模块测试
"""

import sys
import os
import subprocess
import time


def run_command(cmd):
    """运行命令并返回结果"""
    print(f"\n🚀 运行命令: {cmd}")
    print("-" * 60)

    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        elapsed = time.time() - start_time

        print(f"返回码: {result.returncode}")
        if result.stdout:
            print("输出:")
            print(result.stdout)
        if result.stderr and result.returncode != 0:
            print("错误:")
            print(result.stderr)

        print(f"耗时: {elapsed:.2f}秒")
        return result.returncode == 0

    except Exception as e:
        print(f"命令执行失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 70)
    print("📋 股票数据库 - 质量模块测试套件")
    print("=" * 70)

    tests = [
        ("模块导入测试",
         "python -c \"from src.processors.validator import DataValidator; from src.processors.adjustor import StockAdjustor; print('✅ 模块导入成功')\""),
        ("简化解耦测试", "python test_quality_模块.py"),
        ("验证器单元测试", "python -m unittest tests.processors.test_validator -v"),
        ("复权计算器单元测试", "python -m unittest tests.processors.test_adjustor -v"),
        ("集成测试", "python test_integration.py"),
    ]

    results = []

    for test_name, test_cmd in tests:
        print(f"\n📊 测试: {test_name}")
        success = run_command(test_cmd)
        results.append((test_name, success))
        time.sleep(1)  # 短暂延迟

    # 汇总结果
    print("\n" + "=" * 70)
    print("📈 测试结果汇总")
    print("=" * 70)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{test_name:30} {status}")

    print(f"\n总计: {passed}/{total} 通过 ({passed / total * 100:.1f}%)")

    if passed == total:
        print("\n🎉 所有测试通过!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} 个测试失败")
        return 1


if __name__ == "__main__":
    sys.exit(main())