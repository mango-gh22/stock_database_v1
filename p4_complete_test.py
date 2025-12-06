# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\p4_complete_test.py
# File Name: p4_complete_test
# @ Author: mango-gh22
# @ Date：2025/12/6 20:13
"""
desc 
"""
"""
P4阶段完整测试脚本
"""
import sys
import os
import subprocess


def run_test(test_name, command):
    """运行测试"""
    print(f"\n🔧 {test_name}")
    print("-" * 40)
    print(f"执行: {command}")

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        print("✅ 成功")
        # 显示部分输出
        if result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            for line in lines[:10]:  # 只显示前10行
                print(f"  {line}")
            if len(lines) > 10:
                print(f"  ... (共{len(lines)}行)")
    else:
        print("❌ 失败")
        if result.stderr.strip():
            print(f"错误: {result.stderr[:200]}...")

    return result.returncode


def main():
    print("🚀 P4阶段：数据查询与分析 - 完整测试")
    print("=" * 60)

    tests = [
        # 1. 验证现有数据
        ("验证数据", "python main.py --action validate"),

        # 2. 测试已有P4命令
        ("P4查询测试", "python main.py --action p4_query_test"),

        # 3. 测试P4指标
        ("P4指标测试", "python main.py --action p4_indicators_test"),

        # 4. 测试P4导出
        ("P4导出测试", "python main.py --action p4_export_test"),

        # 5. 测试完整P4
        ("P4完整测试", "python main.py --action p4_full_test"),

        # 6. 直接查询
        ("直接查询", "python main.py --action query --symbol 000001.SZ --limit 3"),
    ]

    passed_tests = 0
    total_tests = len(tests)

    for test_name, command in tests:
        returncode = run_test(test_name, command)
        if returncode == 0:
            passed_tests += 1

    print("\n" + "=" * 60)
    print(f"📋 测试结果: {passed_tests}/{total_tests} 通过")

    if passed_tests >= 3:
        print("\n🎉 P4阶段基本功能可用！")

        # 运行简化查询引擎测试
        print("\n🔍 运行简化查询引擎测试...")
        try:
            sys.path.insert(0, '.')
            from src.query.simple_query_engine import test_simple_engine
            test_simple_engine()
        except Exception as e:
            print(f"⚠️  简化引擎测试失败: {e}")

        print("\n📝 下一步:")
        print("1. 如果需要，运行数据库修复:")
        print("   mysql -u root -p < fix_reserved_keywords.sql")
        print("2. 创建Git标签:")
        print("   git add .")
        print("   git commit -m '完成P4阶段：数据查询与分析'")
        print("   git tag v0.4.0")
        print("   git push origin v0.4.0")
    else:
        print("\n⚠️  部分测试失败，需要检查")
        print("\n建议运行快速修复:")
        print("  python add_p4_commands.py")
        print("  python quick_p4_test.py")


if __name__ == "__main__":
    main()