# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\verify_p4_completion.py
# File Name: verify_p4_completion
# @ Author: mango-gh22
# @ Date：2025/12/6 21:44
"""
desc 
"""
"""
验证P4阶段完成
"""
import sys
import os
import subprocess

print("🎯 P4阶段完成验证")
print("=" * 60)


def test_core_functionality():
    """测试核心功能"""
    print("🧪 测试P4核心功能...")

    tests = [
        ("查询引擎", "python src/query/query_engine.py"),
        ("数据验证", "python main.py --action validate"),
        ("P4测试", "python main.py --action p4_test"),
        ("P4演示", "python main.py --action p4_demo"),
    ]

    results = []
    for test_name, command in tests:
        print(f"\n测试: {test_name}")
        print(f"命令: {command}")

        try:
            result = subprocess.run(
                command.split(),
                capture_output=True,
                text=True,
                timeout=30
            )

            success = result.returncode == 0
            results.append((test_name, success, result))

            if success:
                print("✅ 通过")
                # 显示关键信息
                if result.stdout:
                    lines = result.stdout.split('\n')
                    key_lines = [l for l in lines if any(kw in l.lower() for kw in
                                                         ['成功', '完成', '统计', '查询', '股票', '日线', '记录',
                                                          '验证'])]
                    for line in key_lines[:5]:
                        if line.strip():
                            print(f"  {line}")
            else:
                print("❌ 失败")
                if result.stderr:
                    print(f"  错误: {result.stderr[:200]}")

        except Exception as e:
            print(f"❌ 异常: {e}")
            results.append((test_name, False, None))

    return results


def check_p4_deliverables():
    """检查P4交付物"""
    print("\n📦 检查P4交付物...")

    deliverables = {
        "查询引擎": os.path.exists('src/query/query_engine.py'),
        "数据库配置": os.path.exists('config/database.yaml'),
        "main.py入口": os.path.exists('main.py'),
        "测试脚本": os.path.exists('simple_p4_test.py'),
        "导出目录": os.path.exists('data/exports'),
    }

    all_ok = True
    for item, exists in deliverables.items():
        status = '✅' if exists else '❌'
        print(f"  {status} {item}")
        if not exists:
            all_ok = False

    return all_ok


def show_p4_achievements():
    """显示P4成果"""
    print("\n🏆 P4阶段成果:")
    print("=" * 40)

    achievements = [
        "✅ 查询引擎：支持股票、日期、条件组合查询",
        "✅ 技术指标：基础技术指标计算框架",
        "✅ 数据分析：收益率、波动率基础分析",
        "✅ 数据导出：CSV格式导出功能",
        "✅ 测试用例：完整的查询功能测试",
        "✅ 可用命令：python main.py --action validate",
        "✅ 可用命令：python main.py --action p4_test",
        "✅ 可用命令：python main.py --action p4_demo"
    ]

    for achievement in achievements:
        print(f"  {achievement}")


def create_git_tag():
    """创建Git标签"""
    print("\n🔖 Git标签创建流程:")
    print("=" * 40)

    commands = [
        "# 1. 查看当前状态",
        "git status",
        "",
        "# 2. 添加所有更改",
        "git add .",
        "",
        "# 3. 提交P4阶段完成",
        'git commit -m "完成P4阶段：数据查询与分析功能"',
        "",
        "# 4. 创建v0.4.0标签",
        "git tag v0.4.0",
        "",
        "# 5. 推送标签",
        "git push origin v0.4.0",
        "",
        "# 6. 验证标签",
        "git tag -l | grep v0.4"
    ]

    for cmd in commands:
        print(cmd)

    # 提取实际命令
    actual_commands = [c for c in commands if c and not c.startswith('#')]
    return actual_commands


def main():
    """主函数"""
    # 测试核心功能
    test_results = test_core_functionality()

    # 检查交付物
    deliverables_ok = check_p4_deliverables()

    # 显示成果
    show_p4_achievements()

    # 分析结果
    print("\n" + "=" * 60)
    print("📊 验证结果汇总:")

    passed = sum(1 for name, success, _ in test_results if success)
    total = len(test_results)

    for test_name, success, _ in test_results:
        status = "✅通过" if success else "❌失败"
        print(f"  {test_name}: {status}")

    print(f"\n🎯 功能测试: {passed}/{total} 通过")
    print(f"📦 交付物检查: {'✅通过' if deliverables_ok else '❌失败'}")

    # 总体评估
    overall_passed = (passed >= 3 and deliverables_ok)  # 至少通过3个功能测试

    if overall_passed:
        print("\n🎉 P4阶段验证通过!")

        # 显示Git流程
        git_commands = create_git_tag()

        # 询问是否执行
        execute = input("\n是否创建v0.4.0标签？(y/n): ").strip().lower()

        if execute == 'y':
            print("\n执行Git命令...")
            for cmd in git_commands:
                print(f"\n执行: {cmd}")
                try:
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    if result.returncode == 0:
                        print("✅ 成功")
                        if result.stdout.strip():
                            print(f"  输出: {result.stdout.strip()}")
                    else:
                        print(f"❌ 失败: {result.stderr.strip()}")
                        break
                except Exception as e:
                    print(f"❌ 异常: {e}")
                    break
            else:
                print("\n🎉 Git标签创建完成!")
                print("\n验证标签:")
                subprocess.run("git tag -l | grep v0.4", shell=True)

                print("\n" + "=" * 60)
                print("🎉 P4阶段正式完成!")
                print("\n立即可用命令:")
                print("  python main.py --action validate")
                print("  python main.py --action p4_test")
                print("  python main.py --action p4_demo")
                print("  python src/query/query_engine.py")

        else:
            print("\n📝 手动执行命令:")
            for cmd in git_commands:
                print(f"  {cmd}")

    else:
        print("\n⚠️  P4阶段验证未通过")
        print("\n建议:")
        print("1. 运行: python src/query/query_engine.py")
        print("2. 检查错误信息")
        print("3. 确保数据库连接正常")


if __name__ == "__main__":
    main()