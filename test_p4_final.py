
"""
P4阶段最终测试脚本
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 P4阶段最终测试")
    print("=" * 60)

    try:
        # 1. 测试数据库连接
        print("\n🔗 1. 测试数据库连接...")
        from src.database.connection import test_connection
        if not test_connection():
            print("❌ 数据库连接失败，终止测试")
            return

        # 2. 测试查询引擎
        print("\n🚀 2. 测试查询引擎...")
        from src.query.query_engine import run_p4_test
        run_p4_test()

        # 3. 测试main.py命令
        print("\n📝 3. 测试main.py命令...")
        import subprocess

        # 测试validate命令
        print("   运行: python main.py --action validate")
        result = subprocess.run(
            ['python', 'main.py', '--action', 'validate'],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print("✅ validate命令执行成功")
            # 显示关键信息
            lines = result.stdout.split('\n')
            for line in lines:
                if any(keyword in line for keyword in ['股票总数', '日线数据', '总记录数', '数据验证报告']):
                    print(f"   {line}")
        else:
            print(f"❌ validate命令失败: {result.stderr[:200]}")

        print("\n" + "=" * 60)
        print("🎉 P4阶段测试完成!")

    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
