
"""
P4快速测试 - 使用简单查询引擎
"""
import sys
import os
sys.path.insert(0, '.')

def main():
    print("🚀 P4阶段快速测试")
    print("=" * 50)

    try:
        # 直接使用简单查询引擎
        from src.query.simple_query import quick_test
        quick_test()

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
