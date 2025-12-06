# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_main_py_complete.py
# File Name: fix_main_py_complete
# @ Author: mango-gh22
# @ Date：2025/12/6 21:39
"""
desc 
"""
"""
修复main.py完整版本
"""
import re

print("🔧 修复main.py完整版本")
print("=" * 60)

with open('main.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. 首先检查action参数定义
print("1. 检查action参数定义...")

# 查找action参数
action_pattern = r'add_argument.*--action.*choices=\[([^\]]+)\]'
match = re.search(action_pattern, content, re.DOTALL)

if match:
    actions_str = match.group(1)
    print(f"找到action参数: {actions_str[:100]}...")

    # 检查是否包含validate
    if 'validate' not in actions_str:
        print("❌ action参数中没有validate，需要添加...")

        # 添加validate
        if actions_str.endswith(','):
            new_actions = actions_str + " 'validate'"
        else:
            new_actions = actions_str + ", 'validate'"

        new_content = content.replace(actions_str, new_actions)
        content = new_content
        print("✅ 已添加validate到action参数")
    else:
        print("✅ action参数中已有validate")
else:
    print("❌ 未找到action参数定义")

# 2. 检查validate_data函数是否在main函数中调用
print("\n2. 检查validate_data函数调用...")

# 查找main函数中的action处理
if 'elif action == "validate":' in content:
    print("✅ 找到validate动作处理")

    # 检查是否调用validate_data()
    start = content.find('elif action == "validate":')
    end = content.find('\\n    elif', start + 1)
    if end == -1:
        end = len(content)

    validate_block = content[start:end]

    if 'validate_data()' in validate_block:
        print("✅ validate动作调用validate_data()")
    else:
        print("❌ validate动作没有调用validate_data()，修复...")

        new_block = '''
    elif action == "validate":
        validate_data()'''

        new_content = content[:start] + new_block + content[end:]
        content = new_content
        print("✅ 已修复validate动作处理")
else:
    print("❌ 未找到validate动作处理，需要添加...")

    # 在合适位置添加validate动作处理
    # 查找其他action作为参考
    if 'elif action == "p4_full_test":' in content:
        # 在p4_full_test之后添加
        insert_pos = content.find('elif action == "p4_full_test":')
        # 找到这个块的结束
        test_end = content.find('\\n    elif', insert_pos + 1)
        if test_end == -1:
            test_end = len(content)

        # 添加validate
        validate_code = '''
    elif action == "validate":
        validate_data()'''

        new_content = content[:test_end] + validate_code + content[test_end:]
        content = new_content
        print("✅ 已添加validate动作处理")
    else:
        print("⚠️  无法找到合适位置添加validate")

# 3. 检查p4_test动作
print("\n3. 检查p4_test动作...")

# 首先检查action参数中是否有p4_test
if "'p4_test'" not in content and '"p4_test"' not in content:
    print("action参数中没有p4_test，添加...")

    # 找到action参数并添加
    action_pattern = r'choices=\[([^\]]+)\]'
    match = re.search(action_pattern, content)

    if match:
        actions_str = match.group(1)
        if actions_str.endswith(','):
            new_actions = actions_str + " 'p4_test'"
        else:
            new_actions = actions_str + ", 'p4_test'"

        new_content = content.replace(actions_str, new_actions)
        content = new_content
        print("✅ 已添加p4_test到action参数")

# 检查p4_test动作处理
if 'elif action == "p4_test":' not in content:
    print("添加p4_test动作处理...")

    # 在validate之前添加
    if 'elif action == "validate":' in content:
        validate_pos = content.find('elif action == "validate":')

        p4_test_code = '''
    elif action == "p4_test":
        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)

        try:
            from src.query.query_engine import test_query_engine
            test_query_engine()
        except Exception as e:
            print(f"❌ P4测试失败: {e}")
            import traceback
            traceback.print_exc()'''

        new_content = content[:validate_pos] + p4_test_code + '\\n' + content[validate_pos:]
        content = new_content
        print("✅ 已添加p4_test动作处理")

# 4. 保存修改
with open('main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("\n" + "=" * 60)
print("✅ 修复完成!")
print("\n测试命令:")
print("  python main.py --action validate")
print("  python main.py --action p4_test")
print("  python main.py --action p4_full_test")