# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\analyze_main.py
# File Name: analyze_main
# @ Author: mango-gh22
# @ Date：2025/12/6 21:35
"""
desc 
"""
"""
分析main.py结构
"""
import re

print("🔍 分析main.py结构")
print("=" * 60)

with open('main.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. 查找validate_data函数
print("1. 查找validate_data函数...")
if 'def validate_data():' in content:
    print("✅ 找到validate_data函数")

    # 提取函数内容
    start = content.find('def validate_data():')
    end = content.find('\\ndef ', start + 1)
    if end == -1:
        end = len(content)

    func_content = content[start:end]
    print(f"函数长度: {len(func_content)} 字符")
    print(f"前200字符: {func_content[:200]}")

    # 检查是否有print语句
    if 'print(' in func_content:
        print("✅ 函数中有print语句")
    else:
        print("❌ 函数中没有print语句")
else:
    print("❌ 未找到validate_data函数")

# 2. 查找validate动作处理
print("\n2. 查找validate动作处理...")
if 'elif action == "validate":' in content:
    print("✅ 找到validate动作处理")

    # 提取处理代码
    start = content.find('elif action == "validate":')
    end = content.find('\\n    elif', start + 1)
    if end == -1:
        end = len(content)

    action_code = content[start:end]
    print(f"处理代码: {action_code.strip()}")

    # 检查是否调用validate_data
    if 'validate_data()' in action_code:
        print("✅ validate动作调用validate_data()")
    else:
        print("❌ validate动作没有调用validate_data()")
else:
    print("❌ 未找到validate动作处理")

# 3. 查找所有action
print("\n3. 查找所有action定义...")

# 查找argument parser中的choices
pattern = r'choices=\[([^\]]+)\]'
match = re.search(pattern, content)
if match:
    actions = match.group(1)
    print(f"ArgumentParser中的actions: {actions}")
else:
    print("未找到ArgumentParser中的choices")

# 查找所有elif action ==
actions = re.findall(r'elif action == "([^"]+)"', content)
print(f"处理中的actions: {actions}")

print("\n" + "=" * 60)
print("分析完成!")