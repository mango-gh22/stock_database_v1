# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\check_main_py.py
# File Name: check_main_py
# @ Author: mango-gh22
# @ Date：2025/12/6 21:32
"""
desc 
"""
"""
检查main.py的validate动作
"""
import subprocess
import sys

print("🔍 检查main.py validate动作")
print("=" * 60)

# 运行validate命令
print("运行: python main.py --action validate")
result = subprocess.run(
    [sys.executable, 'main.py', '--action', 'validate'],
    capture_output=True,
    text=True
)

print(f"返回码: {result.returncode}")
print(f"标准输出长度: {len(result.stdout)} 字符")
print(f"标准错误长度: {len(result.stderr)} 字符")

if result.stdout:
    print("\n📋 标准输出:")
    print("-" * 40)
    print(result.stdout[:500])  # 显示前500字符

if result.stderr:
    print("\n❌ 标准错误:")
    print("-" * 40)
    print(result.stderr[:500])  # 显示前500字符

# 检查是否成功
if result.returncode == 0 and '数据验证报告' in result.stdout:
    print("\n✅ validate动作工作正常!")
else:
    print("\n⚠️  validate动作可能有问题")