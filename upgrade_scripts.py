# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\upgrade_scripts.py
# File Name: upgrade_scripts
# @ Author: mango-gh22
# @ Date：2026/1/10 22:53
"""
desc 
"""

# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键升级脚本：自动删除冗余文件并移动文件到正确位置
"""

import os
import sys
import shutil
from datetime import datetime
from pathlib import Path


def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent


def delete_redundant_files():
    """删除冗余脚本"""
    redundant_files = [
        # 删除的脚本
        'scripts/download_a50_batch.py',
        'scripts/update_a50_factors.py',
        'scripts/update_table_schema.py',
        'scripts/test_batch_run.py',
        'scripts/quick_factor_test.py',
        'scripts/quick_validate_all.py',
        'src/config/pipeline_config_loader.py',
        'verify_data.py',  # 根目录下的

        # 备份文件
        'scripts/download_a50_complete.py.backup',
        'run.py.backup',
    ]

    root = get_project_root()
    deleted_count = 0

    print("\n" + "=" * 60)
    print("🗑️  清理冗余文件")
    print("=" * 60)

    for file_path in redundant_files:
        full_path = root / file_path
        if full_path.exists():
            try:
                if full_path.is_dir():
                    shutil.rmtree(full_path)
                else:
                    full_path.unlink()
                print(f"✅ 删除: {file_path}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ 删除失败 {file_path}: {e}")
        else:
            print(f"⏭️  跳过: {file_path} (不存在)")

    print(f"\n共删除 {deleted_count} 个冗余文件")
    return deleted_count


def move_files_to_correct_location():
    """移动文件到正确位置"""
    print("\n" + "=" * 60)
    print("📂 整理文件位置")
    print("=" * 60)

    moves = [
        # (源, 目标)
        ('verify_data.py', 'scripts/verify_data.py'),
    ]

    root = get_project_root()
    moved_count = 0

    for src, dst in moves:
        src_path = root / src
        dst_path = root / dst

        if src_path.exists():
            try:
                # 确保目标目录存在
                dst_path.parent.mkdir(parents=True, exist_ok=True)

                # 移动文件
                shutil.move(str(src_path), str(dst_path))
                print(f"✅ 移动: {src} -> {dst}")
                moved_count += 1
            except Exception as e:
                print(f"❌ 移动失败 {src}: {e}")

    print(f"\n共移动 {moved_count} 个文件")
    return moved_count


def create_upgrade_marker():
    """创建升级标记文件"""
    marker_file = get_project_root() / '.upgraded_v1.0'
    with open(marker_file, 'w', encoding='utf-8') as f:
        f.write(f"Upgrade completed at {datetime.now().isoformat()}\n")
        f.write("Redundant files removed and scripts consolidated.\n")

    print(f"\n✅ 升级标记已创建: {marker_file}")


def verify_cleanup():
    """验证清理结果"""
    print("\n" + "=" * 60)
    print("🔍 验证清理结果")
    print("=" * 60)

    root = get_project_root()

    # 检查应删除的文件是否还存在
    check_files = [
        'scripts/download_a50_batch.py',
        'scripts/update_a50_factors.py',
        'verify_data.py',  # 根目录下的
    ]

    issues = 0

    for file_path in check_files:
        full_path = root / file_path
        if full_path.exists():
            print(f"❌ 问题: {file_path} 仍然存在")
            issues += 1
        else:
            print(f"✅ 正常: {file_path} 已删除")

    # 检查新文件是否存在
    new_files = [
        'scripts/verify_data.py',
        'run.py',  # 应该是新的统一入口
    ]

    for file_path in new_files:
        full_path = root / file_path
        if full_path.exists():
            print(f"✅ 正常: {file_path} 存在")
        else:
            print(f"❌ 问题: {file_path} 不存在")
            issues += 1

    if issues == 0:
        print("\n🎉 所有检查通过！升级完成")
    else:
        print(f"\n⚠️  发现 {issues} 个问题，请手动检查")

    return issues == 0


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("🚀 股票数据库系统升级工具 v1.0")
    print("=" * 60)
    print("此脚本将自动清理冗余文件并整理项目结构")
    print("\n⚠️  警告：此操作将永久删除冗余文件，请确保已备份重要数据！")

    confirm = input("\n继续升级吗？(yes/no): ")

    if confirm.lower() != 'yes':
        print("\n升级已取消")
        return 1

    # 执行升级步骤
    print("\n开始执行升级...")

    # 1. 删除冗余文件
    deleted = delete_redundant_files()

    # 2. 移动文件
    moved = move_files_to_correct_location()

    # 3. 创建升级标记
    create_upgrade_marker()

    # 4. 验证结果
    success = verify_cleanup()

    # 5. 最终提示
    print("\n" + "=" * 60)
    print("📋 升级完成摘要")
    print("=" * 60)
    print(f"删除冗余文件: {deleted} 个")
    print(f"移动整理文件: {moved} 个")
    print(f"验证结果: {'✅ 通过' if success else '❌ 有问题'}")

    print("\n🎯 升级后推荐命令:")
    print("  # 统一入口")
    print("  python run.py validate                  # 验证数据")
    print("  python run.py download --group a50      # 下载A50")
    print("  python run.py factor-update --group a50 # 更新因子")
    print("  python run.py indicator-calc            # 计算指标")

    print("\n✨ 升级完成！项目结构已优化")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())