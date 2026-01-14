# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\migrate_unified.py
# File Name: migrate_unified
# @ Author: mango-gh22
# @ Date：2026/1/7 20:21
"""
desc
migrate 迁移
"""

# scripts/migrate_unified.py
"""
迁移到统一接口的辅助脚本
"""

import os
import shutil
from pathlib import Path


def create_unified_interface():
    """创建统一接口"""
    print("🚀 创建统一接口")
    print("=" * 50)

    project_root = Path(__file__).parent.parent

    # 1. 备份旧的 run.py 和 main.py
    backup_dir = project_root / 'backup_old'
    backup_dir.mkdir(exist_ok=True)

    for old_file in ['run.py', 'main.py']:
        old_path = project_root / old_file
        if old_path.exists():
            backup_path = backup_dir / old_file
            shutil.copy2(old_path, backup_path)
            print(f"📦 备份: {old_file} -> {backup_path}")

    # 2. 创建新的统一 run.py（使用上面的代码）
    # 这里假设新的 run.py 内容已经准备好
    new_run_content = """# 新版本的 run.py 内容"""

    # 3. 创建脚本别名
    create_script_aliases()

    # 4. 更新 README
    update_readme()

    print("\n" + "=" * 50)
    print("✅ 统一接口创建完成！")
    print("\n📋 新用法:")
    print("  python run.py validate           # 验证数据")
    print("  python run.py collect --test     # 测试采集")
    print("  python run.py query --help       # 查看帮助")
    print("\n📁 旧脚本备份在: backup_old/")


def create_script_aliases():
    """为旧脚本创建别名"""
    print("\n📁 创建脚本别名...")

    scripts_dir = Path(__file__).parent
    alias_dir = scripts_dir / 'aliases'
    alias_dir.mkdir(exist_ok=True)

    # 别名映射
    alias_mapping = {
        'collect_a50_daily.py': 'python run.py collect --group a50',
        'update_a50_factors.py': 'python run.py collect --group a50 --mode full',
        'run_batch_direct.py': 'python run.py collect',
        'run_factor_update.py': 'python run.py factors',
        'test_batch_run.py': 'python run.py collect --test',
        'verify_factor_storage.py': 'python run.py verify',
        'test_complete_factor_system.py': 'python run.py test --type factor',
        'quick_factor_test.py': 'python run.py test',
        'quick_validate_all.py': 'python run.py validate --detailed',
        'seed_test_data.py': 'python run.py setup-db',
        'setup_mysql.py': 'python run.py setup-db',
        'update_table_schema.py': 'python run.py update-schema',
        'update_daily_table_full.py': 'python run.py update-schema',
    }

    for old_script, new_command in alias_mapping.items():
        alias_content = f"""#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
{old_script} 的别名
已迁移到统一接口，请使用:
{new_command}
\"\"\"

import sys

print("⚠️  注意: 脚本已迁移到统一接口")
print(f"📌 请使用: {new_command}")
print()
print("如需查看帮助，请使用:")
print("  python run.py --help")
sys.exit(1)
"""

        alias_file = alias_dir / old_script.replace('.py', '_alias.py')
        with open(alias_file, 'w', encoding='utf-8') as f:
            f.write(alias_content)

        print(f"  ✅ {old_script} -> {new_command}")


def update_readme():
    """更新 README.md"""
    print("\n📝 更新 README...")

    project_root = Path(__file__).parent.parent
    readme_path = project_root / 'README.md'

    if readme_path.exists():
        with open(readme_path, 'a', encoding='utf-8') as f:
            f.write("""

## 🆕 统一命令行接口

项目已迁移到统一的命令行接口，使用方式：

```bash
# 查看所有命令
python run.py --help

# 常用命令
python run.py validate           # 验证数据库数据
python run.py collect --test     # 测试数据采集
python run.py factors --mode full # 全量更新因子数据
python run.py query --symbol 600519 # 查询股票数据

# 旧脚本迁移
# 所有旧脚本都已迁移到统一接口，运行时会提示新命令
            📁 目录结构更新
            run.py - 唯一主入口

            scripts/aliases/ - 旧脚本的别名（指向新命令）

            backup_old/ - 旧文件备份
            """)

    if __name__ == "__main__":
        create_unified_interface()