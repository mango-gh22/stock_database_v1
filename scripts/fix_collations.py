# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\fix_collations.py
# File Name: fix_collations
# @ Author: mango-gh22
# @ Date：2025/12/13 15:47
"""
desc 统一排序规则
"""

# scripts/fix_collations.py
"""
数据库排序规则独立修复脚本
无需导入项目模块，直接运行即可
"""

import mysql.connector
import yaml
import os
from pathlib import Path


def load_database_config():
    """直接从配置文件和环境变量加载数据库配置"""
    # 获取项目根目录
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # 1. 尝试读取 database.yaml
    config_path = project_root / 'config' / 'database.yaml'

    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        print("请确保数据库配置文件位于: config/database.yaml")
        return None

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        # 提取MySQL配置
        mysql_config = config_data.get('database', {}).get('mysql', {})

        # 获取密码（优先从环境变量，再尝试 .env 文件）
        password = os.getenv('DB_PASSWORD')

        if not password:
            # 尝试从 .env 文件读取
            env_path = project_root / '.env'
            if env_path.exists():
                with open(env_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip() and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == 'DB_PASSWORD':
                                password = value
                                break

        if not password:
            print("❌ 数据库密码未找到")
            print("请设置环境变量 DB_PASSWORD 或在 .env 文件中配置:")
            print("   DB_PASSWORD=你的数据库密码")
            return None

        config = {
            'host': mysql_config.get('host', 'localhost'),
            'port': mysql_config.get('port', 3306),
            'user': mysql_config.get('user', 'root'),
            'password': password,
            'database': mysql_config.get('database', 'stock_database'),
            'charset': mysql_config.get('charset', 'utf8mb4')
        }

        print(f"✅ 成功加载数据库配置")
        print(f"   主机: {config['host']}:{config['port']}")
        print(f"   数据库: {config['database']}")
        print(f"   用户: {config['user']}")

        return config

    except Exception as e:
        print(f"❌ 读取配置文件失败: {e}")
        return None


def fix_collations(target_collation='utf8mb4_unicode_ci'):
    """统一所有表的排序规则"""
    config = load_database_config()
    if not config:
        return False

    try:
        print(f"\n正在连接数据库...")
        conn = mysql.connector.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )

        print(f"✅ 成功连接到数据库: {config['database']}")

    except mysql.connector.Error as err:
        print(f"❌ 数据库连接失败: {err}")
        print("\n可能的原因:")
        print(f"1. MySQL服务未运行 (host: {config['host']}, port: {config['port']})")
        print(f"2. 用户 {config['user']} 权限不足")
        print(f"3. 数据库 {config['database']} 不存在")
        print(f"4. 密码错误")
        return False

    cursor = conn.cursor()

    print(f"\n{'=' * 60}")
    print(f"开始修复排序规则 -> 目标: {target_collation}")
    print(f"{'=' * 60}")

    try:
        # 获取所有表名
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        if not tables:
            print("数据库中没有表")
            conn.close()
            return True

        print(f"发现 {len(tables)} 张表")

        success_count = 0
        failed_tables = []
        repaired_tables = []

        for i, table in enumerate(tables, 1):
            try:
                # 获取表的当前排序规则
                cursor.execute(f"SHOW TABLE STATUS LIKE '{table}'")
                table_status = cursor.fetchone()

                if not table_status:
                    print(f"{i:3d}. {table:35} -> 无法获取状态")
                    continue

                current_collation = table_status[14]  # Collation字段位置

                if current_collation == target_collation:
                    print(f"{i:3d}. {table:35} -> ✅ 已使用目标规则，跳过")
                    continue

                print(f"{i:3d}. {table:35} -> 修复中 ({current_collation} → {target_collation})")

                # 修复表排序规则
                alter_sql = f"ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 COLLATE {target_collation}"
                cursor.execute(alter_sql)

                # 验证修复结果
                cursor.execute(f"SHOW TABLE STATUS LIKE '{table}'")
                new_status = cursor.fetchone()
                new_collation = new_status[14] if new_status else None

                if new_collation == target_collation:
                    print(f"     {' ' * 35}   ✅ 修复成功")
                    success_count += 1
                    repaired_tables.append(table)
                else:
                    print(f"     {' ' * 35}   ❌ 修复后验证失败")
                    failed_tables.append((table, "修复后验证失败"))

            except mysql.connector.Error as e:
                print(f"     {' ' * 35}   ❌ 修复失败: {e}")
                failed_tables.append((table, str(e)))
            except Exception as e:
                print(f"     {' ' * 35}   ❌ 未知错误: {e}")
                failed_tables.append((table, str(e)))

        # 提交更改
        conn.commit()

        print(f"\n{'=' * 60}")
        print("修复完成总结")
        print(f"{'=' * 60}")
        print(f"总表数: {len(tables)}")
        print(f"成功修复: {success_count} 张表")

        if repaired_tables:
            print(f"\n已修复的表:")
            for table in repaired_tables:
                print(f"  - {table}")

        if failed_tables:
            print(f"\n❌ 修复失败的表 ({len(failed_tables)}):")
            for table, error in failed_tables:
                print(f"  - {table}: {error}")
            print(f"\n💡 建议:")
            print(f"  1. 检查这些表是否有特殊约束或索引")
            print(f"  2. 可以尝试手动修复单个表:")
            print(f"     ALTER TABLE `表名` CONVERT TO CHARACTER SET utf8mb4 COLLATE {target_collation};")
        else:
            print(f"\n✅ 所有表修复成功！")

        # 特别提示关键表
        key_tables = ['stock_basic_info', 'index_info', 'stock_index_constituent']
        print(f"\n🔑 关键表状态检查:")
        for table in key_tables:
            cursor.execute(f"SHOW TABLE STATUS LIKE '{table}'")
            status = cursor.fetchone()
            if status:
                collation = status[14]
                status_icon = "✅" if collation == target_collation else "❌"
                print(f"  {status_icon} {table:25} -> {collation}")
            else:
                print(f"  ⚠️  {table:25} -> 表不存在")

        cursor.close()
        conn.close()

        return len(failed_tables) == 0

    except Exception as e:
        print(f"\n❌ 修复过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except:
            pass

        return False


def quick_fix_for_validation():
    """快速修复：只修改连接排序规则，不修改表结构"""
    print("正在尝试快速修复方案...")
    print("此方案只修改导入脚本中的连接设置，不修改数据库表结构")

    # 找到导入脚本路径
    script_dir = Path(__file__).parent
    import_script = script_dir.parent / 'src' / 'data' / 'import_csi_a50.py'

    if not import_script.exists():
        print(f"❌ 找不到导入脚本: {import_script}")
        return False

    try:
        with open(import_script, 'r', encoding='utf-8') as f:
            content = f.read()

        # 查找 validate_import 方法
        if 'def validate_import' in content:
            # 在游标创建后添加设置语句
            old_code = 'cursor = connection.cursor(dictionary=True)'
            new_code = 'cursor = connection.cursor(dictionary=True)\n            cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")'

            if new_code in content:
                print("✅ 导入脚本已包含修复代码")
                return True

            if old_code in content:
                content = content.replace(old_code, new_code)

                with open(import_script, 'w', encoding='utf-8') as f:
                    f.write(content)

                print("✅ 已成功修改导入脚本")
                print("   在 validate_import 方法中添加了排序规则设置")
                return True
            else:
                print("❌ 找不到需要替换的代码模式")
                return False
        else:
            print("❌ 找不到 validate_import 方法")
            return False

    except Exception as e:
        print(f"❌ 修改脚本失败: {e}")
        return False


if __name__ == "__main__":
    print("数据库排序规则修复工具")
    print("=" * 60)

    # 显示当前目录信息
    current_dir = Path(__file__).parent
    print(f"脚本目录: {current_dir}")
    print(f"项目根目录: {current_dir.parent}")

    print("\n请选择修复方案:")
    print("1. 完整修复（推荐）- 统一所有表的排序规则")
    print("2. 快速修复 - 只修改导入脚本的连接设置")
    print("3. 检查当前状态")
    print("4. 退出")

    choice = input("\n请输入选择 (1/2/3/4): ").strip()

    if choice == '1':
        print("\n注意：完整修复将修改数据库表结构！")
        print("建议先备份数据库（如果数据重要）")
        confirm = input("确定要执行完整修复吗？(yes/no): ")

        if confirm.lower() in ['yes', 'y', '是']:
            success = fix_collations('utf8mb4_unicode_ci')
            if success:
                print("\n🎉 完整修复成功完成！")
                print("请重新运行导入程序: python import_a50.py")
            else:
                print("\n⚠️  修复过程中遇到问题，请检查上方错误信息")
        else:
            print("操作已取消")

    elif choice == '2':
        if quick_fix_for_validation():
            print("\n🎉 快速修复完成！")
            print("请重新运行导入程序: python import_a50.py")
        else:
            print("\n❌ 快速修复失败")

    elif choice == '3':
        # 简单检查当前状态
        print("\n当前数据库状态检查:")
        config = load_database_config()
        if config:
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                print(f"数据库中有 {len(tables)} 张表")
                conn.close()
            except Exception as e:
                print(f"检查失败: {e}")

    elif choice == '4':
        print("已退出")

    else:
        print("无效选择，请重新运行脚本")