# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\diagnose_collations.py
# File Name: diagnose_collations
# @ Author: mango-gh22
# @ Date：2025/12/13 14:56
"""
desc 诊断脚本内容-数据库排序问题

数据库字符集和排序规则诊断脚本
用于检测和修复 'Illegal mix of collations' 错误
"""

import mysql.connector
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def get_database_config():
    """
    从多个来源获取数据库配置
    兼容 secret_loader 和直接读取配置
    """
    try:
        # 尝试从 secret_loader 获取
        from src.config.secret_loader import get_database_config as get_config_from_secret
        return get_config_from_secret()
    except ImportError as e:
        print(f"警告: 无法导入 secret_loader: {e}")
        print("尝试从环境变量或直接配置获取...")

    try:
        # 尝试读取 database.yaml
        import yaml
        config_path = project_root / 'config' / 'database.yaml'

        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)

            mysql_config = config_data.get('database', {}).get('mysql', {})

            # 从环境变量获取密码
            import os
            password = os.getenv('DB_PASSWORD')

            return {
                'host': mysql_config.get('host', 'localhost'),
                'port': mysql_config.get('port', 3306),
                'user': mysql_config.get('user', 'root'),
                'password': password,
                'database': mysql_config.get('database', 'stock_database'),
                'charset': mysql_config.get('charset', 'utf8mb4')
            }
    except Exception as e:
        print(f"读取配置文件失败: {e}")

    # 返回默认配置
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': None,
        'database': 'stock_database',
        'charset': 'utf8mb4'
    }


def check_database_collations():
    """检查数据库和各表的排序规则"""

    config = get_database_config()

    # 检查密码是否设置
    if not config['password']:
        print("❌ 数据库密码未设置")
        print("请设置环境变量 DB_PASSWORD 或在 .env 文件中配置")
        print("\n解决方法:")
        print("1. 创建或编辑 .env 文件:")
        print("   DB_PASSWORD=你的数据库密码")
        print("\n2. 或者在运行脚本前设置环境变量:")
        print("   export DB_PASSWORD=你的数据库密码")
        print("   python scripts/diagnose_collations.py")
        return []

    try:
        print("正在连接数据库...")
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
        print("\n请检查:")
        print(f"1. MySQL服务是否运行 (host: {config['host']}, port: {config['port']})")
        print(f"2. 用户 {config['user']} 是否存在并有权限")
        print(f"3. 数据库 {config['database']} 是否存在")
        print(f"4. 密码是否正确")
        return []

    cursor = conn.cursor(dictionary=True)

    print("\n" + "=" * 60)
    print("数据库字符集诊断报告")
    print("=" * 60)

    try:
        # 1. 检查数据库默认排序规则
        cursor.execute(f"""
            SELECT 
                DEFAULT_CHARACTER_SET_NAME, 
                DEFAULT_COLLATION_NAME,
                SCHEMA_NAME
            FROM information_schema.SCHEMATA 
            WHERE SCHEMA_NAME = '{config['database']}'
        """)
        db_info = cursor.fetchone()

        if not db_info:
            print(f"❌ 数据库 {config['database']} 不存在")
            conn.close()
            return []

        print(f"\n📊 数据库: {db_info['SCHEMA_NAME']}")
        print(f"   默认字符集: {db_info['DEFAULT_CHARACTER_SET_NAME']}")
        print(f"   默认排序规则: {db_info['DEFAULT_COLLATION_NAME']}")

        # 2. 检查所有表的排序规则
        print(f"\n📋 表列表及排序规则检查:")
        print("-" * 80)

        cursor.execute("""
            SELECT 
                TABLE_NAME, 
                TABLE_COLLATION,
                ENGINE,
                TABLE_ROWS,
                TABLE_COMMENT
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """, (config['database'],))

        tables = cursor.fetchall()

        if not tables:
            print("数据库中没有表")
            conn.close()
            return []

        inconsistent_tables = []
        problem_tables_found = []

        print(f"{'表名':<25} {'排序规则':<25} {'状态':<10} {'行数':<10}")
        print("-" * 80)

        for table in tables:
            table_name = table['TABLE_NAME']
            table_collation = table['TABLE_COLLATION']
            db_collation = db_info['DEFAULT_COLLATION_NAME']

            # 检查是否一致
            is_consistent = (table_collation == db_collation)
            status = "✅ 一致" if is_consistent else "❌ 不一致"

            print(f"{table_name:<25} {table_collation:<25} {status:<10} {table['TABLE_ROWS']:<10}")

            if not is_consistent:
                inconsistent_tables.append(table_name)

            # 特别关注问题相关的表
            if table_name in ['stock_basic_info', 'index_info', 'stock_index_constituent']:
                problem_tables_found.append(table_name)

        print("-" * 80)

        # 3. 特别检查问题表的结构
        print(f"\n🔍 关键表详细结构 (验证时涉及的表):")

        for table_name in ['stock_basic_info', 'index_info', 'stock_index_constituent']:
            print(f"\n表: {table_name}")

            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as exists_flag
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (config['database'], table_name))

            exists_result = cursor.fetchone()

            if exists_result['exists_flag'] == 0:
                print(f"   ❌ 表不存在")
                continue

            # 获取表结构
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            create_table_result = cursor.fetchone()

            if create_table_result and 'Create Table' in create_table_result:
                create_sql = create_table_result['Create Table']

                # 提取字符集和排序规则信息
                if 'CHARSET=' in create_sql:
                    charset_start = create_sql.find('CHARSET=') + 8
                    charset_end = create_sql.find(' ', charset_start)
                    charset = create_sql[charset_start:charset_end] if charset_end != -1 else create_sql[charset_start:]
                    print(f"   字符集: {charset}")

                if 'COLLATE=' in create_sql:
                    collate_start = create_sql.find('COLLATE=') + 8
                    collate_end = create_sql.find(' ', collate_start)
                    collate = create_sql[collate_start:collate_end] if collate_end != -1 else create_sql[collate_start:]
                    print(f"   排序规则: {collate}")

            # 检查字符串列
            cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    COLLATION_NAME, 
                    CHARACTER_SET_NAME,
                    COLUMN_TYPE,
                    IS_NULLABLE
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = %s 
                  AND TABLE_NAME = %s
                  AND COLLATION_NAME IS NOT NULL
                ORDER BY ORDINAL_POSITION
            """, (config['database'], table_name))

            columns = cursor.fetchall()

            if columns:
                print(f"   字符串列 ({len(columns)} 个):")
                for col in columns:
                    nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
                    print(
                        f"     - {col['COLUMN_NAME']:20} {col['COLUMN_TYPE']:15} {col['COLLATION_NAME']:25} {nullable}")
            else:
                print("   没有字符串列")

        # 4. 检查当前会话设置
        print(f"\n⚙️  当前数据库会话设置:")
        cursor.execute("SHOW VARIABLES LIKE 'character_set_%'")
        charset_vars = cursor.fetchall()

        for var in charset_vars:
            if 'client' in var['Variable_name'] or 'connection' in var['Variable_name']:
                print(f"   {var['Variable_name']:30} = {var['Value']}")

        cursor.execute("SHOW VARIABLES LIKE 'collation_%'")
        collation_vars = cursor.fetchall()

        for var in collation_vars:
            if 'connection' in var['Variable_name']:
                print(f"   {var['Variable_name']:30} = {var['Value']}")

        # 5. 诊断总结
        print(f"\n" + "=" * 60)
        print("诊断总结")
        print("=" * 60)

        if inconsistent_tables:
            print(f"❌ 发现 {len(inconsistent_tables)} 个表的排序规则与数据库默认不一致:")
            for table in inconsistent_tables:
                print(f"   - {table}")

            print(f"\n💡 问题分析:")
            print(f"   错误信息 'Illegal mix of collations' 通常是因为:")
            print(f"   1. 这些表的排序规则不一致")
            print(f"   2. 在 JOIN 或 WHERE 条件中进行字符串比较时")
            print(f"   3. MySQL 无法自动转换不同的排序规则")

            print(f"\n🚀 解决方案:")
            print(f"   运行修复脚本统一排序规则:")
            print(f"   python scripts/fix_collations.py")

        else:
            print(f"✅ 所有表排序规则一致")

            print(f"\n💡 如果仍然有错误，可能的原因:")
            print(f"   1. 连接时未指定字符集")
            print(f"   2. 查询中使用了不同的排序规则函数")
            print(f"   3. 临时表或子查询使用了默认排序规则")

            print(f"\n🔧 临时解决方案:")
            print(f"   在查询前执行: SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

        print(f"\n📋 涉及验证的关键表:")
        found_all = all(table in [t['TABLE_NAME'] for t in tables]
                        for table in ['stock_basic_info', 'index_info', 'stock_index_constituent'])

        if found_all:
            print("✅ 所有关键表都存在")
        else:
            missing = [table for table in ['stock_basic_info', 'index_info', 'stock_index_constituent']
                       if table not in [t['TABLE_NAME'] for t in tables]]
            print(f"❌ 缺失的表: {missing}")

        cursor.close()
        conn.close()

        return inconsistent_tables

    except Exception as e:
        print(f"❌ 诊断过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

        try:
            cursor.close()
            conn.close()
        except:
            pass

        return []


if __name__ == "__main__":
    print("开始数据库字符集诊断...")
    print("=" * 60)

    try:
        inconsistent_tables = check_database_collations()

        if inconsistent_tables:
            print(f"\n⚠️  发现不一致的表，建议修复后再运行导入程序")
            sys.exit(1)
        else:
            print(f"\n✅ 诊断完成，未发现排序规则不一致问题")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n操作被用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 诊断失败: {e}")
        sys.exit(1)