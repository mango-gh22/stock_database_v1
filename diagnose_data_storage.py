# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_data_storage.py
# File Name: diagnose_data_storage
# @ Author: mango-gh22
# @ Date：2025/12/28 18:33
"""
desc 
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnose_data_storage.py
# File Name: diagnose_data_storage
# @ Author: mango-gh22
# @ Date：2025/12/28
"""
数据存储诊断工具 - 专门诊断数据无法存储的问题
"""

import sys
import os
import mysql.connector
from mysql.connector import errorcode
import time
import pandas as pd
from datetime import datetime, timedelta
import hashlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class StorageDiagnostic:
    def __init__(self):
        self.connections = {}
        self.test_results = []

    def test_connection(self, name, host='localhost', port=3306, user='root', password='', database='stock_database'):
        """测试数据库连接"""
        print(f"\n🔧 测试连接: {name}")
        print(f"   主机: {host}:{port}")
        print(f"   用户: {user}")
        print(f"   数据库: {database}")

        try:
            conn = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                autocommit=False  # 显式关闭自动提交
            )

            cursor = conn.cursor(dictionary=True)

            # 检查基本信息
            cursor.execute("SELECT DATABASE() as db, USER() as user, @@autocommit as autocommit")
            info = cursor.fetchone()

            # 检查表是否存在
            cursor.execute("""
                SELECT table_name, table_rows, create_time, update_time 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = 'stock_daily_data'
            """)
            table_info = cursor.fetchone()

            # 检查表结构
            cursor.execute("DESC stock_daily_data")
            columns = cursor.fetchall()

            conn.close()

            result = {
                'name': name,
                'success': True,
                'info': info,
                'table_exists': table_info is not None,
                'table_info': table_info,
                'column_count': len(columns) if columns else 0
            }

            print(f"   ✅ 连接成功")
            print(f"     当前数据库: {info['db']}")
            print(f"     当前用户: {info['user']}")
            print(f"     自动提交: {info['autocommit']}")
            print(f"     表存在: {'是' if table_info else '否'}")

            if table_info:
                print(f"     表行数: {table_info.get('table_rows', 0):,}")
                print(f"     创建时间: {table_info.get('create_time')}")

            return result

        except mysql.connector.Error as err:
            print(f"   ❌ 连接失败: {err}")
            return {
                'name': name,
                'success': False,
                'error': str(err)
            }

    def test_project_connector(self):
        """测试项目中的DatabaseConnector"""
        print(f"\n🔧 测试项目DatabaseConnector")
        try:
            from src.database.db_connector import DatabaseConnector

            connector = DatabaseConnector()

            # 测试获取连接
            with connector.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # 检查连接信息
                cursor.execute("SELECT DATABASE() as db, USER() as user, @@autocommit as autocommit")
                info = cursor.fetchone()

                # 检查表
                cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data")
                count_result = cursor.fetchone()

                result = {
                    'name': 'ProjectConnector',
                    'success': True,
                    'info': info,
                    'row_count': count_result['count'],
                    'autocommit_mode': conn.get_autocommit() if hasattr(conn, 'get_autocommit') else 'Unknown'
                }

                print(f"   ✅ DatabaseConnector 连接成功")
                print(f"     数据库: {info['db']}")
                print(f"     用户: {info['user']}")
                print(f"     自动提交(系统): {info['autocommit']}")
                print(f"     表记录数: {count_result['count']:,}")

                if hasattr(conn, 'get_autocommit'):
                    print(f"     自动提交(连接): {conn.get_autocommit()}")

            return result

        except Exception as e:
            print(f"   ❌ DatabaseConnector 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'name': 'ProjectConnector',
                'success': False,
                'error': str(e)
            }

    def test_storage_workflow(self):
        """测试完整的数据存储工作流"""
        print(f"\n🚀 测试完整存储工作流")

        try:
            # 1. 创建测试数据
            test_symbol = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            test_data = pd.DataFrame({
                'symbol': [test_symbol],
                'trade_date': [datetime.now().strftime('%Y-%m-%d')],
                'open': [100.0],
                'high': [105.0],
                'low': [95.0],
                'close': [102.0],
                'volume': [1000000],
                'amount': [102000000]
            })

            print(f"   创建测试数据: {test_symbol}")

            # 2. 使用项目中的数据存储模块
            try:
                from src.data.data_storage import DataStorage

                storage = DataStorage()

                # 记录插入前的时间戳
                before_insert = datetime.now()

                # 3. 尝试存储数据
                print(f"   调用DataStorage存储数据...")

                # 这里需要根据实际DataStorage的接口调整
                # 假设DataStorage有store_daily_data方法
                if hasattr(storage, 'store_daily_data'):
                    result = storage.store_daily_data(test_data)
                elif hasattr(storage, 'save_data'):
                    result = storage.save_data(test_data, table_name='stock_daily_data')
                else:
                    # 尝试通用方法
                    result = storage.store(test_data)

                print(f"   存储结果: {result}")

                # 4. 立即验证
                time.sleep(0.5)  # 等待可能的异步操作

                # 使用原始连接验证
                conn = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password='',
                    database='stock_database',
                    autocommit=True
                )

                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
                records = cursor.fetchall()

                cursor.execute("""
                    SELECT COUNT(*) as total, 
                           MAX(created_time) as latest_created,
                           MAX(updated_time) as latest_updated
                    FROM stock_daily_data 
                    WHERE symbol = %s
                """, (test_symbol,))
                stats = cursor.fetchone()

                # 检查是否有在插入时间之后创建/更新的记录
                cursor.execute("""
                    SELECT * FROM stock_daily_data 
                    WHERE symbol = %s 
                    AND (created_time > %s OR updated_time > %s)
                """, (test_symbol, before_insert, before_insert))
                recent_records = cursor.fetchall()

                conn.close()

                result = {
                    'test_symbol': test_symbol,
                    'records_found': len(records),
                    'total_in_table': stats['total'],
                    'latest_created': stats['latest_created'],
                    'latest_updated': stats['latest_updated'],
                    'recent_records': len(recent_records),
                    'before_insert_time': before_insert,
                    'data_stored': 'partial' if len(records) > 0 and len(records) < len(test_data) else
                    'full' if len(records) == len(test_data) else 'none'
                }

                print(f"   验证结果:")
                print(f"     找到记录数: {len(records)}")
                print(f"     表中总数: {stats['total']}")
                print(f"     最新创建时间: {stats['latest_created']}")
                print(f"     最新更新时间: {stats['latest_updated']}")
                print(f"     插入后新记录: {len(recent_records)}")
                print(f"     数据存储状态: {result['data_stored']}")

                # 5. 清理测试数据
                if len(records) > 0:
                    cleanup_conn = mysql.connector.connect(
                        host='localhost',
                        user='root',
                        password='',
                        database='stock_database',
                        autocommit=True
                    )
                    cleanup_cursor = cleanup_conn.cursor()
                    cleanup_cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
                    cleanup_conn.commit()
                    cleanup_conn.close()
                    print(f"   清理测试数据: 删除了 {cleanup_cursor.rowcount} 条记录")

                return result

            except Exception as e:
                print(f"   ❌ DataStorage测试失败: {e}")
                import traceback
                traceback.print_exc()
                return {
                    'error': str(e),
                    'test_symbol': test_symbol
                }

        except Exception as e:
            print(f"   ❌ 工作流测试失败: {e}")
            return {'error': str(e)}

    def test_transaction_isolation(self):
        """测试事务隔离级别"""
        print(f"\n🔄 测试事务隔离级别")

        try:
            # 创建两个连接来模拟并发
            conn1 = mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='stock_database',
                autocommit=False
            )

            conn2 = mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='stock_database',
                autocommit=True  # 自动提交
            )

            cursor1 = conn1.cursor(dictionary=True)
            cursor2 = conn2.cursor(dictionary=True)

            # 检查隔离级别
            cursor1.execute("SELECT @@transaction_isolation as isolation_level")
            isolation1 = cursor1.fetchone()

            cursor2.execute("SELECT @@transaction_isolation as isolation_level")
            isolation2 = cursor2.fetchone()

            test_symbol = f"TRANS_TEST_{int(time.time())}"

            # 在conn1中插入但不提交
            print(f"   在conn1中插入数据(不提交)...")
            cursor1.execute("""
                INSERT INTO stock_daily_data 
                (symbol, trade_date, open_price, close_price, volume, created_time, updated_time)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            """, (test_symbol, '2025-12-28', 100, 101, 1000000))

            # 在conn2中检查是否能看到
            print(f"   在conn2中检查数据...")
            cursor2.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
            count_before_commit = cursor2.fetchone()['count']

            # conn1提交
            conn1.commit()
            print(f"   conn1提交事务...")

            # 再次在conn2中检查
            cursor2.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
            count_after_commit = cursor2.fetchone()['count']

            # 清理
            cursor2.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (test_symbol,))
            conn2.commit()

            conn1.close()
            conn2.close()

            result = {
                'isolation_level_conn1': isolation1['isolation_level'],
                'isolation_level_conn2': isolation2['isolation_level'],
                'visible_before_commit': count_before_commit > 0,
                'visible_after_commit': count_after_commit > 0,
                'isolation_working': count_before_commit == 0 and count_after_commit > 0
            }

            print(f"   隔离级别(conn1): {isolation1['isolation_level']}")
            print(f"   隔离级别(conn2): {isolation2['isolation_level']}")
            print(f"   提交前是否可见: {'是' if result['visible_before_commit'] else '否'}")
            print(f"   提交后是否可见: {'是' if result['visible_after_commit'] else '否'}")
            print(f"   事务隔离是否正常: {'✅ 是' if result['isolation_working'] else '❌ 否'}")

            return result

        except Exception as e:
            print(f"   ❌ 事务测试失败: {e}")
            return {'error': str(e)}

    def check_table_structure(self):
        """检查表结构"""
        print(f"\n📋 检查表结构")

        try:
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='stock_database'
            )

            cursor = conn.cursor(dictionary=True)

            # 获取表结构
            cursor.execute("DESC stock_daily_data")
            columns = cursor.fetchall()

            # 获取索引信息
            cursor.execute("SHOW INDEX FROM stock_daily_data")
            indexes = cursor.fetchall()

            # 获取约束信息
            cursor.execute("""
                SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE 
                FROM information_schema.TABLE_CONSTRAINTS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'stock_daily_data'
            """)
            constraints = cursor.fetchall()

            conn.close()

            print(f"   列结构 ({len(columns)} 列):")
            for col in columns:
                null_allowed = "NULL" if col['Null'] == 'YES' else "NOT NULL"
                default = f"DEFAULT {col['Default']}" if col['Default'] else ""
                print(f"     {col['Field']:20} {col['Type']:20} {null_allowed:12} {default}")

            print(f"\n   索引 ({len(indexes)} 个):")
            for idx in indexes:
                non_unique = "非唯一" if idx['Non_unique'] else "唯一"
                print(f"     {idx['Key_name']:15} {non_unique:8} 列: {idx['Column_name']}")

            print(f"\n   约束 ({len(constraints)} 个):")
            for cons in constraints:
                print(f"     {cons['CONSTRAINT_NAME']:20} {cons['CONSTRAINT_TYPE']}")

            return {
                'column_count': len(columns),
                'index_count': len(indexes),
                'constraint_count': len(constraints)
            }

        except Exception as e:
            print(f"   ❌ 表结构检查失败: {e}")
            return {'error': str(e)}

    def run_all_tests(self):
        """运行所有诊断测试"""
        print("=" * 60)
        print("🔍 数据存储问题诊断工具")
        print("=" * 60)

        # 测试1: 基本连接
        test1 = self.test_connection(
            name='DirectConnection',
            host='localhost',
            user='root',
            password='',
            database='stock_database'
        )

        # 测试2: 项目连接器
        test2 = self.test_project_connector()

        # 测试3: 检查表结构
        test3 = self.check_table_structure()

        # 测试4: 事务隔离
        test4 = self.test_transaction_isolation()

        # 测试5: 完整存储工作流
        test5 = self.test_storage_workflow()

        # 汇总结果
        print(f"\n" + "=" * 60)
        print("📊 诊断结果汇总")
        print("=" * 60)

        issues = []

        if not test1.get('success'):
            issues.append("1. 直接数据库连接失败")

        if not test2.get('success'):
            issues.append("2. 项目DatabaseConnector连接失败")
        else:
            if test2.get('row_count', 0) == 0:
                issues.append("3. 数据库表中没有数据")

        if test4 and not test4.get('isolation_working', True):
            issues.append("4. 事务隔离可能有问题")

        if test5 and test5.get('data_stored') == 'none':
            issues.append("5. DataStorage无法存储数据")

        if issues:
            print(f"❌ 发现 {len(issues)} 个问题:")
            for issue in issues:
                print(f"   {issue}")

            print(f"\n💡 建议:")

            if "直接数据库连接失败" in issues:
                print(f"   1. 检查MySQL服务是否运行")
                print(f"   2. 检查数据库用户名/密码")
                print(f"   3. 确认数据库 'stock_database' 是否存在")

            if "项目DatabaseConnector连接失败" in issues:
                print(f"   1. 检查 config/database.yaml 配置")
                print(f"   2. 检查 src/database/db_connector.py 实现")

            if "数据库表中没有数据" in issues:
                print(f"   1. 运行数据库初始化脚本")
                print(f"   2. 检查数据采集管道是否正常工作")

            if "事务隔离可能有问题" in issues:
                print(f"   1. 检查代码中是否有未提交的事务")
                print(f"   2. 检查autocommit设置")

            if "DataStorage无法存储数据" in issues:
                print(f"   1. 检查src/data/data_storage.py的store方法")
                print(f"   2. 检查是否有异常被静默处理")
                print(f"   3. 检查数据库权限")
        else:
            print(f"✅ 所有基础测试通过")
            print(f"\n💡 如果数据仍然无法存储，请检查:")
            print(f"   1. 具体的数据存储代码逻辑")
            print(f"   2. 是否有隐藏的事务回滚")
            print(f"   3. 是否有多层缓存导致数据未实际写入")

        return {
            'tests': {
                'direct_connection': test1,
                'project_connector': test2,
                'table_structure': test3,
                'transaction_isolation': test4,
                'storage_workflow': test5
            },
            'issues': issues
        }


def main():
    """主函数"""
    diagnostic = StorageDiagnostic()
    results = diagnostic.run_all_tests()

    print(f"\n📝 生成诊断报告...")

    # 生成报告文件
    report_file = f"storage_diagnostic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("数据存储问题诊断报告\n")
        f.write("=" * 60 + "\n\n")

        f.write("测试时间: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n\n")

        if results['issues']:
            f.write("❌ 发现问题:\n")
            for issue in results['issues']:
                f.write(f"  • {issue}\n")
        else:
            f.write("✅ 基础测试通过\n")

        f.write("\n详细测试结果:\n")
        f.write("-" * 40 + "\n")

        for test_name, test_result in results['tests'].items():
            f.write(f"\n{test_name}:\n")
            if isinstance(test_result, dict):
                for key, value in test_result.items():
                    if key not in ['table_info', 'info']:
                        f.write(f"  {key}: {value}\n")

    print(f"✅ 诊断报告已保存到: {report_file}")

    # 如果发现问题，建议下一步操作
    if results['issues']:
        print(f"\n🔧 建议下一步:")
        print(f"   1. 查看详细报告: {report_file}")
        print(f"   2. 检查关键代码文件")
        print(f"   3. 运行数据库验证脚本")

        # 询问是否查看关键文件
        print(f"\n📁 需要检查的关键文件:")
        print(f"   • src/database/db_connector.py")
        print(f"   • src/data/data_storage.py")
        print(f"   • config/database.yaml")
        print(f"   • scripts/schema/create_tables_fixed.sql")


if __name__ == "__main__":
    main()