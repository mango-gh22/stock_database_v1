# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/data\test_database.py
# File Name: test_database
# @ File: test_database.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 21:43
"""
desc 
"""
# tests/data/test_database.py
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_database_connection():
    """测试数据库连接"""
    try:
        from src.database.db_connector import DatabaseConnector
        db = DatabaseConnector()

        # 测试连接
        if db.test_connection():
            print("✅ 数据库连接测试通过")

            # 测试创建数据库
            if db.create_database_if_not_exists():
                print("✅ 数据库创建/确认完成")
                return True
            else:
                print("❌ 数据库创建失败")
                return False
        else:
            print("❌ 数据库连接失败")
            return False

    except Exception as e:
        print(f"❌ 数据库测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("开始数据库测试...")
    if test_database_connection():
        print("\n✅ 数据库测试通过！")
        sys.exit(0)
    else:
        print("\n❌ 数据库测试失败！")
        sys.exit(1)