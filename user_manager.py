# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\user_manager.py
# File Name: user_manager
# @ Author: mango-gh22
# @ Date：2026/1/1 8:43
"""
desc 
"""
# user_manager.py
import mysql.connector
from getpass import getpass


def recreate_stock_user():
    """删除并重新创建 stock_user"""
    print("🔄 重新创建 stock_user 用户")
    print("=" * 50)

    # 首先用你知道的账户登录（比如你平时在 MySQL Workbench 中用的账户）
    print("\n1. 请输入一个能登录 MySQL 的账户:")
    root_user = input("   用户名 (如 root): ")
    root_password = getpass("   密码: ")

    try:
        # 连接 MySQL
        conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            user=root_user,
            password=root_password
        )
        cursor = conn.cursor()

        print("\n✅ MySQL 连接成功")

        # 2. 删除现有用户（如果存在）
        cursor.execute("DROP USER IF EXISTS 'stock_user'@'localhost'")
        print("   ✅ 已删除旧的 stock_user 用户")

        # 3. 创建新用户
        stock_password = getpass("\n2. 为 stock_user 设置新密码: ")

        create_user_sql = f"""
            CREATE USER 'stock_user'@'localhost' 
            IDENTIFIED BY '{stock_password}'
        """
        cursor.execute(create_user_sql)

        # 4. 授予权限
        grant_sql = """
            GRANT ALL PRIVILEGES ON stock_database.* 
            TO 'stock_user'@'localhost'
        """
        cursor.execute(grant_sql)

        # 5. 刷新权限
        cursor.execute("FLUSH PRIVILEGES")

        conn.commit()

        print("\n🎉 stock_user 用户创建成功！")
        print("=" * 50)
        print(f"用户名: stock_user")
        print(f"密码: {'*' * len(stock_password)}")
        print(f"权限: 对 stock_database 数据库拥有全部权限")
        print(f"主机: localhost")

        # 6. 更新 .env 文件
        with open('.env', 'r') as f:
            lines = f.readlines()

        with open('.env', 'w') as f:
            for line in lines:
                if line.startswith('DB_PASSWORD='):
                    f.write(f'DB_PASSWORD={stock_password}\n')
                else:
                    f.write(line)

        print("\n✅ 已自动更新 .env 文件中的 DB_PASSWORD")

        conn.close()

    except mysql.connector.Error as err:
        print(f"\n❌ MySQL 错误: {err}")
    except Exception as e:
        print(f"\n❌ 其他错误: {e}")


if __name__ == "__main__":
    recreate_stock_user()