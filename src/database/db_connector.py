# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/database\db_connector.py
# File Name: db_connector
# @ File: db_connector.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 21:13
"""
desc 数据库连接模块
"""

# src/database/db_connector.py
# src/database/db_connector.py
import mysql.connector
from mysql.connector import Error
import yaml
import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """数据库连接器类"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据库连接器

        Args:
            config_path: 数据库配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.connection = None

    def _load_config(self) -> Dict[str, Any]:
        """加载数据库配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 支持两种配置格式：
            # 1. 你的嵌套格式: {'database': {'mysql': {...}}}
            # 2. 扁平格式: {'host': 'localhost', 'port': 3306, ...}

            if 'database' in config and 'mysql' in config['database']:
                # 嵌套格式
                db_config = config['database']['mysql']
                logger.info("使用嵌套格式数据库配置")
            elif all(key in config for key in ['host', 'port', 'user', 'password']):
                # 扁平格式
                db_config = config
                logger.info("使用扁平格式数据库配置")
            else:
                # 尝试在根目录查找配置
                db_config = config.get('mysql', config)

            required_keys = ['host', 'port', 'user', 'password']
            missing_keys = [key for key in required_keys if key not in db_config]

            if missing_keys:
                raise ValueError(f"数据库配置文件缺少必要字段: {missing_keys}")

            # 获取数据库名，如果不存在则使用默认
            database_name = db_config.get('database', 'stock_data')

            logger.info(f"数据库配置加载成功: {db_config['host']}:{db_config['port']}/{database_name}")
            return db_config

        except FileNotFoundError:
            logger.error(f"数据库配置文件不存在: {self.config_path}")

            # 尝试使用环境变量
            env_config = self._load_from_env()
            if env_config:
                return env_config
            raise

        except Exception as e:
            logger.error(f"加载数据库配置文件失败: {e}")
            raise

    def _load_from_env(self) -> Optional[Dict[str, Any]]:
        """从环境变量加载数据库配置"""
        try:
            import os
            from dotenv import load_dotenv
            load_dotenv()

            host = os.getenv('DB_HOST')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')

            if not all([host, user, password]):
                return None

            config = {
                'host': host,
                'port': int(os.getenv('DB_PORT', '3306')),
                'user': user,
                'password': password,
                'database': os.getenv('DB_NAME', 'stock_data'),
                'charset': os.getenv('DB_CHARSET', 'utf8mb4')
            }

            logger.info("从环境变量加载数据库配置")
            return config

        except Exception as e:
            logger.warning(f"从环境变量加载配置失败: {e}")
            return None

    def get_connection(self, autocommit: bool = True):
        """
        获取数据库连接

        Args:
            autocommit: 是否自动提交

        Returns:
            mysql.connector.connection.MySQLConnection
        """
        try:
            if self.connection is None or not self.connection.is_connected():
                # 连接参数
                connection_params = {
                    'host': self.config['host'],
                    'port': self.config.get('port', 3306),
                    'user': self.config['user'],
                    'password': self.config['password'],
                    'charset': self.config.get('charset', 'utf8mb4'),
                    'collation': 'utf8mb4_unicode_ci',
                    'use_unicode': True,
                    'autocommit': autocommit,
                }

                # 如果有数据库名则添加
                if 'database' in self.config:
                    connection_params['database'] = self.config['database']

                self.connection = mysql.connector.connect(**connection_params)
                logger.debug(f"数据库连接建立: {self.config['host']}:{self.config.get('port', 3306)}")

            return self.connection

        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def use_database(self, database_name: str) -> bool:
        """
        使用指定数据库

        Args:
            database_name: 数据库名称

        Returns:
            是否成功
        """
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(f"USE {database_name}")
            cursor.close()

            # 更新配置中的数据库名
            self.config['database'] = database_name

            logger.info(f"切换到数据库: {database_name}")
            return True

        except Error as e:
            logger.error(f"切换数据库失败: {e}")
            return False

    def create_database_if_not_exists(self) -> bool:
        """创建数据库（如果不存在）"""
        try:
            connection = self.get_connection()
            cursor = connection.cursor()

            database_name = self.config.get('database', 'stock_data')

            # 创建数据库
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {database_name} 
                DEFAULT CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
            """)

            # 切换到该数据库
            cursor.execute(f"USE {database_name}")

            cursor.close()

            logger.info(f"数据库 '{database_name}' 创建/确认完成")
            return True

        except Error as e:
            logger.error(f"创建数据库失败: {e}")
            return False

    def test_connection(self) -> bool:
        """测试数据库连接是否正常"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 尝试执行简单查询
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()

            cursor.close()

            if version:
                logger.info(f"✅ 数据库连接测试成功 - MySQL版本: {version[0]}")
                return True
            return False

        except Error as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False

    def close_connection(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.debug("数据库连接已关闭")
            self.connection = None

    def execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """
        执行SQL查询

        Args:
            query: SQL查询语句
            params: 查询参数
            fetch: 是否获取结果

        Returns:
            查询结果（如果fetch=True）
        """
        connection = None
        cursor = None

        try:
            connection = self.get_connection()
            cursor = connection.cursor(dictionary=True)

            cursor.execute(query, params or ())

            if fetch and cursor.description:
                result = cursor.fetchall()
                logger.debug(f"查询执行成功，返回{len(result)}行")
                return result
            else:
                affected_rows = cursor.rowcount
                logger.debug(f"查询执行成功，影响{affected_rows}行")
                return affected_rows

        except Error as e:
            logger.error(f"执行查询失败: {e}")
            logger.error(f"SQL: {query}")
            if params:
                logger.error(f"参数: {params}")
            raise
        finally:
            if cursor:
                cursor.close()
            # 注意：这里不关闭connection，保持连接池

    def __enter__(self):
        """上下文管理器入口"""
        self.get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_connection()


# 使用示例
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    try:
        db = DatabaseConnector()

        if db.test_connection():
            print("✅ 数据库连接正常")

            # 测试创建和使用数据库
            if db.create_database_if_not_exists():
                print("✅ 数据库创建/确认完成")

                # 测试查询
                result = db.execute_query("SHOW TABLES")
                if result:
                    print(f"✅ 数据库中有 {len(result)} 个表:")
                    for row in result:
                        print(f"  - {row['Tables_in_stock_data']}")
                else:
                    print("ℹ️ 数据库中没有表")
        else:
            print("❌ 数据库连接失败")

    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback

        traceback.print_exc()