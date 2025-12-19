# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/database\db_connector.py
# @ Author: mango-gh22
# @ Date：2025/12/5 21:13

"""
数据库连接模块 - 增强版
支持环境变量、配置文件、secret_loader三种方式获取密码
"""

import mysql.connector
from mysql.connector import Error, pooling
import yaml
import logging
import os
import time
from typing import Optional, Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """数据库连接器类 - 增强版"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据库连接器

        Args:
            config_path: 数据库配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_and_validate_config()
        self.connection_pool = None
        self._init_connection_pool()

        logger.info(f"数据库连接器初始化完成: {self.config['host']}:{self.config.get('port', 3306)}")

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """
        加载并验证配置（增强版）
        优先级：secret_loader > 环境变量 > 配置文件 > 默认值
        """
        # 1. 尝试从配置文件加载
        file_config = self._load_config_file()

        # 2. 从环境变量加载（会覆盖文件配置）
        env_config = self._load_from_env()
        if env_config:
            self._merge_configs(file_config, env_config)

        # 3. 从secret_loader获取密码（最高优先级）
        self._apply_secret_loader_password(file_config)

        # 4. 应用默认值并验证
        self._apply_defaults_and_validate(file_config)

        return file_config

    def _load_config_file(self) -> Dict[str, Any]:
        """加载配置文件"""
        config_file = Path(self.config_path)

        if not config_file.exists():
            logger.warning(f"配置文件不存在: {config_file}")
            return {}

        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # 替换环境变量占位符
            for key, value in os.environ.items():
                placeholder = f'${{{key}}}'
                if placeholder in content:
                    content = content.replace(placeholder, value)

            config = yaml.safe_load(content)

            # 支持多种配置格式
            if 'database' in config and 'mysql' in config['database']:
                # 嵌套格式: {'database': {'mysql': {...}}}
                db_config = config['database']['mysql']
                logger.info("使用嵌套格式数据库配置")
                return db_config
            elif all(key in config for key in ['host', 'port', 'user', 'password']):
                # 扁平格式
                logger.info("使用扁平格式数据库配置")
                return config
            else:
                # 尝试其他可能的键
                possible_keys = ['mysql', 'db', 'database_config']
                for key in possible_keys:
                    if key in config:
                        logger.info(f"使用配置键: {key}")
                        return config[key]

                # 如果都没有，返回整个配置
                logger.warning("未找到标准数据库配置格式，使用整个配置")
                return config

        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _load_from_env(self) -> Optional[Dict[str, Any]]:
        """从环境变量加载数据库配置"""
        try:
            from dotenv import load_dotenv
            load_dotenv()

            env_config = {}

            # 基础配置
            if os.getenv('DB_HOST'):
                env_config['host'] = os.getenv('DB_HOST')
            if os.getenv('DB_PORT'):
                env_config['port'] = int(os.getenv('DB_PORT'))
            if os.getenv('DB_USER'):
                env_config['user'] = os.getenv('DB_USER')
            if os.getenv('DB_PASSWORD'):
                env_config['password'] = os.getenv('DB_PASSWORD')
            if os.getenv('DB_NAME'):
                env_config['database'] = os.getenv('DB_NAME')
            if os.getenv('DB_CHARSET'):
                env_config['charset'] = os.getenv('DB_CHARSET')

            # 连接池配置
            if os.getenv('DB_POOL_SIZE'):
                env_config['pool_size'] = int(os.getenv('DB_POOL_SIZE'))
            if os.getenv('DB_POOL_NAME'):
                env_config['pool_name'] = os.getenv('DB_POOL_NAME')

            if env_config:
                logger.info("从环境变量加载数据库配置")
                return env_config

        except Exception as e:
            logger.warning(f"从环境变量加载配置失败: {e}")

        return None

    def _apply_secret_loader_password(self, config: Dict[str, Any]):
        """从secret_loader获取密码（最高优先级）"""
        try:
            # 动态导入secret_loader
            import importlib.util
            import sys

            # 尝试导入secret_loader
            secret_loader_path = Path(__file__).parent.parent / 'config' / 'secret_loader.py'

            if secret_loader_path.exists():
                spec = importlib.util.spec_from_file_location("secret_loader", str(secret_loader_path))
                secret_loader = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(secret_loader)

                # 获取密码
                password = secret_loader.get_db_password()
                if password:
                    config['password'] = password
                    logger.info("从secret_loader获取数据库密码")
                    return True
            else:
                logger.debug("secret_loader.py文件不存在")

        except Exception as e:
            logger.warning(f"从secret_loader获取密码失败: {e}")

        return False

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]):
        """合并配置字典"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_configs(base[key], value)
            else:
                base[key] = value

    def _apply_defaults_and_validate(self, config: Dict[str, Any]):
        """应用默认值并验证配置"""
        # 默认值
        defaults = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '',
            'database': 'stock_database',
            'charset': 'utf8mb4',
            'pool_size': 5,
            'pool_name': 'stock_pool',
            'autocommit': True,
        }

        # 应用默认值
        for key, value in defaults.items():
            if key not in config:
                config[key] = value
                logger.debug(f"应用默认值: {key}={value}")

        # 验证必要字段
        required_keys = ['host', 'port', 'user']
        missing_keys = [key for key in required_keys if not config.get(key)]

        if missing_keys:
            logger.error(f"数据库配置缺少必要字段: {missing_keys}")
            raise ValueError(f"数据库配置缺少必要字段: {missing_keys}")

        # 检查密码
        if not config.get('password'):
            logger.warning("数据库密码未设置，连接可能会失败")

    def _init_connection_pool(self):
        """初始化连接池"""
        try:
            pool_size = self.config.get('pool_size', 5)
            pool_name = self.config.get('pool_name', 'stock_pool')

            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config.get('password', ''),
                database=self.config.get('database', ''),
                charset=self.config.get('charset', 'utf8mb4'),
                autocommit=self.config.get('autocommit', True),
            )

            logger.info(f"数据库连接池初始化成功: 大小={pool_size}")

        except Error as e:
            logger.error(f"初始化连接池失败: {e}")
            self.connection_pool = None

    def get_connection(self, autocommit: bool = None):
        """
        获取数据库连接

        Args:
            autocommit: 是否自动提交，None则使用配置值

        Returns:
            mysql.connector.connection.MySQLConnection
        """
        try:
            if autocommit is None:
                autocommit = self.config.get('autocommit', True)

            if self.connection_pool:
                connection = self.connection_pool.get_connection()
                connection.autocommit = autocommit
                logger.debug("从连接池获取连接")
            else:
                connection = mysql.connector.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config.get('password', ''),
                    database=self.config.get('database', ''),
                    charset=self.config.get('charset', 'utf8mb4'),
                    autocommit=autocommit,
                )
                logger.debug("创建新的数据库连接")

            return connection

        except Error as e:
            logger.error(f"数据库连接失败: {e}")

            # 重试机制
            return self._retry_get_connection(e, autocommit)

    def _retry_get_connection(self, initial_error, autocommit):
        """重试获取连接"""
        max_attempts = 3
        delay = 1  # 初始延迟1秒

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"重试连接数据库 (尝试 {attempt}/{max_attempts})...")
                time.sleep(delay * attempt)  # 指数退避

                connection = mysql.connector.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config.get('password', ''),
                    database=self.config.get('database', ''),
                    charset=self.config.get('charset', 'utf8mb4'),
                    autocommit=autocommit,
                )

                logger.info(f"重试连接成功 (第{attempt}次尝试)")
                return connection

            except Error as e:
                logger.warning(f"重试连接失败 (尝试 {attempt}): {e}")

        logger.error(f"所有重试尝试均失败，初始错误: {initial_error}")
        raise initial_error

    def test_connection(self) -> bool:
        """测试数据库连接是否正常"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # 尝试执行简单查询
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()

            cursor.close()
            conn.close()

            if version:
                logger.info(f"✅ 数据库连接测试成功 - MySQL版本: {version[0]}")
                return True
            return False

        except Error as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False

    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库信息"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            # 获取版本和数据库信息
            cursor.execute("SELECT VERSION() as version, DATABASE() as database_name")
            db_info = cursor.fetchone()

            # 获取所有表
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            table_names = [list(table.values())[0] for table in tables]

            cursor.close()
            conn.close()

            info = {
                'version': db_info['version'] if db_info else 'Unknown',
                'database': db_info['database_name'] if db_info else 'Unknown',
                'tables': table_names,
                'table_count': len(table_names),
                'config': {
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'user': self.config['user'],
                    'database': self.config.get('database', 'Unknown'),
                }
            }

            return info

        except Error as e:
            logger.error(f"获取数据库信息失败: {e}")
            return {
                'version': 'Unknown',
                'database': 'Unknown',
                'tables': [],
                'table_count': 0,
                'config': self.config
            }

    # 保留原有方法，保持兼容性
    def use_database(self, database_name: str) -> bool:
        """使用指定数据库"""
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(f"USE {database_name}")
            cursor.close()
            connection.close()

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

            database_name = self.config.get('database', 'stock_database')

            # 创建数据库
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {database_name} 
                DEFAULT CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
            """)

            # 切换到该数据库
            cursor.execute(f"USE {database_name}")

            cursor.close()
            connection.close()

            logger.info(f"数据库 '{database_name}' 创建/确认完成")
            return True

        except Error as e:
            logger.error(f"创建数据库失败: {e}")
            return False

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
                connection.commit()
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
            if connection:
                connection.close()

    def close_all_connections(self):
        """关闭所有连接"""
        if self.connection_pool:
            try:
                self.connection_pool._remove_connections()
                logger.info("数据库连接池已清理")
            except:
                pass

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_all_connections()


# 便捷函数
def create_db_connector(config_path: str = 'config/database.yaml') -> DatabaseConnector:
    """创建数据库连接器的便捷函数"""
    return DatabaseConnector(config_path)


def test_database_connection(config_path: str = 'config/database.yaml') -> bool:
    """测试数据库连接的便捷函数"""
    try:
        db = DatabaseConnector(config_path)
        return db.test_connection()
    except Exception as e:
        logger.error(f"测试数据库连接失败: {e}")
        return False


# 使用示例
if __name__ == "__main__":
    import logging

    # 配置详细日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试增强版数据库连接器")
    print("=" * 50)

    try:
        # 测试连接
        success = test_database_connection()

        if success:
            print("✅ 数据库连接测试成功!")

            # 获取详细数据库信息
            db = DatabaseConnector()
            db_info = db.get_database_info()

            print(f"\n📊 数据库信息:")
            print(f"  版本: {db_info['version']}")
            print(f"  数据库: {db_info['database']}")
            print(f"  表数量: {db_info['table_count']}")
            print(f"  配置: {db_info['config']['user']}@{db_info['config']['host']}:{db_info['config']['port']}")

            if db_info['tables']:
                print(f"  表列表 (前10个):")
                for table in db_info['tables'][:10]:
                    print(f"    - {table}")
                if len(db_info['tables']) > 10:
                    print(f"    ... 还有 {len(db_info['tables']) - 10} 个表")
            else:
                print("  ℹ️ 数据库中没有表")
        else:
            print("\n❌ 数据库连接测试失败")
            print("\n💡 调试建议:")
            print("1. 检查MySQL服务是否运行")
            print("2. 检查 config/database.yaml 配置")
            print("3. 检查 .env 文件中的 DB_PASSWORD")
            print("4. 检查 secret_loader.py 是否能正确获取密码")
            print("5. 尝试直接连接: mysql -u用户名 -p密码 -h主机 -P端口")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback

        traceback.print_exc()