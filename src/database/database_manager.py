# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/database\database_manager.py
# File Name: database_manager
# @ File: database_manager.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 18:25
"""
desc 数据库管理模块
"""

# src/database/database_manager.py
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import mysql.connector
from mysql.connector import Error
import yaml

from src.config.logging_config import setup_logging
from src.database.db_connector import DatabaseConnector

logger = setup_logging()


class DatabaseManager:
    """数据库管理类，负责创建表结构和管理数据库"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化数据库管理器

        Args:
            config_path: 数据库配置文件路径
        """
        self.db_connector = DatabaseConnector(config_path)
        self.config_path = config_path

    def execute_sql_file(self, sql_file_path: str) -> bool:
        """
        执行SQL文件

        Args:
            sql_file_path: SQL文件路径

        Returns:
            bool: 执行是否成功
        """
        try:
            # 读取SQL文件
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 分割SQL语句（以分号分隔）
            sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            success_count = 0
            total_count = len(sql_statements)

            for i, sql_stmt in enumerate(sql_statements, 1):
                try:
                    cursor.execute(sql_stmt)
                    logger.info(f"执行SQL语句 {i}/{total_count} 成功")
                    success_count += 1
                except Error as e:
                    logger.error(f"执行SQL语句 {i}/{total_count} 失败: {e}")
                    logger.error(f"SQL语句: {sql_stmt[:200]}...")  # 只打印前200个字符
                    # 继续执行其他语句

            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"SQL文件执行完成: 成功 {success_count}/{total_count} 条语句")
            return success_count == total_count

        except Exception as e:
            logger.error(f"执行SQL文件失败: {e}")
            return False

    def create_all_tables(self) -> bool:
        """
        创建所有数据表

        Returns:
            bool: 创建是否成功
        """
        sql_file_path = Path('scripts/schema/create_tables.sql')

        if not sql_file_path.exists():
            logger.error(f"SQL文件不存在: {sql_file_path}")
            return False

        logger.info("开始创建数据库表...")
        return self.execute_sql_file(str(sql_file_path))

    def drop_all_tables(self) -> bool:
        """
        删除所有数据表（谨慎使用！）

        Returns:
            bool: 删除是否成功
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            # 获取所有表名
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            if not tables:
                logger.info("数据库中没有表需要删除")
                return True

            # 禁用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            # 删除所有表
            for (table_name,) in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.info(f"删除表: {table_name}")
                except Error as e:
                    logger.error(f"删除表 {table_name} 失败: {e}")

            # 启用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            connection.commit()
            cursor.close()
            connection.close()

            logger.info("所有表删除完成")
            return True

        except Exception as e:
            logger.error(f"删除表失败: {e}")
            return False

    def check_table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() 
                AND table_name = %s
            """, (table_name,))

            exists = cursor.fetchone()[0] > 0

            cursor.close()
            connection.close()

            return exists

        except Exception as e:
            logger.error(f"检查表是否存在失败: {e}")
            return False

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            dict: 表结构信息，如果表不存在返回None
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor(dictionary=True)

            # 获取表基本信息
            cursor.execute("""
                SELECT 
                    TABLE_NAME,
                    TABLE_ROWS,
                    AVG_ROW_LENGTH,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    CREATE_TIME,
                    UPDATE_TIME,
                    TABLE_COLLATION
                FROM information_schema.tables
                WHERE table_schema = DATABASE() 
                AND table_name = %s
            """, (table_name,))

            table_info = cursor.fetchone()

            if not table_info:
                cursor.close()
                connection.close()
                return None

            # 获取列信息
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            table_info['columns'] = columns

            # 获取索引信息
            cursor.execute(f"SHOW INDEX FROM {table_name}")
            indexes = cursor.fetchall()
            table_info['indexes'] = indexes

            cursor.close()
            connection.close()

            return table_info

        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            return None

    def get_all_tables(self) -> List[str]:
        """
        获取数据库中所有表名

        Returns:
            list: 表名列表
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()

            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]

            cursor.close()
            connection.close()

            return tables

        except Exception as e:
            logger.error(f"获取表列表失败: {e}")
            return []

    def create_database_if_not_exists(self) -> bool:
        """
        创建数据库（如果不存在）

        Returns:
            bool: 创建是否成功
        """
        try:
            # 读取配置文件
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            database_name = config['database']

            # 临时连接到MySQL（不指定数据库）
            temp_config = config.copy()
            temp_config.pop('database', None)

            connection = mysql.connector.connect(**temp_config)
            cursor = connection.cursor()

            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name} "
                           f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"数据库 '{database_name}' 创建/确认完成")
            return True

        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            return False

    def backup_database(self, backup_path: str = 'backups/') -> bool:
        """
        备份数据库结构（不包括数据）

        Args:
            backup_path: 备份路径

        Returns:
            bool: 备份是否成功
        """
        try:
            import subprocess
            from datetime import datetime

            # 创建备份目录
            Path(backup_path).mkdir(parents=True, exist_ok=True)

            # 读取配置文件
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            database_name = config['database']
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = Path(backup_path) / f"{database_name}_schema_{timestamp}.sql"

            # 使用mysqldump备份结构
            cmd = [
                'mysqldump',
                '-h', config['host'],
                '-P', str(config['port']),
                '-u', config['user'],
                f"--password={config['password']}",
                '--no-data',  # 不备份数据
                '--skip-comments',
                '--skip-add-drop-table',
                database_name
            ]

            with open(backup_file, 'w', encoding='utf-8') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)

            if result.returncode == 0:
                logger.info(f"数据库结构备份成功: {backup_file}")
                return True
            else:
                logger.error(f"数据库备份失败: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"备份数据库失败: {e}")
            return False


# 使用示例
if __name__ == "__main__":
    db_manager = DatabaseManager()

    # 创建数据库（如果不存在）
    db_manager.create_database_if_not_exists()

    # 创建所有表
    db_manager.create_all_tables()

    # 检查表是否创建成功
    tables = db_manager.get_all_tables()
    print(f"数据库中的表: {tables}")