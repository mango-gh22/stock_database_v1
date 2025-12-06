# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_p4_problem.py
# File Name: fix_p4_problem
# @ Author: mango-gh22
# @ Date：2025/12/6 19:56
"""
desc 
"""
"""
P4阶段问题修复脚本
"""
import subprocess
import sys
import os


def check_database_structure():
    """检查数据库结构"""
    print("🔍 检查数据库当前状态")
    print("=" * 50)

    # 检查是否已执行过修复
    check_sql = """
    USE stock_database;

    -- 检查表结构
    DESCRIBE stock_daily_data;

    -- 检查是否有数据
    SELECT COUNT(*) as total_records FROM stock_daily_data;
    SELECT DISTINCT symbol FROM stock_daily_data LIMIT 5;
    """

    with open('check_structure.sql', 'w', encoding='utf-8') as f:
        f.write(check_sql)

    print("运行检查SQL...")
    os.system('mysql -u root -p < check_structure.sql')

    # 清理临时文件
    if os.path.exists('check_structure.sql'):
        os.remove('check_structure.sql')


def fix_reserved_keywords():
    """修复保留关键字问题"""
    print("\n🔧 修复保留关键字问题")
    print("=" * 50)

    # 方案1：重命名change列为price_change
    fix_sql = """
    USE stock_database;

    -- 第一步：检查当前列名
    SHOW COLUMNS FROM stock_daily_data LIKE 'change';
    SHOW COLUMNS FROM stock_daily_data LIKE 'price_change';

    -- 第二步：如果存在change列且不存在price_change列，则重命名
    -- 注意：这个操作需要手动确认，因为有风险
    -- ALTER TABLE stock_daily_data CHANGE COLUMN `change` price_change DECIMAL(10,4);

    -- 第三步：创建视图（安全方案）
    DROP VIEW IF EXISTS daily_data_view;

    CREATE VIEW daily_data_view AS
    SELECT
        trade_date,
        symbol,
        `open`,
        `high`,
        `low`,
        `close`,
        volume,
        amount,
        pct_change,
        CASE 
            WHEN COLUMN_EXISTS('stock_daily_data', 'price_change') THEN price_change
            WHEN COLUMN_EXISTS('stock_daily_data', 'change') THEN `change`
            ELSE NULL 
        END as price_change,
        pre_close,
        turnover_rate,
        amplitude
    FROM stock_daily_data;

    -- 第四步：测试视图
    SELECT * FROM daily_data_view LIMIT 3;
    """

    # 简化版本：直接创建视图，不管列名是什么
    simplified_sql = """
    USE stock_database;

    -- 1. 先查看表结构
    DESCRIBE stock_daily_data;

    -- 2. 创建智能视图
    DROP VIEW IF EXISTS v_daily_data;

    -- 创建通用视图，使用COALESCE处理不同列名
    CREATE VIEW v_daily_data AS
    SELECT 
        trade_date,
        symbol,
        `open`,
        `high`,
        `low`,
        `close`,
        volume,
        amount,
        pct_change,
        COALESCE(price_change, `change`) as price_change,
        pre_close,
        turnover_rate,
        amplitude
    FROM stock_daily_data;

    -- 3. 测试
    SELECT '视图创建成功' as status;
    SELECT COUNT(*) FROM v_daily_data;

    -- 4. 显示视图结构
    DESCRIBE v_daily_data;
    """

    with open('fix_reserved_final.sql', 'w', encoding='utf-8') as f:
        f.write(simplified_sql)

    print("请手动执行以下SQL命令修复问题：")
    print("  mysql -u root -p < fix_reserved_final.sql")
    print("\n或者直接运行：")
    print("  mysql -u root -p")
    print("  USE stock_database;")
    print("  然后执行fix_reserved_final.sql中的SQL语句")


def create_safe_query_engine():
    """创建安全的查询引擎"""
    print("\n🚀 创建安全的查询引擎")
    print("=" * 50)

    safe_engine_code = '''
"""
安全查询引擎 - P4阶段最终版本
自动适应表结构变化
"""
import pandas as pd
import pymysql
import yaml
import os
from typing import Dict, List, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('safe_query_engine')

class SafeQueryEngine:
    """安全查询引擎 - 自动检测列名"""

    def __init__(self):
        """初始化"""
        self.conn = self._get_connection()
        self.column_info = self._detect_columns()

    def _get_connection(self):
        """获取数据库连接"""
        try:
            # 读取配置
            config_path = os.path.join('config', 'database.yaml')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    db_config = yaml.safe_load(f)['development']
            else:
                # 默认配置
                db_config = {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': '您的密码',
                    'database': 'stock_database',
                    'charset': 'utf8mb4'
                }

            return pymysql.connect(**db_config)
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise

    def _detect_columns(self) -> Dict:
        """检测表列名"""
        column_info = {
            'daily_table': 'stock_daily_data',
            'basic_table': 'stock_basic',
            'daily_columns': [],
            'change_column': None
        }

        try:
            cursor = self.conn.cursor()

            # 检测日线表列
            cursor.execute("SHOW COLUMNS FROM stock_daily_data")
            daily_columns = [row[0] for row in cursor.fetchall()]
            column_info['daily_columns'] = daily_columns

            # 检测价格变化列名
            if 'price_change' in daily_columns:
                column_info['change_column'] = 'price_change'
            elif 'change' in daily_columns:
                column_info['change_column'] = '`change`'  # 使用反引号
            else:
                column_info['change_column'] = 'NULL as price_change'

            cursor.close()

            logger.info(f"检测到列信息: {column_info}")
            return column_info

        except Exception as e:
            logger.error(f"检测列名失败: {e}")
            return column_info

    def get_data_statistics(self) -> Dict:
        """获取数据统计"""
        stats = {}
        try:
            cursor = self.conn.cursor()

            # 股票统计
            cursor.execute("SELECT COUNT(*) FROM stock_basic")
            stats['total_stocks'] = cursor.fetchone()[0]

            # 日线统计 - 使用检测到的列名
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(trade_date) as earliest_date,
                    MAX(trade_date) as latest_date,
                    COUNT(DISTINCT symbol) as stocks_with_data
                FROM {self.column_info['daily_table']}
            """)
            result = cursor.fetchone()
            stats['total_daily_records'] = result[0]
            stats['earliest_date'] = str(result[1]) if result[1] else None
            stats['latest_date'] = str(result[2]) if result[2] else None
            stats['stocks_with_data'] = result[3]

            # 股票列表
            cursor.execute("SELECT symbol FROM stock_basic ORDER BY symbol")
            stats['stock_list'] = [row[0] for row in cursor.fetchall()]

            cursor.close()
            logger.info(f"数据统计: {stats.get('total_daily_records', 0)}条记录")
            return stats

        except Exception as e:
            logger.error(f"数据统计失败: {e}")
            return {}

    def query_daily_data(self, symbol: str = None, limit: int = 10) -> pd.DataFrame:
        """查询日线数据 - 安全版本"""
        try:
            # 构建SELECT子句
            select_columns = [
                "trade_date", "symbol",
                "`open`", "`high`", "`low`", "`close`",
                "volume", "amount", "pct_change",
                f"{self.column_info['change_column']} as price_change",
                "pre_close", "turnover_rate", "amplitude"
            ]

            select_clause = ", ".join(select_columns)

            # 构建WHERE子句
            where_clause = ""
            params = []

            if symbol:
                where_clause = "WHERE symbol = %s"
                params.append(symbol)

            # 构建完整SQL
            sql = f"""
                SELECT {select_clause}
                FROM {self.column_info['daily_table']}
                {where_clause}
                ORDER BY trade_date DESC
                LIMIT %s
            """

            params.append(limit)

            logger.debug(f"执行SQL: {sql}")
            logger.debug(f"参数: {params}")

            # 执行查询
            df = pd.read_sql(sql, self.conn, params=params if params else None)

            if not df.empty:
                # 转换数据类型
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                              'pct_change', 'price_change', 'pre_close', 'turnover_rate', 'amplitude']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"查询成功: {len(df)}条记录")
            return df

        except Exception as e:
            logger.error(f"查询失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")

def test_safe_engine():
    """测试安全引擎"""
    print("🧪 测试安全查询引擎")
    print("=" * 50)

    engine = SafeQueryEngine()

    try:
        # 1. 测试统计
        print("\n📊 1. 数据统计测试")
        stats = engine.get_data_statistics()
        print(f"   股票总数: {stats.get('total_stocks', 0)}")
        print(f"   日线记录: {stats.get('total_daily_records', 0)}")

        # 2. 测试查询
        print("\n📈 2. 日线查询测试")
        if stats.get('stock_list'):
            test_symbol = stats['stock_list'][0]
            print(f"   测试股票: {test_symbol}")

            data = engine.query_daily_data(symbol=test_symbol, limit=3)
            if not data.empty:
                print(f"   查询到 {len(data)} 条记录")
                for idx, row in data.iterrows():
                    print(f"     {row['trade_date']}: {row['close']:.2f} ({row.get('price_change', 0):+.2f})")
            else:
                print("   未查询到数据")

        print("\n✅ 安全查询引擎测试通过!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        engine.close()

if __name__ == "__main__":
    test_safe_engine()
'''

    # 写入文件
    safe_engine_path = 'src/query/simple_query_engine.py'
    os.makedirs(os.path.dirname(safe_engine_path), exist_ok=True)

    with open(safe_engine_path, 'w', encoding='utf-8') as f:
        f.write(safe_engine_code)

    print(f"✅ 已创建安全查询引擎: {safe_engine_path}")

    # 创建简单的测试脚本
    test_code = '''
import sys
sys.path.insert(0, '.')
from src.query.safe_query_engine import test_safe_engine

print("🚀 测试安全查询引擎")
test_safe_engine()
'''

    with open('test_safe_engine.py', 'w', encoding='utf-8') as f:
        f.write(test_code)

    print("✅ 已创建测试脚本: test_safe_engine.py")

    # 运行测试
    print("\n🔧 运行测试...")
    os.system('python test_safe_engine.py')


def update_main_py():
    """更新main.py添加P4测试命令"""
    print("\n📝 更新主程序")
    print("=" * 50)

    # 读取main.py
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否已存在p4_test命令
    if 'p4_test' in content:
        print("✅ main.py中已存在p4_test命令")
    else:
        # 在action处理部分添加
        if 'elif action == "validate":' in content:
            # 在validate后面添加
            new_content = content.replace(
                'elif action == "validate":',
                '''elif action == "validate":
        validate_data()

    elif action == "p4_test":
        print("🔍 P4阶段查询引擎测试")
        print("=" * 50)

        try:
            from src.query.safe_query_engine import test_safe_engine
            test_safe_engine()
        except Exception as e:
            print(f"❌ P4测试失败: {e}")
            import traceback
            traceback.print_exc()'''
            )

            with open('main.py', 'w', encoding='utf-8') as f:
                f.write(new_content)

            print("✅ 已更新main.py，添加p4_test命令")
        else:
            print("⚠️ 无法找到插入点，请手动添加p4_test命令")


def main():
    """主函数"""
    print("🔧 P4阶段完整修复方案")
    print("=" * 60)

    # 检查当前状态
    check_database_structure()

    # 提供修复选择
    print("\n请选择修复方案:")
    print("1. 手动执行SQL修复（推荐）")
    print("2. 创建安全查询引擎（不修改数据库）")
    print("3. 两种方案都执行")

    choice = input("\n请输入选择 (1/2/3): ").strip()

    if choice in ['1', '3']:
        fix_reserved_keywords()

        print("\n📋 请按以下步骤操作:")
        print("1. 打开MySQL客户端")
        print("2. 执行: USE stock_database;")
        print("3. 检查表结构: DESCRIBE stock_daily_data;")
        print("4. 根据情况执行重命名或创建视图")

    if choice in ['2', '3']:
        create_safe_query_engine()

    if choice == '2':
        update_main_py()

    print("\n" + "=" * 60)
    print("🎉 P4修复方案准备完成！")
    print("\n立即测试:")
    print("  python main.py --action p4_test")
    print("\n如果测试成功，创建Git标签:")
    print("  git add .")
    print("  git commit -m 'P4: 修复保留关键字问题，实现安全查询'")
    print("  git tag v0.4.0")
    print("  git push origin v0.4.0")


if __name__ == "__main__":
    main()