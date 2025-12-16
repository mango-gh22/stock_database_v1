# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\fix_existing_dates.py
# File Name: fix_existing_dates
# @ Author: mango-gh22
# @ Date：2025/12/15 0:44
"""
desc 数据清洗脚本修复现有数据库数据
"""
# -*- coding: utf-8 -*-
"""
修复现有数据库中的日期格式问题 - 最终版
解决重复数据冲突问题
"""

import pandas as pd
from datetime import datetime
import logging
from src.database.db_connector import DatabaseConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    """获取数据库连接"""
    db = DatabaseConnector()
    return db.get_connection()


def analyze_duplicate_patterns():
    """分析重复数据模式"""
    try:
        logger.info("分析重复数据模式...")
        conn = get_connection()

        # 找出所有重复的symbol+date组合
        query = """
            SELECT symbol, 
                   CASE 
                     WHEN LENGTH(trade_date) = 8 AND trade_date REGEXP '^[0-9]+$' THEN 'old_format'
                     WHEN trade_date LIKE '____-__-__' THEN 'new_format'
                     ELSE 'other_format'
                   END as date_format,
                   COUNT(*) as count
            FROM stock_daily_data
            WHERE trade_date IS NOT NULL
            GROUP BY symbol, date_format
            ORDER BY symbol, date_format
        """

        df_patterns = pd.read_sql_query(query, conn)

        if not df_patterns.empty:
            logger.info("重复数据模式分析:")
            for _, row in df_patterns.iterrows():
                logger.info(f"  {row['symbol']}: {row['date_format']} - {row['count']} 条")

        # 找出具体哪些记录有冲突
        query = """
            SELECT t1.symbol, t1.trade_date as old_date, t2.trade_date as new_date, 
                   t1.id as old_id, t2.id as new_id
            FROM stock_daily_data t1
            JOIN stock_daily_data t2 ON t1.symbol = t2.symbol 
                AND DATE_FORMAT(STR_TO_DATE(t1.trade_date, '%Y%m%d'), '%Y-%m-%d') = t2.trade_date
            WHERE LENGTH(t1.trade_date) = 8 
                AND t1.trade_date REGEXP '^[0-9]+$'
                AND t2.trade_date LIKE '____-__-__'
        """

        df_conflicts = pd.read_sql_query(query, conn)

        if not df_conflicts.empty:
            logger.info(f"发现 {len(df_conflicts)} 条格式冲突记录:")
            for _, row in df_conflicts.head(10).iterrows():  # 只显示前10条
                logger.info(
                    f"  {row['symbol']}: {row['old_date']}(ID:{row['old_id']}) vs {row['new_date']}(ID:{row['new_id']})")

        conn.close()
        return len(df_conflicts)

    except Exception as e:
        logger.error(f"分析重复模式失败: {e}")
        return 0


def fix_duplicate_conflicts():
    """修复重复数据冲突 - 保留新格式，删除旧格式"""
    try:
        logger.info("开始修复重复数据冲突...")
        conn = get_connection()
        cursor = conn.cursor()

        # 找出所有旧格式的记录（YYYYMMDD格式）
        query_old = """
            SELECT id, symbol, trade_date 
            FROM stock_daily_data 
            WHERE LENGTH(trade_date) = 8 
                AND trade_date REGEXP '^[0-9]+$'
        """

        cursor.execute(query_old)
        old_records = cursor.fetchall()

        if not old_records:
            logger.info("无旧格式记录需要处理")
            cursor.close()
            conn.close()
            return 0

        logger.info(f"找到 {len(old_records)} 条旧格式记录")

        # 找出哪些旧记录有对应新格式的重复记录
        delete_ids = []
        keep_ids = []

        for old_id, symbol, old_date in old_records:
            # 将旧格式转换为新格式
            new_date = f"{old_date[:4]}-{old_date[4:6]}-{old_date[6:8]}"

            # 检查是否存在相同symbol+新日期的记录
            query_check = """
                SELECT id FROM stock_daily_data 
                WHERE symbol = %s AND trade_date = %s
            """

            cursor.execute(query_check, (symbol, new_date))
            new_records = cursor.fetchall()

            if new_records:
                # 存在新格式记录，删除旧格式
                delete_ids.append(old_id)
                logger.debug(f"冲突: {symbol} {old_date} -> {new_date}，保留新格式(ID:{new_records[0][0]})")
            else:
                # 没有新格式记录，更新旧格式为新格式
                keep_ids.append(old_id)

        # 删除有冲突的旧记录
        deleted_count = 0
        if delete_ids:
            # 批量删除
            placeholders = ','.join(['%s'] * len(delete_ids))
            delete_query = f"DELETE FROM stock_daily_data WHERE id IN ({placeholders})"

            cursor.execute(delete_query, delete_ids)
            deleted_count = cursor.rowcount

            logger.info(f"删除 {deleted_count} 条冲突的旧格式记录")

        # 更新无冲突的旧记录
        updated_count = 0
        if keep_ids:
            for old_id in keep_ids:
                # 获取旧记录信息
                cursor.execute("SELECT symbol, trade_date FROM stock_daily_data WHERE id = %s", (old_id,))
                symbol, old_date = cursor.fetchone()

                # 转换为新格式
                new_date = f"{old_date[:4]}-{old_date[4:6]}-{old_date[6:8]}"

                # 更新记录
                update_query = "UPDATE stock_daily_data SET trade_date = %s WHERE id = %s"
                cursor.execute(update_query, (new_date, old_id))

                if cursor.rowcount > 0:
                    updated_count += 1
                    logger.debug(f"更新: {symbol} {old_date} -> {new_date}")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"冲突修复完成: 删除 {deleted_count} 条, 更新 {updated_count} 条")
        return deleted_count + updated_count

    except Exception as e:
        logger.error(f"修复冲突失败: {e}")
        import traceback
        traceback.print_exc()
        return 0


def verify_fix_results():
    """验证修复结果"""
    try:
        logger.info("验证修复结果...")
        conn = get_connection()

        # 1. 检查是否还有YYYYMMDD格式
        query_old_format = """
            SELECT COUNT(*) as count
            FROM stock_daily_data 
            WHERE LENGTH(trade_date) = 8 
                AND trade_date REGEXP '^[0-9]+$'
        """

        df_old = pd.read_sql_query(query_old_format, conn)
        old_count = df_old.iloc[0]['count']

        if old_count == 0:
            logger.info("✅ 已无YYYYMMDD格式的记录")
        else:
            logger.warning(f"仍有 {old_count} 条YYYYMMDD格式的记录")

        # 2. 检查重复数据
        query_duplicates = """
            SELECT symbol, trade_date, COUNT(*) as count
            FROM stock_daily_data
            WHERE trade_date IS NOT NULL
            GROUP BY symbol, trade_date
            HAVING COUNT(*) > 1
        """

        df_duplicates = pd.read_sql_query(query_duplicates, conn)

        if df_duplicates.empty:
            logger.info("✅ 无重复数据")
        else:
            logger.warning(f"仍有 {len(df_duplicates)} 组重复数据")
            for _, row in df_duplicates.iterrows():
                logger.warning(f"  重复: {row['symbol']} - {row['trade_date']}: {row['count']} 条")

        # 3. 显示样本数据
        query_sample = """
            SELECT symbol, trade_date, DATE_FORMAT(trade_date, '%Y-%m-%d') as formatted_date
            FROM stock_daily_data
            WHERE symbol IN ('sh600519', 'sz000001', 'sz000858')
            ORDER BY symbol, trade_date
            LIMIT 15
        """

        df_sample = pd.read_sql_query(query_sample, conn)

        if not df_sample.empty:
            logger.info("样本数据:")
            current_symbol = None
            for _, row in df_sample.iterrows():
                if row['symbol'] != current_symbol:
                    logger.info(f"  {row['symbol']}:")
                    current_symbol = row['symbol']
                logger.info(f"    {row['trade_date']}")

        conn.close()

        return old_count == 0 and df_duplicates.empty

    except Exception as e:
        logger.error(f"验证失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("开始修复数据库中的日期格式问题")

    # 步骤1: 分析问题
    conflict_count = analyze_duplicate_patterns()

    if conflict_count == 0:
        logger.info("未发现格式冲突，只需简单格式化")
        # 这里可以添加简单格式化的逻辑
    else:
        # 步骤2: 修复冲突
        fixed_count = fix_duplicate_conflicts()

        if fixed_count > 0:
            logger.info(f"成功修复 {fixed_count} 条记录")

    # 步骤3: 验证结果
    success = verify_fix_results()

    if success:
        logger.info("✅ 修复完成！所有日期格式已统一为YYYY-MM-DD")
    else:
        logger.warning("⚠️ 修复完成，但仍有问题需要手动处理")

    # 建议下一步操作
    logger.info("\n建议下一步操作:")
    logger.info("1. 运行测试确保数据完整: python -m tests.integration.test_date_format")
    logger.info("2. 导入复权因子: python scripts/import_adjustment_factors.py")
    logger.info("3. 标准化股票代码: python scripts/normalize_existing_symbols.py")


if __name__ == "__main__":
    main()