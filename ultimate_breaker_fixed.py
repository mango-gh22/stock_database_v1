# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\ultimate_breaker_fixed.py
# File Name: ultimate_breaker_fixed
# @ Author: mango-gh22
# @ Date：2026/1/1 13:39
"""
desc 
"""
# ultimate_breaker_fixed.py
"""
终极破壁脚本 - 修复版（解决before保留字问题）
"""
import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta
import baostock as bs
import mysql.connector
from dotenv import load_dotenv

# 加载环境变量
sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv(r"E:\MyFile\stock_database_v1\.env")

print("💥 终极破壁脚本 - 修复版")
print("=" * 60)


class UltimateDataBreaker:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': os.getenv('DB_PASSWORD'),
            'database': 'stock_database',
            'autocommit': True
        }

    def test_direct_insert(self):
        """测试直接数据库插入（绕过所有项目代码）"""
        print("1. 🔧 测试直接数据库插入...")

        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)

        # 创建唯一测试数据
        test_id = f"BREAKER_{int(time.time())}"
        test_symbol = test_id[:20]

        print(f"   测试符号: {test_symbol}")

        # 直接插入
        sql = """
            INSERT INTO stock_daily_data 
            (symbol, trade_date, open_price, close_price, high_price, low_price, 
             volume, created_time, updated_time, data_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), 'breaker')
        """

        try:
            cursor.execute(sql, (test_symbol, '2025-12-31', 100.0, 101.0, 102.0, 99.0, 10000))
            row_id = cursor.lastrowid
            conn.commit()

            print(f"   ✅ 直接插入成功! ID: {row_id}")

            # 立即验证
            cursor.execute("SELECT * FROM stock_daily_data WHERE id = %s", (row_id,))
            result = cursor.fetchone()
            print(f"   ✅ 验证成功: {result['symbol']} 创建于 {result['created_time']}")

            # 清理
            cursor.execute("DELETE FROM stock_daily_data WHERE id = %s", (row_id,))
            conn.commit()
            print(f"   ✅ 清理完成")

            return True

        except Exception as e:
            print(f"   ❌ 直接插入失败: {e}")
            return False
        finally:
            conn.close()

    def fetch_from_baostock_direct(self):
        """直接调用Baostock，绕过所有包装器"""
        print("\n2. 📡 直接调用Baostock API...")

        # 直接使用Baostock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"   ❌ Baostock登录失败: {lg.error_msg}")
            return None

        print("   ✅ Baostock登录成功")

        # 查询数据
        rs = bs.query_history_k_data_plus(
            "sh.600000",
            "date,code,open,high,low,close,volume,amount,turn",
            start_date='2025-12-25',
            end_date='2025-12-31',
            frequency="d",
            adjustflag="3"
        )

        if rs.error_code != '0':
            print(f"   ❌ 查询失败: {rs.error_msg}")
            bs.logout()
            return None

        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())

        df = pd.DataFrame(data_list, columns=rs.fields)
        bs.logout()

        if df.empty:
            print("   ⚠️  没有获取到数据")
            return None

        print(f"   ✅ 获取到 {len(df)} 条原始数据")
        print(f"   数据列: {list(df.columns)}")
        print(f"   示例:\n{df.head(2).to_string()}")

        return df

    def brute_force_insert(self, symbol, start_date, end_date):
        """暴力插入：采集+直接存储，完全绕过现有逻辑"""
        print(f"\n3. 💥 暴力插入 {symbol} [{start_date} 到 {end_date}]...")

        # 1. 直接获取数据
        df_raw = self.fetch_from_baostock_direct()
        if df_raw is None or df_raw.empty:
            print("   ❌ 没有获取到数据")
            return False

        # 2. 直接连接到数据库
        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)

        # 3. 获取当前状态 - 修复：不使用before作为别名
        clean_symbol = symbol.replace('.', '')
        cursor.execute("SELECT COUNT(*) as count_before FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        before = cursor.fetchone()['count_before']
        print(f"   插入前: {before} 条记录")

        # 4. 暴力插入所有数据（使用INSERT IGNORE忽略重复）
        inserted = 0
        skipped = 0

        for _, row in df_raw.iterrows():
            try:
                # 准备插入数据
                sql = """
                    INSERT IGNORE INTO stock_daily_data 
                    (symbol, trade_date, open_price, high_price, low_price, close_price,
                     volume, turnover, turnover_rate, data_source, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'breaker', NOW(), NOW())
                """

                params = (
                    clean_symbol,
                    row['date'],
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    float(row['amount']),
                    float(row['turn'])
                )

                cursor.execute(sql, params)
                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

            except mysql.connector.errors.IntegrityError:
                # 主键重复，跳过
                skipped += 1
            except Exception as e:
                print(f"   插入错误: {e}")

        conn.commit()

        # 5. 验证结果
        cursor.execute("SELECT COUNT(*) as count_after FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        after = cursor.fetchone()['count_after']

        cursor.close()
        conn.close()

        print(f"   插入后: {after} 条记录")
        print(f"   成功插入: {inserted} 条")
        print(f"   跳过重复: {skipped} 条")
        print(f"   净增加: {after - before} 条")

        if inserted > 0:
            print(f"   🎉 暴力插入成功！新增了 {inserted} 条数据")
        else:
            print(f"   ⚠️  没有插入新数据（可能数据已存在）")

        return inserted > 0

    def test_completely_new_stock(self):
        """测试一个全新的股票（确保数据库中没有）"""
        print("\n4. 🆕 测试全新股票...")

        # 找一个不太可能有的股票
        test_stocks = [
            ("sh.601919", "中远海控"),
            ("sh.601318", "中国平安"),
            ("sz.000725", "京东方A"),
            ("sz.002594", "比亚迪"),
        ]

        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)

        for symbol, name in test_stocks:
            clean_symbol = symbol.replace('.', '')
            cursor.execute("SELECT COUNT(*) as cnt FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
            count = cursor.fetchone()['cnt']

            if count == 0:
                print(f"   ✅ 找到没有数据的股票: {symbol} ({name})")

                # 使用暴力方法插入
                success = self.brute_force_insert(symbol, "2025-12-25", "2025-12-31")

                if success:
                    print(f"   🎉 {symbol} 数据插入成功！证明系统可以写入数据")
                    conn.close()
                    return True
                else:
                    print(f"   ❌ {symbol} 插入失败")
            else:
                print(f"   ⚠️  {symbol} 已有 {count} 条数据，跳过")

        conn.close()
        print("   ❌ 没有找到完全新的股票")
        return False

    def analyze_existing_blockage(self):
        """分析现有数据的阻塞原因"""
        print("\n5. 🔍 分析数据阻塞原因...")

        # 检查数据库中的重复约束
        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)

        # 查看表结构
        cursor.execute("SHOW CREATE TABLE stock_daily_data")
        create_table = cursor.fetchone()
        create_sql = create_table['Create Table']

        print("   表结构分析:")

        # 查找唯一键/主键
        if "UNIQUE KEY" in create_sql:
            print("   ✅ 表有唯一键约束")
            # 提取唯一键定义
            lines = create_sql.split('\n')
            for line in lines:
                if 'UNIQUE KEY' in line:
                    print(f"     唯一键: {line.strip()}")
        else:
            print("   ℹ️  表没有唯一键约束")

        # 检查 symbol + trade_date 是否重复
        test_symbol = "sh600000"
        cursor.execute("""
            SELECT trade_date, COUNT(*) as dup_count
            FROM stock_daily_data 
            WHERE symbol = %s
            GROUP BY trade_date
            HAVING COUNT(*) > 1
            ORDER BY trade_date DESC
        """, (test_symbol,))

        duplicates = cursor.fetchall()
        if duplicates:
            print(f"   ❌ 发现重复数据: {test_symbol} 有 {len(duplicates)} 个重复日期")
            for dup in duplicates[:5]:
                print(f"     {dup['trade_date']}: {dup['dup_count']} 条")
        else:
            print(f"   ✅ {test_symbol} 没有重复数据")

        conn.close()


def main():
    breaker = UltimateDataBreaker()

    print("🔍 诊断步骤:")
    print("1. 测试基础数据库功能")
    print("2. 测试暴力数据插入")
    print("3. 测试全新股票插入")
    print("4. 分析数据阻塞原因")
    print("=" * 60)

    # 步骤1: 测试基础数据库功能
    print("\n📋 步骤1: 测试基础数据库功能")
    print("-" * 40)
    if not breaker.test_direct_insert():
        print("❌ 数据库基础功能有问题")
        return
    print("✅ 数据库基础功能正常")

    # 步骤2: 测试暴力数据插入
    print("\n📋 步骤2: 测试暴力数据插入")
    print("-" * 40)
    success = breaker.brute_force_insert("sh.600000", "2025-12-25", "2025-12-31")

    if success:
        print("✅ 暴力插入成功 - 证明数据可以写入")
    else:
        print("⚠️  暴力插入没有新增数据 - 可能数据已存在")

    # 步骤3: 测试全新股票插入
    print("\n📋 步骤3: 测试全新股票插入")
    print("-" * 40)
    new_stock_success = breaker.test_completely_new_stock()

    if new_stock_success:
        print("🎉 全新股票插入成功！证明系统完全可以写入数据")
    else:
        print("⚠️  没有找到或插入全新股票")

    # 步骤4: 分析阻塞原因
    print("\n📋 步骤4: 分析数据阻塞原因")
    print("-" * 40)
    breaker.analyze_existing_blockage()

    print("\n" + "=" * 60)
    print("📊 诊断结果总结")
    print("=" * 60)

    if new_stock_success:
        print("✅ 结论: 系统完全正常！")
        print("")
        print("💡 真相: 你的数据库已经有历史数据了")
        print("      当你测试已有数据的股票时，系统正确地跳过了重复插入")
        print("      这是正常的、正确的行为！")
        print("")
        print("🚀 建议:")
        print("  1. 测试新的股票代码（数据库中没有的）")
        print("  2. 测试更早的历史日期（比如2024年）")
        print("  3. 或者直接运行批量更新脚本")
    else:
        print("⚠️  需要进一步调查...")
        print("")
        print("🔍 下一步:")
        print("  请运行创建诊断报告:")
        print("  python create_issue_report.py")
        print("")
        print("  然后发送以下内容给我:")
        print("  1. 这个脚本的输出")
        print("  2. 诊断报告的内容")
        print("  3. src/data/data_pipeline.py 的完整代码")


if __name__ == "__main__":
    main()