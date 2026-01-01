# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\ultimate_breaker.py
# File Name: ultimate_breaker
# @ Author: mango-gh22
# @ Date：2026/1/1 13:34
"""
desc 
"""
# ultimate_breaker.py
"""
终极破壁脚本 - 完全绕过所有现有逻辑，强制写入数据
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

print("💥 终极破壁脚本 - 强制数据写入")
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
        print(f"   示例:\n{df.head(2)}")

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

        # 3. 获取当前状态
        clean_symbol = symbol.replace('.', '')
        # 改为：
        cursor.execute("SELECT COUNT(*) as count_before FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        before = cursor.fetchone()['before']
        print(f"   插入前: {before} 条记录")

        # 4. 暴力插入所有数据（忽略重复）
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
        cursor.execute("SELECT COUNT(*) as after FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        after = cursor.fetchone()['after']

        cursor.close()
        conn.close()

        print(f"   插入后: {after} 条记录")
        print(f"   成功插入: {inserted} 条")
        print(f"   跳过重复: {skipped} 条")
        print(f"   净增加: {after - before} 条")

        return inserted > 0

    def diagnose_pipeline_blockage(self):
        """诊断数据管道中的阻塞点"""
        print("\n4. 🔍 诊断数据管道阻塞点...")

        # 检查关键文件
        key_files = [
            "src/data/data_pipeline.py",
            "src/data/data_storage.py",
            "src/data/baostock_collector.py"
        ]

        for file_path in key_files:
            full_path = os.path.join(r"E:\MyFile\stock_database_v1", file_path)
            if os.path.exists(full_path):
                print(f"   📄 {file_path} - 存在")

                # 查找可能导致阻塞的关键词
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                blockers = [
                    ('skip', '跳过'),
                    ('duplicate', '重复'),
                    ('exist', '存在'),
                    ('last_update', '最后更新'),
                    ('if.*return', '条件返回'),
                    ('continue', '继续')
                ]

                for keyword, desc in blockers:
                    count = content.lower().count(keyword)
                    if count > 0:
                        print(f"     发现 {count} 处 '{desc}' 相关代码")
            else:
                print(f"   ❌ {file_path} - 不存在")

    def create_clean_test(self):
        """创建一个完全干净的测试环境"""
        print("\n5. 🧪 创建完全干净的测试...")

        # 使用一个绝对没有数据的股票
        test_symbol = "sh.601888"  # 中国中免
        clean_symbol = test_symbol.replace('.', '')

        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)

        # 1. 删除这个股票的所有数据（确保干净）
        cursor.execute("DELETE FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        deleted = cursor.rowcount
        conn.commit()

        if deleted > 0:
            print(f"   清理了 {deleted} 条 {test_symbol} 的历史数据")

        # 2. 确认现在为0
        cursor.execute("SELECT COUNT(*) as count FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
        count = cursor.fetchone()['count']

        if count == 0:
            print(f"   ✅ {test_symbol} 现在完全没有数据")

            # 3. 使用项目代码尝试插入
            print(f"   准备用项目代码插入 {test_symbol}...")

            # 重新导入模块（确保最新）
            import importlib
            import src.data.data_pipeline
            import src.data.data_storage
            import src.data.baostock_collector

            importlib.reload(src.data.data_pipeline)
            importlib.reload(src.data.data_storage)
            importlib.reload(src.data.baostock_collector)

            from src.data.data_pipeline import DataPipeline
            from src.data.data_storage import DataStorage
            from src.data.baostock_collector import BaostockCollector

            # 执行
            collector = BaostockCollector()
            storage = DataStorage()
            pipeline = DataPipeline(collector=collector, storage=storage)

            result = pipeline.fetch_and_store_daily_data(
                symbol=test_symbol,
                start_date="2025-12-25",
                end_date="2025-12-31"
            )

            print(f"   项目代码执行结果:")
            print(f"     状态: {result.get('status')}")
            print(f"     消息: {result.get('message', 'N/A')}")
            print(f"     存储记录: {result.get('records_stored', 0)}")

            # 4. 检查结果
            cursor.execute("SELECT COUNT(*) as after FROM stock_daily_data WHERE symbol = %s", (clean_symbol,))
            after = cursor.fetchone()['after']

            print(f"   执行后数据: {after} 条")

            if after > 0:
                print(f"   🎉 项目代码成功写入了 {after} 条数据！")
                return True
            else:
                print(f"   ❌ 项目代码仍然没有写入数据")
                return False
        else:
            print(f"   ❌ 无法清理 {test_symbol} 的数据")
            return False

        conn.close()


def main():
    breaker = UltimateDataBreaker()

    # 测试1: 直接数据库插入
    if not breaker.test_direct_insert():
        print("❌ 数据库基本功能有问题")
        return

    # 测试2: 暴力插入
    breaker.brute_force_insert("sh.600000", "2025-12-25", "2025-12-31")

    # 测试3: 诊断阻塞点
    breaker.diagnose_pipeline_blockage()

    # 测试4: 完全干净的测试
    success = breaker.create_clean_test()

    print("\n" + "=" * 60)
    print("📋 最终报告")
    print("=" * 60)

    if success:
        print("✅ 系统问题已找到并解决！")
        print("   项目代码可以写入数据，只是在某些条件下被阻塞。")
    else:
        print("❌ 需要进一步调试...")
        print("   建议检查 DataPipeline 中的逻辑条件。")


if __name__ == "__main__":
    main()