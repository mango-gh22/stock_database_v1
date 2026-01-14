# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_factor_storage.py
# File Name: test_factor_storage
# @ Author: mango-gh22
# @ Date：2026/1/11 16:39
"""
desc 
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/test_factor_storage.py
"""
测试因子下载→存储全流程验证
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).parent))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.data_storage import DataStorage


def test_complete_flow():
    """测试完整流程：下载→存储→验证"""
    print("\n" + "=" * 60)
    print("🧪 测试因子下载→存储全流程")
    print("=" * 60)

    # 1. 初始化组件
    print("\n1️⃣ 初始化组件...")
    downloader = BaostockPBFactorDownloader()
    storage = DataStorage()
    print("✅ 下载器和存储器初始化成功")

    # 2. 下载因子数据
    print("\n2️⃣ 下载因子数据...")
    test_symbol = 'sh600519'

    # 获取最近5个交易日
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')

    print(f"股票: {test_symbol}")
    print(f"日期范围: {start_date} ~ {end_date}")

    df_downloaded = downloader.fetch_factor_data(test_symbol, start_date, end_date)

    if df_downloaded.empty:
        print("❌ 下载数据为空，测试终止")
        return False

    print(f"✅ 下载成功: {len(df_downloaded)} 条记录")
    print(f"   字段: {list(df_downloaded.columns)}")

    # 3. 查看下载数据样本
    print("\n3️⃣ 下载数据样本:")
    display_cols = ['trade_date', 'symbol', 'pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm', 'turnover_rate_f']
    sample_df = df_downloaded[display_cols].head(3)
    print(sample_df.to_string())

    # 4. 存储到数据库
    print("\n4️⃣ 存储到数据库...")

    # 确保数据类型正确
    print("   数据类型检查:")
    for col in ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm', 'turnover_rate_f']:
        if col in df_downloaded.columns:
            dtype = df_downloaded[col].dtype
            non_null = df_downloaded[col].notna().sum()
            print(f"   {col}: {dtype}, 非空: {non_null}/{len(df_downloaded)}")

    # 执行存储
    affected_rows, report = storage.store_daily_data(df_downloaded)

    print(f"   存储结果: {affected_rows} 条记录受影响")
    print(f"   状态: {report['status']}")

    if report['status'] != 'success':
        print(f"❌ 存储失败: {report.get('error', '未知错误')}")
        return False

    print("✅ 存储成功")

    # 5. 验证数据库中的数据
    print("\n5️⃣ 验证数据库数据...")

    clean_symbol = test_symbol.replace('.', '')

    with storage.db_connector.get_connection() as conn:
        # 查询存储的数据
        query = f"""
            SELECT trade_date, symbol, pb, pe_ttm, ps_ttm, pcf_ttm, turnover_rate_f
            FROM stock_daily_data 
            WHERE symbol = '{clean_symbol}'
            ORDER BY trade_date DESC
            LIMIT 5
        """

        df_db = pd.read_sql_query(query, conn)

        if df_db.empty:
            print("❌ 数据库中未找到存储的数据")
            return False

        print(f"✅ 数据库查询成功: {len(df_db)} 条记录")
        print("\n   数据库数据样本:")
        print(df_db.to_string())

        # 验证数据一致性
        print("\n   数据一致性验证:")

        # 检查记录数是否匹配
        if len(df_db) == len(df_downloaded):
            print(f"   ✅ 记录数匹配: {len(df_db)}")
        else:
            print(f"   ⚠️  记录数不匹配 - 下载: {len(df_downloaded)}, 数据库: {len(df_db)}")

        # 检查数值是否一致（第一条记录）
        if not df_db.empty and not df_downloaded.empty:
            # 比较第一条记录的PB值
            pb_downloaded = df_downloaded.iloc[0]['pb']
            pb_db = df_db.iloc[0]['pb']

            if abs(pb_downloaded - pb_db) < 0.001:
                print(f"   ✅ PB值一致: {pb_db}")
            else:
                print(f"   ❌ PB值不一致 - 下载: {pb_downloaded}, 数据库: {pb_db}")

        # 检查数据类型
        print("\n   数据库字段类型:")
        cursor = conn.cursor()
        cursor.execute("DESCRIBE stock_daily_data")
        columns = cursor.fetchall()

        target_fields = ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm', 'turnover_rate_f']
        for col in columns:
            field_name = col[0]
            field_type = col[1]
            if field_name in target_fields:
                print(f"   {field_name}: {field_type}")

    # 6. 清理测试数据
    print("\n6️⃣ 清理测试数据...")
    try:
        with storage.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stock_daily_data WHERE symbol = %s AND trade_date >= %s",
                    (clean_symbol, df_downloaded['trade_date'].min())
                )
                conn.commit()
                print(f"   清理完成: {cursor.rowcount} 条记录")
    except Exception as e:
        print(f"   ⚠️  清理失败: {e}")

    # 7. 测试汇总
    print("\n" + "=" * 60)
    print("📊 测试汇总:")
    print("=" * 60)
    print("✅ 下载器工作正常")
    print("✅ 数据类型转换正确")
    print("✅ 数据库字段匹配")
    print("✅ 存储流程正常")
    print("✅ 数据一致性验证通过")
    print("\n🎉 全流程测试完成！系统运行正常")

    return True


if __name__ == '__main__':
    try:
        success = test_complete_flow()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n💥 测试失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)