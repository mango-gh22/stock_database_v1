# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\download_a50_batch.py
# File Name: download_a50_batch
# @ Author: mango-gh22
# @ Date：2026/1/2 14:19
"""
desc 批量更新A50成分股
批量下载中证A50成分股数据
python scripts/download_a50_batch.py
"""

import sys
from pathlib import Path
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.integrated_pipeline import IntegratedDataPipeline
from src.data.symbol_manager import SymbolManager


def download_a50_batch():
    """批量下载A50成分股"""
    print("🚀 开始批量下载中证A50成分股数据")
    print("=" * 60)

    # 1. 获取A50成分股
    symbol_manager = SymbolManager()
    a50_symbols_raw = symbol_manager.get_symbols('csi_a50')

    # 转换格式: 600519.SH → sh600519
    a50_symbols = []
    for item in a50_symbols_raw:
        if isinstance(item, dict) and 'symbol' in item:
            raw_symbol = item['symbol']
        else:
            raw_symbol = str(item)

        # 转换 600519.SH → sh600519
        if '.' in raw_symbol:
            code, market = raw_symbol.split('.')
            normalized = f"{market.lower()}{code}"
            a50_symbols.append(normalized)
        else:
            a50_symbols.append(raw_symbol)

    print(f"📋 A50成分股数量: {len(a50_symbols)}")
    print(f"   示例: {a50_symbols[:5]}")

    # 2. 设置日期范围（2020年至今）
    start_date = "20200101"
    end_date = datetime.now().strftime("%Y%m%d")
    print(f"📅 日期范围: {start_date} ~ {end_date}")

    # 3. 批量处理
    pipeline = IntegratedDataPipeline()
    results = pipeline.batch_process(a50_symbols, start_date, end_date, max_concurrent=1)  # 3改为1(单线程)

    # 4. 汇总报告
    print("\n" + "=" * 60)
    print("📊 批量下载完成报告")
    print("=" * 60)
    print(f"✅ 成功: {results['success']}/{results['total']} 只股票")
    print(f"❌ 失败: {results['failed']} 只股票")
    print(f"📈 总影响行数: {results['total_rows']}")
    print(f"⏱️  耗时: {results['duration']:.2f}秒")

    if results['failed'] > 0:
        print("\n⚠️ 失败的股票列表:")
        for detail in results['details']:
            if detail['status'] != 'success':
                print(f"   - {detail['symbol']}: {detail.get('reason', 'unknown')}")

    # 5. 验证数据
    print("\n" + "=" * 60)
    print("🔍 数据验证")

    # 随机抽查3只股票
    import random
    sample_symbols = random.sample([s for s in a50_symbols if len(s) == 8], min(3, len(a50_symbols)))

    for symbol in sample_symbols:
        with pipeline.storage.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as cnt, 
                           MIN(trade_date) as min_date, 
                           MAX(trade_date) as max_date
                    FROM stock_daily_data 
                    WHERE symbol = %s
                """, (symbol,))
                row = cursor.fetchone()
                if row and row[0] > 0:
                    print(f"✅ {symbol}: {row[0]}条 ({row[1]} ~ {row[2]})")
                else:
                    print(f"❌ {symbol}: 无数据")

    return results['success'] == results['total']


if __name__ == "__main__":
    success = download_a50_batch()
    sys.exit(0 if success else 1)