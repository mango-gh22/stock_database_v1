# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\collect_a50_daily.py
# @ Author: mango-gh22
# @ Date：2025/12/13 12:42
"""
desc 从 symbols.yaml 读取50只成分股的代码
将股票列表和设定的日期范围传入 batch_process_stocks 方法
desc: 从中证A50成分股列表增量下载日线数据（仅下载缺失日期）
      使用交易日历智能确定数据范围，支持在任意日期（包括休市日）运行
"""
# @ Date：2026/1/18 终极修复版 - 强制因子完整性

import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_daily_downloader import BaostockDailyDownloader
from src.data.data_storage import DataStorage
from src.utils.stock_pool_loader import load_a50_components
from src.utils.enhanced_trade_date_manager import get_enhanced_trade_date_manager
from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def incremental_download(symbols):
    """
    增量下载 - 强制因子完整性版本

    逻辑：
    1. 下载价格+因子数据（一次请求）
    2. 验证因子字段完整性（检查空值率）
    3. 如果因子缺失>50%，触发因子补全
    4. 合并后存储
    """
    if not symbols:
        logger.error("股票列表为空")
        return False

    # 初始化组件
    price_downloader = BaostockDailyDownloader()
    factor_downloader = BaostockPBFactorDownloader()
    storage = DataStorage()
    trade_manager = get_enhanced_trade_date_manager()

    # 获取最后交易日
    global_end_date = trade_manager.get_last_trade_date_str()
    logger.info(f"📅 全局截止日: {global_end_date}")

    success_count = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            logger.info(f"[{i}/{len(symbols)}] 处理 {symbol}")

            # ✅ 步骤1：查询数据库最后日期，确定下载范围
            last_date_str = storage.get_last_update_date(symbol)
            if last_date_str:
                last_dt = datetime.strptime(last_date_str, '%Y-%m-%d')
                start_date = (last_dt + timedelta(days=1)).strftime('%Y%m%d')

                if start_date > global_end_date:
                    logger.info(f"  ⏭️  {symbol} 已最新，跳过")
                    continue
            else:
                start_date = "20240101"
                logger.info(f"  🔄 {symbol} 首次下载，从 {start_date} 开始")

            # ✅ 步骤2：下载价格+因子数据（一次请求）
            logger.info(f"  📥 下载价格+因子数据: {start_date} ~ {global_end_date}")
            price_df = price_downloader.fetch_single_stock(symbol, start_date, global_end_date)

            if price_df is None or price_df.empty:
                logger.warning(f"  ⚠️  {symbol} 无返回数据（可能停牌）")
                continue

            # ✅ 步骤3：验证因子字段完整性（核心修复）
            factor_fields = ['pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm']
            factor_missing = {}

            for field in factor_fields:
                if field not in price_df.columns:
                    factor_missing[field] = 'column_missing'
                else:
                    # 检查空值率
                    null_rate = price_df[field].isna().sum() / len(price_df) * 100
                    if null_rate > 50:  # 空值率>50%视为异常
                        factor_missing[field] = f'null_rate_{null_rate:.1f}%'

            # ✅ 步骤4：如果因子字段缺失或空值率高，触发因子补全
            if factor_missing:
                logger.warning(f"  ⚠️  因子字段异常: {factor_missing}")
                logger.info(f"  🔧 触发因子补全下载: {symbol}")

                # 下载纯因子数据
                factor_df = factor_downloader.fetch_factor_data(symbol, start_date, global_end_date)

                if factor_df is not None and not factor_df.empty:
                    # 合并因子到价格数据（覆盖空值）
                    merge_cols = ['symbol', 'trade_date']
                    df_merged = pd.merge(price_df, factor_df[merge_cols + factor_fields],
                                         on=merge_cols, how='left', suffixes=('', '_factor'))

                    # 用因子数据覆盖空值
                    for field in factor_fields:
                        if field + '_factor' in df_merged.columns:
                            df_merged[field] = df_merged[field + '_factor'].fillna(df_merged[field])
                            df_merged = df_merged.drop(columns=[field + '_factor'])

                    price_df = df_merged
                    logger.info(f"  ✅ 因子补全成功: {len(factor_df)} 条")
                else:
                    logger.error(f"  ❌ 因子补全失败: {symbol}")
                    # 继续存储价格数据（因子留空）

            # ✅ 步骤5：存储数据（价格+因子）
            rows_affected, report = storage.store_daily_data(price_df)

            if report.get('status') == 'success':
                success_count += 1

                # 验证存储后的因子覆盖率
                factor_coverage = {}
                for field in factor_fields:
                    if field in price_df.columns:
                        factor_coverage[field] = price_df[field].notna().sum()

                logger.info(f"  ✅ 存储成功: {rows_affected} 行")
                logger.debug(f"  📊 因子覆盖: {factor_coverage}")
            else:
                logger.error(f"  ❌ 存储失败: {report.get('error')}")

            # ✅ 步骤6：请求间隔
            if i < len(symbols):
                import time, random
                time.sleep(random.uniform(2, 4))

        except Exception as e:
            logger.error(f"  ❌ 处理 {symbol} 失败: {e}", exc_info=True)

    logger.info(f"✅ 增量采集完成！成功更新 {success_count}/{len(symbols)} 只股票")

    # 生成因子覆盖率报告
    if success_count > 0:
        generate_factor_coverage_report(storage, symbols)

    return success_count > 0


def generate_factor_coverage_report(storage, symbols):
    """生成因子覆盖率报告"""
    try:
        with storage.db_connector.get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                placeholders = ','.join(['%s'] * len(symbols))
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN pb IS NOT NULL AND pb != 0 THEN 1 ELSE 0 END) as pb_count,
                        SUM(CASE WHEN pe_ttm IS NOT NULL AND pe_ttm != 0 THEN 1 ELSE 0 END) as pe_count,
                        SUM(CASE WHEN ps_ttm IS NOT NULL AND ps_ttm != 0 THEN 1 ELSE 0 END) as ps_count,
                        SUM(CASE WHEN pcf_ttm IS NOT NULL AND pcf_ttm != 0 THEN 1 ELSE 0 END) as pcf_count
                    FROM stock_daily_data
                    WHERE symbol IN ({placeholders}) AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
                """, tuple(symbols))

                result = cursor.fetchone()

                if result and result['total_records'] > 0:
                    total = result['total_records']
                    logger.info("=" * 50)
                    logger.info("📊 因子覆盖率报告（最近3天）")
                    logger.info(f"  总记录: {total} 条")
                    logger.info(f"  PB: {result['pb_count']}条 ({result['pb_count'] / total * 100:.1f}%)")
                    logger.info(f"  PE: {result['pe_count']}条 ({result['pe_count'] / total * 100:.1f}%)")
                    logger.info(f"  PS: {result['ps_count']}条 ({result['ps_count'] / total * 100:.1f}%)")
                    logger.info(f"  PCF: {result['pcf_count']}条 ({result['pcf_count'] / total * 100:.1f}%)")
                    logger.info("=" * 50)

    except Exception as e:
        logger.warning(f"生成因子报告失败: {e}")


def main(symbols=None):
    """命令行入口"""
    if symbols is None:
        symbols = load_a50_components()

    if not symbols:
        logger.error("未找到股票列表")
        return False

    logger.info(f"📋 加载 {len(symbols)} 只成分股")
    return incremental_download(symbols)


if __name__ == "__main__":
    import sys

    success = main()
    sys.exit(0 if success else 1)