# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_pb_factor_download.py
# File Name: test_pb_factor_downloader
# @ Author: mango-gh22
# @ Date：2026/1/3 11:21
"""
desc PB因子下载器测试脚本（修正版）
"""

import sys
import os
import logging
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def setup_logging():
    """设置日志"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'{log_dir}/pb_factor_download_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )


def main():
    """主函数"""
    print("🚀 PB因子下载器集成测试（修正版）")
    print("=" * 60)
    import pandas as pd

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader

        # 1. 创建下载器
        logger.info("初始化PB因子下载器...")
        downloader = BaostockPBFactorDownloader()

        # 2. 测试登录
        logger.info("登录Baostock...")
        downloader._ensure_logged_in()

        if not hasattr(downloader, 'lg') or not downloader.lg:
            logger.error("Baostock登录失败")
            return False

        logger.info("✅ Baostock登录成功")

        # 3. 准备测试数据
        # 使用A50成分股中的几只
        test_symbols = [
            '600519',  # 贵州茅台
            '000001',  # 平安银行
            '000858',  # 五粮液
        ]

        # 日期范围：最近7天，避免数据过多
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

        logger.info(f"测试配置:")
        logger.info(f"  股票数量: {len(test_symbols)}")
        logger.info(f"  日期范围: {start_date} - {end_date}")
        logger.info(f"  下载因子: {downloader.download_factor_fields}")

        # 4. 执行批量下载
        logger.info("开始批量下载...")
        results = downloader.download_batch_factors(
            symbols=test_symbols,
            start_date=start_date,
            end_date=end_date
        )

        # 5. 分析结果
        logger.info("分析下载结果...")

        successful = len(results)
        total_records = sum(len(df) for df in results.values())

        print("\n" + "=" * 60)
        print("📊 测试结果摘要:")
        print(f"  测试股票数: {len(test_symbols)}")
        print(f"  成功下载: {successful}")
        print(f"  失败: {len(test_symbols) - successful}")
        print(f"  总记录数: {total_records}")

        if not results:
            print("⚠️  无数据下载成功，请检查网络或API配置")
            return False

        # 显示每只股票的记录数
        print("\n📈 各股票下载详情:")
        for symbol, df in results.items():
            # 检查数据质量
            factor_fields_present = []
            for field in downloader.download_factor_fields:
                if field in df.columns and df[field].notna().any():
                    factor_fields_present.append(field)

            print(f"  {symbol}: {len(df)} 条记录")

            if factor_fields_present:
                print(f"    包含因子: {factor_fields_present}")

                # 显示最近日期的数据
                if not df.empty:
                    latest = df.iloc[0]
                    factor_values = []
                    for field in factor_fields_present:
                        if field in latest and pd.notna(latest[field]):
                            factor_values.append(f"{field}={latest[field]:.2f}")

                    if factor_values:
                        print(f"    最新数据: {latest['trade_date']}, {', '.join(factor_values)}")

        # 6. 数据质量统计
        print("\n🔍 数据质量统计:")
        for factor_field in downloader.download_factor_fields:
            total_values = 0
            valid_values = 0

            for df in results.values():
                if factor_field in df.columns:
                    total_values += len(df)
                    valid_values += df[factor_field].notna().sum()

            if total_values > 0:
                coverage_rate = (valid_values / total_values) * 100
                print(f"  {factor_field}: {valid_values}/{total_values} ({coverage_rate:.1f}%)")
            else:
                print(f"  {factor_field}: 无数据")

        # 7. 保存样本数据
        if results:
            import pandas as pd
            sample_dir = "data/test_samples"
            if not os.path.exists(sample_dir):
                os.makedirs(sample_dir)

            for symbol, df in list(results.items())[:3]:  # 保存前3只
                sample_file = f"{sample_dir}/{symbol}_factor_sample.csv"
                df.head(20).to_csv(sample_file, index=False, encoding='utf-8')
                print(f"  💾 {symbol} 样本数据保存到: {sample_file}")

        # 8. 退出登录
        downloader.logout()
        logger.info("✅ 测试完成")

        return successful > 0

    except Exception as e:
        logger.error(f"测试失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)