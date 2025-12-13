# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\collect_a50_daily.py
# File Name: collect_a50_daily
# @ Author: mango-gh22
# @ Date：2025/12/13 12:42
"""
desc 从 symbols.yaml 读取50只成分股的代码
将股票列表和设定的日期范围传入 batch_process_stocks 方法
"""

# scripts/collect_a50_daily.py
import yaml
from datetime import datetime, timedelta
from src.data.integrated_pipeline import IntegratedDataPipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)


def collect_csi_a50_data():
    """采集中证A50指数成分股日线数据"""
    logger.info("🚀 开始采集中证A50指数成分股数据")

    # 1. 加载股票列表
    with open('config/symbols.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    a50_stocks = config.get('csi_a50', [])
    symbols = [stock['symbol'] for stock in a50_stocks]  # 得到 ['000001.SZ', '000002.SZ', ...]

    logger.info(f"📋 加载 {len(symbols)} 只成分股")

    # 2. 设置日期范围 (示例：采集过去一年的历史数据)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

    logger.info(f"📅 采集日期范围: {start_date} 至 {end_date}")

    # 3. 初始化并运行数据管道
    pipeline = IntegratedDataPipeline()

    # 启动批量处理（可调整 max_concurrent 控制并发数）
    report = pipeline.batch_process_stocks(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        adjust='qfq',  # 前复权
        max_concurrent=3  # 建议设置较低并发数以避免对数据源造成压力
    )

    # 4. 打印并保存报告
    logger.info("=" * 60)
    logger.info(f"✅ 数据采集任务完成")
    logger.info(f"   成功: {report['success_count']} 只")
    logger.info(f"   失败: {report['error_count']} 只")
    logger.info(f"   成功率: {report['success_rate']:.1f}%")

    return report


if __name__ == "__main__":
    collect_csi_a50_data()