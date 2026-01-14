# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\factor_storage_manager.py
# @ Author: mango-gh22
# @ Date：2026/1/3 12:41
"""
desc 因子数据存储管理器 - 专门处理PB等因子数据的存储和增量更新
集成DataStorage架构，针对因子数据进行优化
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data/factor_storage_manager.py
# @ Author: mango-gh22
# @ Date：2026/1/3 12:00
"""
desc 因子数据存储管理器 - 复用通用存储逻辑
统一使用 data_storage.py 的预处理能力
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Optional, List
import logging
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data.data_storage import DataStorage
from src.database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class FactorStorageManager:
    """因子数据存储管理器 - 复用通用存储逻辑"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """初始化 - 复用通用数据存储器"""
        self.data_storage = DataStorage(config_path)
        self.db_connector = self.data_storage.db_connector

        # 本次下载的因子字段（根据Baostock实际提供）
        self.download_factor_fields = ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm', 'turnover_rate_f']

        logger.info(f"初始化因子存储管理器，字段: {self.download_factor_fields}")
        logger.info(f"复用通用存储器: {self.data_storage.__class__.__name__}")

    def get_last_factor_date(self, symbol: str) -> Optional[str]:
        """获取最后因子日期"""
        return self.data_storage.get_last_update_date(symbol)

    def calculate_incremental_range(self, symbol: str) -> Tuple[str, str]:
        """计算增量范围"""
        last_date = self.get_last_factor_date(symbol)

        if last_date:
            # 如果已有数据，从次日开始
            from datetime import datetime, timedelta
            last_dt = datetime.strptime(last_date, '%Y-%m-%d')
            start_dt = last_dt + timedelta(days=1)
            start_date = start_dt.strftime('%Y%m%d')
        else:
            # 如果没有数据，从上市日期开始（简化处理）
            start_date = '20240101'

        from src.utils.enhanced_trade_date_manager import get_enhanced_trade_date_manager
        trade_manager = get_enhanced_trade_date_manager()
        end_date = trade_manager.get_last_trade_date_str()

        # 验证日期顺序
        if start_date > end_date:
            logger.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}，数据已最新")
            return None, None

        return start_date, end_date

    def store_factor_data(self, df: pd.DataFrame) -> Tuple[int, Dict]:
        """
        存储因子数据（复用通用存储逻辑）

        Args:
            df: 因子数据DataFrame，必须包含:
                - symbol, trade_date
                - pb, pe_ttm, ps_ttm, pcf_ttm, turnover_rate_f

        Returns:
            (影响行数, 报告字典)
        """
        if df.empty:
            logger.warning("输入数据为空，跳过存储")
            return 0, {'status': 'skipped', 'reason': 'empty_data'}

        logger.info(f"将存储因子数据: {len(df)} 条记录")
        logger.debug(f"数据列: {list(df.columns)}")

        # 确保必需的列存在
        required_cols = ['symbol', 'trade_date'] + self.download_factor_fields
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(f"缺少必需列: {missing_cols}")
            return 0, {'status': 'error', 'reason': f'missing_columns: {missing_cols}'}

        # 数据质量预处理：处理NaN和无效值
        df_processed = self._preprocess_factor_data(df.copy())

        # 复用通用存储器的核心逻辑
        return self.data_storage.store_daily_data(df_processed)

    def _preprocess_factor_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理因子数据（质量保证）
        """
        df_processed = df.copy()

        # 1. 股票代码标准化（确保没有点号）
        if 'symbol' in df_processed.columns:
            df_processed['symbol'] = df_processed['symbol'].apply(
                lambda x: str(x).replace('.', '') if pd.notna(x) else None
            )

        # 2. 日期格式标准化
        if 'trade_date' in df_processed.columns:
            df_processed['trade_date'] = pd.to_datetime(
                df_processed['trade_date'], errors='coerce'
            ).dt.strftime('%Y-%m-%d')

        # 3. 因子字段数值转换和清洗
        for field in self.download_factor_fields:
            if field in df_processed.columns:
                # 转换为数值类型，无效值转为NaN
                df_processed[field] = pd.to_numeric(df_processed[field], errors='coerce')

                # 估值指标清洗：负值和极大值设为NaN
                if field in ['pb', 'pe_ttm', 'ps_ttm', 'pcf_ttm']:
                    df_processed[field] = df_processed[field].apply(
                        lambda x: x if pd.notna(x) and 0 < x < 1e6 else np.nan
                    )

                logger.debug(f"字段 {field}: {df_processed[field].notna().sum()} 条有效")

        # 4. 移除全为空的行
        factor_cols = [col for col in self.download_factor_fields if col in df_processed.columns]
        df_processed = df_processed.dropna(subset=factor_cols, how='all')

        logger.info(f"预处理完成: {len(df_processed)} 条有效记录")
        return df_processed

    def clear_cache(self, symbol: str):
        """清理缓存"""
        if hasattr(self.data_storage, '_table_columns_cache'):
            self.data_storage._table_columns_cache.clear()
            logger.debug(f"清理缓存: {symbol}")

    def get_storage_stats(self) -> Dict:
        """获取存储统计"""
        return self.data_storage.get_download_stats() if hasattr(self.data_storage,
                                                                 'get_download_stats') else {}


def test_factor_storage():
    """测试因子存储"""
    logger = logging.getLogger(__name__)
    logger.info("🧪 测试因子存储管理器")

    try:
        # 初始化
        manager = FactorStorageManager()

        # 创建测试数据
        test_data = pd.DataFrame({
            'symbol': ['sh600519', 'sh600519'],
            'trade_date': ['2026-01-08', '2026-01-09'],
            'pb': [6.5, 6.8],
            'pe_ttm': [20.5, 21.2],
            'ps_ttm': [9.8, 10.1],
            'pcf_ttm': [400.5, 410.2],
            'turnover_rate_f': [0.23, 0.25]
        })

        logger.info(f"测试数据: {len(test_data)} 条")

        # 存储
        affected_rows, report = manager.store_factor_data(test_data)

        logger.info(f"存储结果: {affected_rows} 条，状态: {report['status']}")

        # 验证
        if report['status'] == 'success':
            last_date = manager.get_last_factor_date('600519')
            logger.info(f"最后日期: {last_date}")
            return True
        else:
            logger.error(f"存储失败: {report}")
            return False

    except Exception as e:
        logger.error(f"测试失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO)
    success = test_factor_storage()
    sys.exit(0 if success else 1)