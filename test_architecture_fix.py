# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\test_architecture_fix.py
# File Name: test_architecture_fix
# @ Author: mango-gh22
# @ Date：2025/12/14 8:38
"""
desc
测试架构修复
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger
from src.data.adaptive_storage import AdaptiveDataStorage
from src.data.symbol_manager import SymbolManager
from src.data.date_calculator import DateRangeCalculator

logger = get_logger(__name__)


def test_architecture_fix():
    """测试架构修复"""
    logger.info("🧪 测试架构修复")
    logger.info("=" * 60)

    try:
        # 1. 测试自适应存储器修复
        logger.info("1. 测试自适应存储器修复...")
        storage = AdaptiveDataStorage()

        # 测试新增方法
        test_symbol = 'sh600519'
        last_date = storage.get_last_update_date(test_symbol)
        logger.info(f"   最后更新日期: {test_symbol} -> {last_date}")

        count = storage.get_stock_count(test_symbol)
        logger.info(f"   数据记录数: {test_symbol} -> {count}")

        # 2. 测试符号管理器
        logger.info("\n2. 测试符号管理器...")
        symbol_manager = SymbolManager()

        # 获取所有组
        groups = symbol_manager.get_all_groups()
        logger.info(f"   可用符号组: {list(groups.keys())}")

        # 获取A50符号
        a50_symbols = symbol_manager.get_symbols('csi_a50')
        logger.info(f"   CSI A50符号数: {len(a50_symbols)}")
        if a50_symbols:
            logger.info(f"   示例符号: {a50_symbols[:3]}")

        # 3. 测试日期计算器
        logger.info("\n3. 测试日期计算器...")
        date_calculator = DateRangeCalculator(storage)

        # 测试不同模式
        modes = ['incremental', 'batch_init', 'specific']
        for mode in modes:
            start_date, end_date = date_calculator.calculate_range(
                test_symbol, mode, {'days_back': 30}
            )
            logger.info(f"   {mode}模式: {start_date} - {end_date}")

            # 验证日期范围
            is_valid = date_calculator.validate_date_range(start_date, end_date)
            logger.info(f"     有效性: {'✓' if is_valid else '✗'}")

        # 4. 测试分割大日期范围
        logger.info("\n4. 测试日期范围分割...")
        large_ranges = date_calculator.split_large_range('20200101', '20241231', max_days=180)
        for i, (chunk_start, chunk_end) in enumerate(large_ranges, 1):
            logger.info(f"   第{i}段: {chunk_start} - {chunk_end}")

        # 5. 测试符号验证
        logger.info("\n5. 测试符号验证...")
        test_symbols = ['600519', '000001.SZ', 'invalid_code', 'sh688981']
        validation = symbol_manager.validate_symbols(test_symbols)

        logger.info(f"   有效符号: {len(validation['valid'])}个")
        logger.info(f"   无效符号: {len(validation['invalid'])}个")
        logger.info(f"   标准化结果: {validation['normalized']}")

        logger.info("\n✅ 架构修复测试通过")
        return True

    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = test_architecture_fix()
    sys.exit(0 if success else 1)