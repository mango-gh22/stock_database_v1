# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/examples\p6_adjustment_factor_usage.py
# File Name: p6_adjustment_factor_usage
# @ Author: mango-gh22
# @ Date：2026/1/2 19:28
"""
desc 
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/examples/p6_adjustment_factor_usage.py
# File Name: p6_adjustment_factor_usage
# @ Author: mango-gh22
# @ Date: 2026/01/02
"""
P6阶段复权因子使用示例
演示：下载、存储、查询完整流程
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from src.data.adjustment_factor_manager import AdjustmentFactorManager
from src.data.symbol_manager import get_symbol_manager


def demo_basic_usage():
    """基础使用示例"""
    print("=" * 70)
    print("P6阶段复权因子基础使用示例")
    print("=" * 70)

    # 1. 初始化管理器
    print("\n1. 初始化管理器...")
    manager = AdjustmentFactorManager()
    print("✅ 管理器初始化完成")

    # 2. 获取股票列表（使用CSI A50）
    print("\n2. 获取股票列表...")
    symbol_manager = get_symbol_manager()
    symbols = symbol_manager.get_symbols('csi_a50')[:5]  # 测试前5只
    print(f"📋 股票列表: {symbols}")

    # 3. 批量下载复权因子（单线程）
    print("\n3. 批量下载复权因子...")
    results = manager.download_batch(
        symbols,
        mode='incremental'  # 智能增量模式
    )
    print(f"✅ 下载完成: {len(results)} 只股票")

    # 4. 查询单个因子
    print("\n4. 查询复权因子...")
    test_symbol = symbols[0]
    test_date = '20220630'  # 2022年6月30日

    factor = manager.get_adjustment_factor(test_symbol, test_date, factor_type='forward')
    print(f"  {test_symbol} 在 {test_date} 的前复权因子: {factor}")

    # 5. 查询历史因子序列
    print("\n5. 查询历史因子...")
    df_factors = manager.get_factors_for_symbol(test_symbol)
    if not df_factors.empty:
        print(f"  共 {len(df_factors)} 条记录")
        print(df_factors.head())

    # 6. 查看统计
    print("\n6. 统计信息...")
    stats = manager.get_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # 7. 清理
    manager.cleanup()
    print("\n✅ 示例执行完成")


def demo_incremental_update():
    """增量更新示例"""
    print("\n" + "=" * 70)
    print("增量更新示例")
    print("=" * 70)

    manager = AdjustmentFactorManager()

    # 选择1只股票
    symbol = 'sh600519'

    print(f"\n📊 更新 {symbol}...")

    # 第一次运行（全量）
    print("  第一次运行（全量）...")
    results1 = manager.download_batch([symbol], mode='full')
    print(f"    下载 {len(results1.get(symbol, pd.DataFrame()))} 条")

    # 第二次运行（增量）
    print("  第二次运行（增量）...")
    results2 = manager.download_batch([symbol], mode='incremental')
    print(f"    新增 {len(results2.get(symbol, pd.DataFrame()))} 条")

    manager.cleanup()
    print("✅ 增量更新完成")


def demo_query_integration():
    """与行情数据集成查询"""
    print("\n" + "=" * 70)
    print("复权因子与行情数据集成示例")
    print("=" * 70)

    manager = AdjustmentFactorManager()

    # 查询某股票复权因子
    symbol = 'sh600519'
    factors_df = manager.get_factors_for_symbol(symbol)

    if not factors_df.empty:
        print("\n📈 复权因子数据:")
        print(factors_df[['ex_date', 'cash_div', 'forward_factor']].head())

        # 获取最近一个除权日
        latest_ex_date = factors_df['ex_date'].iloc[0]
        print(f"\n📅 最近除权日: {latest_ex_date}")

        # 查询该日期的因子
        factor = manager.get_adjustment_factor(symbol, latest_ex_date)
        print(f"  前复权因子: {factor}")

        # 计算不复权价格到前复权价格（示例）
        print("\n💡 价格复权示例:")
        print("  如需将不复权价格转为前复权:")
        print("  复权价 = 原始价 * 前复权因子")
        print(f"  示例: 2000元 * {factor:.6f} = {2000 * factor:.2f}元")

    manager.cleanup()


def demo_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 70)
    print("错误处理示例")
    print("=" * 70)

    manager = AdjustmentFactorManager()

    # 1. 无效股票代码
    print("\n1. 无效股票代码...")
    try:
        result = manager.update_symbol("sh999999")
        print(f"  结果: {result}（优雅返回False）")
    except Exception as e:
        print(f"  ❌ 不应抛出异常: {e}")

    # 2. 无效日期范围
    print("\n2. 无效日期范围...")
    try:
        range_tuple = manager.date_calculator.calculate_download_range(
            "sh600519",
            mode='specific',
            custom_params={'date_range': {'start': '20250101', 'end': '20240101'}}  # 开始晚于结束
        )
        print(f"  范围: {range_tuple}（应返回None）")
    except Exception as e:
        print(f"  ❌ 日期验证失败: {e}")

    # 3. 查询不存在的因子
    print("\n3. 查询不存在的因子...")
    factor = manager.get_adjustment_factor("sh600519", "19000101")  # 远古日期
    print(f"  返回因子: {factor}（应返回1.0）")

    manager.cleanup()
    print("\n✅ 错误处理示例完成")


if __name__ == "__main__":
    print("🚀 P6阶段复权因子使用示例")
    print("注意：本示例需数据库连接和Baostock网络访问")

    try:
        # 运行基础示例
        demo_basic_usage()

        # 运行增量更新示例
        demo_incremental_update()

        # 运行集成查询示例
        demo_query_integration()

        # 运行错误处理示例
        demo_error_handling()

        print("\n" + "=" * 70)
        print("🎉 所有示例执行完成！")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ 示例执行失败: {e}")
        import traceback

        traceback.print_exc()