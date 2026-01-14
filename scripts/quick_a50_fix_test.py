# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\quick_a50_fix_test.py
# File Name: quick_a50_fix_test
# @ Author: mango-gh22
# @ Date：2026/1/3 23:45
"""
desc 
"""
# File Path: E:/MyFile/stock_database_v1/scripts/quick_a50_fix_test.py
"""
快速测试A50修复
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from src.data.a50_fixer import A50SymbolFixer


def test_current_config():
    """测试当前配置文件"""
    print("📋 测试当前配置文件")
    print("=" * 50)

    try:
        # 读取当前配置
        config_path = 'config/symbols.yaml'
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        print(f"配置文件路径: {config_path}")

        if 'csi_a50' in config:
            a50_data = config['csi_a50']
            print(f"找到csi_a50数据，类型: {type(a50_data)}")
            print(f"数据数量: {len(a50_data) if isinstance(a50_data, list) else 'N/A'}")

            # 显示前5个
            print("\n前5个数据项:")
            for i, item in enumerate(a50_data[:5], 1):
                print(f"  [{i}] {item}")
                try:
                    fixed = A50SymbolFixer.fix_symbol(item)
                    print(f"       -> {fixed}")
                except Exception as e:
                    print(f"       -> ❌ 错误: {e}")

            # 测试批量修复
            print("\n🧪 批量修复测试:")
            fixed_symbols = A50SymbolFixer.batch_fix_symbols(a50_data[:5])
            print(f"修复结果: {fixed_symbols}")

        else:
            print("❌ 未找到csi_a50配置")

    except Exception as e:
        print(f"❌ 读取配置失败: {e}")


def test_download_with_fixed_symbols():
    """测试使用修复后的符号进行下载"""
    print("\n🚀 测试下载修复后的符号")
    print("=" * 50)

    # 测试股票列表
    test_symbols = [
        {'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38},
        {'name': '宁德时代', 'symbol': '300750.SZ', 'weight': 8.45},
        {'name': '中国平安', 'symbol': '601318.SH', 'weight': 6.89},
    ]

    try:
        from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader

        downloader = BaostockPBFactorDownloader()

        for item in test_symbols:
            try:
                # 修复符号
                fixed_symbol = A50SymbolFixer.fix_symbol(item)
                print(f"\n📥 测试下载: {item['symbol']} -> {fixed_symbol}")

                # 尝试下载最近5天数据
                end_date = '20251231'
                start_date = '20251220'

                data = downloader.fetch_factor_data(fixed_symbol, start_date, end_date)

                if data.empty:
                    print(f"  ⚠️  无数据")
                else:
                    print(f"  ✅ 下载成功: {len(data)} 条记录")
                    print(f"     字段: {list(data.columns)}")

                    # 显示样本数据
                    if 'pb' in data.columns:
                        pb_values = data['pb'].dropna()
                        if len(pb_values) > 0:
                            print(f"     PB范围: {pb_values.min():.2f} - {pb_values.max():.2f}")

            except Exception as e:
                print(f"  ❌ 下载失败: {e}")

        downloader.logout()
        print("\n✅ 下载测试完成")

    except Exception as e:
        print(f"❌ 测试失败: {e}")


def main():
    """主函数"""
    print("🔧 A50快速修复测试")
    print("=" * 60)

    try:
        # 测试当前配置
        test_current_config()

        # 测试下载
        test_download_with_fixed_symbols()

        print("\n" + "=" * 60)
        print("💡 修复完成！现在可以运行:")
        print("   python scripts/update_a50_factors.py --test")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)