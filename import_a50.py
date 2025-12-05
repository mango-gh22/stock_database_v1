# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\import_a50.py
# File Name: import_a50
# @ File: import_a50.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 23:03
"""
desc 
"""
# import_a50.py
import sys

sys.path.insert(0, '.')

print("=== 导入中证A50成分股 ===")

try:
    from src.data.import_csi_a50 import CSI_A50_Importer

    importer = CSI_A50_Importer()
    print(f"找到 {len(importer.csi_a50_symbols)} 只中证A50成分股")

    print("1. 导入指数信息...")
    if importer.import_index_info():
        print("✅ 指数信息导入成功")
    else:
        print("❌ 指数信息导入失败")

    print("2. 导入股票基本信息...")
    if importer.import_stock_basic_info():
        print("✅ 股票基本信息导入成功")
    else:
        print("❌ 股票基本信息导入失败")

    print("3. 导入成分股关联信息...")
    if importer.import_constituent_info():
        print("✅ 成分股关联信息导入成功")
    else:
        print("❌ 成分股关联信息导入失败")

    print("4. 验证导入结果...")
    validation = importer.validate_import()

    print(f"\n📊 导入验证结果:")
    print(f"   股票表: {validation.get('row_counts', {}).get('stock_basic_info', 0)} 条")
    print(f"   指数表: {validation.get('row_counts', {}).get('index_info', 0)} 条")
    print(f"   关联表: {validation.get('row_counts', {}).get('stock_index_constituent', 0)} 条")

    if validation.get('csi_a50_validation', {}).get('constituent_count', 0) == len(importer.csi_a50_symbols):
        print("\n🎉 中证A50成分股导入完成！")
    else:
        print("\n⚠️  导入可能不完整")

except Exception as e:
    print(f"❌ 导入失败: {e}")
    import traceback

    traceback.print_exc()