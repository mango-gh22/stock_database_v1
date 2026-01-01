# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\fix_missing_method.py
# File Name: fix_missing_method
# @ Author: mango-gh22
# @ Date：2026/1/1 11:07
"""
desc 
"""
# fix_missing_method.py
import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, r"E:\MyFile\stock_database_v1")
load_dotenv()

print("🔧 修复缺失的方法")
print("=" * 60)

# 1. 查看 DataStorage 的原始代码
print("1. 📄 查看 DataStorage 类结构...")
storage_path = os.path.join("src", "data", "data_storage.py")

if os.path.exists(storage_path):
    with open(storage_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找类定义
    lines = content.split('\n')
    class_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('class DataStorage'):
            class_start = i
            break

    if class_start is not None:
        print(f"   找到 DataStorage 类 (第{class_start + 1}行)")

        # 显示前50行类内容
        print("   类内容开头:")
        for i in range(class_start, min(class_start + 50, len(lines))):
            print(f"     {lines[i]}")

    # 检查是否有 get_last_update_date 方法
    if 'def get_last_update_date' in content:
        print("   ✅ 文件中存在 get_last_update_date 方法")
    else:
        print("   ❌ 文件中不存在 get_last_update_date 方法")

else:
    print(f"   ❌ 文件不存在: {storage_path}")

# 2. 添加缺失的方法
print("\n2. 🔧 添加缺失的方法...")
try:
    # 创建一个临时修复版本
    fixed_method = '''
    def get_last_update_date(self, symbol: str = None, table_name: str = None) -> str:
        """
        获取指定股票的最后更新日期

        Args:
            symbol: 股票代码
            table_name: 表名

        Returns:
            最后更新日期字符串，如 '2025-12-31'，如果不存在则返回 None
        """
        try:
            if table_name is None:
                table_name = self.supported_tables.get('daily', 'stock_daily_data')

            with self.db_connector.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    if symbol:
                        # 查询指定股票的最后更新日期
                        clean_symbol = symbol.replace('.', '')
                        query = f"""
                            SELECT MAX(trade_date) as last_date 
                            FROM {table_name} 
                            WHERE symbol = %s
                        """
                        cursor.execute(query, (clean_symbol,))
                    else:
                        # 查询整个表的最后更新日期
                        query = f"""
                            SELECT MAX(trade_date) as last_date 
                            FROM {table_name}
                        """
                        cursor.execute(query)

                    result = cursor.fetchone()

                    if result and result['last_date']:
                        return result['last_date'].strftime('%Y-%m-%d') if hasattr(result['last_date'], 'strftime') else str(result['last_date'])
                    else:
                        return None

        except Exception as e:
            logger.warning(f"获取最后更新日期失败: {e}")
            return None
'''

    print(f"   准备添加的方法:\n{fixed_method[:200]}...")

    # 询问是否要修复
    response = input("\n   是否要将此方法添加到 DataStorage 类中？(y/n): ")

    if response.lower() == 'y':
        # 读取原文件
        with open(storage_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # 找到最后一个方法的位置
        insert_pos = len(lines)
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].strip().startswith('def ') or lines[i].strip().startswith('class '):
                insert_pos = i + 1
                break

        # 插入新方法
        lines.insert(insert_pos, '\n' + fixed_method + '\n')

        # 备份原文件
        backup_path = storage_path + '.backup'
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

        # 写入新文件
        with open(storage_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

        print(f"   ✅ 已修复 DataStorage 类")
        print(f"   原文件已备份到: {backup_path}")
    else:
        print("   ℹ️ 跳过修复")

except Exception as e:
    print(f"   ❌ 修复失败: {e}")

# 3. 测试修复后的 DataPipeline
print("\n3. 🧪 测试修复后的系统...")
try:
    # 重新导入模块以获取更新
    import importlib
    import src.data.data_storage

    # 重新加载模块
    importlib.reload(src.data.data_storage)

    from src.data.data_storage import DataStorage
    from src.data.baostock_collector import BaostockCollector
    from src.data.data_pipeline import DataPipeline

    print("   ✅ 模块重新加载成功")

    # 测试 get_last_update_date 方法
    storage = DataStorage()
    date = storage.get_last_update_date('sh.600000')
    print(f"   get_last_update_date('sh.600000') = {date}")

    # 测试完整管道
    collector = BaostockCollector()
    pipeline = DataPipeline(collector=collector, storage=storage)

    result = pipeline.fetch_and_store_daily_data(
        symbol='sh.600000',
        start_date='2025-12-25',
        end_date='2025-12-31'
    )

    print(f"   管道执行结果: {result.get('status')}")

except Exception as e:
    print(f"   ❌ 测试失败: {e}")
    import traceback

    traceback.print_exc()

print("\n" + "=" * 60)
print("🎯 修复完成")