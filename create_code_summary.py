# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\create_code_summary.py
# File Name: create_code_summary
# @ Author: mango-gh22
# @ Date：2026/1/1 13:32
"""
desc 
"""
# create_code_summary.py
import os
import json


def create_project_summary():
    """创建项目代码摘要"""
    summary = {
        "project_structure": {},
        "key_files": {},
        "issues_found": []
    }

    # 关键文件列表
    key_files = [
        "src/data/data_pipeline.py",
        "src/data/data_storage.py",
        "src/data/baostock_collector.py",
        "src/database/db_connector.py",
        "config/database.yaml",
        ".env"
    ]

    for file_path in key_files:
        full_path = os.path.join(r"E:\MyFile\stock_database_v1", file_path)
        if os.path.exists(full_path):
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 只保存关键部分（避免太大）
            lines = content.split('\n')
            key_lines = []

            # 提取关键代码（类定义、关键方法）
            for i, line in enumerate(lines):
                if any(keyword in line for keyword in [
                    'class ', 'def ', '__init__', 'store_daily_data',
                    'fetch_and_store_daily_data', 'get_last_update_date'
                ]):
                    key_lines.append(f"第{i + 1}行: {line.strip()}")

            summary["key_files"][file_path] = {
                "size": len(content),
                "lines": len(lines),
                "key_sections": key_lines[:20]  # 只取前20个关键行
            }

    # 保存摘要
    with open("project_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("✅ 项目摘要已保存到 project_summary.json")

    # 也输出到控制台
    print("\n关键文件摘要:")
    for file, info in summary["key_files"].items():
        print(f"\n📄 {file}:")
        print(f"   大小: {info['size']} 字符, 行数: {info['lines']}")
        for line in info['key_sections']:
            print(f"   {line}")


if __name__ == "__main__":
    create_project_summary()