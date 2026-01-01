# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\create_issue_report.py
# File Name: create_issue_report
# @ Author: mango-gh22
# @ Date：2026/1/1 13:33
"""
desc 
"""
# create_issue_report.py
"""
创建详细的问题诊断报告
"""
import os
import sys
from datetime import datetime


def analyze_data_pipeline():
    """分析 DataPipeline 的问题"""
    report = []

    pipeline_path = os.path.join(r"E:\MyFile\stock_database_v1", "src/data/data_pipeline.py")

    if os.path.exists(pipeline_path):
        with open(pipeline_path, 'r', encoding='utf-8') as f:
            content = f.read()

        lines = content.split('\n')

        # 查找 fetch_and_store_daily_data 方法
        in_method = False
        method_lines = []

        for i, line in enumerate(lines):
            if 'def fetch_and_store_daily_data' in line:
                in_method = True
                report.append(f"🔍 找到关键方法: fetch_and_store_daily_data (第{i + 1}行)")

            if in_method:
                method_lines.append((i + 1, line))

                # 检查可能导致跳过的代码
                if any(keyword in line.lower() for keyword in ['skip', 'continue', 'return', 'if', 'else']):
                    report.append(f"   第{i + 1}行: {line.strip()}")

                # 方法结束（下一个def或类结束）
                if i > 0 and 'def ' in line and 'fetch_and_store_daily_data' not in line:
                    break

        # 分析关键逻辑
        report.append("\n🔬 方法逻辑分析:")

        # 查找日期检查逻辑
        for i, line in method_lines:
            if 'last_update' in line or 'latest_date' in line:
                report.append(f"   第{i}行 - 日期检查: {line.strip()}")

        # 查找存储调用
        for i, line in method_lines:
            if 'store_daily_data' in line:
                report.append(f"   第{i}行 - 存储调用: {line.strip()}")

    return report


def create_full_report():
    """创建完整报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"issue_report_{timestamp}.txt"

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("股票数据库系统问题诊断报告\n")
        f.write(f"生成时间: {datetime.now()}\n")
        f.write("=" * 60 + "\n\n")

        # 分析数据管道
        f.write("1. 数据管道分析\n")
        f.write("-" * 40 + "\n")
        pipeline_issues = analyze_data_pipeline()
        for issue in pipeline_issues:
            f.write(issue + "\n")

        # 环境信息
        f.write("\n2. 环境信息\n")
        f.write("-" * 40 + "\n")
        f.write(f"Python版本: {sys.version}\n")
        f.write(f"工作目录: {os.getcwd()}\n")
        f.write(f"项目路径: E:\\MyFile\\stock_database_v1\n")

        # 关键文件状态
        f.write("\n3. 关键文件状态\n")
        f.write("-" * 40 + "\n")

        key_files = [
            ".env",
            "config/database.yaml",
            "src/data/data_pipeline.py",
            "src/data/data_storage.py",
            "src/data/baostock_collector.py"
        ]

        for file in key_files:
            full_path = os.path.join(r"E:\MyFile\stock_database_v1", file)
            if os.path.exists(full_path):
                size = os.path.getsize(full_path)
                f.write(f"✅ {file} - 存在 ({size} bytes)\n")
            else:
                f.write(f"❌ {file} - 不存在\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("问题描述:\n")
        f.write("数据库'纹丝不动'，数据无法写入，但所有测试显示系统正常。\n")
        f.write("怀疑是 DataPipeline 中的逻辑条件阻止了数据写入。\n")

    print(f"✅ 诊断报告已保存到: {report_file}")

    # 也输出到控制台
    with open(report_file, 'r', encoding='utf-8') as f:
        print(f.read())


if __name__ == "__main__":
    create_full_report()