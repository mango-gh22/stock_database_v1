# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\create_release_note.py
# File Name: create_release_note
# @ Author: mango-gh22
# @ Date：2025/12/14 12:16
"""
desc 创建版本说明
"""

# create_release_note.py
import json
from datetime import datetime

release_info = {
    "version": "v0.5.1",
    "release_date": datetime.now().strftime("%Y-%m-%d"),
    "phase": "P5-块1完成",
    "title": "三种更新模式技术方案实现",
    "features": [
        "StockDataPipeline统一数据管道",
        "增量更新模式 - 日常数据维护",
        "批量初始化模式 - 全市场/指数初始化",
        "指定股票模式 - 特定股票更新",
        "自适应数据存储 - 自动适配表结构",
        "管道配置系统 - YAML配置驱动",
        "详细报告系统 - JSON格式执行报告"
    ],
    "modules_added": [
        "src/data/integrated_pipeline.py (重构)",
        "src/config/pipeline_config_loader.py",
        "config/pipeline_config.yaml"
    ],
    "modules_enhanced": [
        "src/data/adaptive_storage.py",
        "src/data/enhanced_processor.py",
        "src/data_processing/base_processor.py"
    ],
    "test_files": [
        "test_architecture_fix.py"
    ],
    "git_branch": "feature/v0.5.0-pipeline-refactor",
    "git_commit": "3f85fc6",
    "next_phase": "P5-块2：数据质量管理模块"
}

with open("RELEASE_v0.5.1.json", "w", encoding="utf-8") as f:
    json.dump(release_info, f, ensure_ascii=False, indent=2)

print("✅ 版本说明已创建: RELEASE_v0.5.1.json")