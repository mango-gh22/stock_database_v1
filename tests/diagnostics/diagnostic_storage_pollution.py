# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1\diagnostic_storage_pollution.py
# File Name: diagnostic_storage_pollution
# @ Author: mango-gh22
# @ Date：2026/1/1 18:59
"""
desc 
"""
# diagnostic_storage_pollution.py
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

print("=== 存储层污染诊断 ===")
print("\n1. 模块导入前 DataStorage ID:")
try:
    from src.data.data_storage import DataStorage as OriginalDS
    print(f"   原始 DataStorage: {id(OriginalDS)}")
    print(f"   来自: {OriginalDS.__module__}")
except Exception as e:
    print(f"   导入失败: {e}")

print("\n2. 模块导入后 DataStorage ID:")
try:
    from src.data.data_pipeline import DataStorage as PipelineDS
    print(f"   管道 DataStorage: {id(PipelineDS)}")
    print(f"   来自: {PipelineDS.__module__}")
    print(f"   是否是同一个类: {OriginalDS is PipelineDS}")
except Exception as e:
    print(f"   导入失败: {e}")

print("\n3. AdaptiveDataStorage ID:")
try:
    from src.data.adaptive_storage import AdaptiveDataStorage
    print(f"   AdaptiveDataStorage: {id(AdaptiveDataStorage)}")
    print(f"   是否与管道DS相同: {AdaptiveDataStorage is PipelineDS}")
except Exception as e:
    print(f"   导入失败: {e}")

print("\n4. log_data_update 方法签名对比:")
import inspect
try:
    orig_sig = inspect.signature(OriginalDS.log_data_update)
    print(f"   原始签名: {orig_sig}")
except: pass

try:
    adapt_sig = inspect.signature(AdaptiveDataStorage.log_data_update)
    print(f"   Adaptive签名: {adapt_sig}")
except: pass

print("\n5. 当前调用链分析:")
import importlib
import src.data.data_scheduler as ds_mod
importlib.reload(ds_mod)  # 强制重新加载
scheduler = ds_mod.DataScheduler()
print(f"   Scheduler存储类型: {type(scheduler.storage)}")
print(f"   是否为Adaptive: {type(scheduler.storage).__name__ == 'AdaptiveDataStorage'}")