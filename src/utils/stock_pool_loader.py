# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\stock_pool_loader.py
# File Name: stock_pool_loader
# @ Author: mango-gh22
# @ Date：2025/12/27 17:37
"""
desc 
"""
# src/utils/stock_pool_loader.py
# -*- coding: utf-8 -*-
"""
股票池加载器 - 支持从配置文件或数据库加载成分股
"""

import os
import yaml
from pathlib import Path
from typing import List
from src.utils.code_converter import normalize_stock_code
from src.config.pipeline_config_loader import get_pipeline_config


def load_a50_components() -> List[str]:
    """
    加载中证A50指数成分股列表（标准化格式：sh601318, sz300750...）

    优先级：
    1. 从 config/symbols.yaml 中读取 csi_a50 列表
    2. 若无，则返回空列表（需手动维护）
    """
    config = get_pipeline_config()

    # 获取 csi_a50 配置
    a50_group = config.get('symbol_groups', {}).get('csi_a50', {})
    symbols_source = a50_group.get('symbols_source')

    if not symbols_source:
        raise ValueError("未配置 csi_a50 的 symbols_source")

    # 处理相对路径
    if isinstance(symbols_source, str):
        if symbols_source == 'database':
            # TODO: 未来可对接数据库查询
            raise NotImplementedError("数据库加载成分股暂未实现")
        else:
            # 假设是文件路径，如 'config/symbols.yaml'
            symbols_path = Path(__file__).parent.parent.parent / symbols_source
            if symbols_path.exists():
                return _load_symbols_from_file(symbols_path)
            else:
                raise FileNotFoundError(f"成分股文件不存在: {symbols_path}")

        print("✅ 成分股加载成功，前3只:", normalized[:3])
    return []


def _load_symbols_from_file(file_path: Path) -> List[str]:
    """从 YAML/CSV 文件加载股票代码"""
    if file_path.suffix.lower() in ['.yaml', '.yml']:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        symbols_raw = data.get('csi_a50', [])

        # 新增：处理对象列表格式
        if isinstance(symbols_raw, list) and len(symbols_raw) > 0:
            if isinstance(symbols_raw[0], dict):
                # 格式: [{symbol: "600519.SH", ...}, ...]
                symbol_list = [item['symbol'] for item in symbols_raw if 'symbol' in item]
            else:
                # 格式: ["600519.SH", ...]
                symbol_list = symbols_raw
        else:
            symbol_list = []

    elif file_path.suffix.lower() == '.csv':
        import pandas as pd
        df = pd.read_csv(file_path)
        # 假设第一列是代码，或有 'symbol' 列
        if 'symbol' in df.columns:
            symbol_list = df['symbol'].dropna().tolist()
        else:
            symbol_list = df.iloc[:, 0].dropna().tolist()
    else:
        raise ValueError(f"不支持的文件格式: {file_path}")

    # 标准化代码
    normalized = []
    for code in symbol_list:
        try:
            norm = normalize_stock_code(str(code))
            normalized.append(norm)
        except Exception as e:
            print(f"⚠️ 跳过无效代码 {code}: {e}")

    return list(dict.fromkeys(normalized))  # 去重保序