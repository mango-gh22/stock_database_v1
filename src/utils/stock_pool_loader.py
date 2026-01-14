# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\stock_pool_loader.py
# File Name: stock_pool_loader
# @ Author: mango-gh22
# @ Date：2025/12/27 17:37
"""
desc 股票池加载器 - 支持从配置文件或数据库加载成分股
"""

import sys
from pathlib import Path
import yaml
import logging
import pandas as pd

# 添加项目路径
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.config.config_loader import ConfigLoader
from src.utils.code_converter import normalize_stock_code
from src.database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)


def load_symbols_from_db():
    """
    从数据库读取已存在的股票代码（用于因子更新）

    Returns:
        List[str]: 标准化后的股票代码列表
    """
    try:
        db = DatabaseConnector()

        with db.get_connection() as conn:
            # 查询数据库中所有有数据的股票
            df = pd.read_sql(
                "SELECT DISTINCT symbol FROM stock_daily_data ORDER BY symbol",
                conn
            )

            if df.empty:
                logger.warning("数据库中无股票数据")
                return []

            # 标准化并去重
            symbols = []
            for symbol in df['symbol'].tolist():
                try:
                    normalized = normalize_stock_code(symbol)
                    symbols.append(normalized)
                except ValueError as e:
                    logger.warning(f"股票代码转换失败 {symbol}: {e}")
                    continue

            # 去重并保持顺序
            seen = set()
            unique_symbols = []
            for s in symbols:
                if s not in seen:
                    seen.add(s)
                    unique_symbols.append(s)

            logger.info(f"从数据库加载 {len(unique_symbols)} 只股票")
            return unique_symbols

    except Exception as e:
        logger.error(f"从数据库加载股票失败: {e}", exc_info=True)
        return []


def load_a50_components(config_path: str = 'config/symbols.yaml') -> list:
    """
    从配置文件加载A50成分股（用于新增股票到数据库）

    Args:
        config_path: 配置文件路径

    Returns:
        标准化后的A50代码列表
    """
    try:
        config = ConfigLoader.load_yaml_config(config_path)

        if not config:
            logger.error(f"无法加载配置文件: {config_path}")
            return []

        raw_symbols = config.get('csi_a50', [])
        symbols = []

        for item in raw_symbols:
            if isinstance(item, dict) and 'symbol' in item:
                raw_symbol = item['symbol']
            elif isinstance(item, str):
                raw_symbol = item
            else:
                continue

            try:
                normalized = normalize_stock_code(raw_symbol)
                symbols.append(normalized)
            except ValueError as e:
                logger.warning(f"股票代码转换失败 {raw_symbol}: {e}")
                continue

        # 去重并保持顺序
        seen = set()
        unique_symbols = []
        for s in symbols:
            if s not in seen:
                seen.add(s)
                unique_symbols.append(s)

        logger.info(f"从配置文件加载 {len(unique_symbols)} 只A50成分股")
        return unique_symbols

    except Exception as e:
        logger.error(f"加载配置文件失败: {e}", exc_info=True)
        return []


def get_symbols(source: str = 'db'):
    """
    统一股票代码获取入口

    Args:
        source: 'db' (从数据库) 或 'config' (从配置文件)

    Returns:
        List[str]: 股票代码列表
    """
    if source == 'db':
        return load_symbols_from_db()
    elif source == 'config':
        return load_a50_components()
    else:
        logger.warning(f"未知的代码源: {source}，默认使用数据库")
        return load_symbols_from_db()