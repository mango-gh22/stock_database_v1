# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\symbol_manager.py
# File Name: symbol_manager
# @ Author: mango-gh22
# @ Date：2025/12/14 8:36
"""
desc
符号管理器 - 管理不同指数组的股票代码
"""

import yaml
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
import pandas as pd
from src.utils.logger import get_logger
from src.utils.code_converter import normalize_stock_code
from src.database.db_connector import DatabaseConnector

logger = get_logger(__name__)


class SymbolManager:
    """符号管理器"""

    def __init__(self, config_path: str = 'config/symbols.yaml'):
        self.config_path = Path(config_path)
        self.symbol_groups = self._load_symbol_groups()
        self.db_connector = DatabaseConnector()

    def _load_symbol_groups(self) -> Dict[str, Any]:
        """加载符号组配置"""
        if not self.config_path.exists():
            logger.warning(f"符号配置文件不存在: {self.config_path}")
            return {}

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 支持新格式（带组信息）和旧格式（只有列表）
            if isinstance(config, dict) and 'symbol_groups' in config:
                return config['symbol_groups']
            elif isinstance(config, dict):
                # 旧格式：直接包含组
                groups = {}
                for group_name, group_data in config.items():
                    if isinstance(group_data, list):
                        # 旧格式：列表直接就是符号
                        groups[group_name] = {
                            'symbols': group_data,
                            'name': group_name,
                            'description': f'{group_name}指数成分股',
                            'source': 'config'
                        }
                    elif isinstance(group_data, dict):
                        # 已经是指定格式
                        groups[group_name] = group_data
                return groups
            else:
                logger.warning(f"符号配置格式不支持: {type(config)}")
                return {}

        except Exception as e:
            logger.error(f"加载符号配置失败: {e}")
            return {}

    def get_symbols(self, group_name: str, source: str = 'config') -> List[str]:
        """
        获取指定组的股票符号

        Args:
            group_name: 组名，如 'csi_a50'
            source: 来源 'config' 或 'database'

        Returns:
            股票符号列表
        """
        symbols = []

        # 从配置获取
        if source == 'config' and group_name in self.symbol_groups:
            group_info = self.symbol_groups[group_name]

            if isinstance(group_info, list):
                # 旧格式：直接是列表
                symbols = group_info
            elif isinstance(group_info, dict):
                # 新格式：字典包含symbols字段
                if 'symbols' in group_info and isinstance(group_info['symbols'], list):
                    symbols = group_info['symbols']
                elif 'symbols_file' in group_info:
                    # 从文件加载
                    file_path = Path(group_info['symbols_file'])
                    if file_path.exists():
                        symbols = self._load_symbols_from_file(file_path)

        # 从数据库获取
        elif source == 'database':
            symbols = self._get_symbols_from_database(group_name)

        # 标准化符号
        normalized_symbols = []
        for symbol in symbols:
            if isinstance(symbol, dict) and 'symbol' in symbol:
                # 如果是字典格式，提取symbol字段
                symbol_str = symbol['symbol']
            elif isinstance(symbol, str):
                symbol_str = symbol
            else:
                continue

            try:
                normalized = normalize_stock_code(symbol_str)
                normalized_symbols.append(normalized)
            except Exception as e:
                logger.warning(f"标准化符号失败 {symbol_str}: {e}")

        return normalized_symbols

    def _load_symbols_from_file(self, file_path: Path) -> List[str]:
        """从文件加载符号"""
        try:
            if file_path.suffix == '.txt':
                with open(file_path, 'r', encoding='utf-8') as f:
                    symbols = [line.strip() for line in f if line.strip()]
            elif file_path.suffix == '.yaml':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    symbols = data.get('symbols', [])
            else:
                logger.warning(f"不支持的文件格式: {file_path.suffix}")
                symbols = []

            return symbols

        except Exception as e:
            logger.error(f"从文件加载符号失败 {file_path}: {e}")
            return []

    def _get_symbols_from_database(self, group_name: str) -> List[str]:
        """从数据库获取符号"""
        try:
            # 根据组名决定查询逻辑
            if group_name == 'csi_a50':
                query = """
                    SELECT DISTINCT symbol FROM stock_index_constituent 
                    WHERE index_code = 'csi_a50' AND is_current = 1
                    ORDER BY weight DESC
                """
            elif group_name == 'csi_300':
                query = """
                    SELECT DISTINCT symbol FROM stock_index_constituent 
                    WHERE index_code = 'csi_300' AND is_current = 1
                    ORDER BY symbol
                """
            else:
                # 通用查询
                query = """
                    SELECT DISTINCT symbol FROM stock_basic_info 
                    WHERE list_status = 'L'
                    ORDER BY symbol
                """

            with self.db_connector.get_connection() as conn:
                df = pd.read_sql_query(query, conn)
                return df['symbol'].tolist()

        except Exception as e:
            logger.error(f"从数据库获取符号失败 {group_name}: {e}")
            return []

    def get_all_groups(self) -> Dict[str, Dict]:
        """获取所有组信息"""
        return self.symbol_groups

    def get_group_info(self, group_name: str) -> Optional[Dict]:
        """获取组信息"""
        return self.symbol_groups.get(group_name)

    def add_symbols_to_group(self, group_name: str, symbols: List[str],
                             overwrite: bool = False) -> bool:
        """
        添加符号到组

        Args:
            group_name: 组名
            symbols: 符号列表
            overwrite: 是否覆盖现有符号

        Returns:
            是否成功
        """
        try:
            # 加载现有配置
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # 确保组存在
            if 'symbol_groups' not in config:
                config['symbol_groups'] = {}

            if group_name not in config['symbol_groups']:
                config['symbol_groups'][group_name] = {
                    'name': group_name,
                    'description': f'{group_name}指数成分股',
                    'symbols': []
                }

            group_config = config['symbol_groups'][group_name]

            # 标准化符号
            normalized_symbols = []
            for symbol in symbols:
                try:
                    normalized = normalize_stock_code(symbol)
                    normalized_symbols.append(normalized)
                except Exception as e:
                    logger.warning(f"标准化符号失败 {symbol}: {e}")

            # 添加或覆盖符号
            if overwrite or 'symbols' not in group_config:
                group_config['symbols'] = normalized_symbols
            else:
                # 合并，去重
                existing = set(group_config['symbols'])
                new_symbols = set(normalized_symbols)
                group_config['symbols'] = list(existing | new_symbols)

            # 保存配置
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False,
                          allow_unicode=True, indent=2)

            logger.info(f"添加{len(normalized_symbols)}个符号到组 {group_name}")
            return True

        except Exception as e:
            logger.error(f"添加符号到组失败 {group_name}: {e}")
            return False

    def validate_symbols(self, symbols: List[str]) -> Dict[str, List]:
        """
        验证符号有效性

        Args:
            symbols: 符号列表

        Returns:
            验证结果字典
        """
        results = {
            'valid': [],
            'invalid': [],
            'normalized': []
        }

        for symbol in symbols:
            try:
                normalized = normalize_stock_code(symbol)
                results['valid'].append(symbol)
                results['normalized'].append(normalized)
            except Exception as e:
                results['invalid'].append({'symbol': symbol, 'error': str(e)})

        return results


# 单例实例
_symbol_manager = None


def get_symbol_manager(config_path: str = 'config/symbols.yaml') -> SymbolManager:
    """获取符号管理器（单例模式）"""
    global _symbol_manager
    if _symbol_manager is None:
        _symbol_manager = SymbolManager(config_path)
    return _symbol_manager