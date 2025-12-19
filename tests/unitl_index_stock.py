# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\unitl_index_stock.py
# File Name: unitl_index_stock
# @ Author: mango-gh22
# @ Date：2025/12/13 11:35
"""
desc 统一接口封装（推荐）--获取指数成分股
"""

"""
统一的指数成分股获取接口
支持多种数据源，提供一致的API
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd


class IndexComponentsFetcher(ABC):
    """指数成分股获取器抽象基类"""

    @abstractmethod
    def get_index_list(self) -> Dict[str, str]:
        """获取指数清单"""
        pass

    @abstractmethod
    def get_components(self, index_code: str, **kwargs) -> List[Dict]:
        """获取成分股"""
        pass

    @abstractmethod
    def search_index(self, keyword: str) -> Dict[str, str]:
        """搜索指数"""
        pass


class IndexComponentsManager:
    """指数成分股管理器"""

    def __init__(self):
        self.fetchers = {}
        self.current_fetcher = None

    def register_fetcher(self, name: str, fetcher: IndexComponentsFetcher):
        """注册数据源获取器"""
        self.fetchers[name] = fetcher

    def set_fetcher(self, name: str):
        """设置当前使用的获取器"""
        if name in self.fetchers:
            self.current_fetcher = self.fetchers[name]
            return True
        return False

    def get_available_fetchers(self) -> List[str]:
        """获取可用的数据源列表"""
        return list(self.fetchers.keys())

    def get_index_list(self) -> Dict[str, str]:
        """获取指数清单"""
        if self.current_fetcher:
            return self.current_fetcher.get_index_list()
        return {}

    def get_components(self, index_code: str, **kwargs) -> List[Dict]:
        """获取成分股"""
        if self.current_fetcher:
            return self.current_fetcher.get_components(index_code, **kwargs)
        return []

    def search_index(self, keyword: str) -> Dict[str, str]:
        """搜索指数"""
        if self.current_fetcher:
            return self.current_fetcher.search_index(keyword)
        return {}

    def save_to_csv(self, components: List[Dict], filename: str):
        """保存成分股到CSV"""
        if components:
            df = pd.DataFrame(components)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            return True
        return False

    def save_to_excel(self, components: List[Dict], filename: str):
        """保存成分股到Excel"""
        if components:
            df = pd.DataFrame(components)
            df.to_excel(filename, index=False)
            return True
        return False


# 使用示例
def setup_and_demo():
    """设置并演示统一接口"""

    # 创建管理器
    manager = IndexComponentsManager()

    # 注册不同的数据源（需要先实例化具体的fetcher类）
    # manager.register_fetcher('baostock', BaoStockIndexComponents())
    # manager.register_fetcher('ths', THSIndexComponents())

    # 设置当前使用的数据源
    # manager.set_fetcher('baostock')

    # 使用管理器获取数据
    # indices = manager.get_index_list()
    # components = manager.get_components('沪深300')

    return manager


if __name__ == "__main__":
    # 运行主演示
    main()