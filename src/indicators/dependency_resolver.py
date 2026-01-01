# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/indicators\dependency_resolver.py
# File Name: dependency_resolver
# @ Author: mango-gh22
# @ Date：2025/12/21 9:00
"""
desc 依赖解析器
"""

"""
File: src/indicators/dependency_resolver.py
Desc: 指标依赖关系解析器 - 管理指标间的依赖关系
"""
from typing import Dict, List, Set, Optional
import networkx as nx
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DependencyType(Enum):
    """依赖类型"""
    REQUIRED = "required"  # 必须的依赖
    OPTIONAL = "optional"  # 可选的依赖
    CACHED = "cached"  # 缓存依赖


class IndicatorDependency:
    """指标依赖项"""

    def __init__(self, indicator_name: str,
                 dependency_type: DependencyType = DependencyType.REQUIRED,
                 parameters: Optional[Dict] = None):
        """
        初始化依赖项

        Args:
            indicator_name: 依赖的指标名称
            dependency_type: 依赖类型
            parameters: 依赖指标的参数
        """
        self.indicator_name = indicator_name
        self.dependency_type = dependency_type
        self.parameters = parameters or {}


class DependencyResolver:
    """依赖关系解析器"""

    def __init__(self):
        """初始化依赖解析器"""
        self.dependency_graph = nx.DiGraph()
        self.indicator_dependencies: Dict[str, List[IndicatorDependency]] = {}

        # 初始化内置依赖关系
        self._init_builtin_dependencies()

    def _init_builtin_dependencies(self):
        """初始化内置指标依赖关系"""
        # MACD 依赖移动平均线
        self.add_dependency("macd", "moving_average",
                            parameters={'type': 'ema', 'period': 12})
        self.add_dependency("macd", "moving_average",
                            parameters={'type': 'ema', 'period': 26})

        # RSI 依赖价格变化
        self.add_dependency("rsi", "price_change",
                            dependency_type=DependencyType.REQUIRED)

        # 布林带 依赖移动平均线和标准差
        self.add_dependency("bollinger_bands", "moving_average",
                            parameters={'type': 'sma', 'period': 20})
        self.add_dependency("bollinger_bands", "std_dev",
                            parameters={'period': 20})

        logger.info("初始化内置指标依赖关系完成")

    def add_dependency(self, indicator_name: str,
                       dependency_name: str,
                       dependency_type: DependencyType = DependencyType.REQUIRED,
                       parameters: Optional[Dict] = None):
        """
        添加指标依赖关系

        Args:
            indicator_name: 指标名称
            dependency_name: 依赖的指标名称
            dependency_type: 依赖类型
            parameters: 依赖参数
        """
        # 添加到图
        self.dependency_graph.add_edge(dependency_name, indicator_name)

        # 添加到依赖字典
        if indicator_name not in self.indicator_dependencies:
            self.indicator_dependencies[indicator_name] = []

        dependency = IndicatorDependency(
            indicator_name=dependency_name,
            dependency_type=dependency_type,
            parameters=parameters
        )

        self.indicator_dependencies[indicator_name].append(dependency)

        logger.debug(f"添加依赖关系: {dependency_name} -> {indicator_name} ({dependency_type.value})")

    def get_dependencies(self, indicator_name: str) -> List[IndicatorDependency]:
        """
        获取指标的依赖项

        Args:
            indicator_name: 指标名称

        Returns:
            依赖项列表
        """
        return self.indicator_dependencies.get(indicator_name, [])

    def get_dependency_chain(self, indicator_name: str) -> List[str]:
        """
        获取完整的依赖链

        Args:
            indicator_name: 指标名称

        Returns:
            依赖链列表（从基础到高级）
        """
        if not nx.is_directed_acyclic_graph(self.dependency_graph):
            raise ValueError("依赖图中存在循环依赖")

        try:
            # 获取所有前置依赖
            ancestors = list(nx.ancestors(self.dependency_graph, indicator_name))

            # 拓扑排序
            sorted_nodes = list(nx.topological_sort(self.dependency_graph))

            # 过滤出相关的节点并排序
            relevant_nodes = [node for node in sorted_nodes
                              if node == indicator_name or node in ancestors]

            return relevant_nodes

        except Exception as e:
            logger.error(f"获取依赖链失败: {e}")
            return [indicator_name]

    def resolve_calculation_order(self, indicator_names: List[str]) -> List[str]:
        """
        解析计算顺序

        Args:
            indicator_names: 需要计算的指标列表

        Returns:
            按依赖关系排序的计算顺序
        """
        if not indicator_names:
            return []

        # 获取所有相关指标
        all_indicators = set()
        for indicator_name in indicator_names:
            all_indicators.update(self.get_dependency_chain(indicator_name))

        # 获取完整的有向无环图子图
        subgraph = self.dependency_graph.subgraph(all_indicators)

        if not nx.is_directed_acyclic_graph(subgraph):
            raise ValueError("存在循环依赖，无法确定计算顺序")

        try:
            # 拓扑排序
            calculation_order = list(nx.topological_sort(subgraph))
            logger.debug(f"计算顺序: {calculation_order}")
            return calculation_order

        except Exception as e:
            logger.error(f"解析计算顺序失败: {e}")
            return indicator_names

    def validate_dependencies(self, indicator_name: str,
                              available_indicators: Set[str]) -> bool:
        """
        验证依赖是否满足

        Args:
            indicator_name: 指标名称
            available_indicators: 可用的指标集合

        Returns:
            是否满足依赖
        """
        dependencies = self.get_dependencies(indicator_name)

        for dep in dependencies:
            if dep.dependency_type == DependencyType.REQUIRED:
                if dep.indicator_name not in available_indicators:
                    logger.warning(f"指标 {indicator_name} 缺少必要依赖: {dep.indicator_name}")
                    return False

        return True

    def get_required_dependencies(self, indicator_name: str) -> List[str]:
        """
        获取必需的依赖项

        Args:
            indicator_name: 指标名称

        Returns:
            必需的依赖项名称列表
        """
        dependencies = self.get_dependencies(indicator_name)
        required = [dep.indicator_name for dep in dependencies
                    if dep.dependency_type == DependencyType.REQUIRED]
        return list(set(required))

    def visualize_dependencies(self, output_file: Optional[str] = None):
        """
        可视化依赖关系

        Args:
            output_file: 输出文件路径（如 .png, .pdf）
        """
        try:
            import matplotlib.pyplot as plt

            pos = nx.spring_layout(self.dependency_graph)
            plt.figure(figsize=(12, 8))

            nx.draw_networkx_nodes(self.dependency_graph, pos, node_size=2000,
                                   node_color='lightblue', alpha=0.8)
            nx.draw_networkx_edges(self.dependency_graph, pos, edge_color='gray',
                                   arrows=True, arrowsize=20)
            nx.draw_networkx_labels(self.dependency_graph, pos, font_size=10)

            plt.title("指标依赖关系图")
            plt.axis('off')

            if output_file:
                plt.savefig(output_file, dpi=300, bbox_inches='tight')
                logger.info(f"依赖关系图已保存到: {output_file}")
            else:
                plt.show()

        except ImportError:
            logger.warning("matplotlib 未安装，无法可视化依赖关系")
        except Exception as e:
            logger.error(f"可视化依赖关系失败: {e}")