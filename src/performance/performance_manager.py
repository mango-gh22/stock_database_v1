# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance\performance_manager_fixed.py
# File Name: performance_manager_fixed
# @ Author: mango-gh22
# @ Date：2025/12/22 0:53
"""
desc
修复的性能管理器 - 三层防御架构版
"""
# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/performance/performance_manager.py

from typing import Dict, Any, Optional
from pathlib import Path
import logging
import threading
import time
import pandas as pd

logger = logging.getLogger(__name__)


class PerformanceManager:
    """性能管理器 - 正式版（原 Fixed 版）"""

    def __init__(self, config_path: Optional[str] = None):
        # 加载配置
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / 'config' / 'performance.yaml'

        config_path = str(config_path)

        # 加载并验证配置
        self.config = self._load_and_validate_config(config_path)

        # 初始化各个模块
        self._init_modules()

        logger.info("性能管理器初始化完成")

    def _load_and_validate_config(self, config_path: str) -> Dict[str, Any]:
        """加载并验证配置 - 修复版"""
        try:
            from ..config.config_loader import ConfigLoader
            from ..config.config_validator import ConfigValidator

            raw_config = ConfigLoader.load_yaml_config(config_path)
            if not raw_config:
                logger.warning(f"配置文件为空: {config_path}")
                raw_config = {}

            # 验证和修复配置
            config = ConfigValidator.validate_and_fix(raw_config)

            # 确保必要配置存在
            config = self._ensure_required_config(config)

            logger.info(f"成功加载并验证配置文件: {config_path}")
            return config

        except Exception as e:
            logger.error(f"加载配置文件失败 {config_path}: {e}")
            return self._get_default_config()

    def _ensure_required_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """确保必要配置存在 - 修复版"""
        import copy
        result = copy.deepcopy(config)

        # 确保 indicators 配置
        if 'indicators' not in result:
            result['indicators'] = {}

        # 确保 cache 配置
        if 'cache' not in result['indicators']:
            result['indicators']['cache'] = {}

        # 确保 parallel 配置
        if 'parallel' not in result['indicators']:
            result['indicators']['parallel'] = {}

        # 确保 monitoring 配置
        if 'monitoring' not in result:
            result['monitoring'] = {}

        return result

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'indicators': {
                'cache': {'enabled': True, 'max_size': 1000, 'ttl': 3600},
                'parallel': {'enabled': True, 'max_workers': 4, 'timeout': 300}
            },
            'monitoring': {
                'enabled': True,
                'performance': {'interval': 10, 'history_size': 1000}
            }
        }

    def _init_modules(self):
        """初始化所有模块 - 三层防御架构"""
        try:
            self._init_real_modules()
            logger.info("✅ 所有模块初始化成功（第一层：正式版）")
        except Exception as e:
            logger.error(f"第一层初始化失败: {e}", exc_info=True)
            try:
                self._create_safe_modules()
                logger.warning("⚠️  进入安全模式（第二层：SafeModule）")
            except Exception as e2:
                logger.critical(f"第二层初始化失败: {e2}", exc_info=True)
                try:
                    self._create_stub_modules()
                    logger.critical("🚨 进入Stub模式（第三层）")
                except Exception as e3:
                    raise RuntimeError("所有初始化层均失败")

    def _init_real_modules(self):
        """初始化正式版本模块（第一层）"""
        # 并行计算器
        parallel_config = self.config.get('indicators', {}).get('parallel', {})
        try:
            from .parallel_calculator_fixed import ParallelCalculatorFixed
            self.parallel_calculator = ParallelCalculatorFixed(parallel_config)
            logger.info(f"✅ 并行计算器初始化成功")
        except ImportError:
            from .parallel_calculator import ParallelCalculator
            self.parallel_calculator = ParallelCalculator(parallel_config)
            logger.info("⚠️  使用基础版并行计算器")

        # 缓存管理器
        cache_config = self.config.get('indicators', {}).get('cache', {})
        try:
            from .cache_strategy_fixed import CacheManagerFixed
            self.cache_manager = CacheManagerFixed(cache_config)
            logger.info(f"✅ 缓存管理器初始化成功")
        except ImportError:
            from .cache_strategy import CacheManager
            self.cache_manager = CacheManager(cache_config)
            logger.info("⚠️  使用基础版缓存管理器")

        # 内存管理器
        memory_config = self.config.get('memory_management', {})
        try:
            from .memory_manager_fixed import MemoryManagerFixed
            self.memory_manager = MemoryManagerFixed(memory_config)
            logger.info("✅ 内存管理器初始化成功")
        except ImportError:
            from .memory_manager import MemoryManager
            self.memory_manager = MemoryManager(memory_config)
            logger.info("⚠️  使用基础版内存管理器")

        # 性能监控器
        monitor_config = self.config.get('monitoring', {})
        if not isinstance(monitor_config, dict):
            monitor_config = {}

        performance_config = monitor_config.get('performance', {})
        fixed_monitor_config = {
            'interval': performance_config.get('interval', 10),
            'history_size': performance_config.get('history_size', 1000),
            'alerts': monitor_config.get('alerts', {}),
            'enabled': monitor_config.get('enabled', True)
        }

        from ..monitoring.performance_monitor import PerformanceMonitor
        self.performance_monitor = PerformanceMonitor(fixed_monitor_config)
        logger.info(f"✅ 性能监控器初始化")

        # 指标验证器
        validation_config = monitor_config.get('validation', {})
        from ..monitoring.indicator_validator import IndicatorValidator
        self.indicator_validator = IndicatorValidator(validation_config)
        logger.info("✅ 指标验证器初始化")

        # 计算日志器
        log_config = monitor_config.get('calculation_log', {})
        from ..monitoring.calculation_logger import CalculationLogger
        self.calculation_logger = CalculationLogger(log_config)
        logger.info("✅ 计算日志器初始化")

    def _create_safe_modules(self):
        """创建安全的占位模块（第二层）"""
        logger.warning("创建安全的占位模块（第二层）")

        class SafeModule:
            def __init__(self, name):
                self.name = name
                self.logger = logging.getLogger(f"SafeModule.{name}")

            def __getattr__(self, name):
                def safe_method(*args, **kwargs):
                    self.logger.debug(f"{self.name}.{name} called (safe mode)")

                    if name == 'optimize_dataframe':
                        return args[0] if args else pd.DataFrame()
                    elif name == 'calculate':
                        if args and callable(args[0]):
                            func, data = args[0], args[1]
                            return [func(item, *args[2:], **kwargs) for item in data]
                    elif name == 'get':
                        return None
                    elif name == 'set':
                        return True
                    elif name == 'get_cache_stats':
                        return {'enabled': False, 'size': 0, 'hit_rate': 0}

                    return None

                return safe_method

        self.parallel_calculator = SafeModule('ParallelCalculator')
        self.cache_manager = SafeModule('CacheManager')
        self.memory_manager = SafeModule('MemoryManager')
        self.performance_monitor = SafeModule('PerformanceMonitor')
        self.indicator_validator = SafeModule('IndicatorValidator')
        self.calculation_logger = SafeModule('CalculationLogger')

    def _create_stub_modules(self):
        """创建占位模块（第三层）"""
        logger.critical("创建占位模块（第三层）")

        class StubModule:
            def __init__(self, config):
                self.config = config

            def __getattr__(self, name):
                return lambda *args, **kwargs: None

        self.parallel_calculator = StubModule({})
        self.cache_manager = StubModule({})
        self.memory_manager = StubModule({})
        self.performance_monitor = StubModule({})
        self.indicator_validator = StubModule({})
        self.calculation_logger = StubModule({})

    def start(self):
        """启动性能管理器"""
        logger.info("启动性能管理器")
        try:
            self.performance_monitor.start()
        except:
            pass

    def stop(self):
        """停止性能管理器"""
        logger.info("停止性能管理器")
        try:
            self.performance_monitor.stop()
        except:
            pass

    def __enter__(self):
        """进入上下文管理器"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        self.stop()

    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化数据框"""
        if df is None or df.empty:
            return df

        try:
            result = self.memory_manager.optimize_dataframe(df)
            if result is None:
                logger.warning("optimize_dataframe 返回 None，使用原始数据")
                return df
            return result
        except Exception as e:
            logger.error(f"优化DataFrame失败: {e}")
            return df

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        return {
            'cache': self.cache_manager.get_cache_stats() if hasattr(self.cache_manager, 'get_cache_stats') else {},
            'status': 'running'
        }

