# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/config\pipeline_config_loader.py
# File Name: pipeline_config_loader
# @ Author: mango-gh22
# @ Date：2025/12/14 8:35
"""
desc 
"""

# src/config/pipeline_config_loader.py
"""
管道配置加载器 - 专门处理三种更新模式配置
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PipelineConfigLoader:
    """管道配置加载器"""

    DEFAULT_CONFIG = {
        'pipeline_modes': {
            'incremental': {
                'enabled': True,
                'default_days_back': 30,
                'max_concurrent': 3,
                'quality_threshold': 50,
                'symbols_source': 'database',
                'retry_times': 3,
                'retry_delay': 1
            },
            'batch_init': {
                'enabled': True,
                'default_start_date': '20200101',
                'max_concurrent': 5,
                'batch_size': 50,
                'force_update': False,
                'default_groups': ['csi_a50']
            },
            'specific': {
                'enabled': True,
                'max_concurrent': 2,
                'default_adjust': 'qfq',
                'skip_existing': True,
                'priority_levels': {
                    'high': 1,
                    'medium': 3,
                    'low': 5
                }
            }
        },
        'performance': {
            'max_workers': 5,
            'request_delay': 0.5,
            'timeout': 30,
            'batch_timeout': 300
        },
        'monitoring': {
            'enable_progress': True,
            'enable_reports': True,
            'report_dir': 'data/reports',
            'log_level': 'INFO'
        }
    }

    @classmethod
    def load_config(cls, config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        加载管道配置

        Args:
            config_path: 配置文件路径，默认使用 config/pipeline_config.yaml

        Returns:
            配置字典
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / 'config' / 'pipeline_config.yaml'

        config_path = Path(config_path)
        config = cls.DEFAULT_CONFIG.copy()

        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    user_config = yaml.safe_load(f)

                # 深度合并配置
                config = cls._deep_merge(config, user_config)
                logger.info(f"管道配置已加载: {config_path}")

            except Exception as e:
                logger.error(f"加载管道配置失败: {e}")
                logger.info("使用默认配置")
        else:
            logger.warning(f"管道配置文件不存在: {config_path}")
            logger.info("使用默认配置，将在{config_path}创建示例配置")
            cls._create_sample_config(config_path)

        return config

    @staticmethod
    def _deep_merge(base: Dict, update: Dict) -> Dict:
        """深度合并字典"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                base[key] = PipelineConfigLoader._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    @staticmethod
    def _create_sample_config(config_path: Path):
        """创建示例配置文件"""
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)

            sample_config = {
                'pipeline_modes': {
                    'incremental': {
                        'enabled': True,
                        'schedule': '0 16 * * *',  # 每日16:00执行
                        'days_back': 30,
                        'max_concurrent': 3,
                        'quality_threshold': 50,
                        'retry_times': 3
                    },
                    'batch_init': {
                        'enabled': True,
                        'schedule': '0 2 * * 6',  # 每周六凌晨2点
                        'max_concurrent': 5,
                        'batch_size': 50,
                        'symbol_groups': ['csi_a50', 'csi_300']
                    },
                    'specific': {
                        'enabled': True,
                        'max_concurrent': 2,
                        'priority_levels': {
                            'high': 1,
                            'medium': 3,
                            'low': 5
                        }
                    }
                },
                'performance': {
                    'max_workers': 5,
                    'request_delay': 0.5,
                    'timeout': 30
                },
                'monitoring': {
                    'enable_progress': True,
                    'enable_reports': True,
                    'report_dir': 'data/reports',
                    'log_level': 'INFO'
                },
                'symbol_groups': {
                    'csi_a50': {
                        'name': '中证A50指数',
                        'description': '核心蓝筹股',
                        'symbols_source': 'config/symbols.yaml',
                        'update_frequency': 'daily',
                        'priority': 'high'
                    },
                    'csi_300': {
                        'name': '沪深300指数',
                        'description': '全市场代表性股票',
                        'symbols_source': 'database',
                        'criteria': "list_status='L' AND market_cap>100e8",
                        'update_frequency': 'daily',
                        'priority': 'medium'
                    }
                }
            }

            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(sample_config, f, default_flow_style=False, allow_unicode=True, indent=2)

            logger.info(f"示例配置文件已创建: {config_path}")

        except Exception as e:
            logger.error(f"创建示例配置失败: {e}")


# 单例实例
_pipeline_config = None


def get_pipeline_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """获取管道配置（单例模式）"""
    global _pipeline_config
    if _pipeline_config is None:
        _pipeline_config = PipelineConfigLoader.load_config(config_path)
    return _pipeline_config