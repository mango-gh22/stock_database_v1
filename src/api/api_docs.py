# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/api\api_docs.py
# File Name: api_docs
# @ Author: mango-gh22
# @ Date：2025/12/21 19:17
"""
Desc: API文档生成器 - 自动生成API文档
"""

import json
import yaml
import inspect
from typing import Dict, List, Optional, Any, Type
from datetime import datetime
import logging
from pathlib import Path

from fastapi import FastAPI, APIRouter
from pydantic import BaseModel, Field
from enum import Enum

from .indicators_api import app as indicators_app
from .async_calculator import AsyncIndicatorCalculator, CalculationStatus

logger = logging.getLogger(__name__)


class APIDocGenerator:
    """API文档生成器"""

    def __init__(self, app: FastAPI,
                 title: str = "股票技术指标计算API",
                 version: str = "1.0.0",
                 output_dir: str = "docs/api"):
        """
        初始化API文档生成器

        Args:
            app: FastAPI应用实例
            title: API标题
            version: API版本
            output_dir: 输出目录
        """
        self.app = app
        self.title = title
        self.version = version
        self.output_dir = Path(output_dir)

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # API端点信息
        self.endpoints: List[Dict[str, Any]] = []

        # 数据模型信息
        self.models: Dict[str, Dict] = {}

    def collect_api_info(self):
        """收集API信息"""
        logger.info("开始收集API信息...")

        # 收集端点信息
        for route in self.app.routes:
            endpoint_info = {
                'path': route.path,
                'methods': list(route.methods) if hasattr(route, 'methods') else [],
                'name': getattr(route, 'name', ''),
                'summary': getattr(route, 'summary', ''),
                'description': getattr(route, 'description', ''),
                'tags': getattr(route, 'tags', []),
                'operation_id': getattr(route, 'operation_id', ''),
            }

            # 提取路径参数
            if hasattr(route, 'path_params'):
                endpoint_info['path_params'] = [
                    {
                        'name': param.name,
                        'type': str(param.annotation),
                        'required': not param.default == inspect.Parameter.empty
                    }
                    for param in route.path_params
                ]

            self.endpoints.append(endpoint_info)

        # 收集数据模型
        self._collect_data_models()

        logger.info(f"收集到 {len(self.endpoints)} 个API端点")
        logger.info(f"收集到 {len(self.models)} 个数据模型")

    def _collect_data_models(self):
        """收集数据模型"""
        # 从indicators_api.py导入的模型
        from .indicators_api import IndicatorRequest, ValidationRequest

        models_to_document = [
            ('IndicatorRequest', IndicatorRequest),
            ('ValidationRequest', ValidationRequest),
            ('CalculationStatus', CalculationStatus),
        ]

        for model_name, model_class in models_to_document:
            model_info = self._extract_model_info(model_name, model_class)
            if model_info:
                self.models[model_name] = model_info

    def _extract_model_info(self, model_name: str, model_class: Type) -> Optional[Dict]:
        """提取模型信息"""
        try:
            if hasattr(model_class, '__annotations__'):
                # Pydantic模型或数据类
                fields = {}

                if hasattr(model_class, 'schema'):
                    # Pydantic模型
                    schema = model_class.schema()
                    fields = schema.get('properties', {})
                else:
                    # 普通数据类
                    annotations = model_class.__annotations__
                    for field_name, field_type in annotations.items():
                        fields[field_name] = {
                            'type': str(field_type),
                            'required': True
                        }

                return {
                    'name': model_name,
                    'type': model_class.__name__,
                    'fields': fields,
                    'description': getattr(model_class, '__doc__', '')
                }

            elif isinstance(model_class, type(Enum)):
                # 枚举类型
                return {
                    'name': model_name,
                    'type': 'enum',
                    'values': {item.name: item.value for item in model_class},
                    'description': model_class.__doc__ or ''
                }

        except Exception as e:
            logger.error(f"提取模型 {model_name} 信息失败: {e}")

        return None

    def generate_openapi_spec(self) -> Dict[str, Any]:
        """生成OpenAPI规范"""
        openapi_spec = {
            'openapi': '3.0.0',
            'info': {
                'title': self.title,
                'version': self.version,
                'description': '股票技术指标计算API - 提供多种技术指标计算服务',
                'contact': {
                    'name': '技术支持',
                    'email': 'support@example.com'
                },
                'license': {
                    'name': 'MIT',
                    'url': 'https://opensource.org/licenses/MIT'
                }
            },
            'servers': [
                {
                    'url': 'http://127.0.0.1:8000',
                    'description': '本地开发服务器'
                },
                {
                    'url': 'https://api.example.com',
                    'description': '生产服务器'
                }
            ],
            'tags': [
                {
                    'name': 'indicators',
                    'description': '技术指标计算相关接口'
                },
                {
                    'name': 'async',
                    'description': '异步计算相关接口'
                },
                {
                    'name': 'system',
                    'description': '系统管理接口'
                }
            ],
            'paths': self._generate_paths(),
            'components': {
                'schemas': self._generate_schemas(),
                'securitySchemes': {
                    'BearerAuth': {
                        'type': 'http',
                        'scheme': 'bearer',
                        'bearerFormat': 'JWT'
                    }
                }
            },
            'externalDocs': {
                'description': '完整文档',
                'url': 'https://github.com/yourusername/stock_database'
            }
        }

        return openapi_spec

    def _generate_paths(self) -> Dict[str, Any]:
        """生成路径定义"""
        paths = {}

        for endpoint in self.endpoints:
            path = endpoint['path']
            if path not in paths:
                paths[path] = {}

            for method in endpoint['methods']:
                method_lower = method.lower()

                # 确定标签
                tags = endpoint.get('tags', [])
                if not tags:
                    if '/indicators/' in path:
                        tags = ['indicators']
                    elif '/async/' in path:
                        tags = ['async']
                    elif path in ['/health', '/docs', '/redoc']:
                        tags = ['system']
                    else:
                        tags = ['general']

                paths[path][method_lower] = {
                    'tags': tags,
                    'summary': endpoint.get('summary', ''),
                    'description': endpoint.get('description', ''),
                    'operationId': endpoint.get('operation_id', ''),
                    'responses': self._generate_responses(path, method_lower),
                    'parameters': self._generate_parameters(endpoint),
                }

        return paths

    def _generate_responses(self, path: str, method: str) -> Dict[str, Any]:
        """生成响应定义"""
        responses = {
            '200': {
                'description': '成功响应',
                'content': {
                    'application/json': {
                        'schema': {
                            'type': 'object',
                            'properties': {
                                'success': {'type': 'boolean'},
                                'data': {'type': 'object'},
                                'message': {'type': 'string'}
                            }
                        }
                    }
                }
            },
            '400': {
                'description': '请求参数错误'
            },
            '404': {
                'description': '资源不存在'
            },
            '500': {
                'description': '服务器内部错误'
            }
        }

        # 特定端点的响应
        if '/indicators/calculate' in path and method == 'post':
            responses['200']['description'] = '指标计算结果'

        elif '/async/task/' in path and method == 'get':
            responses['200']['description'] = '异步任务状态或结果'

        return responses

    def _generate_parameters(self, endpoint: Dict) -> List[Dict]:
        """生成参数定义"""
        parameters = []

        # 路径参数
        if 'path_params' in endpoint:
            for param in endpoint['path_params']:
                parameters.append({
                    'name': param['name'],
                    'in': 'path',
                    'required': param['required'],
                    'schema': {'type': self._map_type(param['type'])},
                    'description': f'路径参数: {param["name"]}'
                })

        # 查询参数（根据路径推断）
        path = endpoint['path']
        if '/indicators/calculate/batch' in path:
            parameters.extend([
                {
                    'name': 'symbols',
                    'in': 'query',
                    'required': True,
                    'schema': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    },
                    'description': '股票代码列表，用逗号分隔'
                },
                {
                    'name': 'indicators',
                    'in': 'query',
                    'required': True,
                    'schema': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    },
                    'description': '指标名称列表，用逗号分隔'
                }
            ])

        return parameters

    def _generate_schemas(self) -> Dict[str, Any]:
        """生成模式定义"""
        schemas = {}

        # 基本响应模式
        schemas['BaseResponse'] = {
            'type': 'object',
            'properties': {
                'success': {'type': 'boolean'},
                'timestamp': {'type': 'string', 'format': 'date-time'},
                'message': {'type': 'string'}
            }
        }

        # 指标请求模式
        schemas['IndicatorRequest'] = {
            'type': 'object',
            'required': ['symbol', 'indicators', 'start_date', 'end_date'],
            'properties': {
                'symbol': {
                    'type': 'string',
                    'example': 'sh600519',
                    'description': '股票代码'
                },
                'indicators': {
                    'type': 'array',
                    'items': {'type': 'string'},
                    'example': ['moving_average', 'rsi', 'macd'],
                    'description': '指标名称列表'
                },
                'start_date': {
                    'type': 'string',
                    'format': 'date',
                    'example': '2024-01-01',
                    'description': '开始日期'
                },
                'end_date': {
                    'type': 'string',
                    'format': 'date',
                    'example': '2024-01-31',
                    'description': '结束日期'
                },
                'use_cache': {
                    'type': 'boolean',
                    'default': True,
                    'description': '是否使用缓存'
                }
            }
        }

        # 异步任务状态模式
        schemas['AsyncTaskStatus'] = {
            'type': 'object',
            'properties': {
                'task_id': {'type': 'string'},
                'status': {
                    'type': 'string',
                    'enum': [status.value for status in CalculationStatus]
                },
                'progress': {'type': 'number', 'minimum': 0, 'maximum': 1},
                'created_at': {'type': 'string', 'format': 'date-time'},
                'started_at': {'type': 'string', 'format': 'date-time'},
                'completed_at': {'type': 'string', 'format': 'date-time'},
                'error': {'type': 'string'}
            }
        }

        # 指标信息模式
        schemas['IndicatorInfo'] = {
            'type': 'object',
            'properties': {
                'name': {'type': 'string'},
                'type': {'type': 'string'},
                'description': {'type': 'string'},
                'parameters': {'type': 'object'},
                'requires_adjusted_price': {'type': 'boolean'},
                'min_data_points': {'type': 'integer'}
            }
        }

        # 添加收集的模型
        for model_name, model_info in self.models.items():
            if model_info['type'] == 'enum':
                schemas[model_name] = {
                    'type': 'string',
                    'enum': list(model_info['values'].values()),
                    'description': model_info['description']
                }
            else:
                schemas[model_name] = {
                    'type': 'object',
                    'properties': model_info['fields'],
                    'description': model_info['description']
                }

        return schemas

    def _map_type(self, python_type: str) -> str:
        """映射Python类型到OpenAPI类型"""
        type_mapping = {
            'str': 'string',
            'int': 'integer',
            'float': 'number',
            'bool': 'boolean',
            'list': 'array',
            'dict': 'object',
            'datetime': 'string',
            'date': 'string',
        }

        for py_type, openapi_type in type_mapping.items():
            if py_type in python_type.lower():
                return openapi_type

        return 'string'

    def generate_markdown_docs(self) -> str:
        """生成Markdown格式文档"""
        lines = []

        # 标题
        lines.append(f"# {self.title}")
        lines.append(f"\n版本: {self.version}")
        lines.append(f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("\n---\n")

        # 概述
        lines.append("## 概述")
        lines.append("\n本API提供股票技术指标计算服务，支持多种技术指标和异步计算。")

        # 快速开始
        lines.append("\n## 快速开始")
        lines.append("\n### 1. 启动服务器")
        lines.append("```bash")
        lines.append("python scripts/start_api_server.py")
        lines.append("```")

        lines.append("\n### 2. 获取可用指标")
        lines.append("```bash")
        lines.append("curl http://127.0.0.1:8000/indicators/available")
        lines.append("```")

        lines.append("\n### 3. 计算指标")
        lines.append("```bash")
        lines.append("curl -X POST http://127.0.0.1:8000/indicators/calculate \\")
        lines.append("  -H 'Content-Type: application/json' \\")
        lines.append("  -d '{")
        lines.append('    "symbol": "sh600519",')
        lines.append('    "indicators": ["moving_average", "rsi"],')
        lines.append('    "start_date": "2024-01-01",')
        lines.append('    "end_date": "2024-01-31"')
        lines.append("  }'")
        lines.append("```")

        # API端点
        lines.append("\n## API端点")

        # 按标签分组
        endpoints_by_tag = {}
        for endpoint in self.endpoints:
            tags = endpoint.get('tags', ['general'])
            for tag in tags:
                if tag not in endpoints_by_tag:
                    endpoints_by_tag[tag] = []
                endpoints_by_tag[tag].append(endpoint)

        for tag, tag_endpoints in endpoints_by_tag.items():
            lines.append(f"\n### {tag.title()} 接口")

            for endpoint in tag_endpoints:
                lines.append(f"\n#### {endpoint['path']}")

                if endpoint.get('description'):
                    lines.append(f"\n{endpoint['description']}")

                lines.append(f"\n**方法**: {', '.join(endpoint['methods'])}")

                if endpoint.get('summary'):
                    lines.append(f"\n**概要**: {endpoint['summary']}")

                # 请求示例
                lines.append("\n**请求示例**:")
                lines.append("```json")

                example = self._generate_example_request(endpoint['path'])
                lines.append(json.dumps(example, indent=2))

                lines.append("```")

                # 响应示例
                lines.append("\n**响应示例**:")
                lines.append("```json")

                example_response = self._generate_example_response(endpoint['path'])
                lines.append(json.dumps(example_response, indent=2))

                lines.append("```")

        # 数据模型
        lines.append("\n## 数据模型")

        for model_name, model_info in self.models.items():
            lines.append(f"\n### {model_name}")

            if model_info.get('description'):
                lines.append(f"\n{model_info['description']}")

            if model_info['type'] == 'enum':
                lines.append("\n**枚举值**:")
                for name, value in model_info['values'].items():
                    lines.append(f"- `{name}`: `{value}`")
            else:
                lines.append("\n**字段**:")
                lines.append("| 字段名 | 类型 | 必填 | 描述 |")
                lines.append("|--------|------|------|------|")

                for field_name, field_info in model_info['fields'].items():
                    field_type = field_info.get('type', 'unknown')
                    required = field_info.get('required', False)
                    description = field_info.get('description', '')

                    lines.append(f"| {field_name} | `{field_type}` | {'是' if required else '否'} | {description} |")

        # 错误处理
        lines.append("\n## 错误处理")
        lines.append("\n### 常见错误码")
        lines.append("| 状态码 | 描述 |")
        lines.append("|--------|------|")
        lines.append("| 200 | 成功 |")
        lines.append("| 400 | 请求参数错误 |")
        lines.append("| 404 | 资源不存在 |")
        lines.append("| 500 | 服务器内部错误 |")

        lines.append("\n### 错误响应格式")
        lines.append("```json")
        lines.append('{')
        lines.append('  "success": false,')
        lines.append('  "error": {')
        lines.append('    "type": "ValidationError",')
        lines.append('    "message": "Invalid parameter",')
        lines.append('    "details": {')
        lines.append('      "field": "start_date",')
        lines.append('      "issue": "Must be a valid date"')
        lines.append('    }')
        lines.append('  }')
        lines.append('}')
        lines.append("```")

        # 使用指南
        lines.append("\n## 使用指南")

        lines.append("\n### 1. 指标计算流程")
        lines.append("```mermaid")
        lines.append("graph TD")
        lines.append("    A[发起指标计算请求] --> B{是否异步?}")
        lines.append("    B -->|是| C[创建异步任务]")
        lines.append("    B -->|否| D[同步计算]")
        lines.append("    C --> E[获取任务ID]")
        lines.append("    E --> F[轮询任务状态]")
        lines.append("    F --> G{任务完成?}")
        lines.append("    G -->|是| H[获取结果]")
        lines.append("    G -->|否| F")
        lines.append("    D --> I[直接返回结果]")
        lines.append("```")

        lines.append("\n### 2. 参数配置建议")
        lines.append("- **移动平均线**: 常用周期 [5, 10, 20, 30, 60]")
        lines.append("- **RSI**: 默认周期14，超买线70，超卖线30")
        lines.append("- **MACD**: 默认参数 (12, 26, 9)")
        lines.append("- **数据要求**: 至少需要20个交易日数据")

        lines.append("\n### 3. 性能优化建议")
        lines.append("- 启用缓存减少重复计算")
        lines.append("- 使用异步计算处理大批量数据")
        lines.append("- 合理设置查询日期范围")
        lines.append("- 批量查询减少API调用次数")

        # 附录
        lines.append("\n## 附录")

        lines.append("\n### A. 可用指标列表")
        lines.append("| 指标名称 | 类型 | 描述 |")
        lines.append("|----------|------|------|")
        lines.append("| moving_average | 趋势 | 移动平均线 |")
        lines.append("| macd | 趋势 | 指数平滑异同移动平均线 |")
        lines.append("| parabolic_sar | 趋势 | 抛物线指标 |")
        lines.append("| ichimoku_cloud | 趋势 | 一目均衡表 |")
        lines.append("| rsi | 动量 | 相对强弱指数 |")
        lines.append("| stochastic | 动量 | 随机指标 |")
        lines.append("| cci | 动量 | 商品通道指数 |")
        lines.append("| williams_r | 动量 | 威廉指标 |")
        lines.append("| bollinger_bands | 波动率 | 布林带 |")
        lines.append("| obv | 成交量 | 能量潮指标 |")

        lines.append("\n### B. 更新日志")
        lines.append("| 版本 | 日期 | 更新内容 |")
        lines.append("|------|------|----------|")
        lines.append(f"| {self.version} | {datetime.now().strftime('%Y-%m-%d')} | 初始版本发布 |")

        return '\n'.join(lines)

    def _generate_example_request(self, path: str) -> Dict[str, Any]:
        """生成示例请求"""
        if '/indicators/calculate' in path:
            return {
                "symbol": "sh600519",
                "indicators": ["moving_average", "rsi"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "use_cache": True
            }

        elif '/async/calculate' in path:
            return {
                "symbol": "sh600519",
                "indicators": ["macd", "bollinger_bands"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "parameters": {
                    "macd": {"fast_period": 12, "slow_period": 26},
                    "bollinger_bands": {"period": 20, "std_dev": 2}
                }
            }

        return {}

    def _generate_example_response(self, path: str) -> Dict[str, Any]:
        """生成示例响应"""
        if '/indicators/available' in path:
            return {
                "success": True,
                "count": 10,
                "indicators": {
                    "moving_average": {
                        "type": "trend",
                        "description": "移动平均线",
                        "parameters": {"periods": [5, 10, 20, 30, 60], "ma_type": "sma"}
                    }
                }
            }

        elif '/indicators/calculate' in path:
            return {
                "success": True,
                "symbol": "sh600519",
                "indicators": ["moving_average", "rsi"],
                "data_count": 21,
                "columns": ["trade_date", "close_price", "MA_5", "MA_10", "RSI"],
                "data": [
                    {
                        "trade_date": "2024-01-02",
                        "close_price": 100.50,
                        "MA_5": 101.20,
                        "MA_10": 102.30,
                        "RSI": 45.60
                    }
                ]
            }

        elif '/async/task/' in path:
            return {
                "success": True,
                "task_id": "abc123def456",
                "status": "completed",
                "progress": 1.0,
                "result_available": True,
                "created_at": "2024-01-01T10:00:00",
                "completed_at": "2024-01-01T10:00:05"
            }

        return {
            "success": True,
            "message": "操作成功",
            "timestamp": datetime.now().isoformat()
        }

    def save_docs(self):
        """保存文档"""
        # 生成OpenAPI规范
        openapi_spec = self.generate_openapi_spec()

        # 保存JSON格式
        json_path = self.output_dir / "openapi.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(openapi_spec, f, indent=2, ensure_ascii=False)

        logger.info(f"保存OpenAPI JSON: {json_path}")

        # 保存YAML格式
        yaml_path = self.output_dir / "openapi.yaml"
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(openapi_spec, f, default_flow_style=False, allow_unicode=True)

        logger.info(f"保存OpenAPI YAML: {yaml_path}")

        # 保存Markdown文档
        markdown_path = self.output_dir / "API_DOCUMENTATION.md"
        markdown_content = self.generate_markdown_docs()

        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        logger.info(f"保存Markdown文档: {markdown_path}")

        # 保存HTML文档（使用redoc）
        html_path = self.output_dir / "redoc.html"
        html_content = self._generate_redoc_html(openapi_spec)

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"保存HTML文档: {html_path}")

        return {
            'json': str(json_path),
            'yaml': str(yaml_path),
            'markdown': str(markdown_path),
            'html': str(html_path)
        }

    def _generate_redoc_html(self, openapi_spec: Dict) -> str:
        """生成ReDoc HTML"""
        spec_json = json.dumps(openapi_spec)

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{self.title} - API文档</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
    <style>
        body {{
            margin: 0;
            padding: 0;
        }}
    </style>
</head>
<body>
    <div id="redoc-container"></div>
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
    <script>
        const spec = {spec_json};

        Redoc.init(spec, {{
            scrollYOffset: 50,
            theme: {{
                colors: {{
                    primary: {{ main: '#1890ff' }}
                }},
                typography: {{
                    fontSize: '16px',
                    fontFamily: 'Roboto, sans-serif',
                    headings: {{
                        fontFamily: 'Montserrat, sans-serif'
                    }}
                }}
            }},
            hideDownloadButton: false,
            expandResponses: "200,201",
            requiredPropsFirst: true,
            sortPropsAlphabetically: false
        }}, document.getElementById('redoc-container'));
    </script>
</body>
</html>
"""
        return html

    def generate_client_code(self, language: str = 'python') -> str:
        """生成客户端代码"""
        if language == 'python':
            return self._generate_python_client()
        elif language == 'javascript':
            return self._generate_javascript_client()
        else:
            raise ValueError(f"不支持的语言: {language}")

    def _generate_python_client(self) -> str:
        """生成Python客户端代码"""
        code = '''"""
股票技术指标API客户端 - 自动生成
"""

import requests
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import time


class StockIndicatorClient:
    """股票指标API客户端"""

    def __init__(self, base_url: str = "http://127.0.0.1:8000", 
                 timeout: int = 30):
        """
        初始化客户端

        Args:
            base_url: API基础URL
            timeout: 请求超时时间
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

        # 设置默认请求头
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def _make_request(self, method: str, endpoint: str, 
                     data: Optional[Dict] = None,
                     params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        发送请求

        Args:
            method: HTTP方法
            endpoint: API端点
            data: 请求体数据
            params: 查询参数

        Returns:
            响应数据
        """
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"API请求失败: {e}")

    def get_available_indicators(self) -> Dict[str, Any]:
        """
        获取可用指标

        Returns:
            可用指标信息
        """
        return self._make_request('GET', '/indicators/available')

    def calculate_indicators(self, symbol: str, 
                            indicators: List[str],
                            start_date: str,
                            end_date: str,
                            use_cache: bool = True) -> Dict[str, Any]:
        """
        计算技术指标

        Args:
            symbol: 股票代码
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存

        Returns:
            计算结果
        """
        data = {
            "symbol": symbol,
            "indicators": indicators,
            "start_date": start_date,
            "end_date": end_date,
            "use_cache": use_cache
        }

        return self._make_request('POST', '/indicators/calculate', data=data)

    def calculate_indicators_async(self, symbol: str,
                                  indicators: List[str],
                                  start_date: str,
                                  end_date: str,
                                  parameters: Optional[Dict[str, Dict]] = None) -> str:
        """
        异步计算指标

        Args:
            symbol: 股票代码
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期
            parameters: 指标参数

        Returns:
            任务ID
        """
        data = {
            "symbol": symbol,
            "indicators": indicators,
            "start_date": start_date,
            "end_date": end_date
        }

        if parameters:
            data["parameters"] = parameters

        response = self._make_request('POST', '/async/calculate', data=data)
        return response.get('task_id')

    def get_async_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        获取异步任务状态

        Args:
            task_id: 任务ID

        Returns:
            任务状态
        """
        return self._make_request('GET', f'/async/task/{task_id}')

    def get_async_task_result(self, task_id: str, 
                             wait: bool = False,
                             timeout: int = 60,
                             poll_interval: float = 1.0) -> Dict[str, Any]:
        """
        获取异步任务结果

        Args:
            task_id: 任务ID
            wait: 是否等待任务完成
            timeout: 等待超时时间
            poll_interval: 轮询间隔

        Returns:
            任务结果
        """
        if not wait:
            return self._make_request('GET', f'/async/task/{task_id}/result')

        # 等待任务完成
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_async_task_status(task_id)

            if status.get('status') == 'completed':
                return self._make_request('GET', f'/async/task/{task_id}/result')
            elif status.get('status') == 'failed':
                raise Exception(f"任务失败: {status.get('error', '未知错误')}")

            time.sleep(poll_interval)

        raise TimeoutError(f"等待任务超时: {task_id}")

    def batch_calculate(self, symbols: List[str],
                       indicators: List[str],
                       start_date: str,
                       end_date: str) -> Dict[str, Any]:
        """
        批量计算指标

        Args:
            symbols: 股票代码列表
            indicators: 指标列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            批量计算结果
        """
        params = {
            "symbols": ",".join(symbols),
            "indicators": ",".join(indicators),
            "start_date": start_date,
            "end_date": end_date
        }

        return self._make_request('GET', '/indicators/calculate/batch', params=params)

    def validate_calculation(self, symbol: str,
                            indicator: str,
                            start_date: str,
                            end_date: str) -> Dict[str, Any]:
        """
        验证指标计算可行性

        Args:
            symbol: 股票代码
            indicator: 指标名称
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            验证结果
        """
        data = {
            "symbol": symbol,
            "indicator": indicator,
            "start_date": start_date,
            "end_date": end_date
        }

        return self._make_request('POST', '/indicators/validate', data=data)

    def health_check(self) -> bool:
        """
        健康检查

        Returns:
            服务是否健康
        """
        try:
            response = self._make_request('GET', '/health')
            return response.get('status') == 'healthy'
        except:
            return False


# 使用示例
if __name__ == "__main__":
    # 创建客户端
    client = StockIndicatorClient(base_url="http://127.0.0.1:8000")

    # 健康检查
    if client.health_check():
        print("✅ API服务正常")
    else:
        print("❌ API服务异常")
        exit(1)

    # 获取可用指标
    indicators = client.get_available_indicators()
    print(f"可用指标数量: {indicators.get('count', 0)}")

    # 计算指标（同步）
    result = client.calculate_indicators(
        symbol="sh600519",
        indicators=["moving_average", "rsi"],
        start_date="2024-01-01",
        end_date="2024-01-31"
    )

    if result.get('success'):
        print(f"✅ 计算成功，数据量: {result.get('data_count', 0)}")
    else:
        print(f"❌ 计算失败: {result.get('error', '未知错误')}")
'''
        return code

    def _generate_javascript_client(self) -> str:
        """生成JavaScript客户端代码"""
        code = '''/**
 * 股票技术指标API客户端 - 自动生成
 */

class StockIndicatorClient {
    /**
     * 初始化客户端
     * @param {string} baseUrl - API基础URL
     * @param {number} timeout - 请求超时时间（毫秒）
     */
    constructor(baseUrl = 'http://127.0.0.1:8000', timeout = 30000) {
        this.baseUrl = baseUrl.replace(/\\/$/, '');
        this.timeout = timeout;
    }

    /**
     * 发送请求
     * @private
     */
    async _makeRequest(method, endpoint, data = null, params = null) {
        const url = new URL(`${this.baseUrl}${endpoint}`);

        if (params) {
            Object.keys(params).forEach(key => {
                url.searchParams.append(key, params[key]);
            });
        }

        const options = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout: this.timeout
        };

        if (data && (method === 'POST' || method === 'PUT')) {
            options.body = JSON.stringify(data);
        }

        try {
            const response = await fetch(url, options);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();

        } catch (error) {
            throw new Error(`API请求失败: ${error.message}`);
        }
    }

    /**
     * 获取可用指标
     * @returns {Promise<Object>}
     */
    async getAvailableIndicators() {
        return await this._makeRequest('GET', '/indicators/available');
    }

    /**
     * 计算技术指标
     * @param {string} symbol - 股票代码
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @param {boolean} useCache - 是否使用缓存
     * @returns {Promise<Object>}
     */
    async calculateIndicators(symbol, indicators, startDate, endDate, useCache = true) {
        const data = {
            symbol: symbol,
            indicators: indicators,
            start_date: startDate,
            end_date: endDate,
            use_cache: useCache
        };

        return await this._makeRequest('POST', '/indicators/calculate', data);
    }

    /**
     * 异步计算指标
     * @param {string} symbol - 股票代码
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @param {Object} parameters - 指标参数
     * @returns {Promise<string>} 任务ID
     */
    async calculateIndicatorsAsync(symbol, indicators, startDate, endDate, parameters = null) {
        const data = {
            symbol: symbol,
            indicators: indicators,
            start_date: startDate,
            end_date: endDate
        };

        if (parameters) {
            data.parameters = parameters;
        }

        const response = await this._makeRequest('POST', '/async/calculate', data);
        return response.task_id;
    }

    /**
     * 获取异步任务状态
     * @param {string} taskId - 任务ID
     * @returns {Promise<Object>}
     */
    async getAsyncTaskStatus(taskId) {
        return await this._makeRequest('GET', `/async/task/${taskId}`);
    }

    /**
     * 获取异步任务结果
     * @param {string} taskId - 任务ID
     * @param {boolean} wait - 是否等待任务完成
     * @param {number} timeout - 等待超时时间（秒）
     * @param {number} pollInterval - 轮询间隔（秒）
     * @returns {Promise<Object>}
     */
    async getAsyncTaskResult(taskId, wait = false, timeout = 60, pollInterval = 1) {
        if (!wait) {
            return await this._makeRequest('GET', `/async/task/${taskId}/result`);
        }

        const startTime = Date.now();
        const timeoutMs = timeout * 1000;
        const pollIntervalMs = pollInterval * 1000;

        while (Date.now() - startTime < timeoutMs) {
            const status = await this.getAsyncTaskStatus(taskId);

            if (status.status === 'completed') {
                return await this._makeRequest('GET', `/async/task/${taskId}/result`);
            } else if (status.status === 'failed') {
                throw new Error(`任务失败: ${status.error || '未知错误'}`);
            }

            await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
        }

        throw new Error(`等待任务超时: ${taskId}`);
    }

    /**
     * 批量计算指标
     * @param {string[]} symbols - 股票代码列表
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @returns {Promise<Object>}
     */
    async batchCalculate(symbols, indicators, startDate, endDate) {
        const params = {
            symbols: symbols.join(','),
            indicators: indicators.join(','),
            start_date: startDate,
            end_date: endDate
        };

        return await this._makeRequest('GET', '/indicators/calculate/batch', null, params);
    }

    /**
     * 验证指标计算可行性
     * @param {string} symbol - 股票代码
     * @param {string} indicator - 指标名称
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @returns {Promise<Object>}
     */
    async validateCalculation(symbol, indicator, startDate, endDate) {
        const data = {
            symbol: symbol,
            indicator: indicator,
            start_date: startDate,
            end_date: endDate
        };

        return await this._makeRequest('POST', '/indicators/validate', data);
    }

    /**
     * 健康检查
     * @returns {Promise<boolean>}
     */
    async healthCheck() {
        try {
            const response = await this._makeRequest('GET', '/health');
            return response.status === 'healthy';
        } catch {
            return false;
        }
    }
}

// 使用示例
async function example() {
    // 创建客户端
    const client = new StockIndicatorClient('http://127.0.0.1:8000');

    try {
        // 健康检查
        const healthy = await client.healthCheck();
        console.log(healthy ? '✅ API服务正常' : '❌ API服务异常');

        if (!healthy) return;

        // 获取可用指标
        const indicators = await client.getAvailableIndicators();
        console.log(`可用指标数量: ${indicators.count || 0}`);

        // 计算指标
        const result = await client.calculateIndicators(
            'sh600519',
            ['moving_average', 'rsi'],
            '2024-01-01',
            '2024-01-31'
        );

        if (result.success) {
            console.log(`✅ 计算成功，数据量: ${result.data_count || 0}`);
        } else {
            console.log(`❌ 计算失败: ${result.error || '未知错误'}`);
        }

    } catch (error) {
        console.error('示例执行失败:', error.message);
    }
}

// 导出客户端
if (typeof module !== 'undefined' && module.exports) {
    module.exports = StockIndicatorClient;
} else if (typeof window !== 'undefined') {
    window.StockIndicatorClient = StockIndicatorClient;
}
'''
        return code


def generate_all_docs():
    """生成所有文档"""
    # 创建API文档生成器
    generator = APIDocGenerator(
        app=indicators_app,
        title="股票技术指标计算API",
        version="1.0.0",
        output_dir="docs/api"
    )

    # 收集API信息
    generator.collect_api_info()

    # 保存文档
    saved_files = generator.save_docs()

    # 生成客户端代码
    python_client = generator.generate_client_code('python')
    javascript_client = generator.generate_client_code('javascript')

    # 保存客户端代码
    client_dir = Path("docs/api/clients")
    client_dir.mkdir(parents=True, exist_ok=True)

    python_client_path = client_dir / "stock_indicator_client.py"
    with open(python_client_path, 'w', encoding='utf-8') as f:
        f.write(python_client)

    javascript_client_path = client_dir / "stock_indicator_client.js"
    with open(javascript_client_path, 'w', encoding='utf-8') as f:
        f.write(javascript_client)

    saved_files['python_client'] = str(python_client_path)
    saved_files['javascript_client'] = str(javascript_client_path)

    logger.info(f"文档生成完成，保存文件: {list(saved_files.keys())}")

    return saved_files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    try:
        files = generate_all_docs()
        print("✅ API文档生成成功!")
        for file_type, file_path in files.items():
            print(f"  {file_type}: {file_path}")
    except Exception as e:
        print(f"❌ API文档生成失败: {e}")
