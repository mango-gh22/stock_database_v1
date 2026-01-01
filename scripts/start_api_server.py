# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\start_api_server.py
# File Name: start_api_server
# @ Author: mango-gh22
# @ Date：2025/12/21 19:43
"""
desc 
"""

"""
File: scripts/start_api_server.py
Desc: API服务器启动脚本
"""
import sys
import os
import argparse
import logging
from pathlib import Path
import uvicorn
import webbrowser
from datetime import datetime
import signal
import asyncio

import importlib.util

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def setup_logging(log_level: str = "INFO", log_file: str = None):
    """设置日志配置"""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 基础配置
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[]
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)

    # 文件处理器（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)

    # 设置特定模块的日志级别
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"日志级别设置为: {log_level}")

    return logger

def check_dependencies():
    """检查依赖"""
    required_packages = [
        'fastapi',
        'uvicorn',
        'pydantic',
        'pandas',
        'numpy',
        'yaml',      # 注意：实际模块名是 'PyYAML'，但导入时用 'yaml'
        'requests'
    ]

    missing_packages = []

    for package in required_packages:
        # 特殊处理 yaml（因为 PyYAML 的包名和模块名不同）
        if package == 'yaml':
            module_name = 'yaml'
        else:
            module_name = package

        if importlib.util.find_spec(module_name) is None:
            missing_packages.append(package)

    return missing_packages

def generate_api_docs():
    """生成API文档"""
    try:
        from src.api.api_docs import generate_all_docs

        logger = logging.getLogger(__name__)
        logger.info("开始生成API文档...")

        files = generate_all_docs()

        logger.info("API文档生成完成:")
        for file_type, file_path in files.items():
            logger.info(f"  {file_type}: {file_path}")

        return files

    except Exception as e:
        logger.error(f"生成API文档失败: {e}")
        return None


def create_sample_config():
    """创建示例配置文件"""
    import yaml

    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)

    # API配置
    api_config = {
        'api': {
            'title': '股票技术指标计算API',
            'version': '1.0.0',
            'description': '提供股票技术指标计算服务',
            'docs_url': '/docs',
            'redoc_url': '/redoc',
            'openapi_url': '/openapi.json'
        },
        'server': {
            'host': '127.0.0.1',
            'port': 8000,
            'reload': True,
            'workers': 1,
            'log_level': 'info'
        },
        'security': {
            'enabled': False,
            'api_key': None,
            'jwt_secret': None
        },
        'limits': {
            'max_request_size': '10MB',
            'rate_limit': '100/minute',
            'timeout': 300
        },
        'cors': {
            'enabled': True,
            'allow_origins': ['*'],
            'allow_methods': ['*'],
            'allow_headers': ['*']
        }
    }

    api_config_path = config_dir / "api_config.yaml"
    with open(api_config_path, 'w', encoding='utf-8') as f:
        yaml.dump(api_config, f, default_flow_style=False)

    # 性能配置
    performance_config = {
        'indicators': {
            'cache': {
                'enabled': True,
                'ttl': 3600,
                'max_size': 1000
            },
            'parallel': {
                'enabled': True,
                'max_workers': 4,
                'timeout': 300
            }
        },
        'query': {
            'batch_size': 100,
            'prefetch': True,
            'compression': True
        },
        'monitoring': {
            'enabled': True,
            'metrics_port': 9090,
            'collect_interval': 60
        }
    }

    performance_config_path = config_dir / "performance.yaml"
    with open(performance_config_path, 'w', encoding='utf-8') as f:
        yaml.dump(performance_config, f, default_flow_style=False)

    return api_config_path, performance_config_path


class APIServer:
    """API服务器管理类"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.server = None
        self.logger = logging.getLogger(__name__)

        # 加载配置
        self.config = self._load_config()

    def _load_config(self):
        """加载配置"""
        default_config = {
            'host': '127.0.0.1',
            'port': 8000,
            'reload': False,
            'workers': 1,
            'log_level': 'info',
            'access_log': True,
            'docs': True
        }

        if self.config_path and Path(self.config_path).exists():
            import yaml
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    user_config = yaml.safe_load(f)

                # 合并配置
                if 'server' in user_config:
                    default_config.update(user_config['server'])

                self.logger.info(f"从 {self.config_path} 加载配置")

            except Exception as e:
                self.logger.warning(f"加载配置文件失败: {e}, 使用默认配置")

        return default_config

    async def start(self):
        """启动服务器"""
        try:
            self.logger.info("启动API服务器...")

            # 导入FastAPI应用
            from src.api.indicators_api import app

            # 配置应用
            app.title = self.config.get('title', '股票技术指标计算API')
            app.version = self.config.get('version', '1.0.0')

            # 添加启动事件
            @app.on_event("startup")
            async def startup_event():
                self.logger.info("🚀 API服务器启动完成")
                self.logger.info(f"📊 文档地址: http://{self.config['host']}:{self.config['port']}/docs")
                self.logger.info(f"📈 ReDoc地址: http://{self.config['host']}:{self.config['port']}/redoc")
                self.logger.info(f"🔧 OpenAPI地址: http://{self.config['host']}:{self.config['port']}/openapi.json")
                self.logger.info(f"🏥 健康检查: http://{self.config['host']}:{self.config['port']}/health")

            @app.on_event("shutdown")
            async def shutdown_event():
                self.logger.info("🛑 API服务器关闭")

            # 配置UVicorn
            config = uvicorn.Config(
                app,
                host=self.config['host'],
                port=self.config['port'],
                reload=self.config['reload'],
                workers=self.config['workers'],
                log_level=self.config['log_level'],
                access_log=self.config['access_log']
            )

            self.server = uvicorn.Server(config)

            # 启动服务器
            await self.server.serve()

        except Exception as e:
            self.logger.error(f"启动服务器失败: {e}")
            raise

    async def stop(self):
        """停止服务器"""
        if self.server:
            self.logger.info("正在停止服务器...")
            self.server.should_exit = True


def signal_handler(signum, frame):
    """信号处理器"""
    logger = logging.getLogger(__name__)
    logger.info(f"接收到信号 {signum}, 正在关闭服务器...")
    sys.exit(0)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='股票技术指标计算API服务器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                        # 使用默认配置启动
  %(prog)s --host 0.0.0.0 --port 8080  # 指定主机和端口
  %(prog)s --config config/api_config.yaml  # 使用配置文件
  %(prog)s --docs-only            # 只生成文档
  %(prog)s --log-file logs/api.log  # 指定日志文件
"""
    )

    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='服务器主机地址 (默认: 127.0.0.1)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='服务器端口 (默认: 8000)'
    )

    parser.add_argument(
        '--reload',
        action='store_true',
        help='启用热重载（开发模式）'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='工作进程数 (默认: 1)'
    )

    parser.add_argument(
        '--log-level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info',
        help='日志级别 (默认: info)'
    )

    parser.add_argument(
        '--log-file',
        help='日志文件路径'
    )

    parser.add_argument(
        '--config',
        help='配置文件路径'
    )

    parser.add_argument(
        '--docs-only',
        action='store_true',
        help='只生成API文档，不启动服务器'
    )

    parser.add_argument(
        '--open-browser',
        action='store_true',
        help='启动后自动打开浏览器'
    )

    parser.add_argument(
        '--generate-config',
        action='store_true',
        help='生成示例配置文件'
    )

    parser.add_argument(
        '--check-deps',
        action='store_true',
        help='检查依赖包'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='股票技术指标API服务器 v1.0.0'
    )

    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()

    # 设置日志
    logger = setup_logging(args.log_level, args.log_file)

    logger.info("=" * 60)
    logger.info("股票技术指标计算API服务器")
    logger.info("=" * 60)

    # 检查依赖
    if args.check_deps:
        missing = check_dependencies()
        if missing:
            logger.error(f"缺少依赖包: {', '.join(missing)}")
            logger.error("请运行: pip install -r requirements.txt")
            return 1
        else:
            logger.info("✅ 所有依赖包已安装")
        return 0

    # 生成配置文件
    if args.generate_config:
        try:
            api_config, perf_config = create_sample_config()
            logger.info(f"✅ 示例配置文件已生成:")
            logger.info(f"  API配置: {api_config}")
            logger.info(f"  性能配置: {perf_config}")
        except Exception as e:
            logger.error(f"生成配置文件失败: {e}")
            return 1
        return 0

    # 只生成文档
    if args.docs_only:
        try:
            files = generate_api_docs()
            if files:
                logger.info("✅ API文档生成成功")
                return 0
            else:
                logger.error("❌ API文档生成失败")
                return 1
        except Exception as e:
            logger.error(f"生成API文档失败: {e}")
            return 1

    # 检查依赖
    missing_packages = check_dependencies()
    if missing_packages:
        logger.error(f"缺少依赖包: {', '.join(missing_packages)}")
        logger.error("请运行: pip install -r requirements.txt")
        return 1

    # 生成API文档
    try:
        logger.info("生成API文档...")
        generate_api_docs()
    except Exception as e:
        logger.warning(f"生成API文档失败: {e}")

    # 设置信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 创建并启动服务器
    try:
        # 创建配置字典
        config = {
            'host': args.host,
            'port': args.port,
            'reload': args.reload,
            'workers': args.workers,
            'log_level': args.log_level,
            'access_log': True,
            'docs': True
        }

        # 如果有配置文件，使用配置文件
        if args.config:
            config['config_path'] = args.config

        # 创建服务器
        server = APIServer(args.config)

        # 更新配置
        server.config.update(config)

        # 打印启动信息
        logger.info("服务器配置:")
        logger.info(f"  主机: {server.config['host']}")
        logger.info(f"  端口: {server.config['port']}")
        logger.info(f"  热重载: {'启用' if server.config['reload'] else '禁用'}")
        logger.info(f"  工作进程: {server.config['workers']}")
        logger.info(f"  日志级别: {server.config['log_level']}")

        if args.config:
            logger.info(f"  配置文件: {args.config}")

        logger.info("-" * 40)
        logger.info("正在启动服务器...")

        # 启动事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # 自动打开浏览器
        if args.open_browser:
            def open_browser():
                import webbrowser
                import time
                time.sleep(2)  # 等待服务器启动
                url = f"http://{server.config['host']}:{server.config['port']}/docs"
                webbrowser.open(url)
                logger.info(f"已打开浏览器: {url}")

            import threading
            browser_thread = threading.Thread(target=open_browser, daemon=True)
            browser_thread.start()

        # 启动服务器
        try:
            loop.run_until_complete(server.start())
        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在关闭服务器...")
        finally:
            loop.run_until_complete(server.stop())
            loop.close()

        logger.info("服务器已关闭")
        return 0

    except Exception as e:
        logger.error(f"服务器运行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())