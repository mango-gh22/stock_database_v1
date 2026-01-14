# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\baostock_factor_base.py
# File Name: baostock_factor_base
# @ Author: mango-gh22
# @ Date：2026/1/3 8:52
"""
desc PB因子下载基础类 - 继承自现有架构
"""

import baostock as bs
import pandas as pd
import time
import random
import logging
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import threading
from pathlib import Path
import os
import sys

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.code_converter import normalize_stock_code
from src.data.baostock_factor_config import get_config_loader

logger = logging.getLogger(__name__)


class BaseFactorDownloader:
    """因子下载基类"""

    def __init__(self, config_path: str = 'config/factor_config.yaml'):
        self.config = get_config_loader(config_path)
        self.download_stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }

        # 强制单线程锁
        self._download_lock = threading.Lock()

        # 请求间隔控制
        self.last_request_time = None
        self.request_interval = self.config.get('execution.request_interval', 1.5)

        # 缓存目录
        self.cache_dir = self.config.get_cache_dir()

        logger.info(f"初始化因子下载器: 单线程模式, 请求间隔={self.request_interval}秒")

    def _login_baostock(self):
        """登录Baostock"""
        try:
            bs.logout()
        except:
            pass

        self.lg = bs.login()
        if self.lg.error_code != '0':
            logger.error(f"❌ Baostock登录失败: {self.lg.error_msg}")
            raise ConnectionError("Baostock login failed")

        logger.info("✅ Baostock登录成功")

    def _ensure_logged_in(self):
        """确保登录状态"""
        if not hasattr(self, 'lg') or not self.lg or self.lg.error_code != '0':
            self._login_baostock()

    def _convert_to_bs_code(self, symbol: str) -> str:
        """转换为Baostock格式"""
        normalized_code = normalize_stock_code(symbol)
        market = normalized_code[:2]
        code_num = normalized_code[2:]
        return f"{market}.{code_num}"

    # def _is_valid_stock(self, bs_code: str) -> bool:  # 重复验证，已在code_converter实现，删除此方法
    #     """验证是否为有效股票代码"""
    #     if not bs_code or '.' not in bs_code:
    #         return False
    #
    #     market, code = bs_code.split('.')
    #
    #     # 上证股票
    #     if market == 'sh':
    #         return code.startswith(('6', '9')) and not code.startswith(('000', '950', '951'))
    #     # 深证股票
    #     elif market == 'sz':
    #         return code.startswith(('00', '30')) and not code.startswith('399')
    #     # 北交所股票
    #     elif market == 'bj':
    #         return code.startswith(('43', '83', '87', '88'))
    #
    #     return False

    def _enforce_rate_limit(self):
        """强制执行请求速率限制"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.request_interval:
                sleep_time = self.request_interval - elapsed + random.uniform(0, 0.5)
                time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _format_date_for_baostock(self, date_str: str) -> str:
        """格式化日期为Baostock格式"""
        if not date_str or len(date_str) < 8:
            return date_str

        clean_date = date_str.replace('-', '').replace('/', '').replace('.', '')

        if len(clean_date) == 8 and clean_date.isdigit():
            return f"{clean_date[0:4]}-{clean_date[4:6]}-{clean_date[6:8]}"
        else:
            logger.warning(f"⚠️ 日期格式异常: {date_str}")
            return date_str

    # def _safe_fetch_data(self, rs, max_rows: int = 10000) -> List:
    def _safe_fetch_data(self, rs) -> List:  # 删除了参数max_rows: int = 10000
        """安全获取数据，防止解压/解码错误"""
        data_list = []
        row_count = 0

        while rs.error_code == '0' and rs.next():
            try:
                row_data = rs.get_row_data()
                if row_data:
                    data_list.append(row_data)
                    row_count += 1

                    if row_count >= max_rows:
                        logger.warning(f"达到最大行数限制 {max_rows}，提前终止")
                        break
            except Exception as e:
                error_msg = str(e).lower()
                if any(kw in error_msg for kw in ['utf-8', 'codec', 'decompress', 'invalid']):
                    logger.warning(f"跳过损坏行 {row_count}: {e}")
                    continue
                else:
                    raise

        return data_list

    def _update_stats(self, success: bool = True, records: int = 0):
        """更新统计信息"""
        self.download_stats['total_requests'] += 1

        if success:
            self.download_stats['successful'] += 1
            self.download_stats['total_records'] += records
        else:
            self.download_stats['failed'] += 1

    def get_download_stats(self) -> Dict[str, Any]:
        """获取下载统计"""
        stats = self.download_stats.copy()

        if stats['total_requests'] > 0:
            stats['success_rate'] = (stats['successful'] / stats['total_requests']) * 100
        else:
            stats['success_rate'] = 0

        if stats['start_time'] and stats['end_time']:
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
        else:
            stats['duration'] = 0

        return stats

    def reset_stats(self):
        """重置统计"""
        self.download_stats = {
            'total_requests': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }

    def logout(self):
        """退出登录"""
        if hasattr(self, 'lg') and self.lg:
            bs.logout()
            logger.info("🔒 Baostock已退出登录")
            self.lg = None

    def __del__(self):
        """析构函数"""
        try:
            self.logout()
        except:
            pass


def test_base_downloader():
    """测试基础下载器"""
    import sys
    import logging as log

    # 配置日志
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试因子下载基础类")
    print("=" * 50)

    try:
        # 创建下载器
        downloader = BaseFactorDownloader()

        # 测试登录
        downloader._ensure_logged_in()
        if not hasattr(downloader, 'lg') or not downloader.lg:
            print("❌ Baostock登录失败")
            return False

        print("✅ Baostock登录成功")

        # 测试代码转换
        test_cases = [
            ('600519', 'sh.600519'),
            ('000001', 'sz.000001'),
            ('sh600519', 'sh.600519'),
            ('sz000001', 'sz.000001')
        ]

        print("\n🔍 测试代码转换:")
        for input_code, expected in test_cases:
            result = downloader._convert_to_bs_code(input_code)
            status = "✅" if result == expected else "❌"
            print(f"  {status} {input_code} -> {result} (期望: {expected})")

        # 测试股票验证
        print("\n🔍 测试股票验证:")
        test_stocks = [
            ('sh.600519', True),  # 贵州茅台
            ('sh.000001', False),  # 上证指数
            ('sz.000001', True),  # 平安银行
            ('sz.399001', False),  # 深证成指
            ('sh.688981', True),  # 中芯国际
        ]

        for bs_code, expected in test_stocks:
            result = downloader._is_valid_stock(bs_code)
            status = "✅" if result == expected else "❌"
            print(
                f"  {status} {bs_code} -> {'股票' if result else '非股票'} (期望: {'股票' if expected else '非股票'})")

        # 测试日期格式化
        print("\n🔍 测试日期格式化:")
        test_dates = [
            ('20250102', '2025-01-02'),
            ('2025-01-02', '2025-01-02'),
            ('2025/01/02', '2025-01-02'),
            ('', ''),
        ]

        for input_date, expected in test_dates:
            result = downloader._format_date_for_baostock(input_date)
            status = "✅" if result == expected else "❌"
            print(f"  {status} {input_date} -> {result} (期望: {expected})")

        # 测试统计更新
        print("\n📊 测试统计更新:")
        downloader.reset_stats()
        downloader._update_stats(success=True, records=100)
        downloader._update_stats(success=False)
        downloader._update_stats(success=True, records=50)

        stats = downloader.get_download_stats()
        print(f"  总请求: {stats['total_requests']}")
        print(f"  成功: {stats['successful']}")
        print(f"  失败: {stats['failed']}")
        print(f"  成功率: {stats['success_rate']:.1f}%")
        print(f"  总记录: {stats['total_records']}")

        # 退出登录
        downloader.logout()
        print("\n✅ 基础下载器测试完成")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_base_downloader()
    sys.exit(0 if success else 1)