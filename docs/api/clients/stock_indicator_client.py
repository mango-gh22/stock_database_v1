"""
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
