# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\a50_fixer.py
# File Name: a50_fixer
# @ Author: mango-gh22
# @ Date：2026/1/3 23:39
"""
desc 修复补丁文件
"""

# File Path: E:/MyFile/stock_database_v1/src/data/a50_fixer.py
"""
A50成分股更新修复补丁
使用现有的 normalize_stock_code 函数修复符号转换问题
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.code_converter import normalize_stock_code
from typing import List, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)


class A50SymbolFixer:
    """A50符号修复器 - 使用现有的代码转换器"""

    @staticmethod
    def fix_symbol(symbol_input: Union[str, Dict]) -> str:
        """
        修复股票代码符号

        Args:
            symbol_input: 输入符号（字符串或字典）

        Returns:
            标准化后的股票代码

        Raises:
            ValueError: 如果无法处理输入
        """
        if isinstance(symbol_input, dict):
            # 字典格式：{'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38}
            if 'symbol' in symbol_input:
                return normalize_stock_code(symbol_input['symbol'])
            elif 'code' in symbol_input:
                return normalize_stock_code(symbol_input['code'])
            else:
                raise ValueError(f"字典中缺少symbol/code字段: {symbol_input}")
        elif isinstance(symbol_input, str):
            # 字符串格式
            return normalize_stock_code(symbol_input)
        else:
            raise ValueError(f"不支持的输入类型: {type(symbol_input)}")

    @staticmethod
    def batch_fix_symbols(symbols: List[Union[str, Dict]]) -> List[str]:
        """
        批量修复股票代码

        Args:
            symbols: 原始符号列表

        Returns:
            标准化后的股票代码列表
        """
        fixed_symbols = []
        errors = []

        for item in symbols:
            try:
                fixed_symbol = A50SymbolFixer.fix_symbol(item)
                fixed_symbols.append(fixed_symbol)
            except Exception as e:
                errors.append({
                    'item': str(item),
                    'error': str(e)
                })
                logger.warning(f"修复符号失败 {item}: {e}")

        if errors:
            logger.warning(f"批量修复中发现 {len(errors)} 个错误")

        return fixed_symbols

    @staticmethod
    def extract_symbol_info(symbol_input: Union[str, Dict]) -> Dict[str, Any]:
        """
        提取符号信息

        Returns:
            包含详细信息的字典
        """
        normalized_symbol = A50SymbolFixer.fix_symbol(symbol_input)

        info = {
            'normalized_symbol': normalized_symbol,
            'original_input': str(symbol_input) if not isinstance(symbol_input, str) else symbol_input
        }

        # 解析市场信息
        if normalized_symbol.startswith('sh'):
            info['exchange'] = 'SH'
            info['market'] = '上海'
            info['pure_code'] = normalized_symbol[2:]
        elif normalized_symbol.startswith('sz'):
            info['exchange'] = 'SZ'
            info['market'] = '深圳'
            info['pure_code'] = normalized_symbol[2:]
        elif normalized_symbol.startswith('bj'):
            info['exchange'] = 'BJ'
            info['market'] = '北京'
            info['pure_code'] = normalized_symbol[2:]
        else:
            info['exchange'] = 'Unknown'
            info['market'] = '未知'
            info['pure_code'] = normalized_symbol

        # 如果是字典输入，保留额外字段
        if isinstance(symbol_input, dict):
            for key, value in symbol_input.items():
                if key not in ['symbol', 'code']:
                    info[key] = value

        return info


# 测试函数
def test_a50_fixer():
    """测试A50修复器"""
    print("🧪 测试A50符号修复器")
    print("=" * 50)

    test_cases = [
        # 输入, 期望输出
        ({'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38}, 'sh600519'),
        ({'name': '宁德时代', 'symbol': '300750.SZ', 'weight': 8.45}, 'sz300750'),
        ({'name': '中国平安', 'symbol': '601318.SH', 'weight': 6.89}, 'sh601318'),
        ({'name': '招商银行', 'symbol': '600036.SH', 'weight': 5.22}, 'sh600036'),
        ({'name': '美的集团', 'symbol': '000333.SZ', 'weight': 4.67}, 'sz000333'),
        ('600519.SH', 'sh600519'),
        ('000001.SZ', 'sz000001'),
        ('300750', 'sz300750'),
        ('sh600519', 'sh600519'),
        ('sz000001', 'sz000001'),
    ]

    passed = 0
    total = len(test_cases)

    for i, (input_data, expected) in enumerate(test_cases, 1):
        try:
            result = A50SymbolFixer.fix_symbol(input_data)
            if result == expected:
                print(f"✅ [{i}] {input_data} -> {result}")
                passed += 1
            else:
                print(f"❌ [{i}] {input_data} -> {result} (期望: {expected})")
        except Exception as e:
            print(f"❌ [{i}] {input_data} -> 错误: {e}")

    # 测试批量处理
    print("\n🧪 测试批量修复:")
    symbols_list = [
        {'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38},
        '000001.SZ',
        {'symbol': '300750.SZ'},
        'invalid_code'
    ]

    fixed = A50SymbolFixer.batch_fix_symbols(symbols_list)
    print(f"输入: {symbols_list}")
    print(f"输出: {fixed}")

    # 测试信息提取
    print("\n🧪 测试信息提取:")
    info = A50SymbolFixer.extract_symbol_info({'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38})
    print(f"输入: {{'name': '贵州茅台', 'symbol': '600519.SH', 'weight': 10.38}}")
    print(f"输出: {info}")

    print(f"\n📊 测试结果: {passed}/{total} 通过")
    return passed == total


if __name__ == "__main__":
    success = test_a50_fixer()
    sys.exit(0 if success else 1)