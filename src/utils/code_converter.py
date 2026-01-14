# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/utils\code_converter.py
# File Name: code_converter
# @ File: code_converter.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/4 23:46
"""
desc 核心工具文件（代码格式转换器）
股票代码格式统一转换器 - 修正版 v1.0.1
P1阶段版本
【重要】项目内所有模块必须调用此函数处理股票代码，禁止自行实现其他转换逻辑。
"""

CODE_PREFIX = {
    "SH": "sh",  # 上海证券交易所
    "SZ": "sz",  # 深圳证券交易所
    "BJ": "bj",  # 北京证券交易所（预留）
}

def normalize_stock_code(raw_code: str) -> str:
    """
    统一股票代码格式为市场标准格式：sh600519 或 sz000001

    支持以下输入格式的自动转换：
    - 600519.SH -> sh600519
    - 000001.SZ -> sz000001
    - 600519 -> sh600519（根据代码范围自动判断）
    - sz.600519 -> sz600519
    - 300750 -> sz300750
    - 688981.SH -> sh688981 (科创板)

    Args:
        raw_code (str): 任意格式的原始股票代码字符串

    Returns:
        str: 标准化后的股票代码

    Raises:
        ValueError: 当无法识别代码格式时抛出
    """
    if not raw_code or not isinstance(raw_code, str):
        raise ValueError(f"无效的股票代码输入: {raw_code}")

    code = raw_code.strip().upper()  # 统一转为大写，方便处理

    # 1. 首先检查是否已经包含标准市场前缀（sh/sz/bj）
    if code.startswith("SH") or code.startswith("SZ") or code.startswith("BJ"):
        # 可能的形式：SH600519 或 sh600519
        prefix = code[:2].lower()  # 取前两位并转小写
        number_part = code[2:]     # 剩余部分
        # 清理数字部分可能残留的非数字字符（如点、空格）
        number_part = ''.join(filter(str.isdigit, number_part))
        if number_part:  # 确保数字部分不为空
            return prefix + number_part
        else:
            raise ValueError(f"股票代码数字部分缺失: {raw_code}")

    # 2. 处理带点分隔符的格式，如 "600519.SH"
    if "." in code:
        parts = code.split(".")
        if len(parts) == 2:
            number_part, market_part = parts
            number_part = ''.join(filter(str.isdigit, number_part))  # 清理数字
            if not number_part:
                raise ValueError(f"股票代码数字部分缺失: {raw_code}")

            # 处理市场标识
            market_part = market_part.upper()
            if market_part in CODE_PREFIX:
                return CODE_PREFIX[market_part] + number_part
            else:
                # 如果市场标识不是标准的SH/SZ，尝试从数字推断
                pass  # 会跳到下面的数字判断逻辑

    # 3. 处理纯数字或清理后的代码
    # 提取所有数字
    digits = ''.join(filter(str.isdigit, code))
    if not digits:
        raise ValueError(f"无法提取股票代码数字: {raw_code}")

    # 将数字部分转换为整数进行范围判断
    try:
        code_num = int(digits)
    except ValueError:
        raise ValueError(f"无效的股票代码数字: {digits}")

    # 根据数字范围判断市场 (这是A股的通用规则)
    # 上证主板/科创板: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx, 689xxx
    if (600000 <= code_num <= 603999) or (605000 <= code_num <= 605999) or (688000 <= code_num <= 689999):
        return "sh" + digits
    # 深证主板/创业板: 000xxx, 001xxx, 002xxx, 003xxx, 300xxx
    elif (0 <= code_num <= 399999):
        return "sz" + digits
    else:
        # 如果都不符合，尝试查找是否隐含了市场标识（处理异常情况）
        code_upper = code.upper()
        if "SH" in code_upper:
            return "sh" + digits
        elif "SZ" in code_upper:
            return "sz" + digits
        else:
            raise ValueError(f"无法识别的股票代码格式或范围: {raw_code} (数字: {digits})")


# 单元测试函数
if __name__ == "__main__":
    test_cases = [
        ("600519.SH", "sh600519"),
        ("000001.sz", "sz000001"),
        ("300750", "sz300750"),
        ("sh600036", "sh600036"),
        ("SZ000858", "sz000858"),
        ("600519", "sh600519"),  # 纯数字，自动推断
        ("000001", "sz000001"),  # 纯数字，自动推断
        (" 600519.SH ", "sh600519"),  # 带空格
        ("sz.000002", "sz000002"),  # 点号在字母后
        ("688981.SH", "sh688981"),  # 科创板
        ("601318", "sh601318"),  # 上证
    ]

    print("=" * 50)
    print("测试股票代码转换器 (修正版):")
    print("-" * 50)

    passed = 0
    failed = 0

    for input_code, expected in test_cases:
        try:
            result = normalize_stock_code(input_code)
            if result == expected:
                print(f"  ✓ {input_code:>15} -> {result:15} (符合预期)")
                passed += 1
            else:
                print(f"  ✗ {input_code:>15} -> {result:15} (预期: {expected})")
                failed += 1
        except ValueError as e:
            print(f"  ✗ {input_code:>15} -> 错误: {e}")
            failed += 1

    print("-" * 50)
    print(f"测试结果: 通过 {passed} / 失败 {failed}")
    print("=" * 50)

    # 运行主程序自带的测试
    if passed == len(test_cases):
        print("\n✅ 所有测试通过！转换器可以正常使用。")
    else:
        print("\n❌ 部分测试失败，请检查代码逻辑。")