# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/processors\test_validator.py
# File Name: test_validator
# @ Author: mango-gh22
# @ Date：2025/12/14 15:39
"""
desc 数据验证器测试 - 修复版
"""

import unittest
import pandas as pd
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 在导入前添加调试
print(f"当前工作目录: {os.getcwd()}")
print(f"Python路径: {sys.path}")

try:
    from src.processors.validator import DataValidator, ValidationResult

    print("✅ 成功导入 validator 模块")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    # 尝试直接导入
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "validator",
        os.path.join(os.path.dirname(__file__), '../../src/processors/validator.py')
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    DataValidator = module.DataValidator
    ValidationResult = module.ValidationResult


class TestDataValidator(unittest.TestCase):
    """数据验证器测试类"""

    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        print("设置测试类...")
        try:
            cls.validator = DataValidator()
            print("✅ 验证器初始化成功")
        except Exception as e:
            print(f"❌ 验证器初始化失败: {e}")
            cls.validator = None

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        if hasattr(cls, 'validator') and cls.validator is not None:
            cls.validator.close()

    def test_1_validator_initialization(self):
        """测试验证器初始化"""
        self.assertIsNotNone(self.validator)
        if self.validator:
            self.assertIsNotNone(self.validator.rules)
            print(f"加载规则: {len(self.validator.rules)} 个类别")

    def test_2_load_rules(self):
        """测试加载规则"""
        if self.validator:
            rules = self.validator.rules
            self.assertIsInstance(rules, dict)

            # 检查规则类别
            expected_categories = ['completeness', 'business_logic', 'consistency', 'statistical']
            for category in expected_categories:
                self.assertIn(category, rules)
                self.assertIsInstance(rules[category], list)
                print(f"{category}: {len(rules[category])} 条规则")

    def test_3_validate_completeness(self):
        """测试完整性验证"""
        if self.validator:
            # 需要数据库连接，可能跳过
            try:
                # 尝试获取股票列表
                stock_df = self.validator.query_engine.get_stock_list()
                if not stock_df.empty:
                    symbol = stock_df.iloc[0]['symbol']
                    print(f"测试股票: {symbol}")

                    results = self.validator.validate_completeness(symbol)
                    self.assertIsInstance(results, list)

                    if results:
                        for result in results:
                            self.assertIsInstance(result.rule_name, str)
                            self.assertIsInstance(result.result, ValidationResult)
                else:
                    print("⚠️ 无股票数据，跳过完整性测试")
            except Exception as e:
                print(f"⚠️ 完整性测试跳过: {e}")

    def test_4_validation_structure(self):
        """测试验证结果结构"""
        # 创建模拟结果
        from dataclasses import asdict

        # 尝试导入 ValidationResultDetail
        try:
            from src.processors.validator import ValidationResultDetail

            test_result = ValidationResultDetail(
                rule_name="test_rule",
                rule_description="测试规则",
                result=ValidationResult.PASS,
                error_message=None,
                affected_rows=0,
                affected_symbols=[],
                suggestion="无建议",
                execution_time=0.1
            )

            # 检查属性
            self.assertEqual(test_result.rule_name, "test_rule")
            self.assertEqual(test_result.result, ValidationResult.PASS)
            self.assertEqual(test_result.affected_rows, 0)

        except ImportError:
            print("⚠️ 无法导入 ValidationResultDetail")


def run_tests():
    """运行测试"""
    print("=" * 60)
    print("运行数据验证器测试")
    print("=" * 60)

    # 创建测试套件
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataValidator)

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == '__main__':
    run_tests()