# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_indicators_basic.py
# File Name: test_indicators_basic
# @ Author: mango-gh22
# @ Date：2025/12/20 22:06
"""
desc 
"""
# # 3. 创建测试脚本
# cat > scripts / test_indicators_basic.py << 'EOF'
"""
基础技术指标测试脚本
"""
import sys
import os
from typing import List, Dict, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.query.enhanced_query_engine import EnhancedQueryEngine


def test_basic_functionality():
    """测试基本功能"""
    print("🧪 测试技术指标基本功能...")

    try:
        # 创建增强查询引擎
        engine = EnhancedQueryEngine()
        print("✅ 增强查询引擎创建成功")

        # 获取可用指标
        indicators = engine.get_available_indicators()
        print(f"✅ 获取到 {len(indicators)} 个可用指标")

        # 测试数据查询
        df = engine.query_daily_data('sh600519', '2024-01-01', '2024-01-31')
        print(f"✅ 数据查询成功，获取到 {len(df)} 条数据")

        # 测试带指标查询（如果数据足够）
        if len(df) > 20:
            result = engine.query_with_indicators(
                symbol='sh600519',
                indicators=['moving_average'],
                start_date='2024-01-01',
                end_date='2024-01-31'
            )
            print(f"✅ 带指标查询成功，结果列数: {len(result.columns)}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_basic_functionality()
    if success:
        print("\n🎉 基础功能测试通过！可以开始P6阶段开发。")
    else:
        print("\n⚠️ 测试失败，请先修复问题。")
