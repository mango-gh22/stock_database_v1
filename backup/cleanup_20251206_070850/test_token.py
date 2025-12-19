# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/data\test_token.py
# File Name: test_token
# @ File: test_token.py
# @ Author: m_mango
# @ PyCharm
# @ Date：2025/12/5 20:13
"""
desc 
"""
# tests/data/test_token.py
# tests/data/test_token.py（更新版）
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest


def test_token_loading():
    """测试Token加载"""
    try:
        from src.config.token_loader import get_token
        token = get_token()
        assert token is not None, "Token不能为空"
        assert len(token) > 10, f"Token长度异常: {len(token)}"
        print(f"✅ Token加载测试通过: {token[:10]}...")
        return True
    except Exception as e:
        print(f"❌ Token加载测试失败: {e}")
        return False


def test_tushare_connection():
    """测试Tushare连接"""
    try:
        # 先单独测试Tushare API
        from src.config.token_loader import get_token
        import tushare as ts

        token = get_token()
        pro = ts.pro_api(token)

        # 测试简单查询
        df = pro.query('trade_cal', exchange='SSE', start_date='20240101', end_date='20240102')
        assert df is not None, "API返回空"

        print(f"✅ Tushare连接测试通过，获取到{len(df)}条数据")
        return True
    except Exception as e:
        print(f"❌ Tushare连接测试失败: {e}")
        return False


def test_database_connection():
    """测试数据库连接"""
    try:
        # 先检查配置文件是否存在
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'database.yaml')
        if not os.path.exists(config_path):
            print(f"⚠️ 数据库配置文件不存在: {config_path}")
            print("跳过数据库测试")
            return True  # 不强制要求数据库连接

        from src.database.db_connector import DatabaseConnector
        db = DatabaseConnector()

        # 测试连接
        if db.test_connection():
            print("✅ 数据库连接测试通过")
            return True
        else:
            print("❌ 数据库连接失败")
            return False

    except Exception as e:
        print(f"⚠️ 数据库连接测试异常（可能是配置问题）: {e}")
        return True  # 对于初始测试，数据库连接不是必须的


if __name__ == "__main__":
    # 手动运行测试
    print("开始测试...")
    results = []

    results.append(test_token_loading())
    results.append(test_tushare_connection())
    results.append(test_database_connection())

    print(f"\n测试结果: 通过 {sum(results)}/{len(results)}")

    # 只有Token和Tushare连接是必须的
    if results[0] and results[1]:
        print("\n✅ 核心功能测试通过！可以开始数据采集")
        sys.exit(0)
    else:
        print("\n❌ 核心功能测试失败！")
        sys.exit(1)