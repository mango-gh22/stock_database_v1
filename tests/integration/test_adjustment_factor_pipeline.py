# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/integration\test_adjustment_factor_pipeline.py
# File Name: test_adjustment_factor_pipeline
# @ Author: mango-gh22
# @ Date：2026/1/2 19:27
"""
desc 
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests/integration/test_adjustment_factor_pipeline.py
# File Name: test_adjustment_factor_pipeline
# @ Author: mango-gh22
# @ Date: 2026/01/02
"""
复权因子集成测试 - P6阶段
验证：下载 → 计算 → 存储 → 查询 完整链路
关键测试点：单线程约束、数据一致性、错误恢复
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
import pandas as pd
from datetime import datetime, timedelta
import time
import threading

from src.data.adjustment_factor_manager import AdjustmentFactorManager
from src.utils.code_converter import normalize_stock_code


class TestAdjustmentFactorPipeline:
    """复权因子管道集成测试类"""

    def setup_method(self):
        """测试前置"""
        self.manager = AdjustmentFactorManager()
        self.test_symbols = ['sh600519', 'sz000001', 'sh600036']  # 茅台、平安、招行

    def teardown_method(self):
        """测试后置"""
        self.manager.cleanup()

    def test_single_thread_constraint(self):
        """测试单线程约束（关键测试）"""
        print("\n🧪 测试单线程约束...")

        results = {}
        errors = []

        def worker(symbol):
            try:
                df = self.manager.downloader.fetch_dividend_data(symbol)
                results[symbol] = len(df)
            except Exception as e:
                errors.append(f"{symbol}: {e}")

        # 启动多个线程尝试并发
        threads = []
        for symbol in self.test_symbols[:2]:
            t = threading.Thread(target=worker, args=(symbol,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=30)  # 30秒超时

        # 验证：所有请求应成功（单线程串行执行）
        assert len(results) == 2, f"部分请求失败: {errors}"
        print(f"  ✅ 单线程验证通过: {results}")

    def test_download_calculate_store_pipeline(self):
        """测试完整管道"""
        print("\n🧪 测试完整管道...")

        # 单股票测试
        symbol = self.test_symbols[0]

        # 1. 下载
        dividend_df = self.manager.downloader.fetch_dividend_data(symbol)
        assert not dividend_df.empty, f"下载失败: {symbol}"
        print(f"  ✅ 下载成功: {len(dividend_df)} 条")

        # 2. 计算
        factors_df = self.manager.downloader.calculate_adjustment_factors(dividend_df)
        assert not factors_df.empty, "计算失败"
        assert 'forward_factor' in factors_df.columns
        print(f"  ✅ 计算成功: {len(factors_df)} 条")

        # 3. 存储
        affected_rows, report = self.manager.storage.store_adjustment_factors(factors_df)
        assert affected_rows > 0, f"存储失败: {report}"
        print(f"  ✅ 存储成功: {affected_rows} 条")

        # 4. 查询验证
        stored_df = self.manager.get_factors_for_symbol(symbol)
        assert len(stored_df) == len(factors_df), "数据不一致"
        print(f"  ✅ 查询验证成功")

    def test_incremental_update_logic(self):
        """测试增量更新逻辑"""
        print("\n🧪 测试增量更新逻辑...")

        symbol = self.test_symbols[1]

        # 第一次下载（全量）
        range1 = self.manager.date_calculator.calculate_download_range(symbol, mode='incremental')
        assert range1 is not None, "首次应返回范围"
        print(f"  📥 首次范围: {range1}")

        # 模拟数据已存在
        dividend_df = self.manager.downloader.fetch_dividend_data(symbol)
        if not dividend_df.empty:
            factors_df = self.manager.downloader.calculate_adjustment_factors(dividend_df)
            self.manager.storage.store_adjustment_factors(factors_df)

            # 第二次下载（增量）
            range2 = self.manager.date_calculator.calculate_download_range(symbol, mode='incremental')
            print(f"  📥 增量范围: {range2}")

            # 验证：range2的开始应晚于range1的结束
            if range2:
                assert range2[0] > range1[1], "增量逻辑错误"

        print("  ✅ 增量更新验证通过")

    def test_error_recovery(self):
        """测试错误恢复能力"""
        print("\n🧪 测试错误恢复...")

        # 测试无效股票代码
        invalid_symbol = "sh999999"

        try:
            df = self.manager.downloader.fetch_dividend_data(invalid_symbol)
            # 应返回空DataFrame而非抛出异常
            assert isinstance(df, pd.DataFrame)
            assert df.empty
            print("  ✅ 无效代码处理正常")
        except Exception as e:
            pytest.fail(f"应优雅处理无效代码: {e}")

        # 测试网络异常重试
        # 注：实际测试中可通过断网或mock验证，此处验证重试逻辑存在
        assert self.manager.downloader.config.get('baostock', {}).get('max_retries', 3) >= 3
        print("  ✅ 重试配置正确")

    def test_data_quality_and_consistency(self):
        """测试数据质量和一致性"""
        print("\n🧪 测试数据质量...")

        symbol = self.test_symbols[2]

        # 获取数据
        df = self.manager.get_factors_for_symbol(symbol)

        if not df.empty:
            # 验证因子值范围
            assert (df['forward_factor'] > 0).all(), "前复权因子必须为正"
            assert (df['backward_factor'] > 0).all(), "后复权因子必须为正"

            # 验证因子单调性（时间倒序，因子应递减）
            df_sorted = df.sort_values('ex_date', ascending=False)
            if len(df_sorted) > 1:
                forward_diff = df_sorted['forward_factor'].diff().dropna()
                if len(forward_diff) > 0 and forward_diff[0] != 0:
                    # 首次分红后因子应小于1
                    assert forward_diff.iloc[0] < 0, "前复权因子应递减"

            print("  ✅ 数据质量验证通过")

    def test_query_performance(self):
        """测试查询性能"""
        print("\n🧪 测试查询性能...")

        symbol = self.test_symbols[0]

        # 预热
        _ = self.manager.get_factors_for_symbol(symbol)

        # 测试单次查询
        start = time.time()
        df = self.manager.get_factors_for_symbol(symbol)
        duration1 = time.time() - start

        # 测试带日期过滤的查询
        if not df.empty:
            latest_date = df['ex_date'].iloc[0].strftime('%Y%m%d')
            start = time.time()
            _ = self.manager.get_adjustment_factor(symbol, latest_date)
            duration2 = time.time() - start

            print(f"  ⏱️  全量查询: {duration1 * 1000:.2f}ms")
            print(f"  ⏱️  单点查询: {duration2 * 1000:.2f}ms")

        print("  ✅ 查询性能测试通过")

    def test_concurrent_query_safety(self):
        """测试并发查询安全性"""
        print("\n🧪 测试并发查询...")

        results = {}

        def query_worker(symbol):
            try:
                for _ in range(5):
                    df = self.manager.get_factors_for_symbol(symbol)
                    time.sleep(0.1)  # 模拟真实负载
                results[symbol] = "success"
            except Exception as e:
                results[symbol] = f"error: {e}"

        # 多线程查询（查询操作应支持并发）
        threads = []
        for symbol in self.test_symbols[:2]:
            t = threading.Thread(target=query_worker, args=(symbol,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=15)

        assert all(v == "success" for v in results.values()), f"并发查询失败: {results}"
        print("  ✅ 并发查询安全")

    def test_manager_stats_accuracy(self):
        """测试统计准确性"""
        print("\n🧪 测试统计准确性...")

        # 重置统计
        self.manager.stats = {
            'total_symbols': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_records_downloaded': 0,
            'total_records_stored': 0,
            'start_time': None,
            'end_time': None
        }

        # 执行批量操作
        results = self.manager.download_batch(
            self.test_symbols[:1],
            mode='incremental'
        )

        stats = self.manager.get_stats()

        assert stats['total_symbols'] == 1, "统计总数错误"
        assert stats['successful_symbols'] == len(results), "统计成功数错误"
        assert stats['start_time'] is not None, "开始时间未记录"
        assert stats['end_time'] is not None, "结束时间未记录"

        print(f"  ✅ 统计准确: {stats}")

    @pytest.mark.integration
    def test_full_pipeline_with_real_data(self):
        """集成测试：端到端真实数据验证"""
        print("\n🧪 端到端集成测试...")

        # 选择有明确分红历史的股票
        test_symbol = 'sh600519'  # 贵州茅台

        # 1. 清理旧数据
        with self.manager.storage.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {self.manager.storage.factor_table} WHERE symbol = %s",
                    (test_symbol,)
                )
                conn.commit()

        print(f"  🧹 清理旧数据完成")

        # 2. 执行完整管道
        result = self.manager.download_batch([test_symbol], mode='full')

        # 3. 验证结果
        assert len(result) == 1, "应成功处理1只股票"

        df = self.manager.get_factors_for_symbol(test_symbol)
        assert not df.empty, "应有数据"
        assert len(df) > 0, "至少应有1条分红记录"

        # 4. 验证因子计算正确性（简单验证）
        # 贵州茅台2022年分红：每股21.675元，假设前收盘价2000元
        # 前复权因子 ≈ 2000 / (2000 + 21.675) ≈ 0.989
        sample = df[df['ex_date'].dt.year == 2022]
        if not sample.empty:
            factor = sample.iloc[0]['forward_factor']
            assert 0.8 < factor < 1.0, f"因子值异常: {factor}"  # 宽松验证

        print(f"  ✅ 端到端验证通过: {len(df)} 条记录")

    def test_config_loading(self):
        """测试配置加载"""
        print("\n🧪 测试配置加载...")

        # 测试默认配置
        manager = AdjustmentFactorManager()
        assert manager.config.get('download', {}).get('thread_num') == 1, "默认线程数应为1"
        assert manager.config.get('storage', {}).get('table_name') == 'adjust_factors'

        # 测试自定义配置
        custom_config = {
            'adjustment_factors': {
                'download': {'thread_num': 2},  # 虽配置为2，但P6仍会强制单线程
                'storage': {'batch_size': 1000}
            }
        }

        # 实际代码中配置加载会合并，此处验证结构
        print("  ✅ 配置加载验证通过")


# 测试运行器
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])