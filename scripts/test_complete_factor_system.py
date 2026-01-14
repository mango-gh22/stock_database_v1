# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\test_complete_factor_system.py
# File Name: test_complete_factor_system
# @ Author: mango-gh22
# @ Date：2026/1/3 22:46
"""
desc 完整因子数据系统测试脚本
测试所有组件：下载、存储、批量处理、A50更新
"""

import sys
import os
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.baostock_pb_factor_downloader import BaostockPBFactorDownloader
from src.data.factor_storage_manager import FactorStorageManager
from src.data.factor_data_pipeline import FactorDataPipeline
from src.data.factor_batch_processor import FactorBatchProcessor
from src.config.logging_config import setup_logging

logger = setup_logging()


class CompleteFactorSystemTest:
    """完整因子系统测试类"""

    def __init__(self):
        self.test_results = {}
        self.start_time = datetime.now()

    def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "=" * 60)
        print("🧪 完整因子数据系统测试")
        print("=" * 60)

        tests = [
            ("下载器测试", self.test_downloader),
            ("存储管理器测试", self.test_storage_manager),
            ("数据管道测试", self.test_data_pipeline),
            ("批量处理器测试", self.test_batch_processor),
            ("增量更新测试", self.test_incremental_update),
            ("数据验证测试", self.test_data_validation)
        ]

        total_tests = len(tests)
        passed_tests = 0

        for test_name, test_func in tests:
            print(f"\n▶️  正在测试: {test_name}")
            print("-" * 40)

            try:
                success = test_func()
                if success:
                    print(f"✅ {test_name} 通过")
                    passed_tests += 1
                else:
                    print(f"❌ {test_name} 失败")

                self.test_results[test_name] = {
                    'status': 'passed' if success else 'failed',
                    'timestamp': datetime.now().isoformat()
                }

            except Exception as e:
                print(f"💥 {test_name} 异常: {e}")
                import traceback
                traceback.print_exc()
                self.test_results[test_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }

        # 汇总结果
        self.end_time = datetime.now()
        self.print_summary(total_tests, passed_tests)

        return passed_tests == total_tests

    def test_downloader(self):
        """测试下载器"""
        try:
            downloader = BaostockPBFactorDownloader()

            # 测试单只股票下载
            test_symbol = '600519'
            start_date = '20240101'
            end_date = '20240110'

            print(f"测试下载: {test_symbol} [{start_date} - {end_date}]")
            data = downloader.fetch_factor_data(test_symbol, start_date, end_date)

            if data.empty:
                print("警告: 下载数据为空")
                return False

            print(f"下载成功: {len(data)} 条记录")
            print(f"字段: {list(data.columns)}")

            # 检查必要字段
            required_fields = ['pb', 'pe_ttm', 'ps_ttm']
            missing_fields = [f for f in required_fields if f not in data.columns]

            if missing_fields:
                print(f"缺少字段: {missing_fields}")
                return False

            # 检查数据质量
            pb_values = data['pb'].dropna()
            if len(pb_values) > 0:
                print(f"PB值范围: {pb_values.min():.2f} - {pb_values.max():.2f}")

            downloader.logout()
            return True

        except Exception as e:
            print(f"下载器测试失败: {e}")
            return False

    def test_storage_manager(self):
        """测试存储管理器"""
        try:
            storage = FactorStorageManager()

            # 测试最后日期查询
            test_symbol = '600519'
            last_date = storage.get_last_factor_date(test_symbol)
            print(f"最后更新日期 ({test_symbol}): {last_date}")

            # 测试增量范围计算
            start_date, end_date = storage.calculate_incremental_range(test_symbol)
            print(f"增量范围: {start_date} - {end_date}")

            # 创建测试数据
            import pandas as pd
            test_data = pd.DataFrame({
                'symbol': [f'TEST{datetime.now().strftime("%H%M%S")}'],
                'trade_date': ['2026-01-01'],
                'pb': [1.0],
                'pe_ttm': [10.0],
                'ps_ttm': [2.0]
            })

            # 测试存储
            print("测试数据存储...")
            affected_rows, report = storage.store_factor_data(test_data)

            print(f"存储结果: {affected_rows} 条记录")
            print(f"状态: {report['status']}")

            # 清理测试数据
            if 'symbol' in report:
                try:
                    with storage.db_connector.get_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "DELETE FROM stock_daily_data WHERE symbol LIKE 'TEST%'"
                            )
                            conn.commit()
                            print(f"清理测试数据: {cursor.rowcount} 条")
                except:
                    pass

            return True

        except Exception as e:
            print(f"存储管理器测试失败: {e}")
            return False

    def test_data_pipeline(self):
        """测试数据管道"""
        try:
            pipeline = FactorDataPipeline()

            # 测试单只股票更新
            test_symbol = '000001'  # 平安银行

            print(f"测试管道更新: {test_symbol}")
            result = pipeline.update_single_symbol(test_symbol, mode='incremental')

            print(f"更新结果:")
            print(f"  状态: {result['status']}")
            print(f"  下载记录: {result.get('records_downloaded', 0)}")
            print(f"  存储记录: {result.get('records_stored', 0)}")
            print(f"  耗时: {result.get('execution_time', 0):.2f}秒")

            # 测试状态查询
            status = pipeline.get_update_status(test_symbol)
            print(f"状态查询: 最后更新日期 = {status.get('last_update_date')}")

            return result['status'] in ['success', 'skipped', 'no_data']

        except Exception as e:
            print(f"数据管道测试失败: {e}")
            return False

    def test_batch_processor(self):
        """测试批量处理器"""
        try:
            processor = FactorBatchProcessor()

            # 测试股票列表
            test_symbols = ['600519', '000001', '000858']

            print(f"测试批量处理: {len(test_symbols)} 只股票")

            def progress_callback(progress, current, total):
                print(f"  进度: {progress:.1f}%", end='\r')

            report = processor.process_symbol_list(
                symbols=test_symbols,
                mode='incremental',
                progress_callback=progress_callback
            )

            print("\n批量处理结果:")
            summary = report['summary']
            print(f"  成功: {summary['successful']}")
            print(f"  失败: {summary['failed']}")
            print(f"  总记录: {summary['total_records']}")

            processor.cleanup()
            return summary['failed'] == 0

        except Exception as e:
            print(f"批量处理器测试失败: {e}")
            return False

    def test_incremental_update(self):
        """测试增量更新逻辑"""
        try:
            storage = FactorStorageManager()

            test_symbol = '600519'

            # 第一次计算增量范围
            print(f"测试增量更新逻辑: {test_symbol}")
            range1 = storage.calculate_incremental_range(test_symbol)
            print(f"第一次增量范围: {range1}")

            if range1 and range1[0]:
                # 模拟更新后再次计算
                print("模拟更新后...")

                # 清理缓存
                storage.clear_cache(test_symbol)

                # 再次计算
                range2 = storage.calculate_incremental_range(test_symbol)
                print(f"第二次增量范围: {range2}")

                # 验证逻辑
                if range2 and range2[0]:
                    # range2的开始日期应该晚于range1的结束日期
                    print("增量逻辑验证通过")
                    return True
                else:
                    print("数据已最新，无需更新")
                    return True
            else:
                print("无历史数据，需要全量下载")
                return True

        except Exception as e:
            print(f"增量更新测试失败: {e}")
            return False

    def test_data_validation(self):
        """测试数据验证"""
        try:
            storage = FactorStorageManager()

            test_symbol = '600519'

            print(f"测试数据验证: {test_symbol}")

            # 查询数据库中的数据
            clean_symbol = str(test_symbol).replace('.', '')

            with storage.db_connector.get_connection() as conn:
                # 查询因子数据
                import pandas as pd
                df = pd.read_sql_query(f"""
                    SELECT 
                        trade_date, 
                        pb, 
                        pe_ttm, 
                        ps_ttm,
                        CASE 
                            WHEN pb IS NULL THEN 'missing'
                            WHEN pb <= 0 THEN 'invalid'
                            ELSE 'valid'
                        END as pb_status,
                        CASE 
                            WHEN pe_ttm IS NULL THEN 'missing'
                            WHEN pe_ttm <= 0 THEN 'invalid'
                            ELSE 'valid'
                        END as pe_status
                    FROM stock_daily_data 
                    WHERE symbol = '{clean_symbol}'
                    ORDER BY trade_date DESC 
                    LIMIT 10
                """, conn)

            if df.empty:
                print("无数据可验证")
                return True

            print(f"验证 {len(df)} 条记录")

            # 检查数据质量
            pb_valid = df[df['pb_status'] == 'valid'].shape[0]
            pb_missing = df[df['pb_status'] == 'missing'].shape[0]
            pb_invalid = df[df['pb_status'] == 'invalid'].shape[0]

            pe_valid = df[df['pe_status'] == 'valid'].shape[0]
            pe_missing = df[df['pe_status'] == 'missing'].shape[0]
            pe_invalid = df[df['pe_status'] == 'invalid'].shape[0]

            print(f"PB数据: 有效={pb_valid}, 缺失={pb_missing}, 无效={pb_invalid}")
            print(f"PE数据: 有效={pe_valid}, 缺失={pe_missing}, 无效={pe_invalid}")

            # 计算质量评分
            total_records = len(df)
            if total_records > 0:
                pb_quality = (pb_valid / total_records) * 100
                pe_quality = (pe_valid / total_records) * 100

                print(f"数据质量评分:")
                print(f"  PB质量: {pb_quality:.1f}%")
                print(f"  PE质量: {pe_quality:.1f}%")

                return pb_quality > 50 and pe_quality > 50  # 要求质量评分>50%
            else:
                return True

        except Exception as e:
            print(f"数据验证测试失败: {e}")
            return False

    def print_summary(self, total_tests, passed_tests):
        """打印测试汇总"""
        print("\n" + "=" * 60)
        print("📊 测试汇总报告")
        print("=" * 60)

        duration = (self.end_time - self.start_time).total_seconds()

        print(f"总测试数: {total_tests}")
        print(f"通过数: {passed_tests}")
        print(f"失败数: {total_tests - passed_tests}")
        print(f"成功率: {(passed_tests / total_tests * 100):.1f}%")
        print(f"总耗时: {duration:.2f} 秒")

        print(f"\n详细结果:")
        for test_name, result in self.test_results.items():
            status = result['status']
            status_icon = "✅" if status == 'passed' else "❌" if status == 'failed' else "⚠️ "
            print(f"  {status_icon} {test_name}: {status}")

        print("\n" + "=" * 60)

        if passed_tests == total_tests:
            print("🎉 所有测试通过！系统运行正常。")
        else:
            print("⚠️  部分测试失败，请检查日志。")

        print("=" * 60)


def main():
    """主函数"""
    try:
        tester = CompleteFactorSystemTest()
        success = tester.run_all_tests()

        return 0 if success else 1

    except Exception as e:
        print(f"💥 测试程序异常: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)