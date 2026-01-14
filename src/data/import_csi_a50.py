# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/data\import_csi_a50.py
# @ Author: m_mango
# @ Date：2025/12/5 18:27
"""
desc 中证A50成分股导入模块
中证A50成分股导入模块（修复版：基于配置文件正向验证）
"""

import logging
from typing import List, Dict, Any
from pathlib import Path

import yaml

from src.config.logging_config import setup_logging
from src.database.database_manager import DatabaseManager

logger = setup_logging()


class CSI_A50_Importer:
    """中证A50成分股导入器（修复验证逻辑）"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        self.db_manager = DatabaseManager(config_path)
        self.csi_a50_symbols = self._load_csi_a50_symbols()

    def _load_csi_a50_symbols(self) -> List[Dict[str, Any]]:
        """加载中证A50成分股配置（来自 symbols.yaml）"""
        try:
            symbols_config_path = Path('config/symbols.yaml')
            if not symbols_config_path.exists():
                logger.error(f"配置文件不存在: {symbols_config_path}")
                return []

            with open(symbols_config_path, 'r', encoding='utf-8') as f:
                symbols_config = yaml.safe_load(f)

            csi_a50_stocks = symbols_config.get('csi_a50', [])
            if not csi_a50_stocks:
                logger.error("配置文件中未找到 csi_a50 成分股列表")
                return []

            logger.info(f"成功加载 {len(csi_a50_stocks)} 只中证A50成分股（来自 symbols.yaml）")
            return csi_a50_stocks

        except Exception as e:
            logger.error(f"加载 symbols.yaml 失败: {e}")
            return []

    def import_index_info(self) -> bool:
        """导入指数基本信息"""
        try:
            connection = self.db_manager.db_connector.get_connection()
            cursor = connection.cursor()

            index_info = {
                'index_code': 'CSI_A50',
                'index_name': '中证A50指数',
                'index_name_en': 'CSI A50 Index',
                'publisher': '中证指数有限公司',
                'index_type': '规模指数',
                'base_date': '2014-12-31',
                'base_point': 1000.00,
                'website': 'https://www.csindex.com.cn/'
            }

            insert_sql = """
                INSERT INTO index_info (
                    index_code, index_name, index_name_en, publisher,
                    index_type, base_date, base_point, website
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    index_name = VALUES(index_name),
                    index_name_en = VALUES(index_name_en),
                    publisher = VALUES(publisher),
                    index_type = VALUES(index_type),
                    base_date = VALUES(base_date),
                    base_point = VALUES(base_point),
                    website = VALUES(website),
                    updated_time = CURRENT_TIMESTAMP
            """

            cursor.execute(insert_sql, tuple(index_info.values()))
            connection.commit()
            cursor.close()
            connection.close()

            logger.info("中证A50指数信息导入成功")
            return True

        except Exception as e:
            logger.error(f"导入指数信息失败: {e}")
            return False

    def import_stock_basic_info(self) -> bool:
        """导入股票基本信息（支持增量更新）"""
        if not self.csi_a50_symbols:
            logger.error("无股票数据可导入")
            return False

        try:
            connection = self.db_manager.db_connector.get_connection()
            cursor = connection.cursor()

            success_count = 0
            total = len(self.csi_a50_symbols)

            insert_sql = """
                INSERT INTO stock_basic_info (
                    symbol, ts_code, name, area, industry, market,
                    list_date, fullname, enname, cnspell, exchange,
                    curr_type, list_status, is_hs
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    area = VALUES(area),
                    industry = VALUES(industry),
                    market = VALUES(market),
                    list_date = VALUES(list_date),
                    fullname = VALUES(fullname),
                    enname = VALUES(enname),
                    cnspell = VALUES(cnspell),
                    exchange = VALUES(exchange),
                    curr_type = VALUES(curr_type),
                    list_status = VALUES(list_status),
                    is_hs = VALUES(is_hs),
                    updated_time = CURRENT_TIMESTAMP
            """

            for i, stock in enumerate(self.csi_a50_symbols, 1):
                try:
                    symbol = stock['symbol']
                    exchange = 'SSE' if symbol.endswith('.SH') else 'SZSE' if symbol.endswith('.SZ') else None

                    data = (
                        symbol,
                        stock.get('ts_code', ''),
                        stock.get('name', ''),
                        stock.get('area', ''),
                        stock.get('industry', ''),
                        stock.get('market', ''),
                        stock.get('list_date', None),
                        stock.get('fullname', ''),
                        stock.get('enname', ''),
                        stock.get('cnspell', ''),
                        exchange,
                        stock.get('curr_type', 'CNY'),
                        stock.get('list_status', 'L'),
                        stock.get('is_hs', 'N')
                    )

                    cursor.execute(insert_sql, data)
                    success_count += 1
                    logger.debug(f"[{i}/{total}] 导入股票基本信息: {symbol}")

                except Exception as e:
                    logger.error(f"导入股票 {stock.get('symbol', 'Unknown')} 失败: {e}")

            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"股票基本信息导入完成: {success_count}/{total} 成功")
            return success_count == total

        except Exception as e:
            logger.error(f"导入股票基本信息异常: {e}")
            return False

    def import_constituent_info(self) -> bool:
        """导入成分股关联信息（保留历史记录，不清理旧数据）"""
        if not self.csi_a50_symbols:
            logger.error("无成分股数据可导入")
            return False

        try:
            connection = self.db_manager.db_connector.get_connection()
            cursor = connection.cursor()

            success_count = 0
            total = len(self.csi_a50_symbols)
            # 使用当前日期作为 start_date（更合理）
            from datetime import date
            start_date = date.today().isoformat()

            insert_sql = """
                INSERT INTO stock_index_constituent (
                    index_code, symbol, weight, start_date, end_date, is_current
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    weight = VALUES(weight),
                    start_date = VALUES(start_date),
                    end_date = VALUES(end_date),
                    is_current = VALUES(is_current),
                    updated_time = CURRENT_TIMESTAMP
            """

            for i, stock in enumerate(self.csi_a50_symbols, 1):
                try:
                    symbol = stock['symbol']
                    weight = float(stock.get('weight', 0.0))

                    cursor.execute(insert_sql, (
                        'CSI_A50',
                        symbol,
                        weight,
                        start_date,
                        None,
                        1
                    ))
                    success_count += 1
                    logger.debug(f"[{i}/{total}] 导入成分股关联: {symbol}")

                except Exception as e:
                    logger.error(f"导入成分股 {symbol} 失败: {e}")

            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"成分股关联信息导入完成: {success_count}/{total} 成功")
            return success_count == total

        except Exception as e:
            logger.error(f"导入成分股关联信息异常: {e}")
            return False

    def validate_import(self) -> Dict[str, Any]:
        """
        验证导入结果（正向验证：以 symbols.yaml 为准）
        不依赖 is_current 字段，不受历史数据干扰。
        """
        if not self.csi_a50_symbols:
            return {}

        expected_symbols = {stock['symbol'] for stock in self.csi_a50_symbols}
        expected_count = len(expected_symbols)
        symbol_list = list(expected_symbols)

        try:
            connection = self.db_manager.db_connector.get_connection()
            cursor = connection.cursor()

            result = {
                'tables_exist': {},
                'row_counts': {},
                'csi_a50_validation': {}
            }

            # 检查表是否存在
            tables = ['stock_basic_info', 'stock_daily_data', 'stock_minute_data',
                      'index_info', 'stock_index_constituent', 'data_update_log']
            for table in tables:
                result['tables_exist'][table] = self.db_manager.check_table_exists(table)

            # 统计各表总行数（仅作参考）
            for table in ['stock_basic_info', 'index_info', 'stock_index_constituent']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                result['row_counts'][table] = cursor.fetchone()[0]

            # 验证 index_info 是否存在
            cursor.execute("SELECT 1 FROM index_info WHERE index_code = 'CSI_A50'")
            result['csi_a50_validation']['index_info'] = cursor.fetchone() is not None

            # ✅ 正向验证 1: 所有配置中的 symbol 是否都在 stock_index_constituent 中（只要存在即可）
            placeholders = ','.join(['%s'] * expected_count)
            cursor.execute(f"""
                SELECT DISTINCT symbol 
                FROM stock_index_constituent 
                WHERE index_code = 'CSI_A50' AND symbol IN ({placeholders})
            """, symbol_list)
            found_in_constituent = {row[0] for row in cursor.fetchall()}
            missing_in_constituent = expected_symbols - found_in_constituent

            result['csi_a50_validation']['constituent_count'] = len(found_in_constituent)
            result['csi_a50_validation']['missing_in_constituent'] = sorted(missing_in_constituent)

            # ✅ 正向验证 2: 所有配置中的 symbol 是否都有 stock_basic_info
            cursor.execute(f"""
                SELECT DISTINCT symbol 
                FROM stock_basic_info 
                WHERE symbol IN ({placeholders})
            """, symbol_list)
            found_in_basic = {row[0] for row in cursor.fetchall()}
            missing_in_basic = expected_symbols - found_in_basic

            result['csi_a50_validation']['matched_basic_info'] = len(found_in_basic)
            result['csi_a50_validation']['missing_in_basic'] = sorted(missing_in_basic)

            cursor.close()
            connection.close()

            return result

        except Exception as e:
            logger.error(f"验证过程异常: {e}")
            return {}

    def run_full_import(self) -> bool:
        """执行完整导入流程"""
        logger.info("🚀 开始中证A50成分股导入流程...")

        steps = [
            ("指数信息", self.import_index_info),
            ("股票基本信息", self.import_stock_basic_info),
            ("成分股关联信息", self.import_constituent_info),
        ]

        for desc, func in steps:
            if not func():
                logger.error(f"❌ {desc} 导入失败")
                return False
            logger.info(f"✅ {desc} 导入成功")

        # 验证
        validation = self.validate_import()
        logger.info("🔍 导入结果验证:")

        v = validation.get('csi_a50_validation', {})
        logger.info(f"  - 指数信息存在: {v.get('index_info', False)}")
        logger.info(f"  - 成分股关联数: {v.get('constituent_count', 0)}/{len(self.csi_a50_symbols)}")
        logger.info(f"  - 基本信息匹配数: {v.get('matched_basic_info', 0)}/{len(self.csi_a50_symbols)}")

        missing_const = v.get('missing_in_constituent', [])
        missing_basic = v.get('missing_in_basic', [])

        if missing_const:
            logger.warning(f"  ⚠️  缺失成分股关联: {missing_const}")
        if missing_basic:
            logger.warning(f"  ⚠️  缺失基本信息: {missing_basic}")

        expected = len(self.csi_a50_symbols)
        success = (
            v.get('index_info', False)
            and v.get('constituent_count', 0) == expected
            and v.get('matched_basic_info', 0) == expected
        )

        if success:
            logger.info("🎉 中证A50成分股导入验证通过！")
            return True
        else:
            logger.error("💥 中证A50成分股导入验证失败！")
            return False


if __name__ == "__main__":
    importer = CSI_A50_Importer()
    if importer.run_full_import():
        print("✅ 中证A50成分股导入成功！")
    else:
        print("❌ 中证A50成分股导入失败，请检查日志。")