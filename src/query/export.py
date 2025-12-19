# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/query\export.py
# File Name: export
# @ Author: m_mango
# @ Date：2025/12/6 16:28
"""
desc 数据导出模块
"""

"""
数据导出模块 - v0.4.0
功能：将查询结果导出为CSV、Excel、JSON格式
"""

import pandas as pd
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataExporter:
    """数据导出器"""

    def __init__(self, export_dir: str = "data/exports"):
        """
        初始化数据导出器

        Args:
            export_dir: 导出目录
        """
        self.export_dir = export_dir
        os.makedirs(export_dir, exist_ok=True)
        self.logger = get_logger(__name__)

    def export_to_csv(self,
                      data: pd.DataFrame,
                      filename: str = None,
                      index: bool = True) -> str:
        """
        导出为CSV文件

        Args:
            data: 要导出的DataFrame
            filename: 文件名（不含扩展名）
            index: 是否包含索引

        Returns:
            str: 文件路径
        """
        if data.empty:
            self.logger.warning("数据为空，跳过导出")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}"

        filepath = os.path.join(self.export_dir, f"{filename}.csv")

        try:
            data.to_csv(filepath, index=index, encoding='utf-8-sig')
            self.logger.info(f"数据已导出到CSV: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"导出CSV失败: {e}")
            return None

    def export_to_excel(self,
                        data_dict: Dict[str, pd.DataFrame],
                        filename: str = None) -> str:
        """
        导出为Excel文件（多工作表）

        Args:
            data_dict: 工作表名到DataFrame的映射
            filename: 文件名（不含扩展名）

        Returns:
            str: 文件路径
        """
        if not data_dict:
            self.logger.warning("数据字典为空，跳过导出")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}"

        filepath = os.path.join(self.export_dir, f"{filename}.xlsx")

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    # 限制工作表名长度
                    safe_sheet_name = sheet_name[:31]  # Excel工作表名最大31字符
                    if not df.empty:
                        df.to_excel(writer, sheet_name=safe_sheet_name, index=True)
                    else:
                        pd.DataFrame(['No data available']).to_excel(
                            writer, sheet_name=safe_sheet_name, index=False
                        )

            self.logger.info(f"数据已导出到Excel: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"导出Excel失败: {e}")
            return None

    def export_to_json(self,
                       data: pd.DataFrame,
                       filename: str = None,
                       orient: str = 'records',
                       date_format: str = 'iso') -> str:
        """
        导出为JSON文件

        Args:
            data: 要导出的DataFrame
            filename: 文件名（不含扩展名）
            orient: JSON格式 ('records', 'split', 'index', 'table', 'values')
            date_format: 日期格式

        Returns:
            str: 文件路径
        """
        if data.empty:
            self.logger.warning("数据为空，跳过导出")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}"

        filepath = os.path.join(self.export_dir, f"{filename}.json")

        try:
            # 转换日期列为字符串
            if date_format == 'iso':
                for col in data.select_dtypes(include=['datetime64']).columns:
                    data[col] = data[col].dt.strftime('%Y-%m-%d')

            json_str = data.to_json(orient=orient, date_format=date_format)

            # 美化JSON输出
            parsed = json.loads(json_str)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)

            self.logger.info(f"数据已导出到JSON: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"导出JSON失败: {e}")
            return None

    def export_stock_analysis(self,
                              query_engine,
                              symbol: str,
                              start_date: str,
                              end_date: str,
                              export_format: str = 'all') -> Dict[str, str]:
        """
        导出股票分析报告

        Args:
            query_engine: 查询引擎实例
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            export_format: 导出格式 ('csv', 'excel', 'json', 'all')

        Returns:
            Dict: 导出文件路径字典
        """
        # 获取原始数据
        data = query_engine.get_daily_data(symbol, start_date, end_date)
        if data.empty:
            self.logger.warning(f"股票{symbol}在指定日期范围内无数据")
            return {}

        # 设置日期索引
        if 'trade_date' in data.columns:
            data = data.set_index('trade_date')

        # 计算技术指标
        from src.query.indicators import TechnicalIndicators
        indicators_data = TechnicalIndicators.calculate_all_indicators(data)

        # 计算分析指标
        from src.query.analytics import StockAnalytics
        returns_data = StockAnalytics.calculate_returns(data, period=1)
        volatility_data = StockAnalytics.calculate_volatility(returns_data)

        # 分析报告
        analysis_report = StockAnalytics.analyze_stock_performance(data)

        # 准备导出数据
        export_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{symbol}_{start_date}_{end_date}_{timestamp}"

        # 导出数据字典
        data_dict = {
            '原始数据': data,
            '技术指标': indicators_data,
            '收益率': returns_data,
            '波动率': volatility_data
        }

        # 根据格式导出
        if export_format in ['csv', 'all']:
            # 分别导出每个数据表为CSV
            for sheet_name, df in data_dict.items():
                if not df.empty:
                    csv_file = self.export_to_csv(
                        df,
                        f"{base_filename}_{sheet_name}",
                        index=True
                    )
                    if csv_file:
                        export_files[f'csv_{sheet_name}'] = csv_file

        if export_format in ['excel', 'all']:
            # 导出为多工作表Excel
            excel_file = self.export_to_excel(data_dict, base_filename)
            if excel_file:
                export_files['excel'] = excel_file

        if export_format in ['json', 'all']:
            # 导出分析报告为JSON
            report_df = pd.DataFrame([analysis_report])
            json_file = self.export_to_json(report_df, f"{base_filename}_分析报告")
            if json_file:
                export_files['json_report'] = json_file

        return export_files