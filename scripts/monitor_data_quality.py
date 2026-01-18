# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\monitor_data_quality.py
# File Name: monitor_data_quality
# @ Author: mango-gh22
# @ Date：2026/1/18 14:22
"""
desc 
"""

# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\monitor_data_quality.py
"""
数据质量监控仪表盘
每日自动运行，生成质量报告
"""

import pandas as pd
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import matplotlib.pyplot as plt

from src.database.db_connector import DatabaseConnector


def monitor_data_quality():
    """监控数据质量并生成报告"""

    db = DatabaseConnector()
    report_dir = Path('reports/quality')
    report_dir.mkdir(parents=True, exist_ok=True)

    with db.get_connection() as conn:
        # 1. 计算核心指标
        quality_metrics = pd.read_sql("""
            SELECT 
                COUNT(DISTINCT symbol) as total_stocks,
                COUNT(*) as total_records,
                AVG(CASE WHEN pb IS NOT NULL THEN 1 ELSE 0 END) as pb_coverage,
                AVG(CASE WHEN pe_ttm IS NOT NULL THEN 1 ELSE 0 END) as pe_coverage,
                MAX(trade_date) as latest_date,
                COUNT(DISTINCT trade_date) as active_days
            FROM stock_daily_data
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        """, conn)

        # 2. 生成HTML报告
        html_content = f"""
        <html>
        <head><title>数据质量监控报告</title></head>
        <body>
            <h1>📊 股票数据库质量日报</h1>
            <p>报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

            <h2>核心指标</h2>
            <table border="1">
                <tr><th>指标</th><th>数值</th></tr>
                <tr><td>活跃股票数</td><td>{quality_metrics.iloc[0]['total_stocks']}</td></tr>
                <tr><td>总记录数</td><td>{quality_metrics.iloc[0]['total_records']:,}</td></tr>
                <tr><td>PB覆盖率</td><td>{quality_metrics.iloc[0]['pb_coverage'] * 100:.2f}%</td></tr>
                <tr><td>PE覆盖率</td><td>{quality_metrics.iloc[0]['pe_coverage'] * 100:.2f}%</td></tr>
                <tr><td>最新日期</td><td>{quality_metrics.iloc[0]['latest_date']}</td></tr>
            </table>

            <h2>风险提示</h2>
            <ul>
                <li>覆盖率低于95%的因子需要关注</li>
                <li>最新日期非交易日需要检查数据源</li>
            </ul>
        </body>
        </html>
        """

        # 3. 保存报告
        report_path = report_dir / f"quality_report_{datetime.now().strftime('%Y%m%d')}.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"✅ 质量报告已生成: {report_path}")


if __name__ == "__main__":
    monitor_data_quality()