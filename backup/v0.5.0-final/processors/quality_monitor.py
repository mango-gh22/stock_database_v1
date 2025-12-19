# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/processors\quality_monitor.py
# File Name: quality_monitor
# @ Author: mango-gh22
# @ Date：2025/12/14 15:37
"""
desc 
"""

# src/processors/quality_monitor.py
"""
质量监控器 - 自动化质量检查与报告
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import schedule
import time
import logging
from typing import Dict, List, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

from src.processors.validator import DataValidator
from src.processors.adjustor import StockAdjustor
from src.query.query_engine import QueryEngine

logger = logging.getLogger(__name__)


class QualityMonitor:
    """质量监控器"""

    def __init__(self, config_path: str = 'config/database.yaml'):
        """
        初始化质量监控器

        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.validator = DataValidator()
        self.adjustor = StockAdjustor()
        self.query_engine = QueryEngine()

        # 监控配置
        self.monitoring_interval = 3600  # 1小时
        self.alert_thresholds = {
            'error_count': 10,
            'warning_count': 50,
            'completeness_rate': 0.95,
            'consistency_rate': 0.98
        }

        # 报警配置
        self.alert_emails = []
        self.slack_webhook = None

        logger.info("质量监控器初始化完成")

    def run_daily_check(self):
        """运行每日质量检查"""
        logger.info("开始每日质量检查")

        check_time = datetime.now()
        report = {
            'timestamp': check_time.isoformat(),
            'checks': [],
            'summary': {},
            'alerts': []
        }

        try:
            # 1. 数据完整性检查
            logger.info("执行数据完整性检查")
            completeness_report = self._check_completeness()
            report['checks'].append({
                'type': 'completeness',
                'result': completeness_report
            })

            # 2. 业务规则验证
            logger.info("执行业务规则验证")
            business_rules_report = self._check_business_rules()
            report['checks'].append({
                'type': 'business_rules',
                'result': business_rules_report
            })

            # 3. 统计异常检测
            logger.info("执行统计异常检测")
            anomaly_report = self._detect_anomalies()
            report['checks'].append({
                'type': 'anomaly_detection',
                'result': anomaly_report
            })

            # 4. 复权因子验证
            logger.info("验证复权因子")
            adjustment_report = self._validate_adjustments()
            report['checks'].append({
                'type': 'adjustment_validation',
                'result': adjustment_report
            })

            # 生成摘要
            report['summary'] = self._generate_daily_summary(report['checks'])

            # 检查是否需要报警
            alerts = self._check_alerts(report['summary'])
            report['alerts'] = alerts

            # 保存报告
            self._save_daily_report(report)

            # 发送报警（如果需要）
            if alerts:
                self._send_alerts(alerts, report)

            logger.info(f"每日质量检查完成: {report['summary']}")

            return report

        except Exception as e:
            logger.error(f"每日质量检查失败: {e}")
            error_report = {
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'checks': []
            }
            self._save_daily_report(error_report)
            return error_report

    def _check_completeness(self) -> Dict:
        """检查数据完整性"""
        try:
            # 获取所有股票
            stock_df = self.query_engine.get_stock_list()
            symbols = stock_df['symbol'].tolist() if not stock_df.empty else []

            total_symbols = len(symbols)
            missing_data_symbols = []

            # 抽样检查
            sample_size = min(50, total_symbols)
            sample_symbols = np.random.choice(symbols, sample_size, replace=False) if symbols else []

            for symbol in sample_symbols:
                # 检查最近30天数据
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

                df = self.query_engine.query_daily_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )

                if df.empty or len(df) < 15:  # 假设至少应该有15个交易日的数据
                    missing_data_symbols.append(symbol)

            completeness_rate = 1 - (len(missing_data_symbols) / sample_size) if sample_size > 0 else 0

            return {
                'total_symbols': total_symbols,
                'sample_size': sample_size,
                'missing_data_count': len(missing_data_symbols),
                'completeness_rate': completeness_rate,
                'missing_symbols': missing_data_symbols[:10]  # 只显示前10个
            }

        except Exception as e:
            logger.error(f"完整性检查失败: {e}")
            return {'error': str(e)}

    def _check_business_rules(self) -> Dict:
        """检查业务规则"""
        try:
            # 使用验证器检查
            # 抽样检查几只股票
            stock_df = self.query_engine.get_stock_list()
            symbols = stock_df['symbol'].tolist() if not stock_df.empty else []

            sample_size = min(10, len(symbols))
            sample_symbols = np.random.choice(symbols, sample_size, replace=False) if symbols else []

            total_violations = 0
            detailed_violations = []

            for symbol in sample_symbols:
                results = self.validator.validate_business_logic(symbol)

                for result in results:
                    if result.result != result.result.PASS:
                        total_violations += result.affected_rows
                        detailed_violations.append({
                            'symbol': symbol,
                            'rule': result.rule_name,
                            'violations': result.affected_rows,
                            'description': result.rule_description
                        })

            return {
                'sample_size': sample_size,
                'total_violations': total_violations,
                'violation_rate': total_violations / (sample_size * 100) if sample_size > 0 else 0,
                'detailed_violations': detailed_violations[:5]  # 只显示前5个
            }

        except Exception as e:
            logger.error(f"业务规则检查失败: {e}")
            return {'error': str(e)}

    def _detect_anomalies(self) -> Dict:
        """检测异常"""
        try:
            # 使用验证器的统计异常检测
            stock_df = self.query_engine.get_stock_list()
            symbols = stock_df['symbol'].tolist() if not stock_df.empty else []

            sample_size = min(20, len(symbols))
            sample_symbols = np.random.choice(symbols, sample_size, replace=False) if symbols else []

            total_anomalies = 0
            anomaly_details = []

            for symbol in sample_symbols:
                results = self.validator.detect_statistical_anomalies(symbol)

                for result in results:
                    if result.result != result.result.PASS:
                        total_anomalies += result.affected_rows
                        anomaly_details.append({
                            'symbol': symbol,
                            'anomaly_type': result.rule_name,
                            'count': result.affected_rows,
                            'description': result.rule_description
                        })

            return {
                'sample_size': sample_size,
                'total_anomalies': total_anomalies,
                'anomaly_rate': total_anomalies / sample_size if sample_size > 0 else 0,
                'anomaly_details': anomaly_details[:5]
            }

        except Exception as e:
            logger.error(f"异常检测失败: {e}")
            return {'error': str(e)}

    def _validate_adjustments(self) -> Dict:
        """验证复权计算"""
        try:
            # 抽样验证几只股票的复权计算
            stock_df = self.query_engine.get_stock_list()
            symbols = stock_df['symbol'].tolist() if not stock_df.empty else []

            sample_size = min(5, len(symbols))
            sample_symbols = np.random.choice(symbols, sample_size, replace=False) if symbols else []

            validation_results = []

            for symbol in sample_symbols:
                result = self.adjustor.validate_adjustment(symbol)
                validation_results.append(result)

            valid_count = sum(1 for r in validation_results if r['has_factors'])

            return {
                'sample_size': sample_size,
                'valid_adjustments': valid_count,
                'validation_rate': valid_count / sample_size if sample_size > 0 else 0,
                'results': validation_results
            }

        except Exception as e:
            logger.error(f"复权验证失败: {e}")
            return {'error': str(e)}

    def _generate_daily_summary(self, checks: List) -> Dict:
        """生成每日摘要"""
        summary = {
            'total_checks': len(checks),
            'check_time': datetime.now().isoformat(),
            'metrics': {}
        }

        for check in checks:
            check_type = check['type']
            result = check['result']

            if 'error' not in result:
                if check_type == 'completeness':
                    summary['metrics']['completeness_rate'] = result.get('completeness_rate', 0)
                elif check_type == 'business_rules':
                    summary['metrics']['violation_rate'] = result.get('violation_rate', 0)
                elif check_type == 'anomaly_detection':
                    summary['metrics']['anomaly_rate'] = result.get('anomaly_rate', 0)
                elif check_type == 'adjustment_validation':
                    summary['metrics']['adjustment_validation_rate'] = result.get('validation_rate', 0)

        # 计算总体质量分数
        quality_score = np.mean(list(summary['metrics'].values())) if summary['metrics'] else 0
        summary['quality_score'] = quality_score

        return summary

    def _check_alerts(self, summary: Dict) -> List[Dict]:
        """检查是否需要报警"""
        alerts = []

        metrics = summary.get('metrics', {})

        # 检查完整性率
        completeness_rate = metrics.get('completeness_rate', 1.0)
        if completeness_rate < self.alert_thresholds['completeness_rate']:
            alerts.append({
                'type': 'completeness',
                'severity': 'WARNING' if completeness_rate > 0.9 else 'ERROR',
                'message': f'数据完整性率低: {completeness_rate:.2%}',
                'threshold': self.alert_thresholds['completeness_rate']
            })

        # 检查违规率
        violation_rate = metrics.get('violation_rate', 0)
        if violation_rate > 0.1:  # 10%违规率
            alerts.append({
                'type': 'business_rules',
                'severity': 'WARNING',
                'message': f'业务规则违规率高: {violation_rate:.2%}',
                'threshold': 0.1
            })

        return alerts

    def _save_daily_report(self, report: Dict):
        """保存每日报告"""
        try:
            # 保存为JSON文件
            report_dir = 'reports/quality'
            import os
            os.makedirs(report_dir, exist_ok=True)

            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{report_dir}/quality_report_{date_str}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            logger.info(f"质量报告已保存: {filename}")

            # 也可以保存到数据库
            self._save_report_to_db(report)

        except Exception as e:
            logger.error(f"保存质量报告失败: {e}")

    def _save_report_to_db(self, report: Dict):
        """保存报告到数据库"""
        try:
            query = """
                INSERT INTO data_quality_log 
                (check_type, check_date, check_result, error_message, 
                 affected_rows, severity_level, suggestion)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                'daily_summary',
                datetime.now().date(),
                'INFO',
                json.dumps(report.get('summary', {})),
                0,
                'INFO',
                'Daily quality check completed'
            )

            self.validator.db_connector.execute_query(query, params)

        except Exception as e:
            logger.error(f"保存报告到数据库失败: {e}")

    def _send_alerts(self, alerts: List[Dict], report: Dict):
        """发送报警"""
        if not self.alert_emails:
            return

        try:
            # 构建报警邮件
            subject = f"数据质量报警 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

            body = "数据质量检查发现以下问题:\n\n"
            for alert in alerts:
                body += f"• [{alert['severity']}] {alert['message']}\n"

            body += f"\n质量分数: {report.get('summary', {}).get('quality_score', 0):.2%}\n"
            body += f"检查时间: {report.get('timestamp', 'N/A')}\n"

            # 发送邮件（简化版）
            # 实际应用中需要配置SMTP服务器
            logger.info(f"需要发送报警: {subject}")
            logger.info(f"报警内容:\n{body}")

            # TODO: 实际邮件发送逻辑
            # self._send_email(subject, body)

        except Exception as e:
            logger.error(f"发送报警失败: {e}")

    def start_monitoring(self, interval_minutes: int = 60):
        """
        启动定时监控

        Args:
            interval_minutes: 监控间隔（分钟）
        """
        logger.info(f"启动质量监控，间隔{interval_minutes}分钟")

        # 立即运行一次
        self.run_daily_check()

        # 设置定时任务
        schedule.every(interval_minutes).minutes.do(self.run_daily_check)

        # 保持运行
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except KeyboardInterrupt:
                logger.info("监控已停止")
                break
            except Exception as e:
                logger.error(f"监控运行错误: {e}")
                time.sleep(300)  # 出错后等待5分钟

    def generate_weekly_report(self) -> Dict:
        """生成每周质量报告"""
        try:
            # 获取最近7天的报告
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)

            report_files = []
            report_dir = 'reports/quality'
            import os
            import glob

            if os.path.exists(report_dir):
                pattern = os.path.join(report_dir, 'quality_report_*.json')
                report_files = glob.glob(pattern)

            weekly_reports = []
            for file in report_files:
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                        weekly_reports.append(report)
                except:
                    continue

            # 分析周度趋势
            trend_analysis = self._analyze_weekly_trend(weekly_reports)

            weekly_report = {
                'period': {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                },
                'report_count': len(weekly_reports),
                'trend_analysis': trend_analysis,
                'recommendations': self._generate_recommendations(trend_analysis)
            }

            # 保存周报
            week_str = end_date.strftime('%Y%W')
            filename = f"{report_dir}/weekly_report_{week_str}.json"

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(weekly_report, f, indent=2, ensure_ascii=False)

            logger.info(f"周度质量报告已生成: {filename}")

            return weekly_report

        except Exception as e:
            logger.error(f"生成周度报告失败: {e}")
            return {'error': str(e)}

    def _analyze_weekly_trend(self, reports: List[Dict]) -> Dict:
        """分析周度趋势"""
        if not reports:
            return {'message': '没有足够的报告数据'}

        trends = {
            'quality_scores': [],
            'completeness_rates': [],
            'dates': []
        }

        for report in reports:
            summary = report.get('summary', {})
            metrics = summary.get('metrics', {})

            trends['quality_scores'].append(summary.get('quality_score', 0))
            trends['completeness_rates'].append(metrics.get('completeness_rate', 0))
            trends['dates'].append(report.get('timestamp', ''))

        # 计算趋势
        if len(trends['quality_scores']) > 1:
            quality_trend = '上升' if trends['quality_scores'][-1] > trends['quality_scores'][0] else '下降'
            completeness_trend = '上升' if trends['completeness_rates'][-1] > trends['completeness_rates'][0] else '下降'
        else:
            quality_trend = '稳定'
            completeness_trend = '稳定'

        return {
            'avg_quality_score': np.mean(trends['quality_scores']) if trends['quality_scores'] else 0,
            'avg_completeness_rate': np.mean(trends['completeness_rates']) if trends['completeness_rates'] else 0,
            'quality_trend': quality_trend,
            'completeness_trend': completeness_trend,
            'best_score': max(trends['quality_scores']) if trends['quality_scores'] else 0,
            'worst_score': min(trends['quality_scores']) if trends['quality_scores'] else 0
        }

    def _generate_recommendations(self, trend_analysis: Dict) -> List[str]:
        """生成改进建议"""
        recommendations = []

        avg_quality = trend_analysis.get('avg_quality_score', 0)
        avg_completeness = trend_analysis.get('avg_completeness_rate', 0)

        if avg_quality < 0.9:
            recommendations.append("整体数据质量有待提升，建议检查数据源和ETL流程")

        if avg_completeness < 0.95:
            recommendations.append("数据完整性不足，建议补充缺失数据或检查数据采集过程")

        if trend_analysis.get('quality_trend') == '下降':
            recommendations.append("数据质量呈下降趋势，需要立即调查原因")

        if not recommendations:
            recommendations.append("数据质量良好，继续保持当前维护流程")

        return recommendations

    def close(self):
        """关闭连接"""
        self.validator.close()
        self.adjustor.close()
        self.query_engine.close()
        logger.info("质量监控器连接已关闭")


def test_quality_monitor():
    """测试质量监控器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试质量监控器")
    print("=" * 50)

    monitor = QualityMonitor()

    try:
        # 运行每日检查
        print("\n📊 运行每日质量检查")
        daily_report = monitor.run_daily_check()

        if 'error' in daily_report:
            print(f"❌ 检查失败: {daily_report['error']}")
        else:
            print(f"✅ 检查完成: {len(daily_report['checks'])}项检查")

            summary = daily_report.get('summary', {})
            print(f"   质量分数: {summary.get('quality_score', 0):.2%}")
            print(f"   检查时间: {summary.get('check_time', 'N/A')}")

            if daily_report['alerts']:
                print(f"   发现 {len(daily_report['alerts'])} 个报警:")
                for alert in daily_report['alerts']:
                    print(f"   [{alert['severity']}] {alert['message']}")
            else:
                print("   无报警")

        # 生成周报
        print("\n📈 生成周度质量报告")
        weekly_report = monitor.generate_weekly_report()

        if 'error' in weekly_report:
            print(f"❌ 周报生成失败: {weekly_report['error']}")
        else:
            print(f"✅ 周报生成完成")
            period = weekly_report.get('period', {})
            print(f"   报告期间: {period.get('start', 'N/A')} 到 {period.get('end', 'N/A')}")
            print(f"   报告数量: {weekly_report.get('report_count', 0)}")

            trend = weekly_report.get('trend_analysis', {})
            print(f"   平均质量分数: {trend.get('avg_quality_score', 0):.2%}")
            print(f"   质量趋势: {trend.get('quality_trend', 'N/A')}")

            recommendations = weekly_report.get('recommendations', [])
            if recommendations:
                print(f"   改进建议:")
                for rec in recommendations:
                    print(f"   • {rec}")

        print("\n🎉 质量监控器测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        monitor.close()


if __name__ == "__main__":
    test_quality_monitor()
