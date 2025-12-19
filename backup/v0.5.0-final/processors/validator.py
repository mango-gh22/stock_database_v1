# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/processors\validator.py
# File Name: validator
# @ Author: mango-gh22
# @ Date：2025/12/14 15:35
"""
desc 数据验证模块 - 实现完整性检查、业务规则验证、异常检测
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union
import yaml
import logging
from dataclasses import dataclass
from enum import Enum

# 修正导入路径 - 根据实际项目结构
from src.query.query_engine import QueryEngine
from src.database.db_connector import DatabaseConnector

# 尝试导入块1的pipeline，如果不存在则定义替代
try:
    from src.data.data_pipeline import ProcessingPipeline, UpdateMode

    HAS_PIPELINE = True
except ImportError:
    # 定义替代的枚举
    class UpdateMode(Enum):
        """更新模式枚举"""
        FAST = "fast"
        STANDARD = "standard"
        FULL = "full"


    class ProcessingPipeline:
        """模拟处理流水线"""

        def __init__(self, config_path: str = 'config/processing.yaml'):
            self.config_path = config_path


    HAS_PIPELINE = False

logger = logging.getLogger(__name__)


class ValidationResult(Enum):
    """验证结果枚举"""
    PASS = "PASS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationRule:
    """验证规则数据类"""
    name: str
    description: str
    rule_type: str
    severity: str
    condition: Optional[str] = None
    sql: Optional[str] = None
    algorithm: Optional[str] = None
    threshold: Optional[float] = None
    is_active: bool = True


@dataclass
class ValidationResultDetail:
    """验证结果详情"""
    rule_name: str
    rule_description: str
    result: ValidationResult
    error_message: Optional[str] = None
    affected_rows: int = 0
    affected_symbols: List[str] = None
    suggestion: Optional[str] = None
    execution_time: float = 0.0

    def __post_init__(self):
        if self.affected_symbols is None:
            self.affected_symbols = []


class DataValidator:
    """数据验证器"""

    def __init__(self, config_path: str = 'config/quality_rules.yaml',
                 db_config_path: str = 'config/database.yaml'):
        """
        初始化数据验证器

        Args:
            config_path: 质量规则配置文件路径
            db_config_path: 数据库配置文件路径
        """
        self.config_path = config_path
        self.db_config_path = db_config_path
        self.rules = self._load_rules()
        self.query_engine = QueryEngine(db_config_path)
        self.db_connector = DatabaseConnector(db_config_path)
        logger.info(f"数据验证器初始化完成，加载 {len(self.rules)} 条规则")

    def _load_rules(self) -> Dict[str, List[ValidationRule]]:
        """从YAML文件加载质量规则"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            rules = {}
            for rule_type, rule_list in config.get('quality_rules', {}).items():
                rules[rule_type] = []
                for rule_config in rule_list:
                    rule = ValidationRule(
                        name=rule_config.get('name'),
                        description=rule_config.get('description', ''),
                        rule_type=rule_type,
                        severity=rule_config.get('severity', 'WARNING'),
                        condition=rule_config.get('condition'),
                        sql=rule_config.get('sql'),
                        algorithm=rule_config.get('algorithm'),
                        threshold=rule_config.get('threshold'),
                        is_active=True
                    )
                    rules[rule_type].append(rule)

            return rules

        except Exception as e:
            logger.error(f"加载质量规则失败: {e}")
            return {}

    def validate_completeness(self, symbol: str = None,
                              start_date: str = None,
                              end_date: str = None) -> List[ValidationResultDetail]:
        """
        数据完整性验证

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            验证结果列表
        """
        results = []
        completeness_rules = self.rules.get('completeness', [])

        for rule in completeness_rules:
            if not rule.is_active:
                continue

            start_time = datetime.now()
            try:
                if rule.sql:
                    # 执行SQL检查
                    query = rule.sql
                    params = []

                    # 添加符号和日期过滤
                    if symbol:
                        if "WHERE" in query.upper():
                            query += f" AND symbol = %s"
                        else:
                            query += f" WHERE symbol = %s"
                        params.append(symbol)

                    if start_date:
                        if "WHERE" in query.upper():
                            query += f" AND trade_date >= %s"
                        else:
                            query += f" WHERE trade_date >= %s"
                        params.append(start_date)

                    if end_date:
                        if "WHERE" in query.upper():
                            query += f" AND trade_date <= %s"
                        else:
                            query += f" WHERE trade_date <= %s"
                        params.append(end_date)

                    result = self.query_engine.execute_custom_query(query, tuple(params) if params else None)

                    if result.empty:
                        validation_result = ValidationResult.PASS
                        affected_rows = 0
                        affected_symbols = []
                        error_msg = None
                    else:
                        validation_result = getattr(ValidationResult, rule.severity)
                        affected_rows = len(result)
                        affected_symbols = result['symbol'].tolist() if 'symbol' in result.columns else []
                        error_msg = f"发现{affected_rows}条违反{rule.description}的记录"

                execution_time = (datetime.now() - start_time).total_seconds()

                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=validation_result,
                    error_message=error_msg,
                    affected_rows=affected_rows,
                    affected_symbols=affected_symbols,
                    suggestion="检查数据源完整性或重新导入数据",
                    execution_time=execution_time
                )

                results.append(result_detail)
                self._log_validation_result(result_detail, symbol)

            except Exception as e:
                logger.error(f"执行完整性规则{rule.name}失败: {e}")
                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=ValidationResult.ERROR,
                    error_message=str(e),
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
                results.append(result_detail)

        return results

    def validate_business_logic(self, symbol: str = None,
                                start_date: str = None,
                                end_date: str = None) -> List[ValidationResultDetail]:
        """
        业务逻辑验证

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            验证结果列表
        """
        results = []
        business_rules = self.rules.get('business_logic', [])

        if not business_rules:
            return results

        # 查询数据
        df = self.query_engine.query_daily_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            limit=10000  # 限制数量防止内存溢出
        )

        if df.empty:
            logger.warning("没有数据可用于业务逻辑验证")
            return results

        for rule in business_rules:
            if not rule.is_active:
                continue

            start_time = datetime.now()
            try:
                if rule.condition:
                    # 解析条件并应用
                    # 这里简化处理，实际应使用安全的表达式求值
                    condition = rule.condition

                    # 替换列名
                    column_mapping = {
                        'open_price': 'open',
                        'close_price': 'close',
                        'high_price': 'high',
                        'low_price': 'low',
                        'volume': 'volume',
                        'pct_change': 'pct_change'
                    }

                    for old_col, new_col in column_mapping.items():
                        condition = condition.replace(old_col, new_col)

                    # 检查数据框中是否存在这些列
                    try:
                        # 使用eval进行条件判断（注意安全性）
                        mask = df.eval(condition)
                        violations = df[~mask]

                        if violations.empty:
                            validation_result = ValidationResult.PASS
                            affected_rows = 0
                            affected_symbols = []
                            error_msg = None
                        else:
                            validation_result = getattr(ValidationResult, rule.severity)
                            affected_rows = len(violations)
                            affected_symbols = violations['symbol'].tolist() if 'symbol' in violations.columns else []
                            error_msg = f"发现{affected_rows}条违反{rule.description}的记录"

                    except Exception as e:
                        logger.warning(f"条件求值失败 {rule.condition}: {e}")
                        continue

                execution_time = (datetime.now() - start_time).total_seconds()

                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=validation_result,
                    error_message=error_msg,
                    affected_rows=affected_rows,
                    affected_symbols=affected_symbols,
                    suggestion="检查数据源或调整验证规则",
                    execution_time=execution_time
                )

                results.append(result_detail)
                self._log_validation_result(result_detail, symbol)

            except Exception as e:
                logger.error(f"执行业务规则{rule.name}失败: {e}")
                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=ValidationResult.ERROR,
                    error_message=str(e),
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
                results.append(result_detail)

        return results

    def detect_statistical_anomalies(self, symbol: str,
                                     start_date: str = None,
                                     end_date: str = None) -> List[ValidationResultDetail]:
        """
        统计异常检测

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            异常检测结果
        """
        results = []
        statistical_rules = self.rules.get('statistical', [])

        if not statistical_rules:
            return results

        # 查询数据
        df = self.query_engine.query_daily_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            limit=1000
        )

        if df.empty or len(df) < 10:
            logger.warning(f"数据不足进行统计异常检测: {symbol}")
            return results

        for rule in statistical_rules:
            if not rule.is_active:
                continue

            start_time = datetime.now()
            try:
                anomalies = []

                if rule.algorithm == 'z_score' and rule.threshold:
                    # Z-score异常检测
                    for column in ['close', 'volume', 'pct_change']:
                        if column in df.columns:
                            z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
                            anomaly_mask = z_scores > rule.threshold
                            anomalies.extend(df[anomaly_mask][['trade_date', column]].to_dict('records'))

                elif rule.algorithm == 'iqr' and rule.threshold:
                    # IQR异常检测
                    for column in ['close', 'volume', 'pct_change']:
                        if column in df.columns:
                            Q1 = df[column].quantile(0.25)
                            Q3 = df[column].quantile(0.75)
                            IQR = Q3 - Q1
                            lower_bound = Q1 - rule.threshold * IQR
                            upper_bound = Q3 + rule.threshold * IQR
                            anomaly_mask = (df[column] < lower_bound) | (df[column] > upper_bound)
                            anomalies.extend(df[anomaly_mask][['trade_date', column]].to_dict('records'))

                if anomalies:
                    validation_result = getattr(ValidationResult, rule.severity)
                    affected_rows = len(anomalies)
                    error_msg = f"发现{affected_rows}个统计异常点，使用{rule.algorithm}算法"
                else:
                    validation_result = ValidationResult.PASS
                    affected_rows = 0
                    error_msg = None

                execution_time = (datetime.now() - start_time).total_seconds()

                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=validation_result,
                    error_message=error_msg,
                    affected_rows=affected_rows,
                    affected_symbols=[symbol],
                    suggestion="检查是否为真实异常或数据错误",
                    execution_time=execution_time
                )

                results.append(result_detail)
                self._log_validation_result(result_detail, symbol)

                # 保存异常到数据库
                if anomalies:
                    self._save_anomalies_to_db(anomalies, rule.name, symbol, rule.algorithm)

            except Exception as e:
                logger.error(f"执行统计规则{rule.name}失败: {e}")
                result_detail = ValidationResultDetail(
                    rule_name=rule.name,
                    rule_description=rule.description,
                    result=ValidationResult.ERROR,
                    error_message=str(e),
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
                results.append(result_detail)

        return results

    def _log_validation_result(self, result: ValidationResultDetail, symbol: str = None):
        """记录验证结果到数据库"""
        try:
            query = """
                INSERT INTO data_quality_log 
                (check_type, symbol, check_date, rule_name, rule_description, 
                 check_result, error_message, affected_rows, severity_level, suggestion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                result.rule_description.split('检查')[0] if '检查' in result.rule_description else 'general',
                symbol,
                datetime.now().date(),
                result.rule_name,
                result.rule_description,
                result.result.value,
                result.error_message,
                result.affected_rows,
                result.result.value,
                result.suggestion
            )

            self.db_connector.execute_query(query, params)

        except Exception as e:
            logger.error(f"记录验证结果失败: {e}")

    def _save_anomalies_to_db(self, anomalies: List[dict], anomaly_type: str,
                              symbol: str, algorithm: str):
        """保存异常检测结果到数据库"""
        try:
            for anomaly in anomalies:
                query = """
                    INSERT INTO data_anomalies 
                    (anomaly_type, symbol, trade_date, field_name, 
                     actual_value, algorithm, confidence)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    actual_value = VALUES(actual_value),
                    algorithm = VALUES(algorithm),
                    confidence = VALUES(confidence)
                """

                # 提取字段名和值
                for field, value in anomaly.items():
                    if field != 'trade_date':
                        field_name = field
                        actual_value = value
                        break

                params = (
                    anomaly_type,
                    symbol,
                    anomaly.get('trade_date'),
                    field_name,
                    actual_value,
                    algorithm,
                    0.8  # 默认置信度
                )

                self.db_connector.execute_query(query, params)

        except Exception as e:
            logger.error(f"保存异常结果失败: {e}")

    def validate_all(self, symbol: str = None,
                     start_date: str = None,
                     end_date: str = None) -> Dict[str, List[ValidationResultDetail]]:
        """
        执行所有验证

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            按类型组织的验证结果
        """
        all_results = {}

        logger.info(f"开始全面验证: symbol={symbol}, date_range={start_date}~{end_date}")

        # 1. 完整性验证
        completeness_results = self.validate_completeness(symbol, start_date, end_date)
        all_results['completeness'] = completeness_results

        # 2. 业务逻辑验证
        business_results = self.validate_business_logic(symbol, start_date, end_date)
        all_results['business_logic'] = business_results

        # 3. 统计异常检测
        if symbol:  # 统计检测需要具体符号
            statistical_results = self.detect_statistical_anomalies(symbol, start_date, end_date)
            all_results['statistical'] = statistical_results

        # 生成摘要报告
        self._generate_summary_report(all_results, symbol)

        return all_results

    def _generate_summary_report(self, results: Dict[str, List[ValidationResultDetail]], symbol: str = None):
        """生成验证摘要报告"""
        total_rules = 0
        passed_rules = 0
        warnings = 0
        errors = 0

        for category, category_results in results.items():
            for result in category_results:
                total_rules += 1
                if result.result == ValidationResult.PASS:
                    passed_rules += 1
                elif result.result == ValidationResult.WARNING:
                    warnings += 1
                elif result.result in [ValidationResult.ERROR, ValidationResult.CRITICAL]:
                    errors += 1

        logger.info(f"验证摘要: 总数={total_rules}, 通过={passed_rules}, "
                    f"警告={warnings}, 错误={errors}")

        if errors > 0:
            logger.error(f"发现{errors}个严重错误，需要立即处理！")

        return {
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'warnings': warnings,
            'errors': errors,
            'pass_rate': passed_rules / total_rules if total_rules > 0 else 0
        }

    def get_validation_summary(self, days: int = 7) -> pd.DataFrame:
        """获取最近验证摘要"""
        try:
            query = """
                SELECT 
                    DATE(check_date) as check_date,
                    check_type,
                    check_result,
                    COUNT(*) as count,
                    SUM(affected_rows) as total_affected_rows
                FROM data_quality_log
                WHERE check_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                GROUP BY DATE(check_date), check_type, check_result
                ORDER BY check_date DESC, check_type
            """

            result = self.db_connector.execute_query(query, (days,))
            df = pd.DataFrame(result) if result else pd.DataFrame()

            return df

        except Exception as e:
            logger.error(f"获取验证摘要失败: {e}")
            return pd.DataFrame()

    def close(self):
        """关闭连接"""
        self.db_connector.close_all_connections()
        logger.info("数据验证器连接已关闭")


# 与块1 pipeline的集成
class ValidationPipeline(ProcessingPipeline):
    """验证流水线 - 继承自块1的ProcessingPipeline"""

    def __init__(self, config_path: str = 'config/processing.yaml'):
        super().__init__(config_path)
        self.validator = DataValidator()

    def run_validation(self, symbol: str = None, update_mode: UpdateMode = UpdateMode.STANDARD):
        """
        运行验证流水线

        Args:
            symbol: 股票代码
            update_mode: 更新模式
        """
        logger.info(f"启动验证流水线: symbol={symbol}, mode={update_mode.value}")

        # 确定日期范围
        end_date = datetime.now().strftime('%Y-%m-%d')

        if update_mode == UpdateMode.FAST:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        elif update_mode == UpdateMode.STANDARD:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        else:  # FULL
            start_date = None

        # 执行验证
        results = self.validator.validate_all(symbol, start_date, end_date)

        # 生成报告
        report = self._generate_validation_report(results)

        logger.info(f"验证流水线完成: {report}")

        return {
            'success': True,
            'results': results,
            'report': report,
            'symbol': symbol,
            'mode': update_mode.value
        }

    def _generate_validation_report(self, results: Dict[str, List[ValidationResultDetail]]) -> Dict:
        """生成验证报告"""
        summary = {}
        issues = []

        for category, category_results in results.items():
            summary[category] = {
                'total': len(category_results),
                'passed': sum(1 for r in category_results if r.result == ValidationResult.PASS),
                'warnings': sum(1 for r in category_results if r.result == ValidationResult.WARNING),
                'errors': sum(
                    1 for r in category_results if r.result in [ValidationResult.ERROR, ValidationResult.CRITICAL])
            }

            # 收集问题
            for result in category_results:
                if result.result != ValidationResult.PASS:
                    issues.append({
                        'category': category,
                        'rule': result.rule_name,
                        'severity': result.result.value,
                        'description': result.rule_description,
                        'affected_rows': result.affected_rows,
                        'message': result.error_message
                    })

        return {
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'issues': issues,
            'has_issues': len(issues) > 0
        }


def test_validator():
    """测试数据验证器"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🧪 测试数据验证器")
    print("=" * 50)

    validator = DataValidator()

    try:
        # 1. 获取股票列表测试
        print("\n📋 1. 获取股票列表")
        stock_df = validator.query_engine.get_stock_list()
        if not stock_df.empty:
            test_symbol = stock_df.iloc[0]['symbol']
            print(f"   测试股票: {test_symbol}")

            # 2. 完整性验证
            print("\n✅ 2. 完整性验证")
            completeness_results = validator.validate_completeness(test_symbol)
            for result in completeness_results:
                print(f"   {result.rule_name}: {result.result.value} ({result.affected_rows}条)")

            # 3. 业务逻辑验证
            print("\n🔍 3. 业务逻辑验证")
            business_results = validator.validate_business_logic(test_symbol)
            for result in business_results:
                print(f"   {result.rule_name}: {result.result.value} ({result.affected_rows}条)")

            # 4. 统计异常检测
            print("\n📊 4. 统计异常检测")
            statistical_results = validator.detect_statistical_anomalies(test_symbol)
            for result in statistical_results:
                print(f"   {result.rule_name}: {result.result.value} ({result.affected_rows}个异常点)")

            # 5. 全面验证
            print("\n🚀 5. 全面验证")
            all_results = validator.validate_all(test_symbol)
            summary = validator._generate_summary_report(all_results, test_symbol)
            print(f"   验证摘要: {summary}")

        print("\n🎉 数据验证器测试完成!")

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        validator.close()


if __name__ == "__main__":
    test_validator()