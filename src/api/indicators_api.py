# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/src/api\indicators_api.py
# File Name: indicators_api
# @ Author: mango-gh22
# @ Date：2025/12/20 19:21
"""
desc
技术指标计算API
提供RESTful API接口
"""
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import pandas as pd
from datetime import datetime, timedelta
import logging
import uvicorn

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))  # 添加项目根
from src.query.enhanced_query_engine import EnhancedQueryEngine

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(
    title="股票技术指标计算API",
    description="提供股票技术指标计算服务",
    version="1.0.0"
)


# 依赖项：查询引擎
def get_query_engine():
    """获取查询引擎实例"""
    return EnhancedQueryEngine()


# 数据模型
class IndicatorRequest(BaseModel):
    """指标计算请求"""
    symbol: str = Field(..., description="股票代码，如：sh600519")
    indicators: List[str] = Field(..., description="指标名称列表")
    start_date: str = Field(..., description="开始日期，格式：YYYY-MM-DD")
    end_date: str = Field(..., description="结束日期，格式：YYYY-MM-DD")
    use_cache: bool = Field(True, description="是否使用缓存")

    class Config:
        # schema_extra = {
        json_schema_extra = {
            "example": {
                "symbol": "sh600519",
                "indicators": ["moving_average", "macd", "rsi"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "use_cache": True
            }
        }


class ValidationRequest(BaseModel):
    """验证请求"""
    symbol: str = Field(..., description="股票代码")
    indicator: str = Field(..., description="指标名称")
    start_date: str = Field(..., description="开始日期")
    end_date: str = Field(..., description="结束日期")


# API端点
@app.get("/")
async def root():
    """API根目录"""
    return {
        "message": "股票技术指标计算API",
        "version": "1.0.0",
        "docs": "/docs",
        "available_endpoints": [
            "/indicators/available",
            "/indicators/calculate",
            "/indicators/validate",
            "/health"
        ]
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/indicators/available")
async def get_available_indicators(
        engine: EnhancedQueryEngine = Depends(get_query_engine)
):
    """
    获取所有可用指标

    Returns:
        可用指标列表和信息
    """
    try:
        indicators = engine.get_available_indicators()
        return {
            "count": len(indicators),
            "indicators": indicators
        }
    except Exception as e:
        logger.error(f"获取可用指标失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/indicators/calculate")
async def calculate_indicators(
        request: IndicatorRequest,
        engine: EnhancedQueryEngine = Depends(get_query_engine)
):
    """
    计算技术指标

    Args:
        request: 指标计算请求

    Returns:
        包含技术指标的数据
    """
    logger.info(f"计算指标请求: {request}")

    try:
        # 计算指标
        result_df = engine.query_with_indicators(
            symbol=request.symbol,
            indicators=request.indicators,
            start_date=request.start_date,
            end_date=request.end_date,
            use_cache=request.use_cache
        )

        if result_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"股票 {request.symbol} 在指定日期范围内无数据"
            )

        # 准备返回数据
        response_data = {
            "symbol": request.symbol,
            "indicators": request.indicators,
            "date_range": f"{request.start_date} - {request.end_date}",
            "data_count": len(result_df),
            "columns": result_df.columns.tolist(),
            "data": result_df.to_dict(orient="records")
        }

        # 添加统计信息
        numeric_cols = result_df.select_dtypes(include=['float64', 'int64']).columns
        if len(numeric_cols) > 0:
            response_data["statistics"] = result_df[numeric_cols].describe().to_dict()

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"计算指标失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/indicators/validate")
async def validate_indicator_calculation(
        request: ValidationRequest,
        engine: EnhancedQueryEngine = Depends(get_query_engine)
):
    """
    验证指标计算可行性

    Args:
        request: 验证请求

    Returns:
        验证结果
    """
    try:
        result = engine.validate_indicator_calculation(
            symbol=request.symbol,
            indicator_name=request.indicator,
            start_date=request.start_date,
            end_date=request.end_date
        )

        return result

    except Exception as e:
        logger.error(f"验证指标计算失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/indicators/calculate/batch")
async def calculate_batch_indicators(
        symbols: List[str] = Query(..., description="股票代码列表"),
        indicators: List[str] = Query(..., description="指标名称列表"),
        start_date: str = Query(..., description="开始日期"),
        end_date: str = Query(..., description="结束日期"),
        engine: EnhancedQueryEngine = Depends(get_query_engine)
):
    """
    批量计算技术指标

    Args:
        symbols: 股票代码列表
        indicators: 指标列表
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        批量计算结果
    """
    logger.info(f"批量计算指标: {len(symbols)} 只股票，{len(indicators)} 个指标")

    try:
        results = {}
        successful = 0
        failed = 0

        for symbol in symbols:
            try:
                result_df = engine.query_with_indicators(
                    symbol=symbol,
                    indicators=indicators,
                    start_date=start_date,
                    end_date=end_date
                )

                if not result_df.empty:
                    results[symbol] = {
                        "data_count": len(result_df),
                        "columns": result_df.columns.tolist(),
                        "data": result_df.to_dict(orient="records")
                    }
                    successful += 1
                else:
                    results[symbol] = {"error": "无数据"}
                    failed += 1

            except Exception as e:
                results[symbol] = {"error": str(e)}
                failed += 1

        return {
            "total_symbols": len(symbols),
            "successful": successful,
            "failed": failed,
            "results": results
        }

    except Exception as e:
        logger.error(f"批量计算失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 启动函数
def start_api_server(host: str = "127.0.0.1", port: int = 8000):
    """启动API服务器"""
    logger.info(f"启动技术指标API服务器: http://{host}:{port}")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )


if __name__ == "__main__":
    start_api_server()