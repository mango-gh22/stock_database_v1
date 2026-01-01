/**
 * 股票技术指标API客户端 - 自动生成
 */

class StockIndicatorClient {
    /**
     * 初始化客户端
     * @param {string} baseUrl - API基础URL
     * @param {number} timeout - 请求超时时间（毫秒）
     */
    constructor(baseUrl = 'http://127.0.0.1:8000', timeout = 30000) {
        this.baseUrl = baseUrl.replace(/\/$/, '');
        this.timeout = timeout;
    }

    /**
     * 发送请求
     * @private
     */
    async _makeRequest(method, endpoint, data = null, params = null) {
        const url = new URL(`${this.baseUrl}${endpoint}`);

        if (params) {
            Object.keys(params).forEach(key => {
                url.searchParams.append(key, params[key]);
            });
        }

        const options = {
            method: method,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout: this.timeout
        };

        if (data && (method === 'POST' || method === 'PUT')) {
            options.body = JSON.stringify(data);
        }

        try {
            const response = await fetch(url, options);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();

        } catch (error) {
            throw new Error(`API请求失败: ${error.message}`);
        }
    }

    /**
     * 获取可用指标
     * @returns {Promise<Object>}
     */
    async getAvailableIndicators() {
        return await this._makeRequest('GET', '/indicators/available');
    }

    /**
     * 计算技术指标
     * @param {string} symbol - 股票代码
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @param {boolean} useCache - 是否使用缓存
     * @returns {Promise<Object>}
     */
    async calculateIndicators(symbol, indicators, startDate, endDate, useCache = true) {
        const data = {
            symbol: symbol,
            indicators: indicators,
            start_date: startDate,
            end_date: endDate,
            use_cache: useCache
        };

        return await this._makeRequest('POST', '/indicators/calculate', data);
    }

    /**
     * 异步计算指标
     * @param {string} symbol - 股票代码
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @param {Object} parameters - 指标参数
     * @returns {Promise<string>} 任务ID
     */
    async calculateIndicatorsAsync(symbol, indicators, startDate, endDate, parameters = null) {
        const data = {
            symbol: symbol,
            indicators: indicators,
            start_date: startDate,
            end_date: endDate
        };

        if (parameters) {
            data.parameters = parameters;
        }

        const response = await this._makeRequest('POST', '/async/calculate', data);
        return response.task_id;
    }

    /**
     * 获取异步任务状态
     * @param {string} taskId - 任务ID
     * @returns {Promise<Object>}
     */
    async getAsyncTaskStatus(taskId) {
        return await this._makeRequest('GET', `/async/task/${taskId}`);
    }

    /**
     * 获取异步任务结果
     * @param {string} taskId - 任务ID
     * @param {boolean} wait - 是否等待任务完成
     * @param {number} timeout - 等待超时时间（秒）
     * @param {number} pollInterval - 轮询间隔（秒）
     * @returns {Promise<Object>}
     */
    async getAsyncTaskResult(taskId, wait = false, timeout = 60, pollInterval = 1) {
        if (!wait) {
            return await this._makeRequest('GET', `/async/task/${taskId}/result`);
        }

        const startTime = Date.now();
        const timeoutMs = timeout * 1000;
        const pollIntervalMs = pollInterval * 1000;

        while (Date.now() - startTime < timeoutMs) {
            const status = await this.getAsyncTaskStatus(taskId);

            if (status.status === 'completed') {
                return await this._makeRequest('GET', `/async/task/${taskId}/result`);
            } else if (status.status === 'failed') {
                throw new Error(`任务失败: ${status.error || '未知错误'}`);
            }

            await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
        }

        throw new Error(`等待任务超时: ${taskId}`);
    }

    /**
     * 批量计算指标
     * @param {string[]} symbols - 股票代码列表
     * @param {string[]} indicators - 指标列表
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @returns {Promise<Object>}
     */
    async batchCalculate(symbols, indicators, startDate, endDate) {
        const params = {
            symbols: symbols.join(','),
            indicators: indicators.join(','),
            start_date: startDate,
            end_date: endDate
        };

        return await this._makeRequest('GET', '/indicators/calculate/batch', null, params);
    }

    /**
     * 验证指标计算可行性
     * @param {string} symbol - 股票代码
     * @param {string} indicator - 指标名称
     * @param {string} startDate - 开始日期
     * @param {string} endDate - 结束日期
     * @returns {Promise<Object>}
     */
    async validateCalculation(symbol, indicator, startDate, endDate) {
        const data = {
            symbol: symbol,
            indicator: indicator,
            start_date: startDate,
            end_date: endDate
        };

        return await this._makeRequest('POST', '/indicators/validate', data);
    }

    /**
     * 健康检查
     * @returns {Promise<boolean>}
     */
    async healthCheck() {
        try {
            const response = await this._makeRequest('GET', '/health');
            return response.status === 'healthy';
        } catch {
            return false;
        }
    }
}

// 使用示例
async function example() {
    // 创建客户端
    const client = new StockIndicatorClient('http://127.0.0.1:8000');

    try {
        // 健康检查
        const healthy = await client.healthCheck();
        console.log(healthy ? '✅ API服务正常' : '❌ API服务异常');

        if (!healthy) return;

        // 获取可用指标
        const indicators = await client.getAvailableIndicators();
        console.log(`可用指标数量: ${indicators.count || 0}`);

        // 计算指标
        const result = await client.calculateIndicators(
            'sh600519',
            ['moving_average', 'rsi'],
            '2024-01-01',
            '2024-01-31'
        );

        if (result.success) {
            console.log(`✅ 计算成功，数据量: ${result.data_count || 0}`);
        } else {
            console.log(`❌ 计算失败: ${result.error || '未知错误'}`);
        }

    } catch (error) {
        console.error('示例执行失败:', error.message);
    }
}

// 导出客户端
if (typeof module !== 'undefined' && module.exports) {
    module.exports = StockIndicatorClient;
} else if (typeof window !== 'undefined') {
    window.StockIndicatorClient = StockIndicatorClient;
}
