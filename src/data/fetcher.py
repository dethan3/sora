"""
数据获取模块

负责通过 yfinance API 获取基金数据，包括：
- 批量获取基金实时价格
- 获取历史价格数据 (支持不同时间周期)
- 获取基金基本信息 (名称、类型等)
- 异常处理和重试机制
- API 限流处理
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import pandas as pd
from loguru import logger


@dataclass
class FundData:
    """基金数据类"""
    code: str
    name: str
    current_price: float
    previous_close: float
    change_percent: float
    volume: int
    market_cap: Optional[float] = None
    currency: str = "USD"
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()


@dataclass
class HistoricalData:
    """历史数据类"""
    code: str
    data: pd.DataFrame
    start_date: datetime
    end_date: datetime
    period: str
    
    def get_mean_price(self, days: int = None) -> float:
        """获取指定天数的平均价格"""
        if days and len(self.data) > days:
            return self.data['Close'].tail(days).mean()
        return self.data['Close'].mean()
    
    def get_volatility(self, days: int = None) -> float:
        """获取价格波动率"""
        if days and len(self.data) > days:
            return self.data['Close'].tail(days).std()
        return self.data['Close'].std()


class DataFetcher:
    """数据获取器"""
    
    def __init__(self, request_timeout: int = 10, max_retries: int = 3, 
                 batch_size: int = 10, rate_limit_delay: float = 0.1):
        """
        初始化数据获取器
        
        Args:
            request_timeout: 请求超时时间(秒)
            max_retries: 最大重试次数
            batch_size: 批处理大小
            rate_limit_delay: API限流延迟(秒)
        """
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        # 缓存基金信息，避免重复请求
        self._fund_info_cache: Dict[str, Dict[str, Any]] = {}
        self._last_request_time = 0
        
        logger.info(f"数据获取器初始化完成 - 超时:{request_timeout}s, 重试:{max_retries}次, 批大小:{batch_size}")
    
    def _apply_rate_limit(self):
        """应用API限流"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def _get_ticker_symbol(self, fund_code: str) -> str:
        """
        将基金代码转换为 yfinance 可识别的股票代码
        
        注意：yfinance 主要支持股票数据，基金数据可能有限
        这里需要根据实际情况调整代码转换逻辑
        """
        # 对于中国基金，可能需要添加后缀
        # 这里先简单处理，实际使用时需要根据基金类型调整
        
        # 如果是6位数字，可能是中国基金
        if fund_code.isdigit() and len(fund_code) == 6:
            # 尝试不同的后缀
            possible_suffixes = ['.SS', '.SZ', '']  # 上海、深圳、或无后缀
            return f"{fund_code}.SS"  # 默认上海
        
        return fund_code
    
    def get_fund_info(self, fund_code: str) -> Optional[Dict[str, Any]]:
        """
        获取基金基本信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            基金信息字典或None
        """
        # 检查缓存
        if fund_code in self._fund_info_cache:
            return self._fund_info_cache[fund_code]
        
        ticker_symbol = self._get_ticker_symbol(fund_code)
        
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                ticker = yf.Ticker(ticker_symbol)
                info = ticker.info
                
                if info and 'symbol' in info:
                    fund_info = {
                        'code': fund_code,
                        'symbol': ticker_symbol,
                        'name': info.get('longName', info.get('shortName', f'Fund-{fund_code}')),
                        'currency': info.get('currency', 'USD'),
                        'market_cap': info.get('marketCap'),
                        'sector': info.get('sector'),
                        'industry': info.get('industry'),
                        'last_update': datetime.now()
                    }
                    
                    # 缓存结果
                    self._fund_info_cache[fund_code] = fund_info
                    logger.debug(f"获取基金信息成功: {fund_code} - {fund_info['name']}")
                    return fund_info
                else:
                    logger.warning(f"基金信息为空: {fund_code}")
                    
            except Exception as e:
                logger.warning(f"获取基金信息失败 (尝试 {attempt + 1}/{self.max_retries}): {fund_code} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
        
        logger.error(f"获取基金信息最终失败: {fund_code}")
        return None
    
    def get_current_data(self, fund_code: str) -> Optional[FundData]:
        """
        获取基金当前数据
        
        Args:
            fund_code: 基金代码
            
        Returns:
            FundData对象或None
        """
        ticker_symbol = self._get_ticker_symbol(fund_code)
        
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                ticker = yf.Ticker(ticker_symbol)
                
                # 获取基本信息
                info = ticker.info
                if not info or 'symbol' not in info:
                    logger.warning(f"无法获取基金基本信息: {fund_code}")
                    continue
                
                # 获取最近的价格数据
                hist = ticker.history(period="2d")  # 获取最近2天数据
                if hist.empty:
                    logger.warning(f"无法获取价格历史: {fund_code}")
                    continue
                
                latest_data = hist.iloc[-1]
                previous_data = hist.iloc[-2] if len(hist) > 1 else latest_data
                
                current_price = float(latest_data['Close'])
                previous_close = float(previous_data['Close'])
                change_percent = ((current_price - previous_close) / previous_close) * 100
                
                fund_data = FundData(
                    code=fund_code,
                    name=info.get('longName', info.get('shortName', f'Fund-{fund_code}')),
                    current_price=current_price,
                    previous_close=previous_close,
                    change_percent=change_percent,
                    volume=int(latest_data.get('Volume', 0)),
                    market_cap=info.get('marketCap'),
                    currency=info.get('currency', 'USD'),
                    last_update=datetime.now()
                )
                
                logger.debug(f"获取当前数据成功: {fund_code} - ¥{current_price:.4f} ({change_percent:+.2f}%)")
                return fund_data
                
            except Exception as e:
                logger.warning(f"获取当前数据失败 (尝试 {attempt + 1}/{self.max_retries}): {fund_code} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"获取当前数据最终失败: {fund_code}")
        return None
    
    def get_historical_data(self, fund_code: str, period: str = "60d") -> Optional[HistoricalData]:
        """
        获取历史数据
        
        Args:
            fund_code: 基金代码
            period: 时间周期 ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
                   或具体天数如 "60d"
            
        Returns:
            HistoricalData对象或None
        """
        ticker_symbol = self._get_ticker_symbol(fund_code)
        
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                ticker = yf.Ticker(ticker_symbol)
                hist = ticker.history(period=period)
                
                if hist.empty:
                    logger.warning(f"历史数据为空: {fund_code}")
                    continue
                
                historical_data = HistoricalData(
                    code=fund_code,
                    data=hist,
                    start_date=hist.index[0].to_pydatetime(),
                    end_date=hist.index[-1].to_pydatetime(),
                    period=period
                )
                
                logger.debug(f"获取历史数据成功: {fund_code} - {len(hist)}条记录 ({period})")
                return historical_data
                
            except Exception as e:
                logger.warning(f"获取历史数据失败 (尝试 {attempt + 1}/{self.max_retries}): {fund_code} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"获取历史数据最终失败: {fund_code}")
        return None
    
    def batch_get_current_data(self, fund_codes: List[str]) -> Dict[str, FundData]:
        """
        批量获取当前数据
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            基金代码到FundData的映射
        """
        results = {}
        
        # 分批处理
        batches = [fund_codes[i:i + self.batch_size] 
                  for i in range(0, len(fund_codes), self.batch_size)]
        
        logger.info(f"开始批量获取 {len(fund_codes)} 只基金数据，分 {len(batches)} 批处理")
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"处理第 {batch_idx + 1}/{len(batches)} 批 ({len(batch)} 只基金)")
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=min(len(batch), 5)) as executor:
                future_to_code = {
                    executor.submit(self.get_current_data, code): code 
                    for code in batch
                }
                
                for future in as_completed(future_to_code):
                    code = future_to_code[future]
                    try:
                        fund_data = future.result()
                        if fund_data:
                            results[code] = fund_data
                        else:
                            logger.warning(f"基金数据获取失败: {code}")
                    except Exception as e:
                        logger.error(f"处理基金数据时出错: {code} - {e}")
            
            # 批次间延迟，避免API限流
            if batch_idx < len(batches) - 1:
                time.sleep(1)
        
        success_count = len(results)
        logger.info(f"批量获取完成: {success_count}/{len(fund_codes)} 只基金成功")
        
        return results
    
    def batch_get_historical_data(self, fund_codes: List[str], 
                                period: str = "60d") -> Dict[str, HistoricalData]:
        """
        批量获取历史数据
        
        Args:
            fund_codes: 基金代码列表
            period: 时间周期
            
        Returns:
            基金代码到HistoricalData的映射
        """
        results = {}
        
        logger.info(f"开始批量获取 {len(fund_codes)} 只基金历史数据 ({period})")
        
        # 使用线程池并发处理，但限制并发数避免API限流
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_code = {
                executor.submit(self.get_historical_data, code, period): code 
                for code in fund_codes
            }
            
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    historical_data = future.result()
                    if historical_data:
                        results[code] = historical_data
                    else:
                        logger.warning(f"历史数据获取失败: {code}")
                except Exception as e:
                    logger.error(f"处理历史数据时出错: {code} - {e}")
        
        success_count = len(results)
        logger.info(f"批量历史数据获取完成: {success_count}/{len(fund_codes)} 只基金成功")
        
        return results
    
    def _format_ticker_symbol(self, fund_code: str) -> str:
        """
        格式化基金代码为 yfinance 可识别的格式
        
        Args:
            fund_code: 原始基金代码
            
        Returns:
            格式化后的代码
        """
        # 中国基金代码格式化规则
        if len(fund_code) == 6 and fund_code.isdigit():
            # 6位数字代码，中国基金
            # 基金代码规则：
            # 110xxx, 160xxx, 161xxx, 162xxx, 163xxx, 164xxx, 165xxx -> 上海 (.SS)
            # 000xxx, 001xxx, 002xxx, 003xxx, 004xxx, 005xxx, 006xxx, 007xxx, 008xxx, 009xxx -> 深圳 (.SZ)
            # 270xxx, 519xxx -> 上海 (.SS)
            
            if (fund_code.startswith(('110', '160', '161', '162', '163', '164', '165', '270', '519')) or
                fund_code.startswith('1')):
                return f"{fund_code}.SS"
            elif fund_code.startswith(('000', '001', '002', '003', '004', '005', '006', '007', '008', '009')):
                return f"{fund_code}.SZ"
            else:
                # 默认尝试上海
                return f"{fund_code}.SS"
        
        return fund_code
    
    def get_data_summary(self, fund_codes: List[str]) -> Dict[str, Any]:
        """
        获取数据获取摘要
        
{{ ... }}
            fund_codes: 基金代码列表
            
        Returns:
            摘要信息
        """
        valid_codes = []
        invalid_codes = []
        
        for code in fund_codes:
            if self.validate_fund_code(code):
                valid_codes.append(code)
            else:
                invalid_codes.append(code)
        
        return {
            'total_funds': len(fund_codes),
            'valid_funds': len(valid_codes),
            'invalid_funds': len(invalid_codes),
            'valid_codes': valid_codes,
            'invalid_codes': invalid_codes,
            'cache_size': len(self._fund_info_cache)
        }
