"""
数据获取模块

负责通过 AKShare API 获取基金数据，包括：
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
import akshare as ak
import pandas as pd
from loguru import logger
from pathlib import Path

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
    
    def __init__(self, fund_codes: List[str] = None, request_timeout: int = 10, 
                 max_retries: int = 3, batch_size: int = 10, rate_limit_delay: float = 0.1,
                 cache_dir: str = "data/cache"):
        """
        初始化数据获取器
        
        Args:
            fund_codes: 需要监控的基金代码列表，如果为None则监控所有基金
            request_timeout: 请求超时时间(秒)
            max_retries: 最大重试次数
            batch_size: 批处理大小
            rate_limit_delay: API限流延迟(秒)
            cache_dir: 缓存目录
        """
        self.fund_codes = fund_codes
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        # 设置缓存目录
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存基金信息，避免重复请求
        self._fund_info_cache: Dict[str, Dict[str, Any]] = {}
        self._last_request_time = 0
        
        # 缓存ETF列表数据，避免频繁全量获取
        self._etf_list_cache: Optional[pd.DataFrame] = None
        self._etf_list_cache_time: Optional[datetime] = None
        self._etf_cache_expire_hours = 6  # ETF列表缓存6小时
        
        logger.info(f"数据获取器初始化完成 - 超时:{request_timeout}s, 重试:{max_retries}次, 批大小:{batch_size}")
    
    def _apply_rate_limit(self):
        """应用API限流"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def _validate_fund_code(self, fund_code: str) -> bool:
        """
        验证基金代码格式
        
        Args:
            fund_code: 基金代码
            
        Returns:
            是否为有效的基金代码
        """
        # 中国基金代码通常为6位数字
        if fund_code.isdigit() and len(fund_code) == 6:
            return True
        return False
    
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
        
        if not self._validate_fund_code(fund_code):
            logger.warning(f"无效的基金代码格式: {fund_code}")
            return None
        
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                # 使用 AKShare 获取基金基本信息
                fund_info_df = ak.fund_individual_basic_info_xq(symbol=fund_code)
                
                if fund_info_df is not None and not fund_info_df.empty:
                    # 将 DataFrame 转换为字典
                    info_dict = dict(zip(fund_info_df['item'], fund_info_df['value']))
                    
                    fund_info = {
                        'code': fund_code,
                        'name': info_dict.get('基金名称', f'Fund-{fund_code}'),
                        'full_name': info_dict.get('基金全称', ''),
                        'fund_type': info_dict.get('基金类型', ''),
                        'currency': 'CNY',  # 中国基金默认人民币
                        'fund_company': info_dict.get('基金公司', ''),
                        'fund_manager': info_dict.get('基金经理', ''),
                        'establishment_date': info_dict.get('成立时间', ''),
                        'scale': info_dict.get('最新规模', ''),
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
        获取基金当前数据 - 优化版本，使用缓存避免重复获取全量数据
        
        Args:
            fund_code: 基金代码
            
        Returns:
            FundData对象或None
        """
        if not self._validate_fund_code(fund_code):
            logger.warning(f"无效的基金代码格式: {fund_code}")
            return None
        
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                # 首先尝试从缓存的ETF数据中获取
                try:
                    etf_df = self._get_etf_list_cached()
                    if etf_df is not None:
                        fund_row = etf_df[etf_df['代码'] == fund_code]
                        
                        if not fund_row.empty:
                            row = fund_row.iloc[0]
                            current_price = float(row['最新价'])
                            previous_close = float(row['昨收'])
                            change_percent = float(row['涨跌幅'])
                            volume = int(row['成交量']) if pd.notna(row['成交量']) else 0
                            
                            # 获取基金名称
                            fund_name = row['名称']
                            
                            fund_data = FundData(
                                code=fund_code,
                                name=fund_name,
                                current_price=current_price,
                                previous_close=previous_close,
                                change_percent=change_percent,
                                volume=volume,
                                market_cap=None,  # ETF数据中可能没有市值信息
                                currency='CNY',
                                last_update=datetime.now()
                            )
                            
                            logger.debug(f"获取ETF当前数据成功: {fund_code} - ¥{current_price:.4f} ({change_percent:+.2f}%)")
                            return fund_data
                
                except Exception as etf_error:
                    logger.debug(f"缓存ETF数据获取失败，尝试其他方式: {fund_code} - {etf_error}")
                
                # 如果ETF获取失败，尝试获取基金基本信息作为备选
                fund_info = self.get_fund_info(fund_code)
                if fund_info:
                    # 创建一个基础的FundData对象
                    fund_data = FundData(
                        code=fund_code,
                        name=fund_info['name'],
                        current_price=0.0,  # 需要从其他接口获取
                        previous_close=0.0,
                        change_percent=0.0,
                        volume=0,
                        market_cap=None,
                        currency='CNY',
                        last_update=datetime.now()
                    )
                    
                    logger.warning(f"仅获取到基金基本信息，价格数据待补充: {fund_code}")
                    return fund_data
                
            except Exception as e:
                logger.warning(f"获取当前数据失败 (尝试 {attempt + 1}/{self.max_retries}): {fund_code} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"获取当前数据最终失败: {fund_code}")
        return None
    
    def _get_cached_historical_data(self, fund_code: str, period: str) -> Optional[pd.DataFrame]:
        """
        从缓存获取历史数据
        
        Args:
            fund_code: 基金代码
            period: 时间周期
            
        Returns:
            缓存的DataFrame或None
        """
        cache_key = f"{fund_code}_{period}"
        cache_file = self.cache_dir / f"historical/{cache_key}.parquet"
        
        if cache_file.exists():
            try:
                # 检查缓存是否过期（默认缓存1天）
                cache_age = time.time() - cache_file.stat().st_mtime
                if cache_age < 86400:  # 24小时
                    df = pd.read_parquet(cache_file)
                    logger.debug(f"从缓存加载历史数据: {fund_code} ({period})")
                    return df
            except Exception as e:
                logger.warning(f"读取缓存失败: {cache_file} - {e}")
        return None
    
    def _save_historical_data_to_cache(self, fund_code: str, period: str, df: pd.DataFrame) -> bool:
        """保存历史数据到缓存"""
        try:
            cache_file = self.cache_dir / f"historical/{fund_code}_{period}.parquet"
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache_file, index=True)
            return True
        except Exception as e:
            logger.warning(f"保存缓存失败: {e}")
            return False
    
    def get_historical_data(self, fund_code: str, period: str = "60d") -> Optional[HistoricalData]:
        """
        获取历史数据（带缓存）
        
        Args:
            fund_code: 基金代码
            period: 时间周期 ("7d", "30d", "60d", "90d", "180d", "1y")
            
        Returns:
            HistoricalData对象或None
        """
        if not self._validate_fund_code(fund_code):
            logger.warning(f"无效的基金代码格式: {fund_code}")
            return None
            
        # 尝试从缓存获取
        cached_data = self._get_cached_historical_data(fund_code, period)
        if cached_data is not None:
            return HistoricalData(
                code=fund_code,
                data=cached_data,
                start_date=cached_data.index[0].to_pydatetime(),
                end_date=cached_data.index[-1].to_pydatetime(),
                period=period
            )
            
        # 计算日期范围
        end_date = datetime.now()
        if period.endswith('d'):
            days = int(period[:-1])
            start_date = end_date - timedelta(days=days)
        elif period == '1y':
            start_date = end_date - timedelta(days=365)
        else:
            start_date = end_date - timedelta(days=60)
            
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                
                # 使用AKShare获取ETF历史分时数据
                hist_df = ak.fund_etf_hist_min_em(
                    symbol=fund_code,
                    period="5",
                    adjust="qfq",
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d")
                )
                
                if hist_df is not None and not hist_df.empty:
                    # 数据清洗和格式化
                    hist_df = self._process_historical_data(hist_df)
                    
                    # 保存到缓存
                    self._save_historical_data_to_cache(fund_code, period, hist_df)
                    
                    return HistoricalData(
                        code=fund_code,
                        data=hist_df,
                        start_date=hist_df.index[0].to_pydatetime(),
                        end_date=hist_df.index[-1].to_pydatetime(),
                        period=period
                    )
                    
            except Exception as e:
                logger.warning(f"获取历史数据失败 (尝试 {attempt + 1}/{self.max_retries}): {fund_code} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"获取历史数据最终失败: {fund_code}")
        return None
        
    def _process_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理历史数据，统一格式和类型"""
        # 重命名列
        df = df.rename(columns={
            '时间': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume'
        })
        
        # 设置时间索引
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # 转换数据类型
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
            
        return df
    
    def batch_get_current_data(self, fund_codes: List[str]) -> Dict[str, FundData]:
        """
        批量获取当前数据 - 优化版本，一次获取ETF列表后批量处理
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            基金代码到FundData的映射
        """
        results = {}
        
        logger.info(f"开始批量获取 {len(fund_codes)} 只基金数据（优化版本）")
        
        try:
            # 一次性获取ETF列表数据（使用缓存）
            etf_df = self._get_etf_list_cached()
            if etf_df is None:
                logger.error("无法获取ETF列表数据，回退到逐个获取模式")
                return self._batch_get_current_data_fallback(fund_codes)
            
            # 批量从缓存数据中提取所需基金信息
            for fund_code in fund_codes:
                try:
                    if not self._validate_fund_code(fund_code):
                        logger.warning(f"跳过无效基金代码: {fund_code}")
                        continue
                    
                    fund_row = etf_df[etf_df['代码'] == fund_code]
                    
                    if not fund_row.empty:
                        row = fund_row.iloc[0]
                        current_price = float(row['最新价'])
                        previous_close = float(row['昨收'])
                        change_percent = float(row['涨跌幅'])
                        volume = int(row['成交量']) if pd.notna(row['成交量']) else 0
                        fund_name = row['名称']
                        
                        fund_data = FundData(
                            code=fund_code,
                            name=fund_name,
                            current_price=current_price,
                            previous_close=previous_close,
                            change_percent=change_percent,
                            volume=volume,
                            market_cap=None,
                            currency='CNY',
                            last_update=datetime.now()
                        )
                        
                        results[fund_code] = fund_data
                        logger.debug(f"基金数据获取成功: {fund_code} - ¥{current_price:.4f}")
                    else:
                        logger.warning(f"基金 {fund_code} 在ETF列表中未找到")
                        
                except Exception as e:
                    logger.error(f"处理基金 {fund_code} 时出错: {e}")
            
            success_count = len(results)
            logger.info(f"批量获取完成: {success_count}/{len(fund_codes)} 只基金成功")
            
        except Exception as e:
            logger.error(f"批量获取过程中出错: {e}")
            logger.info("回退到逐个获取模式")
            return self._batch_get_current_data_fallback(fund_codes)
        
        return results
    
    def _batch_get_current_data_fallback(self, fund_codes: List[str]) -> Dict[str, FundData]:
        """
        批量获取数据的回退方案（逐个获取）
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            基金代码到FundData的映射
        """
        results = {}
        
        # 分批处理
        batches = [fund_codes[i:i + self.batch_size] 
                  for i in range(0, len(fund_codes), self.batch_size)]
        
        logger.info(f"使用回退方案，分 {len(batches)} 批处理")
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"处理第 {batch_idx + 1}/{len(batches)} 批 ({len(batch)} 只基金)")
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=min(len(batch), 3)) as executor:
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
        logger.info(f"回退方案完成: {success_count}/{len(fund_codes)} 只基金成功")
        
        return results
    
    def batch_get_historical_data(self, fund_codes: List[str], 
                                period: str = "60d", 
                                max_workers: int = 3) -> Dict[str, HistoricalData]:
        """
        批量获取历史数据（带缓存和并发控制）
        
        Args:
            fund_codes: 基金代码列表
            period: 时间周期
            max_workers: 最大并发数
            
        Returns:
            基金代码到HistoricalData的映射
        """
        if not fund_codes:
            logger.warning("基金代码列表为空")
            return {}
            
        logger.info(f"开始批量获取 {len(fund_codes)} 只基金历史数据 ({period})，最大并发数: {max_workers}")
        
        results = {}
        failed_codes = []
        
        # 先检查缓存
        for code in fund_codes:
            cached_data = self._get_cached_historical_data(code, period)
            if cached_data is not None:
                results[code] = HistoricalData(
                    code=code,
                    data=cached_data,
                    start_date=cached_data.index[0].to_pydatetime(),
                    end_date=cached_data.index[-1].to_pydatetime(),
                    period=period
                )
        
        # 找出需要从API获取的基金代码
        remaining_codes = [code for code in fund_codes if code not in results]
        
        if remaining_codes:
            # 使用线程池并发获取剩余数据
            with ThreadPoolExecutor(max_workers=min(max_workers, len(remaining_codes))) as executor:
                future_to_code = {
                    executor.submit(self.get_historical_data, code, period): code 
                    for code in remaining_codes
                }
                
                for future in as_completed(future_to_code):
                    code = future_to_code[future]
                    try:
                        historical_data = future.result()
                        if historical_data:
                            results[code] = historical_data
                        else:
                            failed_codes.append(code)
                    except Exception as e:
                        logger.error(f"处理历史数据时出错: {code} - {e}")
                        failed_codes.append(code)
        
        # 记录结果
        success_count = len(results)
        total_count = len(fund_codes)
        cache_hit_rate = (total_count - len(remaining_codes)) / total_count * 100
        
        logger.info(
            f"批量历史数据获取完成: {success_count}/{total_count} 只基金成功, "
            f"缓存命中率: {cache_hit_rate:.1f}%"
        )
        
        if failed_codes:
            logger.warning(f"以下基金获取失败: {', '.join(failed_codes)}")
            
        return results
    
    def _get_fund_market(self, fund_code: str) -> str:
        """
        获取基金所属市场
        
        Args:
            fund_code: 基金代码
            
        Returns:
            市场标识：'SH'(上海) 或 'SZ'(深圳)
        """
        if not fund_code.isdigit() or len(fund_code) != 6:
            return 'UNKNOWN'
        
        # 中国基金/ETF代码规则
        # 上海证券交易所：5开头的ETF (如510xxx, 511xxx等)
        # 深圳证券交易所：1开头的ETF (如159xxx, 160xxx等)
        
        if fund_code.startswith(('510', '511', '512', '513', '515', '516', '517', '518', '588')):
            return 'SH'  # 上海
        elif fund_code.startswith(('159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169')):
            return 'SZ'  # 深圳
        else:
            # 其他情况，根据首位数字判断
            if fund_code.startswith(('0', '2', '3')):
                return 'SZ'
            elif fund_code.startswith(('6', '9')):
                return 'SH'
            else:
                return 'SH'  # 默认上海
    
    def get_data_summary(self, fund_codes: List[str]) -> Dict[str, Any]:
        """
        获取数据获取摘要
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            摘要信息
        """
        valid_codes = []
        invalid_codes = []
        
        for code in fund_codes:
            if self._validate_fund_code(code):
                valid_codes.append(code)
            else:
                invalid_codes.append(code)
        
        # ETF缓存状态
        etf_cache_status = {
            'cached': self._etf_list_cache is not None,
            'cache_time': self._etf_list_cache_time.isoformat() if self._etf_list_cache_time else None,
            'cache_size': len(self._etf_list_cache) if self._etf_list_cache is not None else 0,
            'cache_expired': False
        }
        
        if self._etf_list_cache_time:
            cache_age_hours = (datetime.now() - self._etf_list_cache_time).total_seconds() / 3600
            etf_cache_status['cache_expired'] = cache_age_hours > self._etf_cache_expire_hours
            etf_cache_status['cache_age_hours'] = round(cache_age_hours, 2)
        
        return {
            'total_funds': len(fund_codes),
            'valid_funds': len(valid_codes),
            'invalid_funds': len(invalid_codes),
            'valid_codes': valid_codes,
            'invalid_codes': invalid_codes,
            'fund_info_cache_size': len(self._fund_info_cache),
            'etf_cache_status': etf_cache_status,
            'data_source': 'AKShare API (Optimized)',
            'optimization_enabled': True
        }
    
    def _get_etf_list_cached(self) -> Optional[pd.DataFrame]:
        """
        获取ETF列表（带缓存机制）
        
        Returns:
            包含所有ETF基金信息的DataFrame
        """
        current_time = datetime.now()
        
        # 检查缓存是否有效
        if (self._etf_list_cache is not None and 
            self._etf_list_cache_time is not None and 
            (current_time - self._etf_list_cache_time).total_seconds() < self._etf_cache_expire_hours * 3600):
            logger.debug(f"使用缓存的ETF列表数据 ({len(self._etf_list_cache)}只基金)")
            return self._etf_list_cache
        
        # 缓存过期或不存在，重新获取
        try:
            logger.info("ETF列表缓存过期，重新获取全量数据...")
            self._apply_rate_limit()
            etf_df = ak.fund_etf_spot_em()
            
            # 更新缓存
            self._etf_list_cache = etf_df
            self._etf_list_cache_time = current_time
            
            logger.info(f"ETF列表获取成功并缓存: {len(etf_df)}只基金")
            return etf_df
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            # 如果获取失败但有旧缓存，使用旧缓存
            if self._etf_list_cache is not None:
                logger.warning("使用过期的ETF列表缓存")
                return self._etf_list_cache
            return None
    
    def get_all_etf_list(self) -> Optional[pd.DataFrame]:
        """
        获取所有ETF基金列表（公共接口）
        
        Returns:
            包含所有ETF基金信息的DataFrame
        """
        return self._get_etf_list_cached()
    
    def refresh_etf_list_cache(self) -> bool:
        """
        强制刷新ETF列表缓存
        
        Returns:
            是否刷新成功
        """
        try:
            logger.info("强制刷新ETF列表缓存")
            self._etf_list_cache = None
            self._etf_list_cache_time = None
            result = self._get_etf_list_cached()
            return result is not None
        except Exception as e:
            logger.error(f"刷新ETF列表缓存失败: {e}")
            return False
    
    def get_fund_basic_info_batch(self) -> Optional[List[Dict[str, Any]]]:
        """
        批量获取基金基本信息
        
        Returns:
            包含基金基本信息的列表，每个元素是一个字典，包含单只基金的详细信息
            如果初始化时指定了fund_codes则只返回这些基金的信息
        """
        if not self.fund_codes:
            logger.warning("未指定基金代码，无法获取基金信息")
            return None
            
        result = []
        success_count = 0
        
        for code in self.fund_codes:
            try:
                self._apply_rate_limit()
                
                # 使用雪球接口获取单个基金的详细信息
                fund_info = ak.fund_individual_basic_info_xq(symbol=code, timeout=10)
                
                if fund_info is not None and not fund_info.empty:
                    # 提取并格式化需要的信息
                    info_dict = {
                        'code': code,
                        'name': fund_info.get('item', {}).get('name', ''),
                        'nav': fund_info.get('item', {}).get('nav', 0),  # 单位净值
                        'acc_nav': fund_info.get('item', {}).get('acc_nav', 0),  # 累计净值
                        'nav_date': fund_info.get('item', {}).get('nav_date', ''),  # 净值日期
                        'fund_scale': fund_info.get('item', {}).get('fund_scale', 0),  # 基金规模(亿元)
                        'fund_scale_date': fund_info.get('item', {}).get('fund_scale_date', ''),  # 规模日期
                        'fund_manager': fund_info.get('item', {}).get('fund_manager', ''),  # 基金经理
                        'fund_company': fund_info.get('item', {}).get('fund_company', ''),  # 基金公司
                        'fund_type': fund_info.get('item', {}).get('fund_type', ''),  # 基金类型
                        'create_date': fund_info.get('item', {}).get('create_date', '')  # 成立日期
                    }
                    result.append(info_dict)
                    success_count += 1
                    logger.debug(f"获取基金 {code} 基本信息成功")
                else:
                    logger.warning(f"基金 {code} 信息为空")
                    
            except Exception as e:
                logger.warning(f"获取基金 {code} 基本信息失败: {e}")
                continue
        
        if not result:
            logger.error("所有基金基本信息获取失败")
            return None
            
        logger.info(f"基金基本信息获取完成: 成功 {success_count}/{len(self.fund_codes)} 只基金")
        return result
