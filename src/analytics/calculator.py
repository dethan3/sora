"""
分析计算器模块

提供基金数据的技术分析和统计计算功能
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from loguru import logger

from ..data.fetcher import FundData, HistoricalData


@dataclass
class AnalysisResult:
    """分析结果数据类"""
    fund_code: str
    fund_name: str
    current_price: float
    analysis_date: datetime
    
    # 价格统计
    mean_price: float
    std_price: float
    min_price: float
    max_price: float
    
    # 收益率统计
    daily_returns: List[float]
    mean_return: float
    std_return: float
    sharpe_ratio: float
    
    # 技术指标
    rsi: float
    volatility: float
    price_vs_mean_ratio: float
    
    # 趋势分析
    trend_direction: str  # 'up', 'down', 'sideways'
    trend_strength: float  # 0-1
    
    # 风险评估
    risk_level: str  # 'low', 'medium', 'high'
    max_drawdown: float


class AnalyticsCalculator:
    """分析计算器"""
    
    def __init__(self, analysis_days: int = 60):
        """
        初始化分析计算器
        
        Args:
            analysis_days: 分析天数
        """
        self.analysis_days = analysis_days
        logger.info(f"分析计算器初始化完成 - 分析天数: {analysis_days}")
    
    def analyze_fund(self, fund_data: FundData, 
                    historical_data: Optional[HistoricalData] = None) -> AnalysisResult:
        """
        分析单只基金
        
        Args:
            fund_data: 基金当前数据
            historical_data: 历史数据（可选）
            
        Returns:
            分析结果
        """
        logger.debug(f"开始分析基金: {fund_data.code}")
        
        if historical_data is None or len(historical_data.prices) < 10:
            # 如果没有足够的历史数据，返回基础分析
            return self._basic_analysis(fund_data)
        
        # 完整分析
        return self._full_analysis(fund_data, historical_data)
    
    def _basic_analysis(self, fund_data: FundData) -> AnalysisResult:
        """
        基础分析（仅基于当前数据）
        
        Args:
            fund_data: 基金当前数据
            
        Returns:
            基础分析结果
        """
        current_price = fund_data.current_price
        
        # 基于当前价格的简单估算
        estimated_mean = current_price * 0.98  # 假设当前价格略高于均值
        estimated_std = current_price * 0.05   # 假设5%的标准差
        
        return AnalysisResult(
            fund_code=fund_data.code,
            fund_name=fund_data.name,
            current_price=current_price,
            analysis_date=fund_data.last_update,
            
            # 价格统计（估算）
            mean_price=estimated_mean,
            std_price=estimated_std,
            min_price=current_price * 0.9,
            max_price=current_price * 1.1,
            
            # 收益率统计（默认值）
            daily_returns=[fund_data.change_percent / 100],
            mean_return=0.001,  # 0.1%日均收益
            std_return=0.02,    # 2%日波动
            sharpe_ratio=0.5,   # 默认夏普比率
            
            # 技术指标（估算）
            rsi=50.0,  # 中性RSI
            volatility=0.02,
            price_vs_mean_ratio=current_price / estimated_mean,
            
            # 趋势分析
            trend_direction='sideways' if abs(fund_data.change_percent) < 1 else 
                           ('up' if fund_data.change_percent > 0 else 'down'),
            trend_strength=min(abs(fund_data.change_percent) / 5.0, 1.0),
            
            # 风险评估
            risk_level='medium',
            max_drawdown=0.05
        )
    
    def _full_analysis(self, fund_data: FundData, 
                      historical_data: HistoricalData) -> AnalysisResult:
        """
        完整分析（基于历史数据）
        
        Args:
            fund_data: 基金当前数据
            historical_data: 历史数据
            
        Returns:
            完整分析结果
        """
        prices = np.array(historical_data.prices)
        dates = historical_data.dates
        
        # 价格统计
        mean_price = np.mean(prices)
        std_price = np.std(prices)
        min_price = np.min(prices)
        max_price = np.max(prices)
        
        # 计算日收益率
        daily_returns = np.diff(prices) / prices[:-1]
        mean_return = np.mean(daily_returns)
        std_return = np.std(daily_returns)
        
        # 夏普比率（假设无风险利率为2%年化）
        risk_free_rate = 0.02 / 252  # 日无风险利率
        sharpe_ratio = (mean_return - risk_free_rate) / std_return if std_return > 0 else 0
        
        # RSI计算
        rsi = self._calculate_rsi(prices)
        
        # 波动率（年化）
        volatility = std_return * np.sqrt(252)
        
        # 价格相对均值比率
        price_vs_mean_ratio = fund_data.current_price / mean_price
        
        # 趋势分析
        trend_direction, trend_strength = self._analyze_trend(prices)
        
        # 风险评估
        risk_level = self._assess_risk(volatility, sharpe_ratio)
        max_drawdown = self._calculate_max_drawdown(prices)
        
        return AnalysisResult(
            fund_code=fund_data.code,
            fund_name=fund_data.name,
            current_price=fund_data.current_price,
            analysis_date=fund_data.last_update,
            
            # 价格统计
            mean_price=mean_price,
            std_price=std_price,
            min_price=min_price,
            max_price=max_price,
            
            # 收益率统计
            daily_returns=daily_returns.tolist(),
            mean_return=mean_return,
            std_return=std_return,
            sharpe_ratio=sharpe_ratio,
            
            # 技术指标
            rsi=rsi,
            volatility=volatility,
            price_vs_mean_ratio=price_vs_mean_ratio,
            
            # 趋势分析
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            
            # 风险评估
            risk_level=risk_level,
            max_drawdown=max_drawdown
        )
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """
        计算RSI指标
        
        Args:
            prices: 价格数组
            period: RSI周期
            
        Returns:
            RSI值
        """
        if len(prices) < period + 1:
            return 50.0  # 默认中性值
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _analyze_trend(self, prices: np.ndarray) -> Tuple[str, float]:
        """
        分析价格趋势
        
        Args:
            prices: 价格数组
            
        Returns:
            (趋势方向, 趋势强度)
        """
        if len(prices) < 10:
            return 'sideways', 0.0
        
        # 使用线性回归分析趋势
        x = np.arange(len(prices))
        slope, _ = np.polyfit(x, prices, 1)
        
        # 计算相关系数作为趋势强度
        correlation = np.corrcoef(x, prices)[0, 1]
        trend_strength = abs(correlation)
        
        # 判断趋势方向
        if slope > prices[-1] * 0.001:  # 上涨超过0.1%
            trend_direction = 'up'
        elif slope < -prices[-1] * 0.001:  # 下跌超过0.1%
            trend_direction = 'down'
        else:
            trend_direction = 'sideways'
        
        return trend_direction, trend_strength
    
    def _assess_risk(self, volatility: float, sharpe_ratio: float) -> str:
        """
        评估风险等级
        
        Args:
            volatility: 年化波动率
            sharpe_ratio: 夏普比率
            
        Returns:
            风险等级
        """
        # 基于波动率和夏普比率的风险评估
        if volatility > 0.3 or sharpe_ratio < 0:
            return 'high'
        elif volatility > 0.15 or sharpe_ratio < 0.5:
            return 'medium'
        else:
            return 'low'
    
    def _calculate_max_drawdown(self, prices: np.ndarray) -> float:
        """
        计算最大回撤
        
        Args:
            prices: 价格数组
            
        Returns:
            最大回撤比例
        """
        if len(prices) < 2:
            return 0.0
        
        # 计算累计最高价
        cummax = np.maximum.accumulate(prices)
        
        # 计算回撤
        drawdowns = (prices - cummax) / cummax
        
        # 返回最大回撤（负值）
        return abs(np.min(drawdowns))
    
    def batch_analyze(self, fund_data_list: List[FundData],
                     historical_data_dict: Dict[str, HistoricalData] = None) -> Dict[str, AnalysisResult]:
        """
        批量分析基金
        
        Args:
            fund_data_list: 基金数据列表
            historical_data_dict: 历史数据字典
            
        Returns:
            分析结果字典
        """
        logger.info(f"开始批量分析 {len(fund_data_list)} 只基金")
        
        results = {}
        historical_data_dict = historical_data_dict or {}
        
        for fund_data in fund_data_list:
            try:
                historical_data = historical_data_dict.get(fund_data.code)
                result = self.analyze_fund(fund_data, historical_data)
                results[fund_data.code] = result
                logger.debug(f"基金分析完成: {fund_data.code}")
            except Exception as e:
                logger.error(f"基金分析失败: {fund_data.code} - {e}")
        
        logger.info(f"批量分析完成: {len(results)}/{len(fund_data_list)} 只基金成功")
        return results
    
    def get_analysis_summary(self, results: Dict[str, AnalysisResult]) -> Dict[str, Any]:
        """
        获取分析摘要
        
        Args:
            results: 分析结果字典
            
        Returns:
            摘要信息
        """
        if not results:
            return {}
        
        # 统计信息
        total_funds = len(results)
        risk_distribution = {}
        trend_distribution = {}
        
        avg_sharpe = 0
        avg_volatility = 0
        avg_rsi = 0
        
        for result in results.values():
            # 风险分布
            risk_distribution[result.risk_level] = risk_distribution.get(result.risk_level, 0) + 1
            
            # 趋势分布
            trend_distribution[result.trend_direction] = trend_distribution.get(result.trend_direction, 0) + 1
            
            # 平均指标
            avg_sharpe += result.sharpe_ratio
            avg_volatility += result.volatility
            avg_rsi += result.rsi
        
        avg_sharpe /= total_funds
        avg_volatility /= total_funds
        avg_rsi /= total_funds
        
        return {
            'total_funds': total_funds,
            'risk_distribution': risk_distribution,
            'trend_distribution': trend_distribution,
            'average_metrics': {
                'sharpe_ratio': avg_sharpe,
                'volatility': avg_volatility,
                'rsi': avg_rsi
            }
        }
