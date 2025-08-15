"""
决策引擎模块

基于分析结果提供投资决策建议
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from loguru import logger

from ..analytics.calculator import AnalysisResult
from ..config.manager import StrategyConfig


class DecisionType(Enum):
    """决策类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class Decision:
    """投资决策数据类"""
    fund_code: str
    fund_name: str
    decision_type: DecisionType
    confidence: float  # 0-1，决策信心度
    reasoning: List[str]  # 决策理由
    target_price: Optional[float] = None  # 目标价格
    stop_loss: Optional[float] = None  # 止损价格
    decision_date: datetime = None
    
    def __post_init__(self):
        if self.decision_date is None:
            self.decision_date = datetime.now()


class DecisionEngine:
    """决策引擎"""
    
    def __init__(self, strategy_config: StrategyConfig):
        """
        初始化决策引擎
        
        Args:
            strategy_config: 策略配置
        """
        self.strategy_config = strategy_config
        self.buy_threshold = strategy_config.buy_threshold
        self.sell_threshold = strategy_config.sell_threshold
        
        logger.info(f"决策引擎初始化完成 - 买入阈值: {self.buy_threshold}%, 卖出阈值: {self.sell_threshold}%")
    
    def make_decision(self, analysis_result: AnalysisResult, 
                     is_owned: bool = False,
                     purchase_price: Optional[float] = None) -> Decision:
        """
        基于分析结果做出投资决策
        
        Args:
            analysis_result: 分析结果
            is_owned: 是否已持有
            purchase_price: 购买价格（如果已持有）
            
        Returns:
            投资决策
        """
        logger.debug(f"开始决策分析: {analysis_result.fund_code}")
        
        reasoning = []
        confidence = 0.5  # 基础信心度
        
        # 基于价格相对均值的决策
        price_ratio = analysis_result.price_vs_mean_ratio
        current_price = analysis_result.current_price
        
        # 决策逻辑
        if is_owned:
            # 已持有基金的决策逻辑
            decision_type, confidence, reasoning = self._decide_for_owned(
                analysis_result, purchase_price, price_ratio
            )
        else:
            # 未持有基金的决策逻辑
            decision_type, confidence, reasoning = self._decide_for_watchlist(
                analysis_result, price_ratio
            )
        
        # 计算目标价格和止损价格
        target_price, stop_loss = self._calculate_price_targets(
            current_price, decision_type, analysis_result
        )
        
        decision = Decision(
            fund_code=analysis_result.fund_code,
            fund_name=analysis_result.fund_name,
            decision_type=decision_type,
            confidence=confidence,
            reasoning=reasoning,
            target_price=target_price,
            stop_loss=stop_loss
        )
        
        logger.debug(f"决策完成: {analysis_result.fund_code} - {decision_type.value} (信心度: {confidence:.2f})")
        return decision
    
    def _decide_for_owned(self, analysis_result: AnalysisResult, 
                         purchase_price: Optional[float],
                         price_ratio: float) -> tuple:
        """
        已持有基金的决策逻辑
        
        Returns:
            (决策类型, 信心度, 理由列表)
        """
        reasoning = []
        confidence = 0.5
        
        current_price = analysis_result.current_price
        
        # 计算收益率
        if purchase_price:
            profit_ratio = (current_price - purchase_price) / purchase_price
            reasoning.append(f"当前收益率: {profit_ratio*100:+.2f}%")
        
        # 基于价格相对均值判断
        if price_ratio >= self.sell_threshold / 100:
            # 价格明显高于均值，考虑卖出
            decision_type = DecisionType.SELL
            confidence += 0.3
            reasoning.append(f"当前价格比历史均值高 {(price_ratio-1)*100:.1f}%，建议获利了结")
            
            # 额外卖出信号
            if analysis_result.rsi > 70:
                confidence += 0.1
                reasoning.append(f"RSI指标 {analysis_result.rsi:.1f} 显示超买")
            
            if analysis_result.trend_direction == 'down':
                confidence += 0.1
                reasoning.append("价格趋势转为下跌")
                
        elif price_ratio <= self.buy_threshold / 100:
            # 价格明显低于均值，考虑加仓
            decision_type = DecisionType.BUY
            confidence += 0.2
            reasoning.append(f"当前价格比历史均值低 {(1-price_ratio)*100:.1f}%，可考虑加仓")
            
            # 额外买入信号
            if analysis_result.rsi < 30:
                confidence += 0.1
                reasoning.append(f"RSI指标 {analysis_result.rsi:.1f} 显示超卖")
            
            if analysis_result.trend_direction == 'up':
                confidence += 0.1
                reasoning.append("价格趋势开始上涨")
        else:
            # 价格在合理范围内，持有
            decision_type = DecisionType.HOLD
            reasoning.append("价格在合理范围内，建议继续持有")
            
            # 持有的额外考虑因素
            if analysis_result.sharpe_ratio > 1.0:
                confidence += 0.1
                reasoning.append(f"夏普比率 {analysis_result.sharpe_ratio:.2f} 表现良好")
            
            if analysis_result.risk_level == 'low':
                confidence += 0.1
                reasoning.append("风险等级较低，适合长期持有")
        
        return decision_type, min(confidence, 0.95), reasoning
    
    def _decide_for_watchlist(self, analysis_result: AnalysisResult, 
                            price_ratio: float) -> tuple:
        """
        关注基金的决策逻辑
        
        Returns:
            (决策类型, 信心度, 理由列表)
        """
        reasoning = []
        confidence = 0.4  # 关注基金的基础信心度较低
        
        # 基于价格相对均值判断
        if price_ratio <= self.buy_threshold / 100:
            # 价格低于买入阈值
            decision_type = DecisionType.BUY
            confidence += 0.3
            reasoning.append(f"当前价格比历史均值低 {(1-price_ratio)*100:.1f}%，出现买入机会")
            
            # 额外买入信号
            if analysis_result.rsi < 30:
                confidence += 0.15
                reasoning.append(f"RSI指标 {analysis_result.rsi:.1f} 显示严重超卖")
            elif analysis_result.rsi < 50:
                confidence += 0.05
                reasoning.append(f"RSI指标 {analysis_result.rsi:.1f} 偏低")
            
            if analysis_result.trend_direction == 'up' and analysis_result.trend_strength > 0.6:
                confidence += 0.1
                reasoning.append("价格趋势强劲上涨")
            
            if analysis_result.sharpe_ratio > 0.5:
                confidence += 0.05
                reasoning.append(f"夏普比率 {analysis_result.sharpe_ratio:.2f} 风险调整收益良好")
                
        elif price_ratio >= self.sell_threshold / 100:
            # 价格高于卖出阈值，不建议买入
            decision_type = DecisionType.HOLD
            confidence += 0.1
            reasoning.append(f"当前价格比历史均值高 {(price_ratio-1)*100:.1f}%，不建议买入")
            
            if analysis_result.rsi > 70:
                reasoning.append(f"RSI指标 {analysis_result.rsi:.1f} 显示超买")
        else:
            # 价格在合理范围内
            decision_type = DecisionType.HOLD
            reasoning.append("价格在合理范围内，可继续观察")
            
            # 观察的额外因素
            if analysis_result.trend_direction == 'up':
                confidence += 0.1
                reasoning.append("价格趋势向上，可关注买入时机")
            
            if analysis_result.volatility < 0.15:
                confidence += 0.05
                reasoning.append("波动率较低，风险可控")
        
        return decision_type, min(confidence, 0.85), reasoning
    
    def _calculate_price_targets(self, current_price: float, 
                               decision_type: DecisionType,
                               analysis_result: AnalysisResult) -> tuple:
        """
        计算目标价格和止损价格
        
        Returns:
            (目标价格, 止损价格)
        """
        target_price = None
        stop_loss = None
        
        if decision_type == DecisionType.BUY:
            # 买入时设置目标价格和止损
            target_price = analysis_result.mean_price * 1.1  # 目标价格为均值的110%
            stop_loss = current_price * 0.95  # 止损为当前价格的95%
            
        elif decision_type == DecisionType.SELL:
            # 卖出时的目标价格就是当前价格
            target_price = current_price
            
        return target_price, stop_loss
    
    def batch_decide(self, analysis_results: Dict[str, AnalysisResult],
                    owned_funds: Dict[str, Dict] = None) -> Dict[str, Decision]:
        """
        批量决策
        
        Args:
            analysis_results: 分析结果字典
            owned_funds: 持有基金信息字典
            
        Returns:
            决策结果字典
        """
        logger.info(f"开始批量决策 {len(analysis_results)} 只基金")
        
        decisions = {}
        owned_funds = owned_funds or {}
        
        for fund_code, analysis_result in analysis_results.items():
            try:
                # 检查是否已持有
                fund_info = owned_funds.get(fund_code, {})
                is_owned = bool(fund_info)
                purchase_price = fund_info.get('purchase_price')
                
                decision = self.make_decision(
                    analysis_result, is_owned, purchase_price
                )
                decisions[fund_code] = decision
                
            except Exception as e:
                logger.error(f"决策失败: {fund_code} - {e}")
        
        logger.info(f"批量决策完成: {len(decisions)}/{len(analysis_results)} 只基金成功")
        return decisions
    
    def get_decision_summary(self, decisions: Dict[str, Decision]) -> Dict[str, Any]:
        """
        获取决策摘要
        
        Args:
            decisions: 决策结果字典
            
        Returns:
            摘要信息
        """
        if not decisions:
            return {}
        
        # 统计决策分布
        decision_distribution = {}
        confidence_levels = {'high': 0, 'medium': 0, 'low': 0}
        
        total_decisions = len(decisions)
        avg_confidence = 0
        
        for decision in decisions.values():
            # 决策类型分布
            decision_type = decision.decision_type.value
            decision_distribution[decision_type] = decision_distribution.get(decision_type, 0) + 1
            
            # 信心度分布
            if decision.confidence >= 0.7:
                confidence_levels['high'] += 1
            elif decision.confidence >= 0.5:
                confidence_levels['medium'] += 1
            else:
                confidence_levels['low'] += 1
            
            avg_confidence += decision.confidence
        
        avg_confidence /= total_decisions
        
        return {
            'total_decisions': total_decisions,
            'decision_distribution': decision_distribution,
            'confidence_levels': confidence_levels,
            'average_confidence': avg_confidence
        }
