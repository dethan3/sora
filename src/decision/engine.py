"""
投资决策引擎
基于量化分析结果生成投资决策信号，专为国内ETF联接基金优化
"""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import numpy as np
from loguru import logger

from ..config.manager import ConfigManager
from ..config.manager import StrategyConfig


class DecisionType(Enum):
    """决策类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class DecisionSignal(Enum):
    """投资决策信号枚举"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


@dataclass
class InvestmentDecision:
    """投资决策数据类"""
    fund_code: str
    fund_name: str
    signal: DecisionSignal
    confidence: float  # 0-1之间
    score: float      # 0-1之间的综合评分
    target_position: float  # 建议仓位 0-1之间
    reasons: List[str]
    risk_level: str   # low, medium, high
    expected_return: Optional[float] = None
    max_drawdown_risk: Optional[float] = None
    holding_period: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class DecisionEngine:
    """投资决策引擎 - 专为国内ETF联接基金优化"""
    
    def __init__(self, config_manager: ConfigManager):
        """初始化决策引擎"""
        self.config_manager = config_manager
        self.strategy_config = config_manager.strategy
        
        logger.info("投资决策引擎初始化完成")
    
    def make_decision(self, fund_code: str, fund_name: str, 
                     analysis_result: Dict[str, Any]) -> InvestmentDecision:
        """
        基于分析结果做出投资决策
        
        Args:
            fund_code: 基金代码
            fund_name: 基金名称
            analysis_result: 分析结果字典
            
        Returns:
            InvestmentDecision: 投资决策对象
        """
        try:
            # 计算综合评分
            score = self._calculate_composite_score(analysis_result)
            
            # 确定决策信号
            signal = self._determine_signal(score)
            
            # 计算信心度
            confidence = self._calculate_confidence(analysis_result)
            
            # 计算目标仓位
            target_position = self._calculate_target_position(signal, score, confidence)
            
            # 评估风险等级
            risk_level = self._assess_risk_level(analysis_result)
            
            # 生成决策理由
            reasons = self._generate_reasons(analysis_result, signal)
            
            # 估算预期收益和风险
            expected_return = self._estimate_expected_return(analysis_result, signal)
            max_drawdown_risk = self._estimate_max_drawdown(analysis_result)
            
            # 确定持有期建议
            holding_period = self._suggest_holding_period(signal, analysis_result)
            
            decision = InvestmentDecision(
                fund_code=fund_code,
                fund_name=fund_name,
                signal=signal,
                confidence=confidence,
                score=score,
                target_position=target_position,
                reasons=reasons,
                risk_level=risk_level,
                expected_return=expected_return,
                max_drawdown_risk=max_drawdown_risk,
                holding_period=holding_period
            )
            
            logger.info(f"基金 {fund_code} 决策生成: {signal.value}, 评分: {score:.3f}, 信心度: {confidence:.3f}")
            return decision
            
        except Exception as e:
            logger.error(f"基金 {fund_code} 决策生成失败: {str(e)}")
            return self._create_default_decision(fund_code, fund_name)
    
    def _calculate_composite_score(self, analysis_result: Dict[str, Any]) -> float:
        """计算综合评分"""
        scores = []
        weights = []
        
        # 趋势分析权重：30%
        if 'trend_analysis' in analysis_result:
            trend_score = analysis_result['trend_analysis'].get('score', 0.5)
            scores.append(trend_score)
            weights.append(0.30)
        
        # 技术指标权重：40%
        if 'technical_indicators' in analysis_result:
            tech_score = analysis_result['technical_indicators'].get('composite_score', 0.5)
            scores.append(tech_score)
            weights.append(0.40)
        
        # 动量分析权重：20%
        if 'momentum' in analysis_result:
            momentum_score = analysis_result['momentum'].get('score', 0.5)
            scores.append(momentum_score)
            weights.append(0.20)
        
        # 波动性分析权重：10%
        if 'volatility' in analysis_result:
            volatility = analysis_result['volatility'].get('annualized', 0.2)
            # 波动性越低越好，转换为评分
            volatility_score = max(0, 1 - volatility / 0.5)
            scores.append(volatility_score)
            weights.append(0.10)
        
        if not scores:
            return 0.5
        
        total_weight = sum(weights)
        if total_weight == 0:
            return 0.5
            
        weighted_score = sum(score * weight for score, weight in zip(scores, weights)) / total_weight
        return np.clip(weighted_score, 0, 1)
    
    def _determine_signal(self, score: float) -> DecisionSignal:
        """根据评分确定决策信号"""
        if score >= self.strategy_config.strong_buy_threshold:
            return DecisionSignal.STRONG_BUY
        elif score >= self.strategy_config.buy_threshold:
            return DecisionSignal.BUY
        elif score >= self.strategy_config.hold_threshold:
            return DecisionSignal.HOLD
        elif score >= self.strategy_config.strong_sell_threshold:
            return DecisionSignal.SELL
        else:
            return DecisionSignal.STRONG_SELL
    
    def _calculate_confidence(self, analysis_result: Dict[str, Any]) -> float:
        """计算决策信心度"""
        confidence_factors = []
        
        # 数据完整性
        expected_indicators = ['trend_analysis', 'technical_indicators', 'momentum', 'volatility', 'performance']
        data_completeness = sum(1 for indicator in expected_indicators if indicator in analysis_result) / len(expected_indicators)
        confidence_factors.append(data_completeness)
        
        # 技术指标一致性
        if 'technical_indicators' in analysis_result:
            indicators = analysis_result['technical_indicators']
            buy_signals = 0
            sell_signals = 0
            total_signals = 0
            
            for indicator, data in indicators.items():
                if isinstance(data, dict) and 'signal' in data:
                    total_signals += 1
                    if data['signal'] in ['buy', 'strong_buy']:
                        buy_signals += 1
                    elif data['signal'] in ['sell', 'strong_sell']:
                        sell_signals += 1
            
            if total_signals > 0:
                # 信号一致性越高，信心度越高
                max_consistent = max(buy_signals, sell_signals)
                consistency = max_consistent / total_signals
                confidence_factors.append(consistency)
        
        # 趋势强度
        if 'trend_analysis' in analysis_result:
            trend_strength = analysis_result['trend_analysis'].get('strength', 0.5)
            confidence_factors.append(trend_strength)
        
        return np.mean(confidence_factors) if confidence_factors else 0.5
    
    def _calculate_target_position(self, signal: DecisionSignal, score: float, confidence: float) -> float:
        """计算目标仓位"""
        base_position = {
            DecisionSignal.STRONG_BUY: 0.8,
            DecisionSignal.BUY: 0.6,
            DecisionSignal.HOLD: 0.4,
            DecisionSignal.SELL: 0.2,
            DecisionSignal.STRONG_SELL: 0.0
        }.get(signal, 0.4)
        
        # 根据信心度调整仓位
        adjusted_position = base_position * confidence
        
        # 确保不超过单只基金最大仓位限制
        max_position = self.strategy_config.max_single_position
        return min(adjusted_position, max_position)
    
    def _assess_risk_level(self, analysis_result: Dict[str, Any]) -> str:
        """评估风险等级"""
        risk_factors = []
        
        # 波动性风险
        if 'volatility' in analysis_result:
            volatility = analysis_result['volatility'].get('annualized', 0.2)
            if volatility > 0.3:
                risk_factors.append('high_volatility')
            elif volatility < 0.15:
                risk_factors.append('low_volatility')
        
        # 流动性风险
        if 'liquidity' in analysis_result:
            avg_volume = analysis_result['liquidity'].get('avg_volume', 0)
            if avg_volume < 1000000:  # 日均交易量低于100万
                risk_factors.append('low_liquidity')
        
        # 趋势风险
        if 'trend_analysis' in analysis_result:
            trend_direction = analysis_result['trend_analysis'].get('direction', 'sideways')
            trend_strength = analysis_result['trend_analysis'].get('strength', 0.5)
            if trend_direction == 'downward' and trend_strength > 0.7:
                risk_factors.append('strong_downtrend')
        
        # 综合风险评级
        high_risk_count = sum(1 for factor in risk_factors if 'high' in factor or 'strong_downtrend' in factor)
        low_risk_count = sum(1 for factor in risk_factors if 'low' in factor)
        
        if high_risk_count >= 2:
            return 'high'
        elif low_risk_count >= 2 and high_risk_count == 0:
            return 'low'
        else:
            return 'medium'
    
    def _generate_reasons(self, analysis_result: Dict[str, Any], signal: DecisionSignal) -> List[str]:
        """生成决策理由"""
        reasons = []
        
        # 基于趋势分析
        if 'trend_analysis' in analysis_result:
            trend = analysis_result['trend_analysis']
            direction = trend.get('direction', 'sideways')
            strength = trend.get('strength', 0.5)
            
            if direction == 'upward' and signal in [DecisionSignal.BUY, DecisionSignal.STRONG_BUY]:
                reasons.append(f"上升趋势明确，趋势强度: {strength:.2f}")
            elif direction == 'downward' and signal in [DecisionSignal.SELL, DecisionSignal.STRONG_SELL]:
                reasons.append(f"下降趋势明确，趋势强度: {strength:.2f}")
        
        # 基于技术指标
        if 'technical_indicators' in analysis_result:
            indicators = analysis_result['technical_indicators']
            
            # RSI信号
            if 'rsi' in indicators:
                rsi_data = indicators['rsi']
                rsi_value = rsi_data.get('value', 50)
                if rsi_value < 30 and signal in [DecisionSignal.BUY, DecisionSignal.STRONG_BUY]:
                    reasons.append(f"RSI超卖信号: {rsi_value:.1f}")
                elif rsi_value > 70 and signal in [DecisionSignal.SELL, DecisionSignal.STRONG_SELL]:
                    reasons.append(f"RSI超买信号: {rsi_value:.1f}")
            
            # MACD信号
            if 'macd' in indicators:
                macd_data = indicators['macd']
                macd_signal = macd_data.get('signal', 'neutral')
                if macd_signal == 'bullish' and signal in [DecisionSignal.BUY, DecisionSignal.STRONG_BUY]:
                    reasons.append("MACD金叉买入信号")
                elif macd_signal == 'bearish' and signal in [DecisionSignal.SELL, DecisionSignal.STRONG_SELL]:
                    reasons.append("MACD死叉卖出信号")
        
        # 基于动量分析
        if 'momentum' in analysis_result:
            momentum = analysis_result['momentum']
            momentum_score = momentum.get('score', 0.5)
            if momentum_score > 0.7 and signal in [DecisionSignal.BUY, DecisionSignal.STRONG_BUY]:
                reasons.append(f"动量指标强劲: {momentum_score:.2f}")
            elif momentum_score < 0.3 and signal in [DecisionSignal.SELL, DecisionSignal.STRONG_SELL]:
                reasons.append(f"动量指标疲软: {momentum_score:.2f}")
        
        # 基于波动性分析
        if 'volatility' in analysis_result:
            volatility = analysis_result['volatility']
            vol_value = volatility.get('annualized', 0.2)
            if vol_value < 0.15:
                reasons.append(f"波动性较低，风险可控: {vol_value:.2%}")
            elif vol_value > 0.3:
                reasons.append(f"波动性较高，注意风险: {vol_value:.2%}")
        
        if not reasons:
            reasons.append("基于综合量化分析")
        
        return reasons
    
    def _estimate_expected_return(self, analysis_result: Dict[str, Any], signal: DecisionSignal) -> Optional[float]:
        """估算预期收益率"""
        if 'performance' not in analysis_result:
            return None
        
        performance = analysis_result['performance']
        recent_return = performance.get('1m_return', 0.0)
        
        # 基于信号调整预期收益
        signal_multiplier = {
            DecisionSignal.STRONG_BUY: 1.5,
            DecisionSignal.BUY: 1.2,
            DecisionSignal.HOLD: 1.0,
            DecisionSignal.SELL: 0.8,
            DecisionSignal.STRONG_SELL: 0.5
        }.get(signal, 1.0)
        
        expected_return = recent_return * signal_multiplier
        return np.clip(expected_return, -0.3, 0.3)  # 限制在-30%到30%之间
    
    def _estimate_max_drawdown(self, analysis_result: Dict[str, Any]) -> Optional[float]:
        """估算最大回撤风险"""
        if 'volatility' not in analysis_result:
            return None
        
        volatility = analysis_result['volatility'].get('annualized', 0.2)
        # 简单估算：最大回撤约为年化波动率的1.5倍
        max_drawdown = volatility * 1.5
        return min(max_drawdown, 0.5)  # 最大不超过50%
    
    def _suggest_holding_period(self, signal: DecisionSignal, analysis_result: Dict[str, Any]) -> str:
        """建议持有期"""
        if signal in [DecisionSignal.STRONG_BUY, DecisionSignal.BUY]:
            return "3-6个月"
        elif signal == DecisionSignal.HOLD:
            return "1-3个月"
        else:
            return "立即处理"
    
    def _create_default_decision(self, fund_code: str, fund_name: str) -> InvestmentDecision:
        """创建默认决策（数据不足时）"""
        return InvestmentDecision(
            fund_code=fund_code,
            fund_name=fund_name,
            signal=DecisionSignal.HOLD,
            confidence=0.0,
            score=0.5,
            target_position=0.0,
            reasons=["数据不足，建议观望"],
            risk_level="medium"
        )
    
    def batch_make_decisions(self, fund_analysis_results: Dict[str, Dict[str, Any]]) -> List[InvestmentDecision]:
        """批量生成投资决策"""
        decisions = []
        
        for fund_code, analysis_result in fund_analysis_results.items():
            try:
                fund_name = analysis_result.get('fund_info', {}).get('name', fund_code)
                decision = self.make_decision(fund_code, fund_name, analysis_result)
                decisions.append(decision)
            except Exception as e:
                logger.error(f"批量决策处理基金 {fund_code} 失败: {str(e)}")
                decisions.append(self._create_default_decision(fund_code, fund_code))
        
        # 按评分排序
        decisions.sort(key=lambda x: x.score, reverse=True)
        
        logger.info(f"批量决策完成，共处理 {len(decisions)} 只基金")
        return decisions
    
    def get_portfolio_allocation_suggestion(self, decisions: List[InvestmentDecision], 
                                         total_capital: float = 1.0) -> Dict[str, Any]:
        """获取投资组合配置建议"""
        buy_decisions = [d for d in decisions if d.signal in [DecisionSignal.STRONG_BUY, DecisionSignal.BUY]]
        
        if not buy_decisions:
            return {
                'total_allocation': 0.0,
                'fund_allocations': {},
                'cash_position': total_capital,
                'risk_distribution': {'low': 0, 'medium': 0, 'high': 0}
            }
        
        # 计算总目标仓位
        total_target_position = sum(d.target_position for d in buy_decisions)
        
        # 标准化仓位分配
        fund_allocations = {}
        risk_distribution = {'low': 0, 'medium': 0, 'high': 0}
        
        for decision in buy_decisions:
            if total_target_position > 0:
                normalized_position = (decision.target_position / total_target_position) * min(total_capital, 0.8)  # 最多80%仓位
                fund_allocations[decision.fund_code] = {
                    'allocation': normalized_position,
                    'signal': decision.signal.value,
                    'confidence': decision.confidence,
                    'risk_level': decision.risk_level
                }
                risk_distribution[decision.risk_level] += normalized_position
        
        return {
            'total_allocation': sum(fund_allocations[f]['allocation'] for f in fund_allocations),
            'fund_allocations': fund_allocations,
            'cash_position': total_capital - sum(fund_allocations[f]['allocation'] for f in fund_allocations),
            'risk_distribution': risk_distribution
        }
