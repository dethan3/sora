import math
from typing import Dict, Any

import numpy as np
import pandas as pd
import pytest

from src.config.manager import ConfigManager
from src.decision.engine import DecisionEngine, DecisionSignal
from src.analytics.calculator import AnalyticsCalculator


@pytest.fixture(scope="module")
def config_manager() -> ConfigManager:
    return ConfigManager(config_dir="config")


def minimal_analysis_result(extra_low_mean: Dict[str, Any]) -> Dict[str, Any]:
    """Build a minimal analysis_result dict for DecisionEngine."""
    return {
        # Enough fields to pass DecisionEngine calculations
        'technical_indicators': {  # set composite_score to control base signal
            'composite_score': 0.40,
            'rsi': {'value': 45, 'signal': 'neutral'},
            'macd': {'signal': 'neutral'},
        },
        'volatility': {
            'annualized': 0.20,
        },
        'trend_analysis': {
            'score': 0.40,
            'direction': 'sideways',
            'strength': 0.50,
        },
        'momentum': {
            'score': 0.40,
        },
        'performance': {
            '1m_return': 0.02
        },
        'extra': {
            'low_mean': extra_low_mean
        }
    }


def test_strategy_low_mean_config_parsed(config_manager: ConfigManager):
    lm = config_manager.strategy.low_mean
    assert lm is not None
    assert isinstance(lm.rolling_mean_days, int)
    # Defaults from settings.yaml proposal
    assert math.isclose(lm.buy_threshold, 0.20, rel_tol=1e-9)
    assert math.isclose(lm.strong_buy_threshold, 0.30, rel_tol=1e-9)
    assert lm.windows == [756, 252, 'all']


def test_decision_override_buy_signal(config_manager: ConfigManager):
    engine = DecisionEngine(config_manager)
    # Discount 21% with valid window should trigger at least BUY
    ar = minimal_analysis_result({'discount_ratio': 0.21, 'window_used': '756d'})
    decision = engine.make_decision('510300', '沪深300ETF联接', ar)
    assert decision.signal in (DecisionSignal.BUY, DecisionSignal.STRONG_BUY)
    # First reason mentions low-mean
    assert '低位均值折价' in decision.reasons[0]


def test_decision_position_boost_with_discount(config_manager: ConfigManager):
    engine = DecisionEngine(config_manager)
    # Baseline with no discount (insufficient) to get base position
    base_ar = minimal_analysis_result({'discount_ratio': None, 'window_used': 'insufficient'})
    base_decision = engine.make_decision('510300', '沪深300ETF联接', base_ar)

    # Now with 35% discount, position should be boosted (subject to cap)
    boosted_ar = minimal_analysis_result({'discount_ratio': 0.35, 'window_used': '252d'})
    boosted_decision = engine.make_decision('510300', '沪深300ETF联接', boosted_ar)

    assert boosted_decision.target_position >= base_decision.target_position


def test_compute_low_mean_discount_basic():
    # Synthetic price series rising, so historical rolling mean min occurs early
    prices = np.linspace(1.0, 2.0, 300)
    calc = AnalyticsCalculator(
        analysis_days=60,
        lm_rolling_mean_days=20,
        lm_windows=[252, 'all'],
        lm_min_required_days=60,
        lm_use_ema_fallback=True,
        lm_ema_days=10,
    )
    current_price = prices[-1]
    result = calc._compute_low_mean_discount(prices, list(range(len(prices))), current_price)
    assert result['window_used'] in ('252d', 'all', 'ema_proxy')
    # Discount should be non-negative
    assert result['discount_ratio'] is None or result['discount_ratio'] >= 0.0
