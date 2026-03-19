from datetime import datetime, timedelta

from src.sora.analysis import AnalysisEngine
from src.sora.domain import Asset, AssetType, Market, MarketSeries, PricePoint


def test_analysis_engine_classifies_bullish_trend():
    asset = Asset(code="510300", name="沪深300ETF", asset_type=AssetType.FUND, market=Market.CN)
    base = datetime(2025, 1, 1)
    history = [
        PricePoint(at=base + timedelta(days=idx), value=1 + idx * 0.02)
        for idx in range(40)
    ]
    series = MarketSeries(
        asset=asset,
        source="test",
        currency="CNY",
        as_of=history[-1].at,
        current_value=history[-1].value,
        previous_close=history[-2].value,
        history=history,
    )

    result = AnalysisEngine().analyze("run-1", series)

    assert result.trend == "bullish"
    assert result.snapshot.daily_change_pct > 0
    assert result.score > 50
