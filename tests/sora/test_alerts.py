from datetime import datetime, timedelta

from src.sora.alerts import AlertEvaluator
from src.sora.domain import (
    AlertDirection,
    AlertMetric,
    AlertRule,
    AlertScope,
    AnalysisResult,
    Asset,
    AssetType,
    Market,
    MarketSeries,
    PortfolioOverview,
    PricePoint,
    Snapshot,
)


def _build_result() -> AnalysisResult:
    asset = Asset(code="510300", name="沪深300ETF", asset_type=AssetType.FUND, market=Market.CN)
    base = datetime(2025, 1, 1)
    history = [
        PricePoint(at=base + timedelta(days=idx), value=1 + idx * 0.01)
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
    snapshot = Snapshot(
        asset=asset,
        as_of=series.as_of,
        current_value=series.current_value,
        previous_close=series.previous_close,
        daily_change_pct=2.5,
        change_7d_pct=5.0,
        change_30d_pct=12.0,
        source=series.source,
    )
    return AnalysisResult(
        run_id="run-1",
        asset=asset,
        snapshot=snapshot,
        trend="bullish",
        risk_level="low",
        score=78.0,
        summary="ok",
        metrics={"sma5": 1.0},
    )


def test_alert_evaluator_emits_alert_and_notification():
    result = _build_result()
    rules = [
        AlertRule(
            asset_code="510300",
            metric=AlertMetric.DAILY_CHANGE_PCT,
            direction=AlertDirection.ABOVE,
            threshold=2.0,
            channels=["feishu"],
        )
    ]

    alerts, notifications = AlertEvaluator().evaluate(result, rules)

    assert len(alerts) == 1
    assert alerts[0].asset_code == "510300"
    assert alerts[0].metric_value == 2.5
    assert alerts[0].correlation_key is not None
    assert len(notifications) == 1
    assert notifications[0].channel == "feishu"
    assert notifications[0].payload["metric"] == "daily_change_pct"
    assert notifications[0].correlation_key == alerts[0].correlation_key


def test_alert_evaluator_emits_portfolio_alert_and_notification():
    overview = PortfolioOverview(
        positions=[],
        total_positioned_assets=2,
        assets_with_market_data=2,
        assets_with_entry_baseline=1,
        total_cost_amount=170.0,
        total_market_value=165.0,
        total_unrealized_pnl_amount=-5.0,
        total_unrealized_pnl_pct=-2.94,
        total_daily_pnl_amount=5.0,
        total_entry_value_amount=100.0,
        total_since_entry_pnl_amount=20.0,
        total_since_entry_pnl_pct=20.0,
    )
    rules = [
        AlertRule(
            scope=AlertScope.PORTFOLIO,
            metric=AlertMetric.PORTFOLIO_UNREALIZED_PNL_PCT,
            direction=AlertDirection.BELOW,
            threshold=-2.0,
            channels=["feishu"],
        )
    ]

    alerts, notifications = AlertEvaluator().evaluate_portfolio("run-1", overview, rules)

    assert len(alerts) == 1
    assert alerts[0].scope == AlertScope.PORTFOLIO
    assert alerts[0].asset_code == "portfolio"
    assert alerts[0].metric_value == -2.94
    assert len(notifications) == 1
    assert notifications[0].payload["scope"] == "portfolio"
    assert notifications[0].payload["metric"] == "portfolio_unrealized_pnl_pct"
