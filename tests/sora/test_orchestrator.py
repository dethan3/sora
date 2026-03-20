from datetime import datetime, timedelta

import pytest

from src.sora.analysis import AnalysisEngine
from src.sora.domain import AlertDirection, AlertMetric, AlertRule, Asset, AssetType, Market, MarketSeries, PricePoint
from src.sora.orchestrator import RunAlreadyInProgressError, SoraOrchestrator
from src.sora.providers import ProviderRegistry
from src.sora.repository import SQLiteRepository


class StaticProvider:
    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        base = datetime(2025, 1, 1)
        history = [
            PricePoint(at=base + timedelta(days=idx), value=10 + idx * 0.1)
            for idx in range(45)
        ]
        return MarketSeries(
            asset=asset,
            source="static",
            currency="CNY",
            as_of=history[-1].at,
            current_value=history[-1].value,
            previous_close=history[-2].value,
            history=history,
        )


def test_orchestrator_persists_run_outputs(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )

    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=StaticProvider(),
        engine=AnalysisEngine(),
        lookback_days=90,
    )
    summary = orchestrator.run_once()

    assert summary.total_assets == 1
    assert summary.processed_assets == 1
    assert summary.successful_assets == 1
    assert summary.failed_assets == 0
    assert not summary.failures

    with repository.connect() as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        analysis_count = conn.execute("SELECT COUNT(*) FROM analysis_history").fetchone()[0]
        run_row = conn.execute(
            "SELECT processed_assets, successful_assets, failed_assets, status FROM monitoring_runs"
        ).fetchone()

    assert snapshot_count == 1
    assert analysis_count == 1
    assert run_row["processed_assets"] == 1
    assert run_row["successful_assets"] == 1
    assert run_row["failed_assets"] == 0
    assert run_row["status"] == "completed"


class UnsupportedProvider:
    def supports(self, asset: Asset) -> bool:
        return False

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        raise AssertionError("fetch_market_series should not be called for unsupported assets")


def test_orchestrator_records_unsupported_assets_as_failures(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    repository.upsert_asset(
        Asset(
            code="SPX",
            name="标普500",
            asset_type=AssetType.INDEX,
            market=Market.GLOBAL,
        )
    )

    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=UnsupportedProvider(),
        engine=AnalysisEngine(),
        lookback_days=90,
    )
    summary = orchestrator.run_once()

    assert summary.total_assets == 1
    assert summary.processed_assets == 1
    assert summary.successful_assets == 0
    assert summary.failed_assets == 1
    assert len(summary.failures) == 1

    with repository.connect() as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        analysis_count = conn.execute("SELECT COUNT(*) FROM analysis_history").fetchone()[0]
        run_row = conn.execute(
            "SELECT processed_assets, successful_assets, failed_assets, status FROM monitoring_runs"
        ).fetchone()

    assert snapshot_count == 0
    assert analysis_count == 0
    assert run_row["processed_assets"] == 1
    assert run_row["successful_assets"] == 0
    assert run_row["failed_assets"] == 1
    assert run_row["status"] == "failed"


class FlakyProvider:
    name = "flaky"

    def supports(self, asset: Asset) -> bool:
        return True

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        raise RuntimeError("upstream timeout")


def test_orchestrator_uses_fallback_provider_when_primary_fetch_fails(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )

    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=ProviderRegistry([FlakyProvider(), StaticProvider()]),
        engine=AnalysisEngine(),
        lookback_days=90,
    )
    summary = orchestrator.run_once()

    assert summary.successful_assets == 1
    assert summary.failed_assets == 0
    assert summary.successes[0].snapshot.source == "static"


def test_orchestrator_persists_alerts_and_notifications(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )
    repository.add_alert_rule(
        AlertRule(
            asset_code="510300",
            metric=AlertMetric.DAILY_CHANGE_PCT,
            direction=AlertDirection.ABOVE,
            threshold=0.5,
            channels=["feishu"],
        )
    )
    repository.add_alert_rule(
        AlertRule(
            asset_code="510300",
            metric=AlertMetric.CHANGE_7D_PCT,
            direction=AlertDirection.ABOVE,
            threshold=1.0,
            channels=["telegram"],
        )
    )

    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=StaticProvider(),
        engine=AnalysisEngine(),
        lookback_days=90,
    )
    summary = orchestrator.run_once()

    assert len(summary.alert_events) == 2
    assert len(summary.notification_events) == 2
    assert {event.channel for event in summary.notification_events} == {"feishu", "telegram"}
    assert all(event.alert_event_id is not None for event in summary.notification_events)

    with repository.connect() as conn:
        alert_count = conn.execute("SELECT COUNT(*) FROM alert_events").fetchone()[0]
        notification_count = conn.execute("SELECT COUNT(*) FROM notification_events").fetchone()[0]
        rows = conn.execute(
            """
            SELECT a.metric, n.channel
            FROM notification_events n
            JOIN alert_events a ON a.id = n.alert_event_id
            ORDER BY n.id ASC
            """
        ).fetchall()

    assert alert_count == 2
    assert notification_count == 2
    assert {(row["metric"], row["channel"]) for row in rows} == {
        ("daily_change_pct", "feishu"),
        ("change_7d_pct", "telegram"),
    }


def test_orchestrator_rejects_overlapping_running_run(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )
    active_run_id = repository.start_run(total_assets=1)

    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=StaticProvider(),
        engine=AnalysisEngine(),
        lookback_days=90,
    )

    with pytest.raises(RunAlreadyInProgressError) as exc_info:
        orchestrator.run_once()

    assert active_run_id in str(exc_info.value)
