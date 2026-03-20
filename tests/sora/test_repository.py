from datetime import datetime

from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertRule,
    AnalysisResult,
    Asset,
    AssetType,
    Market,
    NotificationEvent,
    NotificationStatus,
    Snapshot,
)
from src.sora.repository import SQLiteRepository


def _seed_monitoring_history(repository: SQLiteRepository) -> str:
    asset = Asset(
        code="510300",
        name="沪深300ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
        baseline_value=1.0,
        baseline_at=datetime(2025, 1, 1),
    )
    repository.upsert_asset(asset)
    run_id = repository.start_run(total_assets=1)
    snapshot = Snapshot(
        asset=asset,
        as_of=datetime(2025, 1, 2),
        current_value=1.2,
        previous_close=1.1,
        daily_change_pct=9.09,
        change_7d_pct=12.5,
        change_30d_pct=18.0,
        source="test",
    )
    result = AnalysisResult(
        run_id=run_id,
        asset=asset,
        snapshot=snapshot,
        trend="bullish",
        risk_level="low",
        score=82.0,
        summary="trend ok",
        metrics={"daily_change_pct": 9.09},
    )
    alert_events, notification_events = repository.save_run_artifacts(
        result,
        [
            AlertEvent(
                run_id=run_id,
                asset_code=asset.code,
                asset_name=asset.name,
                rule_id=1,
                metric=AlertMetric.DAILY_CHANGE_PCT,
                direction=AlertDirection.ABOVE,
                threshold=5.0,
                metric_value=9.09,
                message="daily_change_pct crossed threshold",
            )
        ],
        [
            NotificationEvent(
                channel="feishu",
                payload={"message": "daily_change_pct crossed threshold"},
            )
        ],
    )
    repository.finish_run(
        run_id=run_id,
        processed_assets=1,
        successful_assets=1,
        failed_assets=0,
        status="completed",
    )
    assert notification_events[0].notification_id is not None
    repository.mark_notification_sent(notification_events[0].notification_id, sent_at=datetime(2025, 1, 2, 9, 0))
    return run_id


def test_repository_upserts_and_lists_assets(tmp_path):
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

    assets = repository.list_assets()
    assert len(assets) == 1
    assert assets[0].code == "510300"
    assert assets[0].asset_type == AssetType.FUND


def test_repository_adds_and_lists_alert_rules(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    created = repository.add_alert_rule(
        AlertRule(
            asset_code="510300",
            metric=AlertMetric.DAILY_CHANGE_PCT,
            direction=AlertDirection.BELOW,
            threshold=-2.0,
            channels=["feishu", "telegram"],
        )
    )

    rules = repository.list_alert_rules()

    assert created.rule_id is not None
    assert len(rules) == 1
    assert rules[0].asset_code == "510300"
    assert rules[0].channels == ["feishu", "telegram"]


def test_repository_stores_asset_baseline(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    baseline_at = datetime(2025, 1, 2, 15, 0)

    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )
    repository.set_asset_baseline("510300", 1.2345, baseline_at)

    asset = repository.list_assets()[0]

    assert asset.baseline_value == 1.2345
    assert asset.baseline_at == baseline_at


def test_repository_stores_asset_position(tmp_path):
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
    repository.set_asset_position("510300", 100.0, 123.45)

    asset = repository.list_assets()[0]

    assert asset.position_units == 100.0
    assert asset.position_cost_amount == 123.45


def test_repository_lists_and_reads_run_records(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    first_run = repository.start_run(total_assets=2)
    repository.finish_run(
        run_id=first_run,
        processed_assets=2,
        successful_assets=2,
        failed_assets=0,
        status="completed",
    )
    second_run = repository.start_run(total_assets=1)

    latest_run = repository.get_latest_run()
    running_run = repository.get_running_run()
    latest_finished_run = repository.get_latest_finished_run()
    runs = repository.list_runs(limit=5)

    assert latest_run is not None
    assert latest_run.run_id == second_run
    assert running_run is not None
    assert running_run.run_id == second_run
    assert latest_finished_run is not None
    assert latest_finished_run.run_id == first_run
    assert [run.run_id for run in runs] == [second_run, first_run]


def test_repository_lists_and_updates_notification_events(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    created = repository.save_notification_event(
        NotificationEvent(
            channel="feishu",
            payload={"message": "hello"},
        )
    )

    pending = repository.list_notification_events()
    assert len(pending) == 1
    assert pending[0].notification_id == created.notification_id
    assert pending[0].status == NotificationStatus.PENDING
    assert pending[0].attempt_count == 0

    assert created.notification_id is not None
    repository.mark_notification_failed(created.notification_id, "network down")
    failed = repository.list_notification_events(statuses=(NotificationStatus.FAILED,))
    assert len(failed) == 1
    assert failed[0].error_message == "network down"
    assert failed[0].attempt_count == 1

    repository.mark_notification_sent(created.notification_id)
    sent = repository.list_notification_events(statuses=(NotificationStatus.SENT,))
    assert len(sent) == 1
    assert sent[0].sent_at is not None
    assert sent[0].attempt_count == 2


def test_repository_reads_asset_overview_and_snapshot_history(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    run_id = _seed_monitoring_history(repository)

    overview = repository.get_asset_overview("510300")
    snapshots = repository.list_snapshots(asset_code="510300", limit=5)
    analysis = repository.get_latest_analysis("510300")

    assert overview is not None
    assert overview.asset.code == "510300"
    assert overview.latest_run is not None
    assert overview.latest_run.run_id == run_id
    assert overview.snapshot is not None
    assert overview.snapshot.current_value == 1.2
    assert overview.analysis is not None
    assert overview.analysis.summary == "trend ok"
    assert len(snapshots) == 1
    assert snapshots[0].run_id == run_id
    assert analysis is not None
    assert analysis.score == 82.0


def test_repository_lists_alert_and_notification_records(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    _seed_monitoring_history(repository)

    alerts = repository.list_alert_events(asset_code="510300", limit=10)
    notifications = repository.list_notification_records(
        asset_code="510300",
        statuses=(NotificationStatus.SENT,),
        limit=10,
    )

    assert len(alerts) == 1
    assert alerts[0].asset_name == "沪深300ETF"
    assert alerts[0].metric == AlertMetric.DAILY_CHANGE_PCT
    assert len(notifications) == 1
    assert notifications[0].asset_code == "510300"
    assert notifications[0].status == NotificationStatus.SENT


def test_repository_builds_portfolio_overview_from_positioned_assets(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    first_asset = Asset(
        code="510300",
        name="沪深300ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
        baseline_value=1.0,
        baseline_at=datetime(2025, 1, 1),
        position_units=100.0,
        position_cost_amount=110.0,
    )
    second_asset = Asset(
        code="159915",
        name="创业板ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
        position_units=50.0,
        position_cost_amount=60.0,
    )
    repository.upsert_asset(first_asset)
    repository.upsert_asset(second_asset)

    run_id = repository.start_run(total_assets=2)
    first_result = AnalysisResult(
        run_id=run_id,
        asset=first_asset,
        snapshot=Snapshot(
            asset=first_asset,
            as_of=datetime(2025, 1, 2),
            current_value=1.2,
            previous_close=1.1,
            daily_change_pct=9.09,
            change_7d_pct=12.5,
            change_30d_pct=18.0,
            source="test",
        ),
        trend="bullish",
        risk_level="low",
        score=82.0,
        summary="trend ok",
        metrics={},
    )
    second_result = AnalysisResult(
        run_id=run_id,
        asset=second_asset,
        snapshot=Snapshot(
            asset=second_asset,
            as_of=datetime(2025, 1, 2),
            current_value=0.9,
            previous_close=1.0,
            daily_change_pct=-10.0,
            change_7d_pct=-8.0,
            change_30d_pct=-12.0,
            source="test",
        ),
        trend="bearish",
        risk_level="medium",
        score=34.0,
        summary="trend weak",
        metrics={},
    )
    repository.save_result(first_result)
    repository.save_result(second_result)
    repository.finish_run(
        run_id=run_id,
        processed_assets=2,
        successful_assets=2,
        failed_assets=0,
        status="completed",
    )

    positions = repository.list_portfolio_positions()
    overview = repository.get_portfolio_overview()

    assert len(positions) == 2
    assert overview.total_positioned_assets == 2
    assert overview.assets_with_market_data == 2
    assert overview.assets_with_entry_baseline == 1
    assert overview.total_cost_amount == 170.0
    assert overview.total_market_value == 165.0
    assert overview.total_unrealized_pnl_amount == -5.0
    assert round(overview.total_unrealized_pnl_pct or 0.0, 2) == -2.94
    assert round(overview.total_daily_pnl_amount, 2) == 5.0
    assert overview.total_entry_value_amount == 100.0
    assert overview.total_since_entry_pnl_amount == 20.0
    assert overview.total_since_entry_pnl_pct == 20.0
