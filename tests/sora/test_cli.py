from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import main as main_module
import yaml
from click.testing import CliRunner

from main import cli
from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertScope,
    AnalysisResult,
    Asset,
    AssetType,
    Market,
    MarketSeries,
    NotificationEvent,
    NotificationStatus,
    PricePoint,
    RunSummary,
    Snapshot,
)
from src.sora.notifications import NotificationDispatcher
from src.sora.repository import SQLiteRepository


def _write_config(config_path: Path, db_path: Path) -> None:
    config_path.write_text(
        yaml.safe_dump(
            {
                "database_path": str(db_path),
                "analysis": {
                    "lookback_days": 90,
                    "short_window": 7,
                    "long_window": 30,
                },
            }
        ),
        encoding="utf-8",
    )


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
        daily_change_pct=8.5,
        change_7d_pct=10.0,
        change_30d_pct=15.0,
        source="test",
    )
    result = AnalysisResult(
        run_id=run_id,
        asset=asset,
        snapshot=snapshot,
        trend="bullish",
        risk_level="low",
        score=82.0,
        summary="ok",
        metrics={"daily_change_pct": 8.5},
    )
    _, notification_events = repository.save_run_artifacts(
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
                metric_value=8.5,
                message="daily change crossed threshold",
            )
        ],
        [
            NotificationEvent(
                channel="feishu",
                payload={"message": "daily change crossed threshold"},
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
    repository.mark_notification_sent(notification_events[0].notification_id)
    return run_id


def _seed_portfolio_history(repository: SQLiteRepository) -> str:
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

    first_run_id = repository.start_run(total_assets=2)
    repository.save_result(
        AnalysisResult(
            run_id=first_run_id,
            asset=first_asset,
            snapshot=Snapshot(
                asset=first_asset,
                as_of=datetime(2025, 1, 1),
                current_value=1.4,
                previous_close=1.3,
                daily_change_pct=7.69,
                change_7d_pct=9.0,
                change_30d_pct=13.0,
                source="test",
            ),
            trend="bullish",
            risk_level="low",
            score=88.0,
            summary="trend strong",
            metrics={},
        )
    )
    repository.save_result(
        AnalysisResult(
            run_id=first_run_id,
            asset=second_asset,
            snapshot=Snapshot(
                asset=second_asset,
                as_of=datetime(2025, 1, 1),
                current_value=1.0,
                previous_close=0.95,
                daily_change_pct=5.26,
                change_7d_pct=6.0,
                change_30d_pct=10.0,
                source="test",
            ),
            trend="bullish",
            risk_level="low",
            score=76.0,
            summary="trend ok",
            metrics={},
        )
    )
    repository.finish_run(
        run_id=first_run_id,
        processed_assets=2,
        successful_assets=2,
        failed_assets=0,
        status="completed",
    )

    latest_run_id = repository.start_run(total_assets=2)
    repository.save_result(
        AnalysisResult(
            run_id=latest_run_id,
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
    )
    repository.save_result(
        AnalysisResult(
            run_id=latest_run_id,
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
    )
    repository.finish_run(
        run_id=latest_run_id,
        processed_assets=2,
        successful_assets=2,
        failed_assets=0,
        status="completed",
    )
    return latest_run_id


def test_cli_init_db_creates_sqlite_file(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)

    result = CliRunner().invoke(cli, ["--config", str(config_path), "init-db"])

    assert result.exit_code == 0
    assert db_path.exists()
    assert "Database ready:" in result.output


def test_cli_watchlist_add_and_list(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    runner = CliRunner()

    add_result = runner.invoke(
        cli,
        [
            "--config",
            str(config_path),
            "watchlist",
            "add",
            "--code",
            "510300",
            "--name",
            "沪深300ETF",
            "--asset-type",
            "fund",
            "--market",
            "cn",
            "--skip-baseline",
        ],
    )
    list_result = runner.invoke(cli, ["--config", str(config_path), "watchlist", "list"])

    assert add_result.exit_code == 0
    assert "Saved asset:" in add_result.output
    assert list_result.exit_code == 0
    assert "Sora Watchlist" in list_result.output
    assert "510300" in list_result.output

    repository = SQLiteRepository(str(db_path))
    assets = repository.list_assets()
    assert len(assets) == 1
    assert assets[0].code == "510300"
    assert assets[0].name == "沪深300ETF"


def test_cli_watchlist_add_can_store_position(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)

    result = CliRunner().invoke(
        cli,
        [
            "--config",
            str(config_path),
            "watchlist",
            "add",
            "--code",
            "510300",
            "--name",
            "沪深300ETF",
            "--asset-type",
            "fund",
            "--market",
            "cn",
            "--skip-baseline",
            "--position-units",
            "100",
            "--position-cost-amount",
            "123.45",
        ],
    )

    assert result.exit_code == 0
    assert "Saved position:" in result.output

    repository = SQLiteRepository(str(db_path))
    asset = repository.list_assets()[0]
    assert asset.position_units == 100.0
    assert asset.position_cost_amount == 123.45


def test_cli_watchlist_add_can_record_baseline(tmp_path, monkeypatch):
    repository = SQLiteRepository(str(tmp_path / "cli.db"))
    repository.initialize()

    class BaselineProvider:
        def supports(self, asset: Asset) -> bool:
            return True

        def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
            base = datetime(2025, 1, 1)
            history = [
                PricePoint(at=base, value=1.0000),
                PricePoint(at=base + timedelta(days=1), value=1.1000),
            ]
            return MarketSeries(
                asset=asset,
                source="stub",
                currency="CNY",
                as_of=history[-1].at,
                current_value=history[-1].value,
                previous_close=history[-2].value,
                history=history,
            )

    monkeypatch.setattr(
        main_module,
        "build_app",
        lambda config_path=None: (
            repository,
            SimpleNamespace(provider=BaselineProvider()),
        ),
    )

    result = CliRunner().invoke(
        main_module.cli,
        [
            "watchlist",
            "add",
            "--code",
            "510300",
            "--name",
            "沪深300ETF",
            "--asset-type",
            "fund",
            "--market",
            "cn",
            "--record-baseline",
        ],
    )

    assert result.exit_code == 0
    assert "Recorded baseline:" in result.output

    asset = repository.list_assets()[0]
    assert asset.baseline_value == 1.1
    assert asset.baseline_at == datetime(2025, 1, 2)


def test_cli_alert_rule_add_and_list(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    runner = CliRunner()

    add_result = runner.invoke(
        cli,
        [
            "--config",
            str(config_path),
            "alert-rule",
            "add",
            "--asset-code",
            "510300",
            "--metric",
            "daily_change_pct",
            "--direction",
            "above",
            "--threshold",
            "1.5",
            "--channel",
            "feishu",
            "--channel",
            "telegram",
        ],
    )
    list_result = runner.invoke(cli, ["--config", str(config_path), "alert-rule", "list"])

    assert add_result.exit_code == 0
    assert "Saved alert rule:" in add_result.output
    assert list_result.exit_code == 0
    assert "Sora Alert Rules" in list_result.output
    assert "510300" in list_result.output

    repository = SQLiteRepository(str(db_path))
    rules = repository.list_alert_rules()
    assert len(rules) == 1
    assert rules[0].metric.value == "daily_change_pct"
    assert rules[0].channels == ["feishu", "telegram"]


def test_cli_alert_rule_add_and_list_supports_portfolio_scope(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    runner = CliRunner()

    add_result = runner.invoke(
        cli,
        [
            "--config",
            str(config_path),
            "alert-rule",
            "add",
            "--scope",
            "portfolio",
            "--metric",
            "portfolio_unrealized_pnl_pct",
            "--direction",
            "below",
            "--threshold",
            "-3",
            "--channel",
            "feishu",
        ],
    )
    list_result = runner.invoke(cli, ["--config", str(config_path), "alert-rule", "list"])

    assert add_result.exit_code == 0
    assert "portfolio:portfolio" in add_result.output
    assert list_result.exit_code == 0
    assert "portfolio" in list_result.output
    assert "portfolio_unrealized_pnl_pct" in list_result.output

    repository = SQLiteRepository(str(db_path))
    rules = repository.list_alert_rules()
    assert len(rules) == 1
    assert rules[0].scope == AlertScope.PORTFOLIO
    assert rules[0].metric == AlertMetric.PORTFOLIO_UNREALIZED_PNL_PCT


def test_cli_run_once_shows_since_entry_gain(monkeypatch):
    asset = Asset(
        code="510300",
        name="沪深300ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
        baseline_value=1.0,
        baseline_at=datetime(2025, 1, 1),
        position_units=100.0,
        position_cost_amount=110.0,
    )
    snapshot = Snapshot(
        asset=asset,
        as_of=datetime(2025, 1, 2),
        current_value=1.2,
        previous_close=1.1,
        daily_change_pct=8.5,
        change_7d_pct=None,
        change_30d_pct=None,
        source="test",
    )
    summary = RunSummary(
        run_id="run-1",
        total_assets=1,
        processed_assets=1,
        successful_assets=1,
        failed_assets=0,
        successes=[
            AnalysisResult(
                run_id="run-1",
                asset=asset,
                snapshot=snapshot,
                trend="bullish",
                risk_level="low",
                score=82.0,
                summary="ok",
                metrics={},
            )
        ],
        failures=[],
    )

    monkeypatch.setattr(
        main_module,
        "build_app",
        lambda config_path=None: (
            None,
            SimpleNamespace(run_once=lambda asset_code=None: summary),
        ),
    )

    result = CliRunner().invoke(main_module.cli, ["run-once"])

    assert result.exit_code == 0
    assert "510300" in result.output
    assert "+20.00" in result.output
    assert "+10.00" in result.output
    assert "+9.09" in result.output


def test_cli_run_once_without_assets_succeeds(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)

    result = CliRunner().invoke(cli, ["--config", str(config_path), "run-once"])

    assert result.exit_code == 0
    assert "No enabled assets found for this run." in result.output

    repository = SQLiteRepository(str(db_path))
    with repository.connect() as conn:
        run_count = conn.execute("SELECT COUNT(*) FROM monitoring_runs").fetchone()[0]

    assert run_count == 1


def test_cli_runs_status_shows_active_and_latest_finished_run(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    repository = SQLiteRepository(str(db_path))
    repository.initialize()

    finished_run = repository.start_run(total_assets=2)
    repository.finish_run(
        run_id=finished_run,
        processed_assets=2,
        successful_assets=2,
        failed_assets=0,
        status="completed",
    )
    active_run = repository.start_run(total_assets=1)

    result = CliRunner().invoke(cli, ["--config", str(config_path), "runs", "status"])

    assert result.exit_code == 0
    assert "Active Run" in result.output
    assert "Latest Finished Run" in result.output
    assert active_run in result.output
    assert finished_run in result.output


def test_cli_runs_list_shows_recent_runs(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    repository = SQLiteRepository(str(db_path))
    repository.initialize()

    first_run = repository.start_run(total_assets=3)
    repository.finish_run(
        run_id=first_run,
        processed_assets=3,
        successful_assets=2,
        failed_assets=1,
        status="partial_failed",
        error_message="1 assets failed",
    )
    second_run = repository.start_run(total_assets=1)
    repository.finish_run(
        run_id=second_run,
        processed_assets=1,
        successful_assets=1,
        failed_assets=0,
        status="completed",
    )

    result = CliRunner().invoke(cli, ["--config", str(config_path), "runs", "list", "--limit", "5"])

    assert result.exit_code == 0
    assert "Sora Runs" in result.output

    runs = repository.list_runs(limit=5)
    assert [run.run_id for run in runs] == [second_run, first_run]


def test_cli_notifications_send_pending_updates_queue(tmp_path, monkeypatch):
    repository = SQLiteRepository(str(tmp_path / "cli.db"))
    repository.initialize()
    created = repository.save_notification_event(
        NotificationEvent(channel="feishu", payload={"message": "hello"})
    )
    assert created.notification_id is not None

    class SuccessNotifier:
        def supports(self, channel: str) -> bool:
            return channel == "feishu"

        def send(self, event: NotificationEvent) -> None:
            return None

    monkeypatch.setattr(
        main_module,
        "build_notification_dispatcher",
        lambda config_path=None: (
            repository,
            NotificationDispatcher(repository=repository, notifiers=[SuccessNotifier()]),
        ),
    )

    result = CliRunner().invoke(main_module.cli, ["notifications", "send-pending", "--limit", "10"])

    assert result.exit_code == 0
    assert "Notification dispatch completed" in result.output
    assert "requested=1 sent=1 failed=0" in result.output

    sent = repository.list_notification_events(statuses=(NotificationStatus.SENT,))
    assert len(sent) == 1


def test_cli_assets_latest_and_history_show_monitoring_data(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    repository = SQLiteRepository(str(db_path))
    repository.initialize()
    _seed_monitoring_history(repository)
    runner = CliRunner()

    latest_result = runner.invoke(
        cli,
        ["--config", str(config_path), "assets", "latest", "--code", "510300"],
    )
    history_result = runner.invoke(
        cli,
        ["--config", str(config_path), "assets", "history", "--code", "510300", "--limit", "5"],
    )

    assert latest_result.exit_code == 0
    assert "Latest Snapshot" in latest_result.output
    assert "Latest Analysis" in latest_result.output
    assert "510300" in latest_result.output
    assert history_result.exit_code == 0
    assert "Sora Snapshot History 510300" in history_result.output


def test_cli_alerts_notifications_and_reports_show_recent_data(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    repository = SQLiteRepository(str(db_path))
    repository.initialize()
    _seed_monitoring_history(repository)
    runner = CliRunner()

    alerts_result = runner.invoke(
        cli,
        ["--config", str(config_path), "alerts", "list", "--code", "510300", "--limit", "5"],
    )
    notifications_result = runner.invoke(
        cli,
        [
            "--config",
            str(config_path),
            "notifications",
            "list",
            "--code",
            "510300",
            "--status",
            "sent",
            "--limit",
            "5",
        ],
    )
    report_result = runner.invoke(
        cli,
        ["--config", str(config_path), "reports", "asset", "--code", "510300", "--format", "json"],
    )

    assert alerts_result.exit_code == 0
    assert "Sora Alerts" in alerts_result.output
    assert "510300" in alerts_result.output
    assert "above" in alerts_result.output
    assert notifications_result.exit_code == 0
    assert "Sora Notifications" in notifications_result.output
    assert "sent" in notifications_result.output
    assert report_result.exit_code == 0
    assert '"asset"' in report_result.output
    assert '"latest_analysis"' in report_result.output
    assert '"510300"' in report_result.output


def test_cli_portfolio_summary_positions_and_report_show_portfolio_view(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    repository = SQLiteRepository(str(db_path))
    repository.initialize()
    _seed_portfolio_history(repository)
    runner = CliRunner()

    summary_result = runner.invoke(
        cli,
        ["--config", str(config_path), "portfolio", "summary"],
    )
    positions_result = runner.invoke(
        cli,
        ["--config", str(config_path), "portfolio", "positions"],
        terminal_width=160,
    )
    report_result = runner.invoke(
        cli,
        ["--config", str(config_path), "reports", "portfolio", "--format", "json"],
    )

    assert summary_result.exit_code == 0
    assert "Portfolio Summary" in summary_result.output
    assert "Positioned Assets: 2" in summary_result.output
    assert "Total Market Value: 165.00" in summary_result.output
    assert "Unrealized PnL: -5.00" in summary_result.output
    assert "Since Entry PnL: +20.00" in summary_result.output
    assert "Peak Market Value: 190.00" in summary_result.output
    assert "Drawdown: -25.00" in summary_result.output
    assert "Largest Position %: +72.73" in summary_result.output
    assert "Top 3 Concentration %: +100.00" in summary_result.output

    assert positions_result.exit_code == 0
    assert "Sora Portfolio Positions" in positions_result.output
    assert "510300" in positions_result.output
    assert "159915" in positions_result.output
    assert "bullish" in positions_result.output
    assert "bearish" in positions_result.output
    assert "Weight %" in positions_result.output
    assert "+72.73" in positions_result.output

    assert report_result.exit_code == 0
    assert '"summary"' in report_result.output
    assert '"positions"' in report_result.output
    assert '"history"' in report_result.output
    assert '"peak_market_value": 190.0' in report_result.output
    assert '"510300"' in report_result.output


def test_cli_providers_list_and_check_show_registry_information(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)
    runner = CliRunner()

    list_result = runner.invoke(cli, ["--config", str(config_path), "providers", "list"])
    check_result = runner.invoke(
        cli,
        [
            "--config",
            str(config_path),
            "providers",
            "check",
            "--code",
            "510300",
            "--asset-type",
            "fund",
            "--market",
            "cn",
        ],
    )

    assert list_result.exit_code == 0
    assert "Sora Providers" in list_result.output
    assert "akshare" in list_result.output
    assert "yahoo_finance" in list_result.output
    assert "snapshot_cache" in list_result.output
    assert check_result.exit_code == 0
    assert "Supported: yes" in check_result.output


def test_cli_watchlist_add_accepts_global_etf_scope(tmp_path):
    config_path = tmp_path / "sora.yaml"
    db_path = tmp_path / "cli.db"
    _write_config(config_path, db_path)

    result = CliRunner().invoke(
        cli,
        [
            "--config",
            str(config_path),
            "watchlist",
            "add",
            "--code",
            "QQQ",
            "--name",
            "Invesco QQQ Trust",
            "--asset-type",
            "fund",
            "--market",
            "global",
            "--skip-baseline",
        ],
    )

    assert result.exit_code == 0
    assert "Saved asset:" in result.output
