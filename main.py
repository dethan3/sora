#!/usr/bin/env python3
"""Sora CLI entrypoint."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from src.sora.analysis import AnalysisEngine
from src.sora.alerts import AlertEvaluator
from src.sora.config import load_config
from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertRule,
    AnalysisRecord,
    Asset,
    AssetOverview,
    AssetType,
    Market,
    NotificationRecord,
    NotificationStatus,
    PortfolioOverview,
    PortfolioPositionOverview,
    RunRecord,
    SnapshotRecord,
)
from src.sora.notifications import NotificationDispatcher
from src.sora.notifiers import WebhookNotifier
from src.sora.orchestrator import SoraOrchestrator
from src.sora.providers import (
    AkshareMarketDataProvider,
    ProviderCapability,
    ProviderRegistry,
    SnapshotCacheMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from src.sora.repository import SQLiteRepository

console = Console(width=160)


def build_provider_registry(repository: SQLiteRepository) -> ProviderRegistry:
    return ProviderRegistry(
        [
            AkshareMarketDataProvider(),
            YahooFinanceMarketDataProvider(),
            SnapshotCacheMarketDataProvider(repository),
        ]
    )


def build_app(config_path: Optional[str] = None) -> tuple[SQLiteRepository, SoraOrchestrator]:
    config = load_config(config_path)
    repository = SQLiteRepository(config.database_path)
    repository.initialize()
    provider = build_provider_registry(repository)
    engine = AnalysisEngine(
        short_window=config.analysis.short_window,
        long_window=config.analysis.long_window,
    )
    orchestrator = SoraOrchestrator(
        repository=repository,
        provider=provider,
        engine=engine,
        alert_evaluator=AlertEvaluator(),
        lookback_days=config.analysis.lookback_days,
    )
    return repository, orchestrator


def build_notification_dispatcher(config_path: Optional[str] = None) -> tuple[SQLiteRepository, NotificationDispatcher]:
    config = load_config(config_path)
    repository = SQLiteRepository(config.database_path)
    repository.initialize()
    dispatcher = NotificationDispatcher(
        repository=repository,
        notifiers=[
            WebhookNotifier(
                config.notifications.webhook_urls,
                timeout_seconds=config.notifications.request_timeout_seconds,
            )
        ],
    )
    return repository, dispatcher


def _should_record_baseline(
    asset: Asset,
    *,
    is_new_asset: bool,
    baseline_mode: str | None,
) -> bool:
    if asset.asset_type != AssetType.FUND:
        return False
    if baseline_mode == "record":
        return True
    if baseline_mode == "skip":
        return False
    if not is_new_asset:
        return False
    return click.confirm(
        "Record current fund value as entry baseline for future gain tracking?",
        default=True,
    )


def _format_baseline_value(asset: Asset) -> str:
    if asset.baseline_value is None:
        return "-"
    return f"{asset.baseline_value:.4f}"


def _format_baseline_at(asset: Asset) -> str:
    if asset.baseline_at is None:
        return "-"
    return asset.baseline_at.strftime("%Y-%m-%d %H:%M")


def _format_since_entry_pct(asset: Asset, current_value: float) -> str:
    change_pct = asset.change_since_baseline_pct(current_value)
    if change_pct is None:
        return "-"
    return f"{change_pct:+.2f}"


def _format_position_units(asset: Asset) -> str:
    if asset.position_units is None:
        return "-"
    return f"{asset.position_units:.2f}"


def _format_position_cost_amount(asset: Asset) -> str:
    if asset.position_cost_amount is None:
        return "-"
    return f"{asset.position_cost_amount:.2f}"


def _format_unrealized_pnl_amount(asset: Asset, current_value: float) -> str:
    pnl_amount = asset.unrealized_pnl_amount(current_value)
    if pnl_amount is None:
        return "-"
    return f"{pnl_amount:+.2f}"


def _format_unrealized_pnl_pct(asset: Asset, current_value: float) -> str:
    pnl_pct = asset.unrealized_pnl_pct(current_value)
    if pnl_pct is None:
        return "-"
    return f"{pnl_pct:+.2f}"


def _format_run_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime or None, got {type(value)!r}")
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_run_duration(run: RunRecord) -> str:
    duration_seconds = run.duration_seconds()
    if duration_seconds is None:
        return "-"
    return f"{duration_seconds:.1f}s"


def _render_run_record(run: RunRecord) -> None:
    console.print(f"Run ID: {run.run_id}")
    console.print(f"Status: {run.status}")
    console.print(f"Started At: {_format_run_datetime(run.started_at)}")
    console.print(f"Finished At: {_format_run_datetime(run.finished_at)}")
    console.print(f"Duration: {_format_run_duration(run)}")
    console.print(
        "Assets: "
        f"total={run.total_assets} processed={run.processed_assets} "
        f"success={run.successful_assets} failed={run.failed_assets}"
    )
    if run.error_message:
        console.print(f"Error: {run.error_message}")


def _render_provider_capabilities(capabilities: list[ProviderCapability]) -> None:
    table = Table(title="Sora Providers")
    table.add_column("Provider")
    table.add_column("Markets")
    table.add_column("Asset Types")
    table.add_column("Notes")
    for capability in capabilities:
        table.add_row(
            capability.provider_name,
            ", ".join(market.value for market in capability.markets),
            ", ".join(asset_type.value for asset_type in capability.asset_types),
            capability.notes or "-",
        )
    console.print(table)


def _format_optional_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}"


def _format_optional_number(value: float | None, *, decimals: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def _format_optional_signed_number(value: float | None, *, decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:+.{decimals}f}"


def _serialize_asset(asset: Asset) -> dict[str, object]:
    return {
        "code": asset.code,
        "name": asset.name,
        "asset_type": asset.asset_type.value,
        "market": asset.market.value,
        "enabled": asset.enabled,
        "created_at": asset.created_at.isoformat(),
        "baseline_value": asset.baseline_value,
        "baseline_at": asset.baseline_at.isoformat() if asset.baseline_at else None,
        "position_units": asset.position_units,
        "position_cost_amount": asset.position_cost_amount,
    }


def _serialize_run(run: RunRecord | None) -> dict[str, object] | None:
    if run is None:
        return None
    return {
        "run_id": run.run_id,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "status": run.status,
        "total_assets": run.total_assets,
        "processed_assets": run.processed_assets,
        "successful_assets": run.successful_assets,
        "failed_assets": run.failed_assets,
        "error_message": run.error_message,
    }


def _serialize_snapshot(snapshot: SnapshotRecord | None) -> dict[str, object] | None:
    if snapshot is None:
        return None
    return {
        "run_id": snapshot.run_id,
        "asset_code": snapshot.asset_code,
        "as_of": snapshot.as_of.isoformat(),
        "current_value": snapshot.current_value,
        "previous_close": snapshot.previous_close,
        "daily_change_pct": snapshot.daily_change_pct,
        "change_7d_pct": snapshot.change_7d_pct,
        "change_30d_pct": snapshot.change_30d_pct,
        "source": snapshot.source,
        "created_at": snapshot.created_at.isoformat(),
    }


def _serialize_analysis(analysis: AnalysisRecord | None) -> dict[str, object] | None:
    if analysis is None:
        return None
    return {
        "run_id": analysis.run_id,
        "asset_code": analysis.asset_code,
        "trend": analysis.trend,
        "risk_level": analysis.risk_level,
        "score": analysis.score,
        "summary": analysis.summary,
        "metrics": analysis.metrics,
        "created_at": analysis.created_at.isoformat(),
    }


def _serialize_alert_event(event: AlertEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "asset_code": event.asset_code,
        "asset_name": event.asset_name,
        "rule_id": event.rule_id,
        "metric": event.metric.value,
        "direction": event.direction.value,
        "threshold": event.threshold,
        "metric_value": event.metric_value,
        "message": event.message,
        "created_at": event.created_at.isoformat(),
    }


def _serialize_notification_record(record: NotificationRecord) -> dict[str, object]:
    return {
        "notification_id": record.notification_id,
        "alert_event_id": record.alert_event_id,
        "run_id": record.run_id,
        "asset_code": record.asset_code,
        "asset_name": record.asset_name,
        "channel": record.channel,
        "status": record.status.value,
        "payload": record.payload,
        "created_at": record.created_at.isoformat(),
        "sent_at": record.sent_at.isoformat() if record.sent_at else None,
        "error_message": record.error_message,
        "attempt_count": record.attempt_count,
    }


def _render_asset_overview(overview: AssetOverview) -> None:
    asset = overview.asset
    console.print("[bold cyan]Asset[/bold cyan]")
    console.print(f"Code: {asset.code}")
    console.print(f"Name: {asset.name}")
    console.print(f"Type: {asset.asset_type.value}")
    console.print(f"Market: {asset.market.value}")
    console.print(f"Enabled: {'yes' if asset.enabled else 'no'}")
    console.print(f"Baseline: {_format_baseline_value(asset)}")
    console.print(f"Baseline At: {_format_baseline_at(asset)}")
    console.print(f"Units: {_format_position_units(asset)}")
    console.print(f"Cost: {_format_position_cost_amount(asset)}")

    if overview.latest_run is not None:
        console.print()
        console.print("[bold cyan]Latest Run[/bold cyan]")
        _render_run_record(overview.latest_run)

    if overview.snapshot is not None:
        snapshot = overview.snapshot
        console.print()
        console.print("[bold cyan]Latest Snapshot[/bold cyan]")
        console.print(f"As Of: {_format_run_datetime(snapshot.as_of)}")
        console.print(f"Current Value: {_format_optional_number(snapshot.current_value)}")
        console.print(f"Previous Close: {_format_optional_number(snapshot.previous_close)}")
        console.print(f"Daily %: {_format_optional_pct(snapshot.daily_change_pct)}")
        console.print(f"7D %: {_format_optional_pct(snapshot.change_7d_pct)}")
        console.print(f"30D %: {_format_optional_pct(snapshot.change_30d_pct)}")
        console.print(f"Source: {snapshot.source}")

    if overview.analysis is not None:
        analysis = overview.analysis
        console.print()
        console.print("[bold cyan]Latest Analysis[/bold cyan]")
        console.print(f"Trend: {analysis.trend}")
        console.print(f"Risk: {analysis.risk_level}")
        console.print(f"Score: {analysis.score:.1f}")
        console.print(f"Summary: {analysis.summary}")


def _build_asset_report_payload(
    overview: AssetOverview,
    alerts: list[AlertEvent],
    notifications: list[NotificationRecord],
) -> dict[str, object]:
    return {
        "asset": _serialize_asset(overview.asset),
        "latest_run": _serialize_run(overview.latest_run),
        "latest_snapshot": _serialize_snapshot(overview.snapshot),
        "latest_analysis": _serialize_analysis(overview.analysis),
        "recent_alerts": [_serialize_alert_event(event) for event in alerts],
        "recent_notifications": [
            _serialize_notification_record(record)
            for record in notifications
        ],
    }


def _build_asset_report_markdown(
    overview: AssetOverview,
    alerts: list[AlertEvent],
    notifications: list[NotificationRecord],
) -> str:
    asset = overview.asset
    lines = [
        f"# Asset Report: {asset.code}",
        "",
        f"- Name: {asset.name}",
        f"- Type: {asset.asset_type.value}",
        f"- Market: {asset.market.value}",
        f"- Enabled: {'yes' if asset.enabled else 'no'}",
        f"- Baseline: {_format_baseline_value(asset)}",
        f"- Baseline At: {_format_baseline_at(asset)}",
        f"- Units: {_format_position_units(asset)}",
        f"- Cost: {_format_position_cost_amount(asset)}",
    ]

    if overview.latest_run is not None:
        lines.extend(
            [
                "",
                "## Latest Run",
                "",
                f"- Run ID: {overview.latest_run.run_id}",
                f"- Status: {overview.latest_run.status}",
                f"- Started At: {_format_run_datetime(overview.latest_run.started_at)}",
                f"- Finished At: {_format_run_datetime(overview.latest_run.finished_at)}",
                f"- Processed: {overview.latest_run.processed_assets}",
                f"- Success: {overview.latest_run.successful_assets}",
                f"- Failed: {overview.latest_run.failed_assets}",
            ]
        )

    if overview.snapshot is not None:
        snapshot = overview.snapshot
        lines.extend(
            [
                "",
                "## Latest Snapshot",
                "",
                f"- As Of: {_format_run_datetime(snapshot.as_of)}",
                f"- Current Value: {_format_optional_number(snapshot.current_value)}",
                f"- Previous Close: {_format_optional_number(snapshot.previous_close)}",
                f"- Daily %: {_format_optional_pct(snapshot.daily_change_pct)}",
                f"- 7D %: {_format_optional_pct(snapshot.change_7d_pct)}",
                f"- 30D %: {_format_optional_pct(snapshot.change_30d_pct)}",
                f"- Source: {snapshot.source}",
            ]
        )

    if overview.analysis is not None:
        analysis = overview.analysis
        lines.extend(
            [
                "",
                "## Latest Analysis",
                "",
                f"- Trend: {analysis.trend}",
                f"- Risk: {analysis.risk_level}",
                f"- Score: {analysis.score:.1f}",
                f"- Summary: {analysis.summary}",
            ]
        )

    lines.extend(["", "## Recent Alerts", ""])
    if alerts:
        for event in alerts:
            lines.append(
                f"- {event.created_at.strftime('%Y-%m-%d %H:%M:%S')} "
                f"{event.metric.value} {event.direction.value} {event.threshold:.2f} "
                f"(value={event.metric_value:.2f})"
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Recent Notifications", ""])
    if notifications:
        for record in notifications:
            sent_at = _format_run_datetime(record.sent_at)
            lines.append(
                f"- {record.created_at.strftime('%Y-%m-%d %H:%M:%S')} "
                f"{record.channel} {record.status.value} attempts={record.attempt_count} sent_at={sent_at}"
            )
    else:
        lines.append("- None")

    return "\n".join(lines)


def _serialize_portfolio_position(position: PortfolioPositionOverview) -> dict[str, object]:
    return {
        "asset": _serialize_asset(position.asset),
        "latest_snapshot": _serialize_snapshot(position.snapshot),
        "latest_analysis": _serialize_analysis(position.analysis),
        "current_value": position.current_value(),
        "market_value": position.market_value(),
        "unrealized_pnl_amount": position.unrealized_pnl_amount(),
        "unrealized_pnl_pct": position.unrealized_pnl_pct(),
        "daily_pnl_amount": position.daily_pnl_amount(),
        "entry_value_amount": position.entry_market_value(),
        "since_entry_pnl_amount": position.since_entry_pnl_amount(),
        "since_entry_pnl_pct": position.since_entry_pnl_pct(),
    }


def _serialize_portfolio_overview(overview: PortfolioOverview) -> dict[str, object]:
    return {
        "total_positioned_assets": overview.total_positioned_assets,
        "assets_with_market_data": overview.assets_with_market_data,
        "assets_with_entry_baseline": overview.assets_with_entry_baseline,
        "total_cost_amount": overview.total_cost_amount,
        "total_market_value": overview.total_market_value,
        "total_unrealized_pnl_amount": overview.total_unrealized_pnl_amount,
        "total_unrealized_pnl_pct": overview.total_unrealized_pnl_pct,
        "total_daily_pnl_amount": overview.total_daily_pnl_amount,
        "total_entry_value_amount": overview.total_entry_value_amount,
        "total_since_entry_pnl_amount": overview.total_since_entry_pnl_amount,
        "total_since_entry_pnl_pct": overview.total_since_entry_pnl_pct,
    }


def _render_portfolio_summary(overview: PortfolioOverview) -> None:
    console.print("[bold cyan]Portfolio Summary[/bold cyan]")
    console.print(f"Positioned Assets: {overview.total_positioned_assets}")
    console.print(f"Assets With Market Data: {overview.assets_with_market_data}")
    console.print(f"Assets With Entry Baseline: {overview.assets_with_entry_baseline}")
    console.print(f"Total Cost: {_format_optional_number(overview.total_cost_amount, decimals=2)}")
    console.print(f"Total Market Value: {_format_optional_number(overview.total_market_value, decimals=2)}")
    console.print(
        f"Unrealized PnL: {_format_optional_signed_number(overview.total_unrealized_pnl_amount, decimals=2)}"
    )
    console.print(f"Unrealized PnL %: {_format_optional_pct(overview.total_unrealized_pnl_pct)}")
    console.print(
        f"Daily PnL: {_format_optional_signed_number(overview.total_daily_pnl_amount, decimals=2)}"
    )
    console.print(
        f"Since Entry PnL: {_format_optional_signed_number(overview.total_since_entry_pnl_amount, decimals=2)}"
    )
    console.print(f"Since Entry %: {_format_optional_pct(overview.total_since_entry_pnl_pct)}")


def _render_portfolio_positions(positions: list[PortfolioPositionOverview]) -> None:
    table = Table(title="Sora Portfolio Positions")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Market")
    table.add_column("Units")
    table.add_column("Cost")
    table.add_column("Current")
    table.add_column("Market Value")
    table.add_column("PnL")
    table.add_column("PnL %")
    table.add_column("Daily PnL")
    table.add_column("Since Entry %")
    table.add_column("Trend")
    table.add_column("Score")
    for position in positions:
        table.add_row(
            position.asset.code,
            position.asset.name,
            position.asset.market.value,
            _format_position_units(position.asset),
            _format_position_cost_amount(position.asset),
            _format_optional_number(position.current_value(), decimals=4),
            _format_optional_number(position.market_value(), decimals=2),
            _format_optional_signed_number(position.unrealized_pnl_amount(), decimals=2),
            _format_optional_pct(position.unrealized_pnl_pct()),
            _format_optional_signed_number(position.daily_pnl_amount(), decimals=2),
            _format_optional_pct(position.since_entry_pnl_pct()),
            position.analysis.trend if position.analysis is not None else "-",
            (
                _format_optional_number(position.analysis.score, decimals=1)
                if position.analysis is not None
                else "-"
            ),
        )
    console.print(table)


def _build_portfolio_report_payload(overview: PortfolioOverview) -> dict[str, object]:
    return {
        "summary": _serialize_portfolio_overview(overview),
        "positions": [
            _serialize_portfolio_position(position)
            for position in overview.positions
        ],
    }


def _build_portfolio_report_markdown(overview: PortfolioOverview) -> str:
    lines = [
        "# Portfolio Report",
        "",
        "## Summary",
        "",
        f"- Positioned Assets: {overview.total_positioned_assets}",
        f"- Assets With Market Data: {overview.assets_with_market_data}",
        f"- Assets With Entry Baseline: {overview.assets_with_entry_baseline}",
        f"- Total Cost: {_format_optional_number(overview.total_cost_amount, decimals=2)}",
        f"- Total Market Value: {_format_optional_number(overview.total_market_value, decimals=2)}",
        f"- Unrealized PnL: {_format_optional_signed_number(overview.total_unrealized_pnl_amount, decimals=2)}",
        f"- Unrealized PnL %: {_format_optional_pct(overview.total_unrealized_pnl_pct)}",
        f"- Daily PnL: {_format_optional_signed_number(overview.total_daily_pnl_amount, decimals=2)}",
        f"- Since Entry PnL: {_format_optional_signed_number(overview.total_since_entry_pnl_amount, decimals=2)}",
        f"- Since Entry %: {_format_optional_pct(overview.total_since_entry_pnl_pct)}",
        "",
        "## Positions",
        "",
    ]
    if not overview.positions:
        lines.append("- None")
        return "\n".join(lines)

    for position in overview.positions:
        lines.extend(
            [
                f"### {position.asset.code}",
                "",
                f"- Name: {position.asset.name}",
                f"- Market: {position.asset.market.value}",
                f"- Units: {_format_position_units(position.asset)}",
                f"- Cost: {_format_position_cost_amount(position.asset)}",
                f"- Current: {_format_optional_number(position.current_value(), decimals=4)}",
                f"- Market Value: {_format_optional_number(position.market_value(), decimals=2)}",
                f"- Unrealized PnL: {_format_optional_signed_number(position.unrealized_pnl_amount(), decimals=2)}",
                f"- Unrealized PnL %: {_format_optional_pct(position.unrealized_pnl_pct())}",
                f"- Daily PnL: {_format_optional_signed_number(position.daily_pnl_amount(), decimals=2)}",
                f"- Since Entry %: {_format_optional_pct(position.since_entry_pnl_pct())}",
                f"- Trend: {position.analysis.trend if position.analysis is not None else '-'}",
                (
                    f"- Score: {_format_optional_number(position.analysis.score, decimals=1)}"
                    if position.analysis is not None
                    else "- Score: -"
                ),
                "",
            ]
        )
    return "\n".join(lines)


@click.group()
@click.option("--config", "config_path", default="config/sora.yaml", show_default=True)
@click.pass_context
def cli(ctx: click.Context, config_path: str) -> None:
    """Sora monitoring engine for fund/index analysis."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@cli.command("init-db")
@click.pass_context
def init_db(ctx: click.Context) -> None:
    """Initialize SQLite tables."""
    repository, _ = build_app(ctx.obj["config_path"])
    repository.initialize()
    console.print(f"[green]Database ready:[/green] {repository.db_path}")


@cli.group()
def watchlist() -> None:
    """Manage watchlist assets."""


@watchlist.command("add")
@click.option("--code", required=True)
@click.option("--name", default="")
@click.option("--asset-type", "asset_type", type=click.Choice(["fund", "index"]), required=True)
@click.option("--market", type=click.Choice(["cn", "global"]), default="cn", show_default=True)
@click.option("--enabled/--disabled", default=True, show_default=True)
@click.option(
    "--record-baseline",
    "baseline_mode",
    flag_value="record",
    default=None,
    help="Record current fund value as the entry baseline.",
)
@click.option(
    "--skip-baseline",
    "baseline_mode",
    flag_value="skip",
    help="Skip baseline capture when adding a new fund.",
)
@click.option("--position-units", type=float, default=None, help="Fund units currently held.")
@click.option(
    "--position-cost-amount",
    type=float,
    default=None,
    help="Total cost amount for the current holding.",
)
@click.pass_context
def watchlist_add(
    ctx: click.Context,
    code: str,
    name: str,
    asset_type: str,
    market: str,
    enabled: bool,
    baseline_mode: str | None,
    position_units: float | None,
    position_cost_amount: float | None,
) -> None:
    """Add or update an asset."""
    repository, orchestrator = build_app(ctx.obj["config_path"])
    existing_assets = repository.list_assets(enabled_only=False, code=code.strip())
    is_new_asset = not existing_assets
    if (position_units is None) != (position_cost_amount is None):
        raise click.ClickException(
            "--position-units and --position-cost-amount must be provided together"
        )
    asset = Asset(
        code=code.strip(),
        name=name.strip() or code.strip(),
        asset_type=AssetType(asset_type),
        market=Market(market),
        enabled=enabled,
    )
    check_support = getattr(orchestrator.provider, "check_support", None)
    if callable(check_support):
        support = check_support(asset)
        if not support.supported:
            raise click.ClickException(
                f"Unsupported asset for current Sora providers: "
                f"{asset.code} ({asset.market.value}/{asset.asset_type.value})"
                f" [{support.reason}]"
            )
    elif not orchestrator.provider.supports(asset):
        raise click.ClickException(
            f"Unsupported asset for current Sora provider: "
            f"{asset.code} ({asset.market.value}/{asset.asset_type.value})"
        )
    repository.upsert_asset(asset)
    console.print(f"[green]Saved asset:[/green] {asset.code} ({asset.asset_type.value})")
    if _should_record_baseline(asset, is_new_asset=is_new_asset, baseline_mode=baseline_mode):
        try:
            series = orchestrator.provider.fetch_market_series(asset, lookback_days=2)
            repository.set_asset_baseline(asset.code, series.current_value, series.as_of)
            console.print(
                f"[green]Recorded baseline:[/green] {asset.code} "
                f"{series.current_value:.4f} @ {series.as_of.strftime('%Y-%m-%d %H:%M')}"
            )
        except Exception as exc:  # noqa: BLE001
            console.print(
                f"[yellow]Could not record baseline for {asset.code}:[/yellow] {exc}"
            )
    if position_units is not None and position_cost_amount is not None:
        repository.set_asset_position(asset.code, position_units, position_cost_amount)
        console.print(
            f"[green]Saved position:[/green] {asset.code} "
            f"units={position_units:.2f} cost={position_cost_amount:.2f}"
        )


@watchlist.command("list")
@click.option("--all", "include_disabled", is_flag=True, help="Include disabled assets.")
@click.pass_context
def watchlist_list(ctx: click.Context, include_disabled: bool) -> None:
    """List assets."""
    repository, _ = build_app(ctx.obj["config_path"])
    assets = repository.list_assets(enabled_only=not include_disabled)
    table = Table(title="Sora Watchlist")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Market")
    table.add_column("Enabled")
    table.add_column("Baseline")
    table.add_column("Baseline At")
    table.add_column("Units")
    table.add_column("Cost")
    for asset in assets:
        table.add_row(
            asset.code,
            asset.name,
            asset.asset_type.value,
            asset.market.value,
            "yes" if asset.enabled else "no",
            _format_baseline_value(asset),
            _format_baseline_at(asset),
            _format_position_units(asset),
            _format_position_cost_amount(asset),
        )
    console.print(table)


@cli.group("providers")
def providers() -> None:
    """Inspect market data providers and support coverage."""


@providers.command("list")
@click.pass_context
def providers_list(ctx: click.Context) -> None:
    """List configured providers and their declared capabilities."""
    repository, _ = build_app(ctx.obj["config_path"])
    registry = build_provider_registry(repository)
    _render_provider_capabilities(registry.capabilities())


@providers.command("check")
@click.option("--code", required=True)
@click.option("--name", default="")
@click.option("--asset-type", "asset_type", type=click.Choice(["fund", "index"]), required=True)
@click.option("--market", type=click.Choice(["cn", "global"]), default="cn", show_default=True)
@click.pass_context
def providers_check(
    ctx: click.Context,
    code: str,
    name: str,
    asset_type: str,
    market: str,
) -> None:
    """Check whether current providers can serve one asset."""
    repository, _ = build_app(ctx.obj["config_path"])
    registry = build_provider_registry(repository)
    asset = Asset(
        code=code.strip(),
        name=name.strip() or code.strip(),
        asset_type=AssetType(asset_type),
        market=Market(market),
    )
    support = registry.check_support(asset)
    console.print(f"Asset: {asset.code} ({asset.market.value}/{asset.asset_type.value})")
    console.print(f"Supported: {'yes' if support.supported else 'no'}")
    console.print(f"Provider: {support.provider_name}")
    if support.normalized_code:
        console.print(f"Normalized Code: {support.normalized_code}")
    if support.reason:
        console.print(f"Reason: {support.reason}")


@cli.group("assets")
def assets() -> None:
    """Inspect asset monitoring results."""


@assets.command("latest")
@click.option("--code", required=True)
@click.pass_context
def assets_latest(ctx: click.Context, code: str) -> None:
    """Show the latest snapshot and analysis for one asset."""
    repository, _ = build_app(ctx.obj["config_path"])
    overview = repository.get_asset_overview(code.strip())
    if overview is None:
        raise click.ClickException(f"Asset not found: {code.strip()}")

    _render_asset_overview(overview)
    if overview.snapshot is None:
        console.print()
        console.print("[yellow]No monitoring data found for this asset yet.[/yellow]")


@assets.command("history")
@click.option("--code", required=True)
@click.option("--limit", default=10, show_default=True, type=int)
@click.pass_context
def assets_history(ctx: click.Context, code: str, limit: int) -> None:
    """List recent snapshot history for one asset."""
    repository, _ = build_app(ctx.obj["config_path"])
    asset = repository.get_asset(code.strip())
    if asset is None:
        raise click.ClickException(f"Asset not found: {code.strip()}")

    snapshots = repository.list_snapshots(asset_code=asset.code, limit=limit)
    if not snapshots:
        console.print("[yellow]No snapshots found for this asset.[/yellow]")
        return

    table = Table(title=f"Sora Snapshot History {asset.code}")
    table.add_column("Run ID")
    table.add_column("As Of")
    table.add_column("Current")
    table.add_column("Daily %")
    table.add_column("7D %")
    table.add_column("30D %")
    table.add_column("Source")
    for snapshot in snapshots:
        table.add_row(
            snapshot.run_id,
            _format_run_datetime(snapshot.as_of),
            _format_optional_number(snapshot.current_value),
            _format_optional_pct(snapshot.daily_change_pct),
            _format_optional_pct(snapshot.change_7d_pct),
            _format_optional_pct(snapshot.change_30d_pct),
            snapshot.source,
        )
    console.print(table)


@cli.group("portfolio")
def portfolio() -> None:
    """Inspect positioned assets as one portfolio."""


@portfolio.command("summary")
@click.option("--all", "include_disabled", is_flag=True, help="Include disabled assets.")
@click.pass_context
def portfolio_summary(ctx: click.Context, include_disabled: bool) -> None:
    """Show the portfolio-level summary for assets with positions."""
    repository, _ = build_app(ctx.obj["config_path"])
    overview = repository.get_portfolio_overview(enabled_only=not include_disabled)
    if not overview.positions:
        console.print("[yellow]No positioned assets found.[/yellow]")
        return
    _render_portfolio_summary(overview)


@portfolio.command("positions")
@click.option("--all", "include_disabled", is_flag=True, help="Include disabled assets.")
@click.pass_context
def portfolio_positions(ctx: click.Context, include_disabled: bool) -> None:
    """List positioned assets with latest market values and PnL."""
    repository, _ = build_app(ctx.obj["config_path"])
    positions = repository.list_portfolio_positions(enabled_only=not include_disabled)
    if not positions:
        console.print("[yellow]No positioned assets found.[/yellow]")
        return
    _render_portfolio_positions(positions)


@cli.command("run-once")
@click.option("--code", default=None, help="Run only for a specific asset code.")
@click.pass_context
def run_once(ctx: click.Context, code: Optional[str]) -> None:
    """Fetch data, analyze, and persist one monitoring run."""
    _, orchestrator = build_app(ctx.obj["config_path"])
    summary = orchestrator.run_once(asset_code=code)

    if summary.total_assets == 0:
        console.print("[yellow]No enabled assets found for this run.[/yellow]")
        return

    table = Table(title=f"Sora Run {summary.run_id}")
    table.add_column("Asset")
    table.add_column("Trend")
    table.add_column("Score")
    table.add_column("Daily %")
    table.add_column("Since Entry %")
    table.add_column("Unrealized PnL")
    table.add_column("PnL %")
    table.add_column("Summary")
    for item in summary.successes:
        table.add_row(
            item.asset.code,
            item.trend,
            f"{item.score:.1f}",
            f"{item.snapshot.daily_change_pct:.2f}",
            _format_since_entry_pct(item.asset, item.snapshot.current_value),
            _format_unrealized_pnl_amount(item.asset, item.snapshot.current_value),
            _format_unrealized_pnl_pct(item.asset, item.snapshot.current_value),
            item.summary,
        )
    console.print(table)

    if summary.failures:
        console.print("[yellow]Failures:[/yellow]")
        console.print_json(json.dumps(summary.failures, ensure_ascii=False))

    console.print(
        f"[bold green]Completed[/bold green] "
        f"{summary.successful_assets}/{summary.processed_assets} assets succeeded"
    )
    if summary.alert_events:
        console.print(f"[cyan]Triggered alerts:[/cyan] {len(summary.alert_events)}")
    if summary.notification_events:
        console.print(f"[cyan]Queued notifications:[/cyan] {len(summary.notification_events)}")


@cli.group("runs")
def runs() -> None:
    """Inspect monitoring run status and history."""


@runs.command("status")
@click.pass_context
def runs_status(ctx: click.Context) -> None:
    """Show active run and latest completed run."""
    repository, _ = build_app(ctx.obj["config_path"])
    running_run = repository.get_running_run()
    latest_finished_run = repository.get_latest_finished_run()

    if running_run is None and latest_finished_run is None:
        console.print("[yellow]No monitoring runs found.[/yellow]")
        return

    if running_run is not None:
        console.print("[bold cyan]Active Run[/bold cyan]")
        _render_run_record(running_run)

    if latest_finished_run is not None:
        if running_run is not None:
            console.print()
        console.print("[bold cyan]Latest Finished Run[/bold cyan]")
        _render_run_record(latest_finished_run)


@runs.command("list")
@click.option("--limit", default=10, show_default=True, type=int)
@click.pass_context
def runs_list(ctx: click.Context, limit: int) -> None:
    """List recent monitoring runs."""
    repository, _ = build_app(ctx.obj["config_path"])
    records = repository.list_runs(limit=limit)
    if not records:
        console.print("[yellow]No monitoring runs found.[/yellow]")
        return

    table = Table(title="Sora Runs")
    table.add_column("Run ID")
    table.add_column("Status")
    table.add_column("Started At")
    table.add_column("Duration")
    table.add_column("Processed")
    table.add_column("Success")
    table.add_column("Failed")
    for run in records:
        table.add_row(
            run.run_id,
            run.status,
            _format_run_datetime(run.started_at),
            _format_run_duration(run),
            str(run.processed_assets),
            str(run.successful_assets),
            str(run.failed_assets),
        )
    console.print(table)


@cli.group("notifications")
def notifications() -> None:
    """Inspect and dispatch queued notifications."""


@notifications.command("send-pending")
@click.option("--limit", default=100, show_default=True, type=int)
@click.pass_context
def notifications_send_pending(ctx: click.Context, limit: int) -> None:
    """Send pending notifications through configured notifiers."""
    _, dispatcher = build_notification_dispatcher(ctx.obj["config_path"])
    summary = dispatcher.dispatch_pending(limit=limit)
    console.print(
        "[bold green]Notification dispatch completed[/bold green] "
        f"requested={summary.requested} sent={summary.sent} failed={summary.failed}"
    )


@notifications.command("list")
@click.option("--code", default=None, help="Optional asset code filter.")
@click.option(
    "--status",
    "statuses",
    multiple=True,
    type=click.Choice([status.value for status in NotificationStatus]),
    help="Filter by notification status. Repeat for multiple values.",
)
@click.option("--limit", default=20, show_default=True, type=int)
@click.pass_context
def notifications_list(
    ctx: click.Context,
    code: str | None,
    statuses: tuple[str, ...],
    limit: int,
) -> None:
    """List notification history."""
    repository, _ = build_app(ctx.obj["config_path"])
    resolved_statuses = (
        tuple(NotificationStatus(status) for status in statuses)
        if statuses
        else tuple(NotificationStatus)
    )
    records = repository.list_notification_records(
        asset_code=code.strip() if code else None,
        statuses=resolved_statuses,
        limit=limit,
    )
    if not records:
        console.print("[yellow]No notification events found.[/yellow]")
        return

    table = Table(title="Sora Notifications")
    table.add_column("ID")
    table.add_column("Asset")
    table.add_column("Channel")
    table.add_column("Status")
    table.add_column("Attempts")
    table.add_column("Created At")
    table.add_column("Sent At")
    table.add_column("Error")
    for record in records:
        table.add_row(
            str(record.notification_id),
            record.asset_code or "-",
            record.channel,
            record.status.value,
            str(record.attempt_count),
            _format_run_datetime(record.created_at),
            _format_run_datetime(record.sent_at),
            record.error_message or "-",
        )
    console.print(table)


@cli.group("alerts")
def alerts() -> None:
    """Inspect alert history."""


@alerts.command("list")
@click.option("--code", default=None, help="Optional asset code filter.")
@click.option("--limit", default=20, show_default=True, type=int)
@click.pass_context
def alerts_list(ctx: click.Context, code: str | None, limit: int) -> None:
    """List alert events."""
    repository, _ = build_app(ctx.obj["config_path"])
    events = repository.list_alert_events(
        asset_code=code.strip() if code else None,
        limit=limit,
    )
    if not events:
        console.print("[yellow]No alert events found.[/yellow]")
        return

    table = Table(title="Sora Alerts")
    table.add_column("ID")
    table.add_column("Created At")
    table.add_column("Asset")
    table.add_column("Metric")
    table.add_column("Direction")
    table.add_column("Threshold")
    table.add_column("Value")
    table.add_column("Run ID")
    for event in events:
        table.add_row(
            str(event.event_id or ""),
            _format_run_datetime(event.created_at),
            event.asset_code,
            event.metric.value,
            event.direction.value,
            f"{event.threshold:.2f}",
            f"{event.metric_value:.2f}",
            event.run_id,
        )
    console.print(table)


@cli.group("reports")
def reports() -> None:
    """Export simple asset reports."""


@reports.command("asset")
@click.option("--code", required=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
)
@click.option("--alerts-limit", default=5, show_default=True, type=int)
@click.option("--notifications-limit", default=5, show_default=True, type=int)
@click.pass_context
def reports_asset(
    ctx: click.Context,
    code: str,
    output_format: str,
    alerts_limit: int,
    notifications_limit: int,
) -> None:
    """Export a latest asset report in markdown or json."""
    repository, _ = build_app(ctx.obj["config_path"])
    overview = repository.get_asset_overview(code.strip())
    if overview is None:
        raise click.ClickException(f"Asset not found: {code.strip()}")

    alerts = repository.list_alert_events(asset_code=code.strip(), limit=alerts_limit)
    notifications = repository.list_notification_records(
        asset_code=code.strip(),
        limit=notifications_limit,
    )
    if output_format == "json":
        payload = _build_asset_report_payload(overview, alerts, notifications)
        console.print_json(json.dumps(payload, ensure_ascii=False))
        return

    console.print(_build_asset_report_markdown(overview, alerts, notifications))


@reports.command("portfolio")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
)
@click.option("--all", "include_disabled", is_flag=True, help="Include disabled assets.")
@click.pass_context
def reports_portfolio(
    ctx: click.Context,
    output_format: str,
    include_disabled: bool,
) -> None:
    """Export a latest portfolio report in markdown or json."""
    repository, _ = build_app(ctx.obj["config_path"])
    overview = repository.get_portfolio_overview(enabled_only=not include_disabled)
    if not overview.positions:
        raise click.ClickException("No positioned assets found.")

    if output_format == "json":
        payload = _build_portfolio_report_payload(overview)
        console.print_json(json.dumps(payload, ensure_ascii=False))
        return

    console.print(_build_portfolio_report_markdown(overview))


@cli.group("alert-rule")
def alert_rule() -> None:
    """Manage alert rules."""


@alert_rule.command("add")
@click.option("--asset-code", default=None, help="Optional asset code. Omit to apply to all assets.")
@click.option(
    "--metric",
    type=click.Choice([metric.value for metric in AlertMetric]),
    required=True,
)
@click.option(
    "--direction",
    type=click.Choice([direction.value for direction in AlertDirection]),
    required=True,
)
@click.option("--threshold", type=float, required=True)
@click.option("--channel", "channels", multiple=True, help="Notification channel. Repeat for multiple.")
@click.option("--enabled/--disabled", default=True, show_default=True)
@click.pass_context
def alert_rule_add(
    ctx: click.Context,
    asset_code: Optional[str],
    metric: str,
    direction: str,
    threshold: float,
    channels: tuple[str, ...],
    enabled: bool,
) -> None:
    """Add an alert rule."""
    repository, _ = build_app(ctx.obj["config_path"])
    rule = repository.add_alert_rule(
        AlertRule(
            asset_code=asset_code.strip() if asset_code else None,
            metric=AlertMetric(metric),
            direction=AlertDirection(direction),
            threshold=threshold,
            channels=list(channels),
            enabled=enabled,
        )
    )
    target = rule.asset_code or "*"
    console.print(
        f"[green]Saved alert rule:[/green] #{rule.rule_id} {target} "
        f"{rule.metric.value} {rule.direction.value} {rule.threshold:.2f}"
    )


@alert_rule.command("list")
@click.option("--all", "include_disabled", is_flag=True, help="Include disabled rules.")
@click.pass_context
def alert_rule_list(ctx: click.Context, include_disabled: bool) -> None:
    """List alert rules."""
    repository, _ = build_app(ctx.obj["config_path"])
    rules = repository.list_alert_rules(enabled_only=not include_disabled)
    table = Table(title="Sora Alert Rules")
    table.add_column("ID")
    table.add_column("Asset")
    table.add_column("Metric")
    table.add_column("Direction")
    table.add_column("Threshold")
    table.add_column("Channels")
    table.add_column("Enabled")
    for rule in rules:
        table.add_row(
            str(rule.rule_id or ""),
            rule.asset_code or "*",
            rule.metric.value,
            rule.direction.value,
            f"{rule.threshold:.2f}",
            ", ".join(rule.channels) or "-",
            "yes" if rule.enabled else "no",
        )
    console.print(table)


if __name__ == "__main__":
    cli()
