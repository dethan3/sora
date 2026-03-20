"""Presentation helpers for the Sora CLI."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.table import Table

from src.sora.domain import (
    AnalysisRecord,
    AlertEvent,
    Asset,
    AssetOverview,
    NotificationRecord,
    PortfolioHistoryPoint,
    PortfolioOverview,
    PortfolioPositionOverview,
    RunRecord,
    SnapshotRecord,
)
from src.sora.providers import ProviderCapability

console = Console(width=160)


def format_baseline_value(asset: Asset) -> str:
    if asset.baseline_value is None:
        return "-"
    return f"{asset.baseline_value:.4f}"


def format_baseline_at(asset: Asset) -> str:
    if asset.baseline_at is None:
        return "-"
    return asset.baseline_at.strftime("%Y-%m-%d %H:%M")


def format_since_entry_pct(asset: Asset, current_value: float) -> str:
    change_pct = asset.change_since_baseline_pct(current_value)
    if change_pct is None:
        return "-"
    return f"{change_pct:+.2f}"


def format_position_units(asset: Asset) -> str:
    if asset.position_units is None:
        return "-"
    return f"{asset.position_units:.2f}"


def format_position_cost_amount(asset: Asset) -> str:
    if asset.position_cost_amount is None:
        return "-"
    return f"{asset.position_cost_amount:.2f}"


def format_unrealized_pnl_amount(asset: Asset, current_value: float) -> str:
    pnl_amount = asset.unrealized_pnl_amount(current_value)
    if pnl_amount is None:
        return "-"
    return f"{pnl_amount:+.2f}"


def format_unrealized_pnl_pct(asset: Asset, current_value: float) -> str:
    pnl_pct = asset.unrealized_pnl_pct(current_value)
    if pnl_pct is None:
        return "-"
    return f"{pnl_pct:+.2f}"


def format_run_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    if not isinstance(value, datetime):
        raise TypeError(f"expected datetime or None, got {type(value)!r}")
    return value.strftime("%Y-%m-%d %H:%M:%S")


def format_run_duration(run: RunRecord) -> str:
    duration_seconds = run.duration_seconds()
    if duration_seconds is None:
        return "-"
    return f"{duration_seconds:.1f}s"


def render_run_record(run: RunRecord) -> None:
    console.print(f"Run ID: {run.run_id}")
    console.print(f"Status: {run.status}")
    console.print(f"Started At: {format_run_datetime(run.started_at)}")
    console.print(f"Finished At: {format_run_datetime(run.finished_at)}")
    console.print(f"Duration: {format_run_duration(run)}")
    console.print(
        "Assets: "
        f"total={run.total_assets} processed={run.processed_assets} "
        f"success={run.successful_assets} failed={run.failed_assets}"
    )
    if run.error_message:
        console.print(f"Error: {run.error_message}")


def render_provider_capabilities(capabilities: list[ProviderCapability]) -> None:
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


def format_optional_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}"


def format_optional_number(value: float | None, *, decimals: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def format_optional_signed_number(value: float | None, *, decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:+.{decimals}f}"


def normalize_float(value: float | None, *, decimals: int = 6) -> float | None:
    if value is None:
        return None
    return round(value, decimals)


def serialize_asset(asset: Asset) -> dict[str, object]:
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


def serialize_run(run: RunRecord | None) -> dict[str, object] | None:
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


def serialize_snapshot(snapshot: SnapshotRecord | None) -> dict[str, object] | None:
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


def serialize_analysis(analysis: AnalysisRecord | None) -> dict[str, object] | None:
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


def serialize_alert_event(event: AlertEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "scope": event.scope.value,
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


def serialize_notification_record(record: NotificationRecord) -> dict[str, object]:
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


def render_asset_overview(overview: AssetOverview) -> None:
    asset = overview.asset
    console.print("[bold cyan]Asset[/bold cyan]")
    console.print(f"Code: {asset.code}")
    console.print(f"Name: {asset.name}")
    console.print(f"Type: {asset.asset_type.value}")
    console.print(f"Market: {asset.market.value}")
    console.print(f"Enabled: {'yes' if asset.enabled else 'no'}")
    console.print(f"Baseline: {format_baseline_value(asset)}")
    console.print(f"Baseline At: {format_baseline_at(asset)}")
    console.print(f"Units: {format_position_units(asset)}")
    console.print(f"Cost: {format_position_cost_amount(asset)}")

    if overview.latest_run is not None:
        console.print()
        console.print("[bold cyan]Latest Run[/bold cyan]")
        render_run_record(overview.latest_run)

    if overview.snapshot is not None:
        snapshot = overview.snapshot
        console.print()
        console.print("[bold cyan]Latest Snapshot[/bold cyan]")
        console.print(f"As Of: {format_run_datetime(snapshot.as_of)}")
        console.print(f"Current Value: {format_optional_number(snapshot.current_value)}")
        console.print(f"Previous Close: {format_optional_number(snapshot.previous_close)}")
        console.print(f"Daily %: {format_optional_pct(snapshot.daily_change_pct)}")
        console.print(f"7D %: {format_optional_pct(snapshot.change_7d_pct)}")
        console.print(f"30D %: {format_optional_pct(snapshot.change_30d_pct)}")
        console.print(f"Source: {snapshot.source}")

    if overview.analysis is not None:
        analysis = overview.analysis
        console.print()
        console.print("[bold cyan]Latest Analysis[/bold cyan]")
        console.print(f"Trend: {analysis.trend}")
        console.print(f"Risk: {analysis.risk_level}")
        console.print(f"Score: {analysis.score:.1f}")
        console.print(f"Summary: {analysis.summary}")


def build_asset_report_payload(
    overview: AssetOverview,
    alerts: list[AlertEvent],
    notifications: list[NotificationRecord],
) -> dict[str, object]:
    return {
        "asset": serialize_asset(overview.asset),
        "latest_run": serialize_run(overview.latest_run),
        "latest_snapshot": serialize_snapshot(overview.snapshot),
        "latest_analysis": serialize_analysis(overview.analysis),
        "recent_alerts": [serialize_alert_event(event) for event in alerts],
        "recent_notifications": [
            serialize_notification_record(record)
            for record in notifications
        ],
    }


def build_asset_report_markdown(
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
        f"- Baseline: {format_baseline_value(asset)}",
        f"- Baseline At: {format_baseline_at(asset)}",
        f"- Units: {format_position_units(asset)}",
        f"- Cost: {format_position_cost_amount(asset)}",
    ]

    if overview.latest_run is not None:
        lines.extend(
            [
                "",
                "## Latest Run",
                "",
                f"- Run ID: {overview.latest_run.run_id}",
                f"- Status: {overview.latest_run.status}",
                f"- Started At: {format_run_datetime(overview.latest_run.started_at)}",
                f"- Finished At: {format_run_datetime(overview.latest_run.finished_at)}",
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
                f"- As Of: {format_run_datetime(snapshot.as_of)}",
                f"- Current Value: {format_optional_number(snapshot.current_value)}",
                f"- Previous Close: {format_optional_number(snapshot.previous_close)}",
                f"- Daily %: {format_optional_pct(snapshot.daily_change_pct)}",
                f"- 7D %: {format_optional_pct(snapshot.change_7d_pct)}",
                f"- 30D %: {format_optional_pct(snapshot.change_30d_pct)}",
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
            sent_at = format_run_datetime(record.sent_at)
            lines.append(
                f"- {record.created_at.strftime('%Y-%m-%d %H:%M:%S')} "
                f"{record.channel} {record.status.value} attempts={record.attempt_count} sent_at={sent_at}"
            )
    else:
        lines.append("- None")

    return "\n".join(lines)


def serialize_portfolio_position(
    position: PortfolioPositionOverview,
    portfolio_market_value: float,
) -> dict[str, object]:
    return {
        "asset": serialize_asset(position.asset),
        "latest_snapshot": serialize_snapshot(position.snapshot),
        "latest_analysis": serialize_analysis(position.analysis),
        "current_value": normalize_float(position.current_value()),
        "market_value": normalize_float(position.market_value()),
        "weight_pct": normalize_float(position.weight_pct(portfolio_market_value)),
        "unrealized_pnl_amount": normalize_float(position.unrealized_pnl_amount()),
        "unrealized_pnl_pct": normalize_float(position.unrealized_pnl_pct()),
        "daily_pnl_amount": normalize_float(position.daily_pnl_amount()),
        "entry_value_amount": normalize_float(position.entry_market_value()),
        "since_entry_pnl_amount": normalize_float(position.since_entry_pnl_amount()),
        "since_entry_pnl_pct": normalize_float(position.since_entry_pnl_pct()),
    }


def serialize_portfolio_history_point(point: PortfolioHistoryPoint) -> dict[str, object]:
    return {
        "as_of": point.as_of.isoformat(),
        "market_value": normalize_float(point.market_value),
    }


def serialize_portfolio_overview(overview: PortfolioOverview) -> dict[str, object]:
    return {
        "total_positioned_assets": overview.total_positioned_assets,
        "assets_with_market_data": overview.assets_with_market_data,
        "assets_with_entry_baseline": overview.assets_with_entry_baseline,
        "total_cost_amount": normalize_float(overview.total_cost_amount),
        "total_market_value": normalize_float(overview.total_market_value),
        "total_unrealized_pnl_amount": normalize_float(overview.total_unrealized_pnl_amount),
        "total_unrealized_pnl_pct": normalize_float(overview.total_unrealized_pnl_pct),
        "total_daily_pnl_amount": normalize_float(overview.total_daily_pnl_amount),
        "total_entry_value_amount": normalize_float(overview.total_entry_value_amount),
        "total_since_entry_pnl_amount": normalize_float(overview.total_since_entry_pnl_amount),
        "total_since_entry_pnl_pct": normalize_float(overview.total_since_entry_pnl_pct),
        "peak_market_value": normalize_float(overview.peak_market_value),
        "drawdown_amount": normalize_float(overview.drawdown_amount),
        "drawdown_pct": normalize_float(overview.drawdown_pct),
        "largest_position_weight_pct": normalize_float(overview.largest_position_weight_pct),
        "top3_position_weight_pct": normalize_float(overview.top3_position_weight_pct),
    }


def render_portfolio_summary(overview: PortfolioOverview) -> None:
    console.print("[bold cyan]Portfolio Summary[/bold cyan]")
    console.print(f"Positioned Assets: {overview.total_positioned_assets}")
    console.print(f"Assets With Market Data: {overview.assets_with_market_data}")
    console.print(f"Assets With Entry Baseline: {overview.assets_with_entry_baseline}")
    console.print(f"Total Cost: {format_optional_number(overview.total_cost_amount, decimals=2)}")
    console.print(f"Total Market Value: {format_optional_number(overview.total_market_value, decimals=2)}")
    console.print(
        f"Unrealized PnL: {format_optional_signed_number(overview.total_unrealized_pnl_amount, decimals=2)}"
    )
    console.print(f"Unrealized PnL %: {format_optional_pct(overview.total_unrealized_pnl_pct)}")
    console.print(
        f"Daily PnL: {format_optional_signed_number(overview.total_daily_pnl_amount, decimals=2)}"
    )
    console.print(
        f"Since Entry PnL: {format_optional_signed_number(overview.total_since_entry_pnl_amount, decimals=2)}"
    )
    console.print(f"Since Entry %: {format_optional_pct(overview.total_since_entry_pnl_pct)}")
    console.print(f"Peak Market Value: {format_optional_number(overview.peak_market_value, decimals=2)}")
    console.print(
        f"Drawdown: {format_optional_signed_number(overview.drawdown_amount, decimals=2)}"
    )
    console.print(f"Drawdown %: {format_optional_pct(overview.drawdown_pct)}")
    console.print(
        f"Largest Position %: {format_optional_pct(overview.largest_position_weight_pct)}"
    )
    console.print(
        f"Top 3 Concentration %: {format_optional_pct(overview.top3_position_weight_pct)}"
    )


def render_portfolio_positions(overview: PortfolioOverview) -> None:
    table = Table(title="Sora Portfolio Positions")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Market")
    table.add_column("Units")
    table.add_column("Cost")
    table.add_column("Current")
    table.add_column("Market Value")
    table.add_column("Weight %")
    table.add_column("PnL")
    table.add_column("PnL %")
    table.add_column("Daily PnL")
    table.add_column("Since Entry %")
    table.add_column("Trend")
    table.add_column("Score")
    for position in overview.positions:
        table.add_row(
            position.asset.code,
            position.asset.name,
            position.asset.market.value,
            format_position_units(position.asset),
            format_position_cost_amount(position.asset),
            format_optional_number(position.current_value(), decimals=4),
            format_optional_number(position.market_value(), decimals=2),
            format_optional_pct(position.weight_pct(overview.total_market_value)),
            format_optional_signed_number(position.unrealized_pnl_amount(), decimals=2),
            format_optional_pct(position.unrealized_pnl_pct()),
            format_optional_signed_number(position.daily_pnl_amount(), decimals=2),
            format_optional_pct(position.since_entry_pnl_pct()),
            position.analysis.trend if position.analysis is not None else "-",
            (
                format_optional_number(position.analysis.score, decimals=1)
                if position.analysis is not None
                else "-"
            ),
        )
    console.print(table)


def build_portfolio_report_payload(overview: PortfolioOverview) -> dict[str, object]:
    return {
        "summary": serialize_portfolio_overview(overview),
        "positions": [
            serialize_portfolio_position(position, overview.total_market_value)
            for position in overview.positions
        ],
        "history": [
            serialize_portfolio_history_point(point)
            for point in overview.history
        ],
    }


def build_portfolio_report_markdown(overview: PortfolioOverview) -> str:
    lines = [
        "# Portfolio Report",
        "",
        "## Summary",
        "",
        f"- Positioned Assets: {overview.total_positioned_assets}",
        f"- Assets With Market Data: {overview.assets_with_market_data}",
        f"- Assets With Entry Baseline: {overview.assets_with_entry_baseline}",
        f"- Total Cost: {format_optional_number(overview.total_cost_amount, decimals=2)}",
        f"- Total Market Value: {format_optional_number(overview.total_market_value, decimals=2)}",
        f"- Unrealized PnL: {format_optional_signed_number(overview.total_unrealized_pnl_amount, decimals=2)}",
        f"- Unrealized PnL %: {format_optional_pct(overview.total_unrealized_pnl_pct)}",
        f"- Daily PnL: {format_optional_signed_number(overview.total_daily_pnl_amount, decimals=2)}",
        f"- Since Entry PnL: {format_optional_signed_number(overview.total_since_entry_pnl_amount, decimals=2)}",
        f"- Since Entry %: {format_optional_pct(overview.total_since_entry_pnl_pct)}",
        f"- Peak Market Value: {format_optional_number(overview.peak_market_value, decimals=2)}",
        f"- Drawdown: {format_optional_signed_number(overview.drawdown_amount, decimals=2)}",
        f"- Drawdown %: {format_optional_pct(overview.drawdown_pct)}",
        f"- Largest Position %: {format_optional_pct(overview.largest_position_weight_pct)}",
        f"- Top 3 Concentration %: {format_optional_pct(overview.top3_position_weight_pct)}",
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
                f"- Units: {format_position_units(position.asset)}",
                f"- Cost: {format_position_cost_amount(position.asset)}",
                f"- Current: {format_optional_number(position.current_value(), decimals=4)}",
                f"- Market Value: {format_optional_number(position.market_value(), decimals=2)}",
                f"- Weight %: {format_optional_pct(position.weight_pct(overview.total_market_value))}",
                f"- Unrealized PnL: {format_optional_signed_number(position.unrealized_pnl_amount(), decimals=2)}",
                f"- Unrealized PnL %: {format_optional_pct(position.unrealized_pnl_pct())}",
                f"- Daily PnL: {format_optional_signed_number(position.daily_pnl_amount(), decimals=2)}",
                f"- Since Entry %: {format_optional_pct(position.since_entry_pnl_pct())}",
                f"- Trend: {position.analysis.trend if position.analysis is not None else '-'}",
                (
                    f"- Score: {format_optional_number(position.analysis.score, decimals=1)}"
                    if position.analysis is not None
                    else "- Score: -"
                ),
                "",
            ]
        )
    lines.extend(["## History", ""])
    if overview.history:
        for point in overview.history[-10:]:
            lines.append(
                f"- {point.as_of.strftime('%Y-%m-%d %H:%M:%S')} value={point.market_value:.2f}"
            )
    else:
        lines.append("- None")
    return "\n".join(lines)
