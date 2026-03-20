#!/usr/bin/env python3
"""Sora CLI entrypoint."""

from __future__ import annotations

import json
from typing import Optional

import click
from rich.table import Table

from src.sora.analysis import AnalysisEngine
from src.sora.alerts import AlertEvaluator
from src.sora.config import load_config
from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertRule,
    AlertScope,
    Asset,
    AssetType,
    Market,
    NotificationStatus,
)
from src.sora.notifications import NotificationDispatcher
from src.sora.notifiers import WebhookNotifier
from src.sora.orchestrator import SoraOrchestrator
from src.sora.presenter import (
    build_asset_report_markdown as _build_asset_report_markdown,
    build_asset_report_payload as _build_asset_report_payload,
    build_portfolio_report_markdown as _build_portfolio_report_markdown,
    build_portfolio_report_payload as _build_portfolio_report_payload,
    console,
    format_baseline_at as _format_baseline_at,
    format_baseline_value as _format_baseline_value,
    format_optional_number as _format_optional_number,
    format_optional_pct as _format_optional_pct,
    format_optional_signed_number as _format_optional_signed_number,
    format_position_cost_amount as _format_position_cost_amount,
    format_position_units as _format_position_units,
    format_run_datetime as _format_run_datetime,
    format_run_duration as _format_run_duration,
    format_since_entry_pct as _format_since_entry_pct,
    format_unrealized_pnl_amount as _format_unrealized_pnl_amount,
    format_unrealized_pnl_pct as _format_unrealized_pnl_pct,
    render_asset_overview as _render_asset_overview,
    render_portfolio_positions as _render_portfolio_positions,
    render_portfolio_summary as _render_portfolio_summary,
    render_provider_capabilities as _render_provider_capabilities,
    render_run_record as _render_run_record,
)
from src.sora.providers import (
    AkshareMarketDataProvider,
    ProviderRegistry,
    SnapshotCacheMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from src.sora.repository import SQLiteRepository


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
    overview = repository.get_portfolio_overview(enabled_only=not include_disabled)
    if not overview.positions:
        console.print("[yellow]No positioned assets found.[/yellow]")
        return
    _render_portfolio_positions(overview)


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
    table.add_column("Scope")
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
            event.scope.value,
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
@click.option(
    "--scope",
    "scope",
    type=click.Choice([scope.value for scope in AlertScope]),
    default=AlertScope.ASSET.value,
    show_default=True,
)
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
    scope: str,
    asset_code: Optional[str],
    metric: str,
    direction: str,
    threshold: float,
    channels: tuple[str, ...],
    enabled: bool,
) -> None:
    """Add an alert rule."""
    repository, _ = build_app(ctx.obj["config_path"])
    resolved_scope = AlertScope(scope)
    resolved_asset_code = asset_code.strip() if asset_code else None
    if resolved_scope == AlertScope.PORTFOLIO and resolved_asset_code is not None:
        raise click.ClickException("portfolio alert rules must not specify --asset-code")
    rule = repository.add_alert_rule(
        AlertRule(
            scope=resolved_scope,
            asset_code=resolved_asset_code,
            metric=AlertMetric(metric),
            direction=AlertDirection(direction),
            threshold=threshold,
            channels=list(channels),
            enabled=enabled,
        )
    )
    target = rule.asset_code or ("portfolio" if rule.scope == AlertScope.PORTFOLIO else "*")
    console.print(
        f"[green]Saved alert rule:[/green] #{rule.rule_id} {rule.scope.value}:{target} "
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
    table.add_column("Scope")
    table.add_column("Asset")
    table.add_column("Metric")
    table.add_column("Direction")
    table.add_column("Threshold")
    table.add_column("Channels")
    table.add_column("Enabled")
    for rule in rules:
        table.add_row(
            str(rule.rule_id or ""),
            rule.scope.value,
            rule.asset_code or ("portfolio" if rule.scope == AlertScope.PORTFOLIO else "*"),
            rule.metric.value,
            rule.direction.value,
            f"{rule.threshold:.2f}",
            ", ".join(rule.channels) or "-",
            "yes" if rule.enabled else "no",
        )
    console.print(table)


if __name__ == "__main__":
    cli()
