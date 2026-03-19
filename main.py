#!/usr/bin/env python3
"""Sora CLI entrypoint."""

from __future__ import annotations

import json
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from src.sora.analysis import AnalysisEngine
from src.sora.alerts import AlertEvaluator
from src.sora.config import load_config
from src.sora.domain import AlertDirection, AlertMetric, AlertRule, Asset, AssetType, Market
from src.sora.orchestrator import SoraOrchestrator
from src.sora.providers.akshare import AkshareMarketDataProvider
from src.sora.repository import SQLiteRepository

console = Console()


def build_app(config_path: Optional[str] = None) -> tuple[SQLiteRepository, SoraOrchestrator]:
    config = load_config(config_path)
    repository = SQLiteRepository(config.database_path)
    repository.initialize()
    provider = AkshareMarketDataProvider()
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
@click.pass_context
def watchlist_add(
    ctx: click.Context,
    code: str,
    name: str,
    asset_type: str,
    market: str,
    enabled: bool,
) -> None:
    """Add or update an asset."""
    repository, orchestrator = build_app(ctx.obj["config_path"])
    asset = Asset(
        code=code.strip(),
        name=name.strip() or code.strip(),
        asset_type=AssetType(asset_type),
        market=Market(market),
        enabled=enabled,
    )
    if not orchestrator.provider.supports(asset):
        raise click.ClickException(
            f"Unsupported asset for current Sora provider: "
            f"{asset.code} ({asset.market.value}/{asset.asset_type.value})"
        )
    repository.upsert_asset(asset)
    console.print(f"[green]Saved asset:[/green] {asset.code} ({asset.asset_type.value})")


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
    for asset in assets:
        table.add_row(
            asset.code,
            asset.name,
            asset.asset_type.value,
            asset.market.value,
            "yes" if asset.enabled else "no",
        )
    console.print(table)


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
    table.add_column("Summary")
    for item in summary.successes:
        table.add_row(
            item.asset.code,
            item.trend,
            f"{item.score:.1f}",
            f"{item.snapshot.daily_change_pct:.2f}",
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
