"""Core domain models for Sora."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class AssetType(str, Enum):
    FUND = "fund"
    INDEX = "index"


class Market(str, Enum):
    CN = "cn"
    GLOBAL = "global"


class AlertMetric(str, Enum):
    DAILY_CHANGE_PCT = "daily_change_pct"
    CHANGE_7D_PCT = "change_7d_pct"
    CHANGE_30D_PCT = "change_30d_pct"
    SCORE = "score"
    PORTFOLIO_UNREALIZED_PNL_AMOUNT = "portfolio_unrealized_pnl_amount"
    PORTFOLIO_UNREALIZED_PNL_PCT = "portfolio_unrealized_pnl_pct"
    PORTFOLIO_DAILY_PNL_AMOUNT = "portfolio_daily_pnl_amount"
    PORTFOLIO_SINCE_ENTRY_PNL_PCT = "portfolio_since_entry_pnl_pct"


class AlertScope(str, Enum):
    ASSET = "asset"
    PORTFOLIO = "portfolio"


class AlertDirection(str, Enum):
    ABOVE = "above"
    BELOW = "below"


class NotificationStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


ASSET_ALERT_METRICS = {
    AlertMetric.DAILY_CHANGE_PCT,
    AlertMetric.CHANGE_7D_PCT,
    AlertMetric.CHANGE_30D_PCT,
    AlertMetric.SCORE,
}

PORTFOLIO_ALERT_METRICS = {
    AlertMetric.PORTFOLIO_UNREALIZED_PNL_AMOUNT,
    AlertMetric.PORTFOLIO_UNREALIZED_PNL_PCT,
    AlertMetric.PORTFOLIO_DAILY_PNL_AMOUNT,
    AlertMetric.PORTFOLIO_SINCE_ENTRY_PNL_PCT,
}


@dataclass(slots=True)
class Asset:
    code: str
    name: str
    asset_type: AssetType
    market: Market
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    baseline_value: float | None = None
    baseline_at: datetime | None = None
    position_units: float | None = None
    position_cost_amount: float | None = None

    def __post_init__(self) -> None:
        self.code = self.code.strip()
        self.name = self.name.strip()
        if not self.code:
            raise ValueError("asset code must not be empty")
        if not self.name:
            raise ValueError("asset name must not be empty")
        if (self.baseline_value is None) != (self.baseline_at is None):
            raise ValueError("asset baseline_value and baseline_at must be set together")
        if self.baseline_value is not None and self.baseline_value <= 0:
            raise ValueError("asset baseline_value must be greater than 0")
        if (self.position_units is None) != (self.position_cost_amount is None):
            raise ValueError("asset position_units and position_cost_amount must be set together")
        if self.position_units is not None and self.position_units <= 0:
            raise ValueError("asset position_units must be greater than 0")
        if self.position_cost_amount is not None and self.position_cost_amount <= 0:
            raise ValueError("asset position_cost_amount must be greater than 0")

    def change_since_baseline_pct(self, current_value: float) -> float | None:
        if self.baseline_value is None:
            return None
        return ((current_value - self.baseline_value) / self.baseline_value) * 100

    def position_market_value(self, current_value: float) -> float | None:
        if self.position_units is None:
            return None
        return current_value * self.position_units

    def unrealized_pnl_amount(self, current_value: float) -> float | None:
        market_value = self.position_market_value(current_value)
        if market_value is None or self.position_cost_amount is None:
            return None
        return market_value - self.position_cost_amount

    def unrealized_pnl_pct(self, current_value: float) -> float | None:
        pnl_amount = self.unrealized_pnl_amount(current_value)
        if pnl_amount is None or self.position_cost_amount is None:
            return None
        return (pnl_amount / self.position_cost_amount) * 100


@dataclass(slots=True)
class PricePoint:
    at: datetime
    value: float


@dataclass(slots=True)
class MarketSeries:
    asset: Asset
    source: str
    currency: str
    as_of: datetime
    current_value: float
    previous_close: float
    history: list[PricePoint]


@dataclass(slots=True)
class Snapshot:
    asset: Asset
    as_of: datetime
    current_value: float
    previous_close: float
    daily_change_pct: float
    change_7d_pct: float | None
    change_30d_pct: float | None
    source: str


@dataclass(slots=True)
class AnalysisResult:
    run_id: str
    asset: Asset
    snapshot: Snapshot
    trend: str
    risk_level: str
    score: float
    summary: str
    metrics: dict[str, float | str | None]


@dataclass(slots=True)
class AlertRule:
    metric: AlertMetric
    direction: AlertDirection
    threshold: float
    channels: list[str] = field(default_factory=list)
    scope: AlertScope = AlertScope.ASSET
    asset_code: str | None = None
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    rule_id: int | None = None

    def __post_init__(self) -> None:
        if self.scope == AlertScope.PORTFOLIO and self.asset_code is not None:
            raise ValueError("portfolio alert rules must not specify asset_code")
        if self.asset_code is not None:
            self.asset_code = self.asset_code.strip() or None
        allowed_metrics = (
            ASSET_ALERT_METRICS
            if self.scope == AlertScope.ASSET
            else PORTFOLIO_ALERT_METRICS
        )
        if self.metric not in allowed_metrics:
            raise ValueError(
                f"metric {self.metric.value} is not supported for {self.scope.value} alert rules"
            )
        normalized_channels: list[str] = []
        seen_channels: set[str] = set()
        for channel in self.channels:
            normalized = channel.strip()
            if not normalized or normalized in seen_channels:
                continue
            normalized_channels.append(normalized)
            seen_channels.add(normalized)
        self.channels = normalized_channels


@dataclass(slots=True)
class AlertEvent:
    run_id: str
    asset_code: str
    asset_name: str
    rule_id: int | None
    metric: AlertMetric
    direction: AlertDirection
    threshold: float
    metric_value: float
    message: str
    scope: AlertScope = AlertScope.ASSET
    correlation_key: str | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    event_id: int | None = None


@dataclass(slots=True)
class NotificationEvent:
    channel: str
    payload: dict[str, Any]
    status: NotificationStatus = NotificationStatus.PENDING
    alert_event_id: int | None = None
    correlation_key: str | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    sent_at: datetime | None = None
    error_message: str | None = None
    attempt_count: int = 0
    notification_id: int | None = None


@dataclass(slots=True)
class RunSummary:
    run_id: str
    total_assets: int
    processed_assets: int
    successful_assets: int
    failed_assets: int
    successes: list[AnalysisResult]
    failures: list[dict[str, str]]
    alert_events: list[AlertEvent] = field(default_factory=list)
    notification_events: list[NotificationEvent] = field(default_factory=list)


@dataclass(slots=True)
class RunRecord:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    total_assets: int
    processed_assets: int
    successful_assets: int
    failed_assets: int
    status: str
    error_message: str | None = None

    def duration_seconds(self) -> float | None:
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()


@dataclass(slots=True)
class SnapshotRecord:
    run_id: str
    asset_code: str
    as_of: datetime
    current_value: float
    previous_close: float
    daily_change_pct: float
    change_7d_pct: float | None
    change_30d_pct: float | None
    source: str
    created_at: datetime


@dataclass(slots=True)
class AnalysisRecord:
    run_id: str
    asset_code: str
    trend: str
    risk_level: str
    score: float
    summary: str
    metrics: dict[str, Any]
    created_at: datetime


@dataclass(slots=True)
class NotificationRecord:
    notification_id: int
    alert_event_id: int | None
    run_id: str | None
    asset_code: str | None
    asset_name: str | None
    channel: str
    status: NotificationStatus
    payload: dict[str, Any]
    created_at: datetime
    sent_at: datetime | None = None
    error_message: str | None = None
    attempt_count: int = 0


@dataclass(slots=True)
class AssetOverview:
    asset: Asset
    latest_run: RunRecord | None = None
    snapshot: SnapshotRecord | None = None
    analysis: AnalysisRecord | None = None


@dataclass(slots=True)
class PortfolioPositionOverview:
    asset: Asset
    snapshot: SnapshotRecord | None = None
    analysis: AnalysisRecord | None = None

    def current_value(self) -> float | None:
        if self.snapshot is None:
            return None
        return self.snapshot.current_value

    def market_value(self) -> float | None:
        current_value = self.current_value()
        if current_value is None:
            return None
        return self.asset.position_market_value(current_value)

    def unrealized_pnl_amount(self) -> float | None:
        current_value = self.current_value()
        if current_value is None:
            return None
        return self.asset.unrealized_pnl_amount(current_value)

    def unrealized_pnl_pct(self) -> float | None:
        current_value = self.current_value()
        if current_value is None:
            return None
        return self.asset.unrealized_pnl_pct(current_value)

    def daily_pnl_amount(self) -> float | None:
        if self.snapshot is None or self.asset.position_units is None:
            return None
        return (self.snapshot.current_value - self.snapshot.previous_close) * self.asset.position_units

    def entry_market_value(self) -> float | None:
        if self.asset.baseline_value is None or self.asset.position_units is None:
            return None
        return self.asset.baseline_value * self.asset.position_units

    def since_entry_pnl_amount(self) -> float | None:
        market_value = self.market_value()
        entry_market_value = self.entry_market_value()
        if market_value is None or entry_market_value is None:
            return None
        return market_value - entry_market_value

    def since_entry_pnl_pct(self) -> float | None:
        since_entry_pnl = self.since_entry_pnl_amount()
        entry_market_value = self.entry_market_value()
        if since_entry_pnl is None or entry_market_value is None or entry_market_value <= 0:
            return None
        return (since_entry_pnl / entry_market_value) * 100

    def weight_pct(self, portfolio_market_value: float) -> float | None:
        market_value = self.market_value()
        if market_value is None or portfolio_market_value <= 0:
            return None
        return (market_value / portfolio_market_value) * 100


@dataclass(slots=True)
class PortfolioHistoryPoint:
    as_of: datetime
    market_value: float


@dataclass(slots=True)
class PortfolioOverview:
    positions: list[PortfolioPositionOverview]
    total_positioned_assets: int
    assets_with_market_data: int
    assets_with_entry_baseline: int
    total_cost_amount: float
    total_market_value: float
    total_unrealized_pnl_amount: float
    total_unrealized_pnl_pct: float | None
    total_daily_pnl_amount: float
    total_entry_value_amount: float
    total_since_entry_pnl_amount: float
    total_since_entry_pnl_pct: float | None
    peak_market_value: float | None = None
    drawdown_amount: float | None = None
    drawdown_pct: float | None = None
    largest_position_weight_pct: float | None = None
    top3_position_weight_pct: float | None = None
    history: list[PortfolioHistoryPoint] = field(default_factory=list)
