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


class AlertDirection(str, Enum):
    ABOVE = "above"
    BELOW = "below"


class NotificationStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


@dataclass(slots=True)
class Asset:
    code: str
    name: str
    asset_type: AssetType
    market: Market
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self) -> None:
        self.code = self.code.strip()
        self.name = self.name.strip()
        if not self.code:
            raise ValueError("asset code must not be empty")
        if not self.name:
            raise ValueError("asset name must not be empty")


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
    asset_code: str | None = None
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    rule_id: int | None = None

    def __post_init__(self) -> None:
        if self.asset_code is not None:
            self.asset_code = self.asset_code.strip() or None
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
