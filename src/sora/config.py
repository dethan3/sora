"""Configuration helpers for Sora."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _default_database_path() -> str:
    return str((PROJECT_ROOT / "data" / "sora.db").resolve())


@dataclass(slots=True)
class AnalysisConfig:
    lookback_days: int = 90
    short_window: int = 7
    long_window: int = 30

    def __post_init__(self) -> None:
        if self.lookback_days <= 1:
            raise ValueError("analysis.lookback_days must be greater than 1")
        if self.short_window <= 0:
            raise ValueError("analysis.short_window must be greater than 0")
        if self.long_window <= 0:
            raise ValueError("analysis.long_window must be greater than 0")
        if self.short_window > self.long_window:
            raise ValueError("analysis.short_window must be less than or equal to analysis.long_window")
        if self.lookback_days <= self.long_window:
            raise ValueError("analysis.lookback_days must be greater than analysis.long_window")


@dataclass(slots=True)
class NotificationsConfig:
    webhook_urls: dict[str, str] = field(default_factory=dict)
    request_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.request_timeout_seconds <= 0:
            raise ValueError("notifications.request_timeout_seconds must be greater than 0")
        normalized: dict[str, str] = {}
        for channel, url in self.webhook_urls.items():
            normalized_channel = str(channel).strip()
            normalized_url = str(url).strip()
            if not normalized_channel or not normalized_url:
                continue
            normalized[normalized_channel] = normalized_url
        self.webhook_urls = normalized


@dataclass(slots=True)
class AppConfig:
    database_path: str = field(default_factory=_default_database_path)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)


def _merge_analysis(data: dict[str, Any]) -> AnalysisConfig:
    return AnalysisConfig(
        lookback_days=int(data.get("lookback_days", 90)),
        short_window=int(data.get("short_window", 7)),
        long_window=int(data.get("long_window", 30)),
    )


def _merge_notifications(data: dict[str, Any]) -> NotificationsConfig:
    return NotificationsConfig(
        webhook_urls=dict(data.get("webhook_urls", {})),
        request_timeout_seconds=float(data.get("request_timeout_seconds", 10.0)),
    )


def _resolve_config_path(config_path: str) -> Path:
    candidate = Path(config_path)
    if candidate.is_absolute():
        return candidate

    cwd_candidate = candidate.resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    return (PROJECT_ROOT / candidate).resolve()


def _resolve_database_path(database_path: str) -> str:
    candidate = Path(database_path)
    if candidate.is_absolute():
        return str(candidate)
    return str((PROJECT_ROOT / candidate).resolve())


def load_config(config_path: str | None = None) -> AppConfig:
    if not config_path:
        return AppConfig()

    path = _resolve_config_path(config_path)
    if not path.exists():
        return AppConfig()

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return AppConfig(
        database_path=_resolve_database_path(str(raw.get("database_path", "data/sora.db"))),
        analysis=_merge_analysis(raw.get("analysis", {})),
        notifications=_merge_notifications(raw.get("notifications", {})),
    )
