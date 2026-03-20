"""Provider protocol definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.sora.domain import Asset, AssetType, Market, MarketSeries


@dataclass(slots=True)
class ProviderCapability:
    provider_name: str
    markets: tuple[Market, ...]
    asset_types: tuple[AssetType, ...]
    notes: str = ""


@dataclass(slots=True)
class ProviderSupport:
    provider_name: str
    supported: bool
    reason: str | None = None
    normalized_code: str | None = None


@dataclass(slots=True)
class ProviderFetchAttempt:
    provider_name: str
    error: str


class ProviderFetchError(RuntimeError):
    def __init__(self, asset: Asset, attempts: list[ProviderFetchAttempt]) -> None:
        self.asset = asset
        self.attempts = attempts
        details = "; ".join(f"{attempt.provider_name}: {attempt.error}" for attempt in attempts)
        message = (
            f"No provider could fetch {asset.code} ({asset.market.value}/{asset.asset_type.value})"
        )
        if details:
            message = f"{message}. Attempts: {details}"
        super().__init__(message)


class MarketDataProvider(Protocol):
    name: str

    def supports(self, asset: Asset) -> bool:
        """Return whether the provider can handle the asset."""

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        """Fetch current value and historical series for an asset."""
