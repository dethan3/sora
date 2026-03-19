"""Provider protocol definitions."""

from __future__ import annotations

from typing import Protocol

from src.sora.domain import Asset, MarketSeries


class MarketDataProvider(Protocol):
    def supports(self, asset: Asset) -> bool:
        """Return whether the provider can handle the asset."""

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        """Fetch current value and historical series for an asset."""
