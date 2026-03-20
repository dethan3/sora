"""Provider fallback backed by locally persisted snapshots."""

from __future__ import annotations

from src.sora.domain import Asset, AssetType, Market, MarketSeries, PricePoint
from src.sora.repository import SQLiteRepository

from .base import ProviderCapability, ProviderSupport


class SnapshotCacheMarketDataProvider:
    name = "snapshot_cache"

    def __init__(self, repository: SQLiteRepository) -> None:
        self.repository = repository

    def capability(self) -> ProviderCapability:
        return ProviderCapability(
            provider_name=self.name,
            markets=(Market.CN, Market.GLOBAL),
            asset_types=(AssetType.FUND, AssetType.INDEX),
            notes="Fallback provider using locally persisted snapshot history. Requires at least 2 snapshots.",
        )

    def check_support(self, asset: Asset) -> ProviderSupport:
        snapshots = self.repository.list_snapshots(asset_code=asset.code, limit=2)
        if len(snapshots) < 2:
            return ProviderSupport(
                provider_name=self.name,
                supported=False,
                reason="requires at least 2 persisted snapshots",
            )
        return ProviderSupport(
            provider_name=self.name,
            supported=True,
            reason="using persisted snapshot history",
        )

    def supports(self, asset: Asset) -> bool:
        return self.check_support(asset).supported

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        snapshots = self.repository.list_snapshots(
            asset_code=asset.code,
            limit=max(lookback_days + 1, 35),
        )
        if len(snapshots) < 2:
            raise ValueError(f"Insufficient persisted snapshot history for {asset.code}")

        ordered = sorted(snapshots, key=lambda item: item.as_of)
        history = [
            PricePoint(at=snapshot.as_of, value=snapshot.current_value)
            for snapshot in ordered
        ]
        return MarketSeries(
            asset=asset,
            source=self.name,
            currency="UNKNOWN",
            as_of=history[-1].at,
            current_value=history[-1].value,
            previous_close=history[-2].value,
            history=history,
        )
