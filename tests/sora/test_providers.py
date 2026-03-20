from datetime import datetime
import json

import pytest

from src.sora.domain import Asset, AssetType, Market, Snapshot
from src.sora.providers import (
    ProviderFetchError,
    ProviderRegistry,
    SnapshotCacheMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from src.sora.repository import SQLiteRepository


class FailingLiveProvider:
    name = "live"

    def supports(self, asset: Asset) -> bool:
        return True

    def fetch_market_series(self, asset: Asset, lookback_days: int):
        raise RuntimeError("network down")


class UnsupportedProvider:
    name = "unsupported"

    def supports(self, asset: Asset) -> bool:
        return False

    def fetch_market_series(self, asset: Asset, lookback_days: int):
        raise AssertionError("should not fetch unsupported asset")


def _seed_cached_snapshots(repository: SQLiteRepository, asset: Asset) -> None:
    run_1 = repository.start_run(total_assets=1)
    repository.save_snapshot(
        run_1,
        Snapshot(
            asset=asset,
            as_of=datetime(2025, 1, 1),
            current_value=1.10,
            previous_close=1.00,
            daily_change_pct=10.0,
            change_7d_pct=None,
            change_30d_pct=None,
            source="akshare",
        ),
    )
    repository.finish_run(
        run_id=run_1,
        processed_assets=1,
        successful_assets=1,
        failed_assets=0,
        status="completed",
    )

    run_2 = repository.start_run(total_assets=1)
    repository.save_snapshot(
        run_2,
        Snapshot(
            asset=asset,
            as_of=datetime(2025, 1, 2),
            current_value=1.20,
            previous_close=1.10,
            daily_change_pct=9.09,
            change_7d_pct=None,
            change_30d_pct=None,
            source="akshare",
        ),
    )
    repository.finish_run(
        run_id=run_2,
        processed_assets=1,
        successful_assets=1,
        failed_assets=0,
        status="completed",
    )


def test_provider_registry_falls_back_to_snapshot_cache(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    asset = Asset(
        code="510300",
        name="沪深300ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
    )
    repository.upsert_asset(asset)
    _seed_cached_snapshots(repository, asset)

    registry = ProviderRegistry(
        [
            FailingLiveProvider(),
            SnapshotCacheMarketDataProvider(repository),
        ]
    )

    series = registry.fetch_market_series(asset, lookback_days=30)

    assert series.source == "snapshot_cache"
    assert series.current_value == 1.20
    assert series.previous_close == 1.10
    assert len(series.history) == 2


def test_provider_registry_raises_detailed_error_when_all_providers_fail(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    asset = Asset(
        code="SPX",
        name="标普500",
        asset_type=AssetType.INDEX,
        market=Market.GLOBAL,
    )

    registry = ProviderRegistry(
        [
            UnsupportedProvider(),
            SnapshotCacheMarketDataProvider(repository),
        ]
    )

    with pytest.raises(ProviderFetchError) as exc_info:
        registry.fetch_market_series(asset, lookback_days=30)

    message = str(exc_info.value)
    assert "unsupported" in message
    assert "requires at least 2 persisted snapshots" in message


def test_snapshot_cache_provider_declares_requirement(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    provider = SnapshotCacheMarketDataProvider(repository)
    asset = Asset(
        code="510300",
        name="沪深300ETF",
        asset_type=AssetType.FUND,
        market=Market.CN,
    )

    support = provider.check_support(asset)
    capability = provider.capability()

    assert not support.supported
    assert "requires at least 2 persisted snapshots" in (support.reason or "")
    assert capability.provider_name == "snapshot_cache"
    assert capability.notes


def test_yahoo_provider_declares_small_global_scope():
    provider = YahooFinanceMarketDataProvider()

    supported_etf = provider.check_support(
        Asset(
            code="qqq",
            name="QQQ",
            asset_type=AssetType.FUND,
            market=Market.GLOBAL,
        )
    )
    supported_index = provider.check_support(
        Asset(
            code="spx",
            name="SPX",
            asset_type=AssetType.INDEX,
            market=Market.GLOBAL,
        )
    )
    unsupported = provider.check_support(
        Asset(
            code="EEM",
            name="EEM",
            asset_type=AssetType.FUND,
            market=Market.GLOBAL,
        )
    )

    assert supported_etf.supported
    assert supported_etf.normalized_code == "QQQ"
    assert supported_index.supported
    assert supported_index.normalized_code == "^GSPC"
    assert not unsupported.supported
    assert "supported global ETF tickers" in (unsupported.reason or "")


def test_yahoo_provider_parses_chart_response(monkeypatch):
    provider = YahooFinanceMarketDataProvider()
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"currency": "USD"},
                    "timestamp": [1735689600, 1735776000],
                    "indicators": {
                        "quote": [
                            {
                                "close": [500.25, 503.75],
                            }
                        ]
                    },
                }
            ],
            "error": None,
        }
    }

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(
        "src.sora.providers.yahoo.urlopen",
        lambda request, timeout=15: FakeResponse(),
    )

    asset = Asset(
        code="QQQ",
        name="QQQ",
        asset_type=AssetType.FUND,
        market=Market.GLOBAL,
    )
    series = provider.fetch_market_series(asset, lookback_days=30)

    assert series.source == "yahoo_finance"
    assert series.currency == "USD"
    assert series.current_value == 503.75
    assert series.previous_close == 500.25
    assert len(series.history) == 2
