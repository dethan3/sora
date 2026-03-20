"""AkShare provider for the first Sora iteration."""

from __future__ import annotations

import akshare as ak
import pandas as pd

from src.sora.domain import Asset, AssetType, Market, MarketSeries, PricePoint

from .base import ProviderCapability, ProviderSupport


class AkshareMarketDataProvider:
    """Best-effort provider for CN funds and CN indices."""

    name = "akshare"
    CN_INDEX_ALIAS = {
        "000001": "sh000001",
        "399001": "sz399001",
        "399006": "sz399006",
    }

    def capability(self) -> ProviderCapability:
        return ProviderCapability(
            provider_name=self.name,
            markets=(Market.CN,),
            asset_types=(AssetType.FUND, AssetType.INDEX),
            notes="Supports CN public funds and selected CN indices via AkShare/Eastmoney.",
        )

    def check_support(self, asset: Asset) -> ProviderSupport:
        if asset.market != Market.CN:
            return ProviderSupport(
                provider_name=self.name,
                supported=False,
                reason="only CN market is supported",
            )
        if asset.asset_type == AssetType.FUND:
            if asset.code.isdigit():
                return ProviderSupport(
                    provider_name=self.name,
                    supported=True,
                    normalized_code=asset.code,
                )
            return ProviderSupport(
                provider_name=self.name,
                supported=False,
                reason="fund code must be numeric",
            )
        if self._can_normalize_index_symbol(asset.code):
            return ProviderSupport(
                provider_name=self.name,
                supported=True,
                normalized_code=self._normalize_index_symbol(asset.code),
            )
        return ProviderSupport(
            provider_name=self.name,
            supported=False,
            reason="CN index code must use sh/sz prefix or a built-in alias",
        )

    def supports(self, asset: Asset) -> bool:
        return self.check_support(asset).supported

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        support = self.check_support(asset)
        if not support.supported:
            raise ValueError(
                f"Asset {asset.code} ({asset.market.value}/{asset.asset_type.value}) "
                f"is not supported by AkshareMarketDataProvider: {support.reason}"
            )

        if asset.asset_type == AssetType.FUND:
            return self._fetch_fund(asset, lookback_days)
        return self._fetch_index(asset, lookback_days)

    def _fetch_fund(self, asset: Asset, lookback_days: int) -> MarketSeries:
        df = ak.fund_open_fund_info_em(symbol=asset.code, indicator="单位净值走势")
        if df is None or df.empty:
            raise ValueError(f"No fund data returned for {asset.code}")

        working = df.rename(columns={"净值日期": "date", "单位净值": "value"})
        working["date"] = pd.to_datetime(working["date"])
        working["value"] = working["value"].astype(float)
        working = working.sort_values("date")
        working = working.tail(max(lookback_days + 1, 35))

        history = [
            PricePoint(at=row.date.to_pydatetime(), value=float(row.value))
            for row in working.itertuples(index=False)
        ]
        if len(history) < 2:
            raise ValueError(f"Insufficient fund history for {asset.code}")

        return MarketSeries(
            asset=asset,
            source="akshare",
            currency="CNY",
            as_of=history[-1].at,
            current_value=history[-1].value,
            previous_close=history[-2].value,
            history=history,
        )

    def _fetch_index(self, asset: Asset, lookback_days: int) -> MarketSeries:
        symbol = self._normalize_index_symbol(asset.code)
        df = ak.stock_zh_index_daily_em(symbol=symbol)
        if df is None or df.empty:
            raise ValueError(f"No index data returned for {asset.code}")

        working = df.tail(max(lookback_days + 1, 35)).copy()
        working["date"] = pd.to_datetime(working["date"])
        working["close"] = working["close"].astype(float)
        working = working.sort_values("date")

        history = [
            PricePoint(at=row.date.to_pydatetime(), value=float(row.close))
            for row in working.itertuples(index=False)
        ]
        if len(history) < 2:
            raise ValueError(f"Insufficient index history for {asset.code}")

        return MarketSeries(
            asset=asset,
            source="akshare",
            currency="CNY",
            as_of=history[-1].at,
            current_value=history[-1].value,
            previous_close=history[-2].value,
            history=history,
        )

    def _normalize_index_symbol(self, code: str) -> str:
        code = code.strip()
        if code in self.CN_INDEX_ALIAS:
            return self.CN_INDEX_ALIAS[code]
        if code.startswith(("sh", "sz")):
            return code
        raise ValueError(
            "CN index code must be sh/sz prefixed or one of the built-in aliases."
        )

    def _can_normalize_index_symbol(self, code: str) -> bool:
        code = code.strip()
        return code in self.CN_INDEX_ALIAS or code.startswith(("sh", "sz"))
