"""Yahoo Finance provider for the first global-market slice."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.sora.domain import Asset, AssetType, Market, MarketSeries, PricePoint

from .base import ProviderCapability, ProviderSupport


class YahooFinanceMarketDataProvider:
    """Public Yahoo chart endpoint for a small global ETF/index scope."""

    name = "yahoo_finance"
    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
    GLOBAL_ETF_SYMBOLS = {
        "QQQ",
        "SPY",
        "VOO",
        "DIA",
        "IWM",
        "VTI",
        "VT",
    }
    GLOBAL_INDEX_ALIASES = {
        "SPX": "^GSPC",
        "GSPC": "^GSPC",
        "^GSPC": "^GSPC",
        "NDX": "^NDX",
        "^NDX": "^NDX",
        "IXIC": "^IXIC",
        "^IXIC": "^IXIC",
        "DJI": "^DJI",
        "^DJI": "^DJI",
    }

    def capability(self) -> ProviderCapability:
        return ProviderCapability(
            provider_name=self.name,
            markets=(Market.GLOBAL,),
            asset_types=(AssetType.FUND, AssetType.INDEX),
            notes=(
                "First global slice: US ETF tickers "
                f"{', '.join(sorted(self.GLOBAL_ETF_SYMBOLS))} and index aliases "
                f"{', '.join(sorted(alias for alias in self.GLOBAL_INDEX_ALIASES if not alias.startswith('^')))}."
            ),
        )

    def check_support(self, asset: Asset) -> ProviderSupport:
        if asset.market != Market.GLOBAL:
            return ProviderSupport(
                provider_name=self.name,
                supported=False,
                reason="only global market assets are handled",
            )

        if asset.asset_type == AssetType.FUND:
            normalized = asset.code.strip().upper()
            if normalized in self.GLOBAL_ETF_SYMBOLS:
                return ProviderSupport(
                    provider_name=self.name,
                    supported=True,
                    normalized_code=normalized,
                )
            return ProviderSupport(
                provider_name=self.name,
                supported=False,
                reason=(
                    "supported global ETF tickers are "
                    + ", ".join(sorted(self.GLOBAL_ETF_SYMBOLS))
                ),
            )

        normalized_index = self._normalize_index_symbol(asset.code)
        if normalized_index is not None:
            return ProviderSupport(
                provider_name=self.name,
                supported=True,
                normalized_code=normalized_index,
            )
        return ProviderSupport(
            provider_name=self.name,
            supported=False,
            reason=(
                "supported global index aliases are "
                + ", ".join(sorted(alias for alias in self.GLOBAL_INDEX_ALIASES if not alias.startswith("^")))
            ),
        )

    def supports(self, asset: Asset) -> bool:
        return self.check_support(asset).supported

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        support = self.check_support(asset)
        if not support.supported or not support.normalized_code:
            raise ValueError(
                f"Asset {asset.code} ({asset.market.value}/{asset.asset_type.value}) "
                f"is not supported by YahooFinanceMarketDataProvider: {support.reason}"
            )

        payload = self._fetch_chart(support.normalized_code, lookback_days)
        timestamps = payload.get("timestamp") or []
        quote = (((payload.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        closes = quote.get("close") or []

        history = [
            PricePoint(
                at=datetime.fromtimestamp(int(timestamp), tz=timezone.utc).replace(tzinfo=None),
                value=float(close),
            )
            for timestamp, close in zip(timestamps, closes)
            if close is not None
        ]
        if len(history) < 2:
            raise ValueError(f"Insufficient Yahoo Finance history for {asset.code}")

        meta = payload.get("meta") or {}
        currency = str(meta.get("currency") or "USD")
        return MarketSeries(
            asset=asset,
            source=self.name,
            currency=currency,
            as_of=history[-1].at,
            current_value=history[-1].value,
            previous_close=history[-2].value,
            history=history,
        )

    def _fetch_chart(self, normalized_code: str, lookback_days: int) -> dict[str, object]:
        now = datetime.now(timezone.utc)
        calendar_days = max(lookback_days * 2, 90)
        period1 = int((now - timedelta(days=calendar_days)).timestamp())
        period2 = int(now.timestamp())
        query = urlencode(
            {
                "interval": "1d",
                "includePrePost": "false",
                "events": "div,splits",
                "period1": period1,
                "period2": period2,
            }
        )
        url = f"{self.BASE_URL}/{normalized_code}?{query}"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urlopen(request, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"Yahoo Finance HTTP {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"Yahoo Finance network error: {exc.reason}") from exc

        chart = data.get("chart") or {}
        error = chart.get("error")
        if error:
            description = error.get("description") or "unknown Yahoo Finance error"
            raise RuntimeError(str(description))
        result = chart.get("result") or []
        if not result:
            raise RuntimeError(f"No Yahoo Finance data returned for {normalized_code}")
        return result[0]

    def _normalize_index_symbol(self, code: str) -> str | None:
        normalized = code.strip().upper()
        return self.GLOBAL_INDEX_ALIASES.get(normalized)
