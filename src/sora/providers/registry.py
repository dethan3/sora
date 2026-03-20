"""Provider registry and fallback execution helpers."""

from __future__ import annotations

from src.sora.domain import Asset, AssetType, Market, MarketSeries

from .base import (
    MarketDataProvider,
    ProviderCapability,
    ProviderFetchAttempt,
    ProviderFetchError,
    ProviderSupport,
)


def _provider_name(provider: MarketDataProvider) -> str:
    return str(getattr(provider, "name", provider.__class__.__name__))


class ProviderRegistry:
    name = "provider_registry"

    def __init__(self, providers: list[MarketDataProvider]) -> None:
        if not providers:
            raise ValueError("providers must not be empty")
        self.providers = list(providers)

    def capabilities(self) -> list[ProviderCapability]:
        capabilities: list[ProviderCapability] = []
        for provider in self.providers:
            capability = getattr(provider, "capability", None)
            if callable(capability):
                capabilities.append(capability())
                continue
            capabilities.append(
                ProviderCapability(
                    provider_name=_provider_name(provider),
                    markets=(Market.CN, Market.GLOBAL),
                    asset_types=(AssetType.FUND, AssetType.INDEX),
                    notes="provider does not declare explicit capabilities",
                )
            )
        return capabilities

    def check_support(self, asset: Asset) -> ProviderSupport:
        reasons: list[str] = []
        for provider in self.providers:
            support = self._resolve_support(provider, asset)
            if support.supported:
                return support
            reasons.append(
                f"{support.provider_name}: {support.reason or 'unsupported'}"
            )
        return ProviderSupport(
            provider_name=self.name,
            supported=False,
            reason="; ".join(reasons) or "no provider declared support",
        )

    def supports(self, asset: Asset) -> bool:
        return self.check_support(asset).supported

    def fetch_market_series(self, asset: Asset, lookback_days: int) -> MarketSeries:
        attempts: list[ProviderFetchAttempt] = []
        for provider in self.providers:
            support = self._resolve_support(provider, asset)
            if not support.supported:
                attempts.append(
                    ProviderFetchAttempt(
                        provider_name=support.provider_name,
                        error=support.reason or "unsupported",
                    )
                )
                continue
            try:
                return provider.fetch_market_series(asset, lookback_days)
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    ProviderFetchAttempt(
                        provider_name=support.provider_name,
                        error=str(exc),
                    )
                )
        raise ProviderFetchError(asset, attempts)

    def _resolve_support(self, provider: MarketDataProvider, asset: Asset) -> ProviderSupport:
        check_support = getattr(provider, "check_support", None)
        if callable(check_support):
            return check_support(asset)

        supports = getattr(provider, "supports", None)
        if callable(supports):
            if supports(asset):
                return ProviderSupport(provider_name=_provider_name(provider), supported=True)
            return ProviderSupport(
                provider_name=_provider_name(provider),
                supported=False,
                reason="provider does not support this asset",
            )
        return ProviderSupport(
            provider_name=_provider_name(provider),
            supported=True,
            reason="provider does not declare support checks",
        )
