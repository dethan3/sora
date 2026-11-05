export * from '@sora/market-data';
import type { MarketSnapshot } from '@sora/market-data';
export function fundPriceSeries(
  data: MarketSnapshot | null,
  code: string,
  currency: string,
) {
  return data?.series.find(
    (series) =>
      series.id === `fund:${code}` &&
      series.instrumentType === 'fund-price' &&
      series.fundCode === code &&
      series.unit ===
        (currency === 'HKD'
          ? 'hkd-per-share'
          : currency === 'USD'
            ? 'usd-per-share'
            : currency === 'CNY'
              ? 'cny-per-share'
              : ''),
  );
}
