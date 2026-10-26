import { marketSnapshotSchema, type MarketSnapshot } from './model';
export const MARKET_CACHE_KEY = 'sora.market.v1';
export function readMarketCache(): MarketSnapshot | null {
  try {
    const raw = localStorage.getItem(MARKET_CACHE_KEY);
    if (!raw || raw.length > 4 * 1024 * 1024) return null;
    const data = marketSnapshotSchema.parse(JSON.parse(raw));
    return data.source === 'market-snapshot' ? data : null;
  } catch {
    return null;
  }
}
export function saveMarketCache(data: MarketSnapshot) {
  if (data.source !== 'market-snapshot') return;
  try {
    localStorage.setItem(MARKET_CACHE_KEY, JSON.stringify(data));
  } catch {
    /* Cached data is optional; never block the page. */
  }
}
export async function loadMarketSnapshot(): Promise<MarketSnapshot> {
  const url =
    import.meta.env.VITE_MARKET_URL ||
    `${import.meta.env.BASE_URL}data/market.json`;
  const response = await fetch(url, {
    signal: AbortSignal.timeout(12000),
    cache: 'no-cache',
  });
  if (!response.ok) throw new Error('Market snapshot unavailable');
  const text = await response.text();
  if (text.length > 4 * 1024 * 1024)
    throw new Error('Market snapshot too large');
  const data = marketSnapshotSchema.parse(JSON.parse(text));
  if (data.source !== 'market-snapshot')
    throw new Error('Refusing mock data in snapshot mode');
  return data;
}
