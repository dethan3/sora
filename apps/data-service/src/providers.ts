import { marketSeriesSchema, type MarketSeries } from '@sora/market-data';
import type { Instrument } from './instruments.ts';

export type Fetcher = typeof fetch;
const maxBytes = 8 * 1024 * 1024;
export async function fetchJson(url: URL, fetcher: Fetcher, timeoutMs: number) {
  const response = await fetcher(url, {
    signal: AbortSignal.timeout(timeoutMs),
    redirect: 'error',
    headers: { accept: 'application/json' },
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  if (Number(response.headers.get('content-length')) > maxBytes)
    throw new Error('Response too large');
  if (!response.body) throw new Error('Empty response');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) throw new Error('Response too large');
      chunks.push(value);
    }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
}
export function normalizeRows(
  raw: unknown,
  instrument: Instrument,
  now: string,
): MarketSeries['history'] {
  if (!Array.isArray(raw) || raw.length < 2 || raw.length > 50000)
    throw new Error('Expected daily history records');
  const today = new Intl.DateTimeFormat('en-CA', {
    timeZone: instrument.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date(now));
  const cutoff = new Date(now);
  cutoff.setUTCDate(cutoff.getUTCDate() - 380);
  const byDate = new Map<string, number>();
  for (const row of raw) {
    if (!row || typeof row !== 'object') throw new Error('Invalid row');
    const record = row as Record<string, unknown>;
    const date = String(record.date ?? record['日期'] ?? '').slice(0, 10);
    const original = record.close ?? record['收盘'];
    const value =
      typeof original === 'number'
        ? original
        : typeof original === 'string' && /^\d+(?:\.\d+)?$/.test(original)
          ? Number(original)
          : NaN;
    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
      !Number.isFinite(Date.parse(date)) ||
      new Date(date).toISOString().slice(0, 10) !== date ||
      !Number.isFinite(value) ||
      value <= 0
    )
      throw new Error('Invalid daily observation');
    if (date > today) throw new Error('Future trading date');
    if (byDate.has(date) && byDate.get(date) !== value)
      throw new Error('Conflicting duplicate date');
    byDate.set(date, value);
  }
  const history = [...byDate]
    .sort(([a], [b]) => a.localeCompare(b))
    .filter(
      ([date]) => date < today && date >= cutoff.toISOString().slice(0, 10),
    )
    .map(([date, value]) => ({ date, value }));
  if (history.length < 2)
    throw new Error('Insufficient recent daily observations');
  return history;
}
function collectedSeries(
  history: MarketSeries['history'],
  instrument: Instrument,
  now: string,
  provider: 'aktools' | 'yahoo',
): MarketSeries {
  const last = history.at(-1)!;
  return marketSeriesSchema.parse({
    id: instrument.id,
    name: instrument.name,
    unit: 'points',
    timeZone: instrument.timeZone,
    session: 'unknown',
    freshness:
      Date.parse(now) - Date.parse(last.date) > 4 * 86400000
        ? 'stale'
        : 'current',
    quote: {
      value: last.value,
      previousClose: history.at(-2)!.value,
      asOf: last.date,
      kind: 'daily-close',
      delayMinutes: null,
    },
    history,
    provenance: {
      provider,
      upstream: provider === 'aktools' ? '新浪财经' : 'Yahoo Finance',
      symbol:
        provider === 'aktools' ? instrument.symbol : instrument.yahooSymbol,
      sourceUrl:
        provider === 'aktools'
          ? instrument.sourceUrl
          : `https://finance.yahoo.com/quote/${encodeURIComponent(instrument.yahooSymbol)}/`,
      adjustment: 'unadjusted',
      retrievedAt: now,
      attemptedAt: now,
      updateStatus: 'ok',
    },
  });
}
export function normalizeAktools(
  raw: unknown,
  instrument: Instrument,
  now: string,
) {
  return collectedSeries(
    normalizeRows(raw, instrument, now),
    instrument,
    now,
    'aktools',
  );
}
export function normalizeYahoo(
  raw: unknown,
  instrument: Instrument,
  now: string,
) {
  const root = raw as {
    chart?: {
      error?: unknown;
      result?: Array<{
        meta?: {
          symbol?: string;
          instrumentType?: string;
          exchangeTimezoneName?: string;
        };
        timestamp?: number[];
        indicators?: { quote?: Array<{ close?: Array<number | null> }> };
      }>;
    };
  };
  const result = root?.chart?.result?.[0];
  if (
    root?.chart?.error ||
    !result ||
    result.meta?.symbol !== instrument.yahooSymbol ||
    result.meta?.instrumentType !== 'INDEX' ||
    result.meta.exchangeTimezoneName !== instrument.timeZone
  )
    throw new Error('Unexpected Yahoo instrument');
  const closes = result.indicators?.quote?.[0]?.close;
  if (!result.timestamp || !closes || result.timestamp.length !== closes.length)
    throw new Error('Invalid Yahoo history');
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: instrument.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const rows = result.timestamp.flatMap((time, index) =>
    closes[index] === null
      ? []
      : [
          {
            date: formatter.format(new Date(time * 1000)),
            close: closes[index],
          },
        ],
  );
  return collectedSeries(
    normalizeRows(rows, instrument, now),
    instrument,
    now,
    'yahoo',
  );
}
export async function getAktools(
  instrument: Instrument,
  baseUrl: string,
  now: string,
  fetcher: Fetcher = fetch,
  timeoutMs = 20000,
) {
  const base = new URL(baseUrl);
  if (
    !['http:', 'https:'].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash
  )
    throw new Error('Invalid AKTools base URL');
  const url = new URL(`/api/public/${instrument.api}`, base);
  url.searchParams.set('symbol', instrument.symbol);
  return normalizeAktools(
    await fetchJson(url, fetcher, timeoutMs),
    instrument,
    now,
  );
}
export async function getYahoo(
  instrument: Instrument,
  now: string,
  fetcher: Fetcher = fetch,
  timeoutMs = 20000,
) {
  const url = new URL(
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(instrument.yahooSymbol)}`,
  );
  url.search = new URLSearchParams({
    range: '2y',
    interval: '1d',
    events: 'history',
  }).toString();
  return normalizeYahoo(
    await fetchJson(url, fetcher, timeoutMs),
    instrument,
    now,
  );
}
