import {
  marketSnapshotSchema,
  type MarketSeries,
  type MarketSnapshot,
} from '@sora/market-data';
import { instruments, type Instrument } from './instruments.ts';
import { getAktools, getYahoo } from './providers.ts';
export type Attempt = {
  id: string;
  provider: 'aktools' | 'yahoo';
  ok: boolean;
  error?: string;
};
export type Collector = (
  instrument: Instrument,
  now: string,
) => Promise<MarketSeries>;
export async function updateSnapshot(options: {
  previous?: MarketSnapshot;
  now?: string;
  primary?: Collector;
  fallback?: Collector;
  baseUrl?: string;
}) {
  const now = options.now ?? new Date().toISOString();
  const previous =
    options.previous?.source === 'market-snapshot'
      ? options.previous
      : undefined;
  const series: MarketSeries[] = [];
  const attempts: Attempt[] = [];
  for (const instrument of instruments) {
    let fresh: MarketSeries | undefined;
    for (const [provider, collect] of [
      [
        'aktools',
        options.primary ??
          ((item, at) =>
            getAktools(item, options.baseUrl ?? 'http://127.0.0.1:8080', at)),
      ],
      ['yahoo', options.fallback],
    ] as const) {
      if (!collect) continue;
      try {
        const result = await collect(instrument, now);
        const validated = marketSnapshotSchema.parse({
          schemaVersion: 1,
          source: 'market-snapshot',
          generatedAt: now,
          series: [result],
        }).series[0];
        if (
          validated.id !== instrument.id ||
          validated.name !== instrument.name ||
          validated.timeZone !== instrument.timeZone ||
          validated.unit !== 'points'
        )
          throw new Error('Instrument mapping mismatch');
        const prior = previous?.series.find(
          (item) => item.id === instrument.id,
        );
        if (
          prior?.quote &&
          validated.quote &&
          validated.quote.asOf < prior.quote.asOf
        )
          throw new Error('Source is older than saved observation');
        const completedAt = options.now ?? new Date().toISOString();
        fresh = {
          ...validated,
          provenance: {
            ...validated.provenance!,
            retrievedAt: completedAt,
            attemptedAt: completedAt,
          },
        };
        attempts.push({ id: instrument.id, provider, ok: true });
        break;
      } catch (error) {
        attempts.push({
          id: instrument.id,
          provider,
          ok: false,
          error:
            error instanceof Error
              ? error.name + ': ' + error.message
              : 'Unknown error',
        });
      }
    }
    if (fresh) series.push(fresh);
    else {
      const prior = previous?.series.find((item) => item.id === instrument.id);
      if (prior?.provenance)
        series.push({
          ...prior,
          freshness: 'stale',
          provenance: {
            ...prior.provenance,
            attemptedAt: now,
            updateStatus: 'failed',
          },
        });
    }
  }
  const snapshot = marketSnapshotSchema.parse({
    schemaVersion: 1,
    source: 'market-snapshot',
    generatedAt: options.now ?? new Date().toISOString(),
    series,
  });
  return {
    snapshot,
    attempts,
    successes: attempts.filter((item) => item.ok).length,
  };
}
export const yahooFallback: Collector = getYahoo;
