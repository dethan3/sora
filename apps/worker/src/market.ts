import { marketSnapshotSchema } from '@sora/market-data';
import type { Env } from './types.ts';
export async function readMarket(env: Env, now = new Date()) {
  const rows = (
    await env.DB.prepare(
      `SELECT i.id,i.public_code,i.symbol,i.name,i.kind,s.series_meta_json,s.published_at,
    (SELECT error FROM ingest_jobs j WHERE j.instrument_id=i.id ORDER BY created_at DESC,id DESC LIMIT 1) AS error
    FROM instruments i JOIN instrument_snapshots s ON s.instrument_id=i.id WHERE i.status='ready' AND i.kind IN ('etf','index') ORDER BY s.published_at DESC,i.id LIMIT 200`,
    ).all()
  ).results;
  const points = (
    await env.DB.prepare(
      'SELECT instrument_id,date,close FROM market_series WHERE instrument_id IN (SELECT value FROM json_each(?)) ORDER BY instrument_id,date',
    )
      .bind(JSON.stringify(rows.map((row) => row.id)))
      .all()
  ).results;
  const histories = new Map<string, { date: string; value: number }[]>();
  for (const point of points) {
    const id = String(point.instrument_id);
    const history = histories.get(id) ?? [];
    history.push({ date: String(point.date), value: Number(point.close) });
    histories.set(id, history);
  }
  const series = [];
  for (const row of rows) {
    const meta = JSON.parse(String(row.series_meta_json));
    const history = histories.get(String(row.id)) ?? [];
    if (history.length < 2 || meta.type === 'nav') continue;
    const last = history.at(-1)!;
    series.push({
      id: row.kind === 'etf' ? `fund:${row.public_code}` : row.id,
      name: row.name,
      instrumentType: row.kind === 'etf' ? 'fund-price' : 'index',
      ...(row.kind === 'etf' ? { fundCode: row.public_code } : {}),
      unit:
        row.kind === 'index'
          ? 'points'
          : (
              {
                CNY: 'cny-per-share',
                HKD: 'hkd-per-share',
                USD: 'usd-per-share',
              } as Record<string, string>
            )[meta.currency],
      timeZone: meta.timeZone,
      session: 'unknown',
      freshness:
        now.getTime() - Date.parse(last.date) > 4 * 86400000
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
        provider: 'aktools',
        upstream: meta.source.name,
        symbol: row.symbol,
        sourceUrl: meta.source.url,
        adjustment: 'unadjusted',
        retrievedAt: meta.source.fetchedAt,
        attemptedAt: row.published_at,
        updateStatus: row.error ? 'failed' : 'ok',
      },
    });
  }
  return marketSnapshotSchema.parse({
    schemaVersion: 1,
    source: 'market-snapshot',
    generatedAt: now.toISOString(),
    series,
  });
}
