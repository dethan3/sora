import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { AddressInfo } from 'node:net';
import { marketSnapshotSchema, isStale } from '@sora/market-data';
import { instruments } from './instruments.ts';
import {
  normalizeAktools,
  normalizeRows,
  normalizeYahoo,
  getAktools,
  fetchJson,
  type Fetcher,
} from './providers.ts';
import { updateSnapshot } from './update.ts';
import { atomicWrite, readSnapshot, withUpdateLock } from './storage.ts';
import { snapshotServer } from './server.ts';

const now = '2026-10-26T10:00:00Z';
const rows = [
  { date: '2026-10-22', close: '100' },
  { date: '2026-10-23', close: 102 },
];
const collect = async (instrument: (typeof instruments)[number]) =>
  normalizeAktools(rows, instrument, now);
const fail = async () => {
  throw new Error('Unavailable');
};
const mockFetch = (data: unknown, status = 200) =>
  (async () =>
    new Response(JSON.stringify(data), {
      status,
      headers: { 'content-type': 'application/json' },
    })) as Fetcher;
async function fixture(t: { after: (fn: () => Promise<void>) => void }) {
  const dir = await mkdtemp(join(tmpdir(), 'sora-data-'));
  t.after(async () => {
    if (!resolve(dir).startsWith(resolve(join(tmpdir(), 'sora-data-'))))
      throw new Error('Unsafe temporary path');
    await rm(dir, { recursive: true, force: true });
  });
  return join(dir, 'market.json');
}
test('normalizes numeric strings, ordering, duplicates and exchange-local daily dates without invented times', () => {
  const series = normalizeAktools(
    [...rows].reverse().concat(rows[0]),
    instruments[0],
    now,
  );
  assert.equal(series.quote?.asOf, '2026-10-23');
  assert.equal(series.quote?.previousClose, 100);
  assert.equal(series.provenance?.symbol, '.NDX');
  assert.equal(series.provenance?.adjustment, 'unadjusted');
  assert.equal(series.quote?.kind, 'daily-close');
  assert.equal(series.history.length, 2);
});
test('rejects invalid observations, conflicting dates, future dates and short history', () => {
  for (const bad of [
    [],
    [{ date: '2026-10-23', close: 1 }],
    rows.concat({ date: '2026-10-23', close: 999 }),
    rows.concat({ date: '2099-01-01', close: 100 }),
    rows.concat({ date: '2026-10-21', close: NaN }),
    [{ date: '2026-10-22', close: 0 }, rows[1]],
  ])
    assert.throws(() => normalizeRows(bad, instruments[0], now));
});
test('does not treat the current trading day as completed daily data', () => {
  const points = normalizeRows(
    rows.concat({ date: '2026-10-26', close: 999 }),
    instruments[2],
    now,
  );
  assert.equal(points.at(-1)?.date, '2026-10-23');
});
test('calls only the fixed AKTools index endpoint and handles HTTP errors', async () => {
  let requested = '';
  const fetcher = (async (url) => {
    requested = String(url);
    return new Response(JSON.stringify(rows));
  }) as Fetcher;
  const series = await getAktools(
    instruments[0],
    'http://127.0.0.1:8080',
    now,
    fetcher,
  );
  assert.equal(new URL(requested).pathname, '/api/public/index_us_stock_sina');
  assert.equal(new URL(requested).searchParams.get('symbol'), '.NDX');
  assert.equal(series.id, 'nasdaq100');
  await assert.rejects(
    getAktools(instruments[0], 'file:///tmp/', now, fetcher),
  );
  await assert.rejects(
    getAktools(
      instruments[0],
      'http://secret:password@example.test/',
      now,
      fetcher,
    ),
  );
  await assert.rejects(
    getAktools(
      instruments[0],
      'http://127.0.0.1:8080',
      now,
      mockFetch({}, 503),
    ),
    /503/,
  );
});
test('bounds download size and rejects non-JSON responses', async () => {
  const large = (async () =>
    new Response('{}', {
      headers: { 'content-length': String(9 * 1024 * 1024) },
    })) as Fetcher;
  await assert.rejects(
    fetchJson(new URL('https://example.test'), large, 1000),
    /large/,
  );
  const html = (async () => new Response('<html>blocked</html>')) as Fetcher;
  await assert.rejects(fetchJson(new URL('https://example.test'), html, 1000));
});
test('validates the exact Yahoo index identity and timezone before using a fallback', () => {
  const raw = {
    chart: {
      error: null,
      result: [
        {
          meta: {
            symbol: '^NDX',
            instrumentType: 'INDEX',
            exchangeTimezoneName: 'America/New_York',
          },
          timestamp: [
            Date.parse('2026-10-22T20:00:00Z') / 1000,
            Date.parse('2026-10-23T20:00:00Z') / 1000,
          ],
          indicators: { quote: [{ close: [100, 102] }] },
        },
      ],
    },
  };
  const series = normalizeYahoo(raw, instruments[0], now);
  assert.equal(series.provenance?.provider, 'yahoo');
  assert.equal(series.quote?.asOf, '2026-10-23');
  raw.chart.result[0].meta.instrumentType = 'ETF';
  assert.throws(() => normalizeYahoo(raw, instruments[0], now));
});
test('prefers AKTools and does not call a fallback on success', async () => {
  let fallbackCalls = 0;
  const result = await updateSnapshot({
    now,
    primary: collect,
    fallback: async () => {
      fallbackCalls++;
      throw new Error('Should not run');
    },
  });
  assert.equal(result.successes, 3);
  assert.equal(result.snapshot.series.length, 3);
  assert.equal(fallbackCalls, 0);
});
test('falls back per instrument and records the actual selected source', async () => {
  const result = await updateSnapshot({
    now,
    primary: fail,
    fallback: async (item) => {
      const series = await collect(item);
      return {
        ...series,
        provenance: {
          ...series.provenance!,
          provider: 'yahoo',
          upstream: 'Yahoo Finance',
          symbol: item.yahooSymbol,
        },
      };
    },
  });
  assert.equal(result.attempts.length, 6);
  assert.equal(result.successes, 3);
  assert.equal(result.snapshot.series[0].provenance?.provider, 'yahoo');
});
test('preserves original quote dates and provenance on partial or complete failure', async () => {
  const previous = (await updateSnapshot({ now, primary: collect })).snapshot;
  const result = await updateSnapshot({
    now: '2026-10-27T10:00:00Z',
    previous,
    primary: fail,
  });
  assert.equal(result.successes, 0);
  assert.equal(result.snapshot.series.length, 3);
  assert.equal(result.snapshot.series[0].quote?.asOf, '2026-10-23');
  assert.equal(result.snapshot.series[0].provenance?.retrievedAt, now);
  assert.equal(result.snapshot.series[0].provenance?.updateStatus, 'failed');
  assert.equal(result.snapshot.series[0].freshness, 'stale');
  assert.equal(
    isStale(previous.series[0], Date.parse('2026-11-01T00:00:00Z')),
    true,
  );
});
test('never fabricates initial missing data or accepts synthetic data as a fallback cache', async () => {
  const empty = await updateSnapshot({ now, primary: fail });
  assert.equal(empty.snapshot.series.length, 0);
  const fake = marketSnapshotSchema.parse({
    schemaVersion: 1,
    source: 'mock',
    generatedAt: now,
    series: [],
  });
  const result = await updateSnapshot({ now, previous: fake, primary: fail });
  assert.equal(result.snapshot.series.length, 0);
});
test('rejects wrong instrument mappings and refuses a quote-date rollback', async () => {
  const previous = (await updateSnapshot({ now, primary: collect })).snapshot;
  const mismatch = await updateSnapshot({
    now,
    previous,
    primary: async () => collect(instruments[1]),
  });
  assert.equal(mismatch.successes, 1);
  const older = await updateSnapshot({
    now,
    previous,
    primary: async (item) =>
      normalizeAktools(
        [
          { date: '2026-10-20', close: 98 },
          { date: '2026-10-21', close: 99 },
        ],
        item,
        now,
      ),
  });
  assert.equal(older.successes, 0);
  assert.equal(older.snapshot.series[0].quote?.asOf, '2026-10-23');
});
test('writes atomically and preserves a corrupt cache for explicit recovery', async (t) => {
  const path = await fixture(t);
  assert.equal(await readSnapshot(path), undefined);
  const snapshot = (await updateSnapshot({ now, primary: collect })).snapshot;
  await atomicWrite(path, snapshot);
  assert.deepEqual(await readSnapshot(path), snapshot);
  await atomicWrite(path, { broken: true });
  await assert.rejects(readSnapshot(path));
  assert.match(await readFile(path, 'utf8'), /broken/);
});
test('rejects overlapping updates and releases its lock after failure', async (t) => {
  const path = (await fixture(t)) + '.lock';
  await withUpdateLock(path, async () => {
    await assert.rejects(
      withUpdateLock(path, async () => {}),
      /EEXIST/,
    );
  });
  await assert.rejects(withUpdateLock(path, fail));
  await withUpdateLock(path, async () => {});
});
test('serves only saved snapshots with ETags and never forwards arbitrary API calls', async (t) => {
  const path = await fixture(t);
  const server = snapshotServer(path);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(
    () =>
      new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      ),
  );
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  assert.equal((await fetch(base + '/api/market')).status, 503);
  assert.equal((await fetch(base + '/api/public/anything')).status, 404);
  assert.equal(
    (await fetch(base + '/api/market', { method: 'POST' })).status,
    405,
  );
  await atomicWrite(
    path,
    (await updateSnapshot({ now, primary: collect })).snapshot,
  );
  const response = await fetch(base + '/api/market');
  assert.equal(response.status, 200);
  assert.equal(
    (
      await fetch(base + '/api/market', {
        headers: { 'If-None-Match': response.headers.get('etag')! },
      })
    ).status,
    304,
  );
  assert.equal(
    (await fetch(base + '/health').then((r) => r.json())).status,
    'ready',
  );
});
