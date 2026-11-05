import { readMarket } from './market.ts';
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { handleApi } from './index.ts';
import { migratedD1 } from './test/d1.ts';
import { randomToken, sha256, SESSION_COOKIE } from './security.ts';
import {
  instrumentKey,
  type Collection,
  type ClaimedJob,
  type DirectoryEntry,
  publicInstrumentSchema,
} from '@sora/market-data/ingest';
import { dispatchPending } from './ingest-model.ts';
import { catalogSeed } from './seed.ts';
import { readFileSync } from 'node:fs';
import type { Env } from './types.ts';
const at = '2026-11-04T02:00:00.000Z';
const entry = (
  symbol = '510300',
  kind: DirectoryEntry['kind'] = 'etf',
  market: DirectoryEntry['market'] = 'CN',
): DirectoryEntry => ({
  id: instrumentKey(market, kind, symbol),
  kind,
  market,
  symbol,
  name: `测试 ${symbol}`,
  currency: ({ CN: 'CNY', US: 'USD', HK: 'HKD' } as const)[market],
  pinyin: 'ceshi',
  sourceName: 'Test source',
  sourceUrl: 'https://example.com/source',
});
const result: Collection = {
  series: {
    type: 'price',
    currency: 'CNY',
    timeZone: 'Asia/Shanghai',
    adjust: 'unadjusted',
    source: { name: '测试日线', url: 'https://example.com', fetchedAt: at },
    history: [
      { date: '2026-11-02', close: '3.1' },
      { date: '2026-11-03', close: '3.2' },
    ],
  },
  holdings: {
    asOf: '2026-09-30',
    coverage: 'top10',
    source: { name: '测试持仓', url: 'https://example.com', fetchedAt: at },
    holdings: [
      {
        id: 'CN:stock:600519',
        market: 'CN',
        symbol: '600519',
        name: '贵州茅台',
        weightBp: 800,
      },
    ],
  },
  warnings: [],
};
async function fixture() {
  const db = await migratedD1();
  let clock = new Date(at);
  const token = 'machine-secret-'.repeat(4);
  const env: Env = {
    DB: db,
    INGEST_TOKEN: token,
    ASSETS: { fetch: async () => new Response('asset') },
  };
  async function user(id: string, role = 'user') {
    await db
      .prepare(
        'INSERT INTO users(id,email,role,created_at,updated_at) VALUES (?,?,?,?,?)',
      )
      .bind(id, `${id}@test.com`, role, at, at)
      .run();
    const raw = randomToken();
    await db
      .prepare(
        'INSERT INTO sessions(id_hash,user_id,created_at,expires_at) VALUES (?,?,?,?)',
      )
      .bind(await sha256(raw), id, at, '2026-12-01T00:00:00.000Z')
      .run();
    return `${SESSION_COOKIE}=${raw}`;
  }
  const admin = await user('admin', 'admin'),
    ordinary = await user('ordinary');
  const call = async (
    method: string,
    path: string,
    body?: unknown,
    cookie: string | null = admin,
    machine = false,
  ) =>
    handleApi(
      new Request(`https://sora.test${path}`, {
        method,
        headers: {
          'Content-Type': 'application/json',
          Origin: 'https://sora.test',
          ...(cookie ? { Cookie: cookie } : {}),
          ...(machine ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
      env,
      { now: () => clock },
    );
  const post = (path: string, body: unknown = {}) =>
    call('POST', `/api/internal${path}`, body, null, true);
  async function directory(
    entries: DirectoryEntry[],
    scope = `${entries[0].market}:${entries[0].kind}`,
  ) {
    const response = await post('/directory/start', {
      scope,
      count: entries.length,
    });
    assert.equal(response.status, 201, await response.clone().text());
    const { id } = (await response.json()) as { id: string };
    assert.equal(
      (await post(`/directory/${id}/batch`, { entries })).status,
      200,
    );
    return { id, publish: () => post(`/directory/${id}/publish`) };
  }
  async function add(e = entry()) {
    assert.equal((await (await directory([e])).publish()).status, 200);
    const r = await call('POST', '/api/admin/instruments', {
      directoryId: e.id,
    });
    assert.equal(r.status, 202, await r.clone().text());
    return (await r.json()) as { jobId: string; instrumentId: string };
  }
  async function claim() {
    const r = await post('/jobs/claim');
    assert.equal(r.status, 200, await r.clone().text());
    return ((await r.json()) as { job: ClaimedJob | null }).job;
  }
  return {
    db,
    env,
    call,
    post,
    directory,
    add,
    claim,
    user,
    admin,
    ordinary,
    time: (v: string) => {
      clock = new Date(v);
    },
  };
}

test('directory staging is atomic, scoped, literal-searchable and rejects incomplete/old generations', async () => {
  const f = await fixture();
  const e = entry();
  assert.equal((await (await f.directory([e])).publish()).status, 200);
  const staged = await f.directory([entry('510500')]);
  assert.equal(
    (
      (await (
        await f.call('GET', '/api/search?q=510300', undefined, null)
      ).json()) as { items: unknown[] }
    ).items.length,
    1,
  );
  const incomplete = await f.post('/directory/start', {
    scope: 'CN:etf',
    count: 2,
  });
  const { id } = (await incomplete.json()) as { id: string };
  await f.post(`/directory/${id}/batch`, { entries: [entry('510880')] });
  assert.equal((await f.post(`/directory/${id}/publish`)).status, 409);
  f.time('2026-11-04T02:01:00.000Z');
  const newer = await f.directory([entry('159915')]);
  assert.equal((await newer.publish()).status, 200);
  assert.equal((await staged.publish()).status, 409);
  assert.equal(
    (
      (await (
        await f.call('GET', '/api/search?q=%', undefined, null)
      ).json()) as { items: unknown[] }
    ).items.length,
    0,
  );
  assert.equal(
    (
      await f.post(`/directory/${id}/batch`, {
        entries: [entry('AAPL', 'stock', 'US')],
      })
    ).status,
    400,
  );
});
test('machine token and live administrator permissions protect queue and mutations', async () => {
  const f = await fixture();
  await f.directory([entry()]);
  assert.equal(
    (await f.call('POST', '/api/internal/jobs/claim', {})).status,
    401,
  );
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/admin/instruments',
        { directoryId: entry().id },
        null,
      )
    ).status,
    401,
  );
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/admin/instruments',
        { directoryId: entry().id },
        f.ordinary,
      )
    ).status,
    403,
  );
  assert.equal(
    (await f.call('GET', '/api/admin/jobs', undefined, f.ordinary)).status,
    403,
  );
});
test('daily request quota counts unique entries, groups users, approves once and keeps requests private', async () => {
  const f = await fixture();
  const entries = Array.from({ length: 7 }, (_, i) => entry(`51030${i}`));
  await (await f.directory(entries)).publish();
  for (const e of entries.slice(0, 5))
    assert.equal(
      (
        await f.call(
          'POST',
          '/api/me/requests',
          { directoryId: e.id },
          f.ordinary,
        )
      ).status,
      201,
    );
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/me/requests',
        { directoryId: entries[0].id },
        f.ordinary,
      )
    ).status,
    200,
  );
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/me/requests',
        { directoryId: entries[5].id },
        f.ordinary,
      )
    ).status,
    429,
  );
  const second = await f.user('second');
  await f.call(
    'POST',
    '/api/me/requests',
    { directoryId: entries[0].id, reason: '研究' },
    second,
  );
  const grouped = (await (
    await f.call('GET', '/api/admin/requests')
  ).json()) as { items: { id: string; requesterCount: number }[] };
  assert.equal(grouped.items[0].requesterCount, 2);
  const own = (await (
    await f.call('GET', '/api/me/requests', undefined, second)
  ).json()) as { items: unknown[] };
  assert.equal(own.items.length, 1);
  assert.equal(
    (await f.call('POST', `/api/admin/requests/${entries[0].id}/approve`, {}))
      .status,
    202,
  );
  assert.equal(
    (await f.call('POST', `/api/admin/requests/${entries[0].id}/approve`, {}))
      .status,
    409,
  );
  assert.equal(
    (
      await f.call('POST', `/api/admin/requests/${entries[1].id}/reject`, {
        note: '暂不收录',
      })
    ).status,
    200,
  );
  const rows = await f.db
    .prepare(
      'SELECT status,review_note FROM instrument_requests WHERE directory_id=?',
    )
    .bind(entries[0].id)
    .all();
  assert.ok(rows.results.every((row) => row.status === 'approved'));
  assert.equal(
    (await f.db.prepare('SELECT COUNT(*) AS n FROM ingest_jobs').first())?.n,
    1,
  );
  f.time('2026-11-04T16:01:00.000Z');
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/me/requests',
        { directoryId: entries[5].id },
        f.ordinary,
      )
    ).status,
    201,
  );
});
test('single active job, lease reclaim and stale worker cannot publish; completion is atomic/idempotent', async () => {
  const f = await fixture();
  const queued = await f.add();
  assert.equal(
    (
      await f.call('POST', '/api/admin/instruments', {
        directoryId: entry().id,
      })
    ).status,
    202,
  );
  const job = (await f.claim())!;
  assert.equal(job.id, queued.jobId);
  assert.equal(await f.claim(), null);
  f.time('2026-11-04T02:11:00.000Z');
  const reclaimed = (await f.claim())!;
  assert.equal(reclaimed.id, job.id);
  assert.notEqual(reclaimed.lease, job.lease);
  assert.equal(
    (await f.post(`/jobs/${job.id}/complete`, { lease: job.lease, result }))
      .status,
    409,
  );
  assert.equal(
    (
      await f.post(`/jobs/${job.id}/complete`, {
        lease: reclaimed.lease,
        result: { ...result, series: { ...result.series, currency: 'USD' } },
      })
    ).status,
    400,
  );
  assert.equal(
    (await f.db.prepare('SELECT COUNT(*) AS n FROM market_series').first())?.n,
    0,
  );
  const completed = await f.post(`/jobs/${job.id}/complete`, {
    lease: reclaimed.lease,
    result,
  });
  assert.equal(completed.status, 200, await completed.clone().text());
  assert.equal(
    (
      await f.post(`/jobs/${job.id}/complete`, {
        lease: reclaimed.lease,
        result,
      })
    ).status,
    200,
  );
  assert.equal(
    (
      await f.post(`/jobs/${job.id}/complete`, {
        lease: reclaimed.lease,
        result: { ...result, warnings: ['changed'] },
      })
    ).status,
    409,
  );
  const detail = publicInstrumentSchema.parse(
    await (
      await f.call('GET', `/api/instruments/${entry().id}`, undefined, null)
    ).json(),
  );
  assert.equal(detail.holdings?.holdings.length, 1);
  assert.deepEqual(detail.impactTags, []);
  assert.equal(detail.companies[0].impactTags.length, 0);
  assert.equal(detail.series?.history.length, 2);
  assert.equal(
    (await f.db.prepare('SELECT COUNT(*) AS n FROM fund_holdings').first())?.n,
    1,
  );
  const market = (await (
    await f.call('GET', '/api/market', undefined, null)
  ).json()) as { series: { unit: string }[] };
  assert.equal(market.series[0].unit, 'cny-per-share');
});
test('failed jobs retry, retain last publication; daily refresh preserves holdings and archive wins', async () => {
  const f = await fixture();
  await f.add();
  const job = (await f.claim())!;
  await f.post(`/jobs/${job.id}/complete`, { lease: job.lease, result });
  await f.post('/refresh');
  const refresh = (await f.claim())!;
  assert.equal(refresh.mode, 'refresh');
  assert.equal(refresh.sinceDate, '2026-11-03');
  assert.equal(
    (
      await f.post(`/jobs/${refresh.id}/fail`, {
        lease: refresh.lease,
        error: 'upstream failed',
      })
    ).status,
    200,
  );
  let detail = (await (
    await f.call('GET', `/api/instruments/${entry().id}`)
  ).json()) as { status: string; holdings: unknown };
  assert.equal(detail.status, 'ready');
  assert.ok(detail.holdings);
  assert.equal(
    (
      await f.call(
        'POST',
        `/api/admin/jobs/${encodeURIComponent(refresh.id)}/retry`,
        {},
      )
    ).status,
    200,
  );
  const retried = (await f.claim())!;
  await f.call('PATCH', `/api/admin/instruments/${entry().id}`, {
    impactTags: [{ label: '需求增长', tone: 'positive' }],
    chainNode: 'ai-chips:design',
    status: 'archived',
    revision: 0,
  });
  assert.equal(
    (
      await f.post(`/jobs/${retried.id}/complete`, {
        lease: retried.lease,
        result: { ...result, holdings: null },
      })
    ).status,
    200,
  );
  assert.equal(
    (await f.call('GET', `/api/instruments/${entry().id}`)).status,
    404,
  );
  assert.equal(
    (
      await f.db
        .prepare('SELECT status FROM instruments WHERE id=?')
        .bind(entry().id)
        .first()
    )?.status,
    'archived',
  );
  assert.equal(
    (
      await f.db
        .prepare(
          'SELECT holdings_json FROM instrument_snapshots WHERE instrument_id=?',
        )
        .bind(entry().id)
        .first()
    )?.holdings_json,
    JSON.stringify(result.holdings),
  );
});
test('dispatch debounce and failed trigger leave queued jobs recoverable', async () => {
  const f = await fixture();
  await f.add();
  f.env.GITHUB_TOKEN = 'test-github';
  f.env.GITHUB_REPOSITORY = 'owner/repo';
  let calls = 0;
  const fail: typeof fetch = async () => {
    calls++;
    return new Response(null, { status: 503 });
  };
  assert.equal(
    (await dispatchPending(f.env, fail, new Date(at))).status,
    'failed',
  );
  assert.equal(
    (await dispatchPending(f.env, fail, new Date(at))).status,
    'pending',
  );
  assert.equal(calls, 1);
  assert.equal(
    (await f.db.prepare('SELECT status FROM ingest_jobs').first())?.status,
    'queued',
  );
  assert.equal(
    (
      await dispatchPending(
        f.env,
        async (input, init) => {
          assert.equal(
            String(input),
            'https://api.github.com/repos/owner/repo/actions/workflows/ingest.yml/dispatches',
          );
          assert.deepEqual(JSON.parse(String(init?.body)), {
            ref: 'main',
            inputs: { mode: 'jobs' },
          });
          return new Response(null, { status: 204 });
        },
        new Date(Date.parse(at) + 61000),
      )
    ).status,
    'sent',
  );
});
test('legacy seed remains identical, multi-market kind collisions are separate', async () => {
  const f = await fixture();
  const data = JSON.parse(
    readFileSync(
      new URL('../../web/public/data/catalog.json', import.meta.url),
      'utf8',
    ),
  );
  await f.db.batch(
    catalogSeed(data, at).map((s) => f.db.prepare(s.sql).bind(...s.params)),
  );
  assert.deepEqual(
    await (await f.call('GET', '/api/catalog', undefined, null)).json(),
    data,
  );
  for (const e of [entry('000001', 'fund'), entry('000001', 'stock')])
    await f.add(e);
  assert.equal(
    (
      await f.db
        .prepare("SELECT COUNT(*) AS n FROM instruments WHERE symbol='000001'")
        .first()
    )?.n,
    2,
  );
});

test('refresh IDs are URL decoded and publication cannot roll holdings backward', async () => {
  const f = await fixture();
  await f.add();
  const job = (await f.claim())!;
  await f.post(`/jobs/${job.id}/complete`, { lease: job.lease, result });
  await f.post('/refresh');
  const refresh = (await f.claim())!;
  assert.match(refresh.id, /refresh:/);
  assert.equal(
    (
      await f.post(`/jobs/${encodeURIComponent(refresh.id)}/heartbeat`, {
        lease: refresh.lease,
        steps: [],
      })
    ).status,
    200,
  );
  assert.equal(
    (
      await f.post(`/jobs/${encodeURIComponent(refresh.id)}/complete`, {
        lease: refresh.lease,
        result: { ...result, holdings: null },
      })
    ).status,
    200,
  );
  await f.call('POST', '/api/admin/instruments', { directoryId: entry().id });
  const full = (await f.claim())!;
  assert.equal(
    (
      await f.post(`/jobs/${full.id}/complete`, {
        lease: full.lease,
        result: {
          ...result,
          holdings: { ...result.holdings!, asOf: '2026-06-30' },
        },
      })
    ).status,
    400,
  );
});

test('same-millisecond directory generations and concurrent publish cannot roll back a scope', async () => {
  const f = await fixture();
  const older = await f.directory([entry('510300')]);
  const newer = await f.directory([entry('510500')]);
  assert.equal((await newer.publish()).status, 200);
  assert.equal((await older.publish()).status, 409);
  const current = await f.directory([entry('510880')]);
  const responses = await Promise.all([current.publish(), current.publish()]);
  assert.deepEqual(responses.map((row) => row.status).sort(), [200, 409]);
  const active = (
    await f.db
      .prepare('SELECT symbol FROM symbol_directory WHERE active=1')
      .all()
  ).results;
  assert.deepEqual(
    active.map((row) => row.symbol),
    ['510880'],
  );
});

test('directory batches are idempotent and unchanged live rows are not rewritten', async () => {
  const f = await fixture();
  const e = entry();
  await (await f.directory([e])).publish();
  const old = await f.db
    .prepare('SELECT updated_at FROM symbol_directory WHERE id=?')
    .bind(e.id)
    .first();
  f.time('2026-11-04T03:00:00.000Z');
  const next = await f.directory([e]);
  assert.equal(
    (await f.post(`/directory/${next.id}/batch`, { entries: [e] })).status,
    200,
  );
  assert.equal(
    (
      await f.db
        .prepare(
          'SELECT COUNT(*) AS n FROM directory_staging WHERE import_id=?',
        )
        .bind(next.id)
        .first()
    )?.n,
    1,
  );
  assert.equal((await next.publish()).status, 200);
  assert.equal(
    (
      await f.db
        .prepare('SELECT updated_at FROM symbol_directory WHERE id=?')
        .bind(e.id)
        .first()
    )?.updated_at,
    old?.updated_at,
  );
  const search = (await (
    await f.call('GET', '/api/search?q=510300')
  ).json()) as { directoryUpdatedAt: string };
  assert.equal(search.directoryUpdatedAt, '2026-11-04T03:00:00.000Z');
});

test('market snapshot reads many published instruments in two D1 queries', async () => {
  const f = await fixture();
  const entries = Array.from({ length: 60 }, (_, i) =>
    entry(String(510000 + i)),
  );
  await f.db
    .prepare(
      `INSERT INTO instruments(id,kind,symbol,market,name,currency,source,status,created_at,updated_at,public_code) SELECT json_extract(value,'$.id'),'etf',json_extract(value,'$.symbol'),'CN',json_extract(value,'$.name'),'CNY','test','ready',?,?,json_extract(value,'$.id') FROM json_each(?)`,
    )
    .bind(at, at, JSON.stringify(entries))
    .run();
  const { history, ...meta } = result.series;
  await f.db
    .prepare(
      `INSERT INTO instrument_snapshots(instrument_id,series_meta_json,published_at) SELECT id,?,? FROM instruments`,
    )
    .bind(JSON.stringify(meta), at)
    .run();
  for (const row of history)
    await f.db
      .prepare(
        'INSERT INTO market_series(instrument_id,date,close,adjust) SELECT id,?,?,? FROM instruments',
      )
      .bind(row.date, row.close, 'unadjusted')
      .run();
  let queries = 0;
  const db = {
    ...f.db,
    prepare: (sql: string) => {
      queries++;
      return f.db.prepare(sql);
    },
  };
  const snapshot = await readMarket({ ...f.env, DB: db }, new Date(at));
  assert.equal(snapshot.series.length, 60);
  assert.equal(queries, 2);
});

test('future source timestamps cannot publish an unreadable market snapshot', async () => {
  const f = await fixture();
  await f.add();
  const job = (await f.claim())!;
  const future = {
    ...result,
    series: {
      ...result.series,
      source: {
        ...result.series.source,
        fetchedAt: '2026-11-04T02:00:01.000Z',
      },
    },
  };
  assert.equal(
    (
      await f.post(`/jobs/${job.id}/complete`, {
        lease: job.lease,
        result: future,
      })
    ).status,
    400,
  );
  assert.equal(
    (await f.db.prepare('SELECT COUNT(*) AS n FROM market_series').first())?.n,
    0,
  );
});
