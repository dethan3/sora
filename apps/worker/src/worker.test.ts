import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import worker from './index.ts';
import { catalogSeed, instrumentId, renderSql } from './seed.ts';
import { migratedD1 } from './test/d1.ts';
import type { Env } from './types.ts';

const staticCatalog = JSON.parse(
  readFileSync(
    new URL('../../web/public/data/catalog.json', import.meta.url),
    'utf8',
  ),
);

async function seededEnv() {
  const db = await migratedD1();
  await db.batch(
    catalogSeed(staticCatalog, '2026-11-03T00:00:00.000Z').map((s) =>
      db.prepare(s.sql).bind(...s.params),
    ),
  );
  const env: Env = {
    DB: db,
    ASSETS: { fetch: async () => new Response('asset') },
  };
  return env;
}

const get = (env: Env, path: string, init?: RequestInit) =>
  worker.fetch(new Request(`https://sora.test${path}`, init), env);

test('catalog served from D1 equals the static reference catalog', async () => {
  const env = await seededEnv();
  const response = await get(env, '/api/catalog');
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), staticCatalog);
});

test('seeding is idempotent', async () => {
  const env = await seededEnv();
  await env.DB.batch(
    catalogSeed(staticCatalog, '2026-11-04T00:00:00.000Z').map((s) =>
      env.DB.prepare(s.sql).bind(...s.params),
    ),
  );
  const count = await env.DB.prepare(
    'SELECT COUNT(*) AS n FROM instruments',
  ).first<{ n: number }>();
  assert.equal(count?.n, staticCatalog.funds.length);
});

test('catalog supports ETag revalidation', async () => {
  const env = await seededEnv();
  const first = await get(env, '/api/catalog');
  const etag = first.headers.get('ETag');
  assert.ok(etag);
  const second = await get(env, '/api/catalog', {
    headers: { 'If-None-Match': etag },
  });
  assert.equal(second.status, 304);
});

test('empty database reports catalog as unavailable', async () => {
  const env: Env = {
    DB: await migratedD1(),
    ASSETS: { fetch: async () => new Response('asset') },
  };
  assert.equal((await get(env, '/api/catalog')).status, 503);
  assert.equal(
    ((await (await get(env, '/api/health')).json()) as { status: string })
      .status,
    'ready',
  );
});

test('routing: unknown api 404, writes 405, other paths go to assets', async () => {
  const env = await seededEnv();
  assert.equal((await get(env, '/api/nope')).status, 404);
  assert.equal(
    (await get(env, '/api/catalog', { method: 'POST' })).status,
    405,
  );
  assert.equal(await (await get(env, '/funds/SOXX')).text(), 'asset');
});

test('instrument ids are market-qualified and SQL rendering escapes quotes', () => {
  assert.equal(instrumentId('2800.HK', '中国香港'), 'HK:2800');
  assert.equal(instrumentId('SOXX', '美国市场'), 'US:SOXX');
  assert.equal(
    renderSql([{ sql: 'SELECT ?, ?', params: ["it's", null] }]),
    "SELECT 'it''s', NULL;",
  );
});
