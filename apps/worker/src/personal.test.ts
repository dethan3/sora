import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFileSync } from 'node:fs';
import { handleApi } from './index.ts';
import { migratedD1, memoryD1 } from './test/d1.ts';
import { catalogSeed } from './seed.ts';
import { randomToken, sha256, SESSION_COOKIE } from './security.ts';
import {
  personalEnvelopeSchema,
  importPreviewSchema,
  emptyPersonal,
  type PersonalState,
  type Position,
} from '@sora/market-data/personal';
import type { Env } from './types.ts';
const at = '2026-11-05T02:00:00.000Z';
const position: Position = {
  code: 'SOXX',
  units: '1.0001',
  costMinor: 123456789012,
  currency: 'USD',
};
async function fixture() {
  const DB = await migratedD1();
  const env: Env = { DB, ASSETS: { fetch: async () => new Response('asset') } };
  const catalog = JSON.parse(
    readFileSync(
      new URL('../../web/public/data/catalog.json', import.meta.url),
      'utf8',
    ),
  );
  await DB.batch(
    catalogSeed(catalog, at).map((s) => DB.prepare(s.sql).bind(...s.params)),
  );
  async function user(id: string) {
    const token = randomToken();
    await DB.prepare(
      'INSERT INTO users(id,email,created_at,updated_at) VALUES (?,?,?,?)',
    )
      .bind(id, `${id}@example.com`, at, at)
      .run();
    await DB.prepare(
      'INSERT INTO sessions(id_hash,user_id,created_at,expires_at) VALUES (?,?,?,?)',
    )
      .bind(await sha256(token), id, at, '2026-12-01T00:00:00.000Z')
      .run();
    return `${SESSION_COOKIE}=${token}`;
  }
  const a = await user('a'),
    b = await user('b');
  const call = (
    method: string,
    path: string,
    body?: unknown,
    cookie: string | null = a,
    headers: Record<string, string> = {},
  ) =>
    handleApi(
      new Request(`https://sora.test${path}`, {
        method,
        headers: {
          Origin: 'https://sora.test',
          'Content-Type': 'application/json',
          ...(cookie ? { Cookie: cookie } : {}),
          ...headers,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
      env,
      { now: () => new Date(at) },
    );
  const get = async (cookie = a) => {
    const response = await call('GET', '/api/me/personal', undefined, cookie);
    assert.equal(response.status, 200, await response.clone().text());
    return personalEnvelopeSchema.parse(await response.json());
  };
  const input = (
    state: PersonalState,
    revision = 0,
    operationId: string = crypto.randomUUID(),
    ownerId = 'a',
  ) => ({ state, revision, operationId, ownerId });
  const write = (state: PersonalState, revision = 0, operationId?: string) =>
    call('POST', '/api/me/personal', input(state, revision, operationId));
  return { DB, env, a, b, call, get, input, write };
}
test('personal state is private, uncached and ignores another user in URL parameters', async () => {
  const f = await fixture();
  assert.equal(
    (await f.call('GET', '/api/me/personal', undefined, null)).status,
    401,
  );
  const state = {
    ...emptyPersonal(),
    watchlist: ['SOXX'],
    positions: [position],
  };
  assert.equal((await f.write(state)).status, 200);
  const own = await f.get();
  assert.equal(own.revision, 1);
  assert.deepEqual(own.state, state);
  assert.deepEqual((await f.get(f.b)).state, emptyPersonal());
  const response = await f.call('GET', '/api/me/personal?userId=b');
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal(((await response.json()) as { ownerId: string }).ownerId, 'a');
  assert.equal(
    (
      await f.call(
        'POST',
        '/api/me/personal',
        f.input(state, 0, crypto.randomUUID(), 'b'),
      )
    ).status,
    409,
  );
});
test('cost and units round-trip exactly; only ready funds with matching currency can be added', async () => {
  const f = await fixture();
  const state = {
    ...emptyPersonal(),
    positions: [
      { ...position, units: '1000000.0000', costMinor: 1000000000000 },
    ],
  };
  assert.equal((await f.write(state)).status, 200);
  assert.deepEqual((await f.get()).state.positions, state.positions);
  for (const row of [
    { ...position, costMinor: 1.5 },
    { ...position, units: '0' },
    { ...position, units: '1.00001' },
    { ...position, currency: 'HKD' },
  ])
    assert.equal(
      (
        await f.write(
          { ...emptyPersonal(), positions: [row as typeof position] },
          1,
        )
      ).status,
      400,
    );
  assert.equal(
    (await f.write({ ...emptyPersonal(), watchlist: ['UNKNOWN'] }, 1)).status,
    400,
  );
  assert.equal((await f.get()).revision, 1);
});
test('concurrent edits cannot overwrite each other and duplicate operations apply only once', async () => {
  const f = await fixture();
  const responses = await Promise.all([
    f.write({ ...emptyPersonal(), watchlist: ['SOXX'] }),
    f.write({ ...emptyPersonal(), watchlist: ['AIQ'] }),
  ]);
  assert.deepEqual(responses.map((row) => row.status).sort(), [200, 409]);
  assert.equal((await f.get()).revision, 1);
  const id = crypto.randomUUID(),
    state = { ...emptyPersonal(), watchlist: ['SOXX', 'AIQ'] };
  const repeated = await Promise.all([
    f.write(state, 1, id),
    f.write(state, 1, id),
  ]);
  assert.deepEqual(
    repeated.map((row) => row.status),
    [200, 200],
  );
  assert.equal((await f.get()).revision, 2);
  assert.equal((await f.write(emptyPersonal(), 2, id)).status, 409);
  assert.equal((await f.write(emptyPersonal(), 2)).status, 200);
  assert.equal((await f.write(state, 1, id)).status, 200);
  assert.deepEqual((await f.get()).state, emptyPersonal());
  assert.equal((await f.get()).revision, 3);
});
test('import previews unsupported rows, requires explicit conflict choices, merges watchlist and is idempotent', async () => {
  const f = await fixture();
  await f.write({
    ...emptyPersonal(),
    watchlist: ['SOXX'],
    positions: [position],
  });
  const source = {
    ...emptyPersonal(),
    watchlist: ['SOXX', 'AIQ', 'UNKNOWN'],
    positions: [
      { ...position, units: '2.0000' },
      { ...position, code: 'AIQ', currency: 'HKD' } as typeof position,
    ],
  };
  const response = await f.call('POST', '/api/me/import/preview', {
    ownerId: 'a',
    localState: source,
  });
  assert.equal(response.status, 200, await response.clone().text());
  const preview = importPreviewSchema.parse(await response.json());
  assert.equal(preview.unavailable.length, 2);
  assert.equal(preview.conflicts.length, 1);
  assert.deepEqual(preview.eligible.watchlist, ['SOXX', 'AIQ']);
  assert.equal((await f.get()).revision, 1);
  const input = {
    ownerId: 'a',
    revision: preview.revision,
    operationId: crypto.randomUUID(),
    localState: preview.eligible,
    choices: {},
  };
  assert.equal((await f.call('POST', '/api/me/import', input)).status, 400);
  input.choices = { SOXX: 'cloud' };
  assert.equal((await f.call('POST', '/api/me/import', input)).status, 200);
  assert.equal((await f.get()).state.positions[0].units, position.units);
  assert.equal((await f.call('POST', '/api/me/import', input)).status, 200);
  assert.equal((await f.get()).revision, 2);
  assert.equal(
    (
      await f.call('POST', '/api/me/import', {
        ...input,
        operationId: crypto.randomUUID(),
        choices: { SOXX: 'local' },
      })
    ).status,
    409,
  );
  assert.equal((await f.get()).state.positions[0].units, position.units);
});
test('retained archived records remain exportable/removable but cannot be edited or newly added', async () => {
  const f = await fixture();
  const state = {
    ...emptyPersonal(),
    watchlist: ['SOXX'],
    positions: [position],
  };
  await f.write(state);
  await f.DB.prepare(
    "UPDATE instruments SET status='archived' WHERE public_code='SOXX'",
  ).run();
  assert.equal(
    (await f.write({ ...state, watchlist: ['SOXX', 'AIQ'] }, 1)).status,
    200,
  );
  assert.equal(
    (await f.write({ ...state, positions: [{ ...position, units: '2' }] }, 2))
      .status,
    400,
  );
  assert.deepEqual((await f.get()).state.positions, [position]);
  assert.equal((await f.write(emptyPersonal(), 2)).status, 200);
  assert.equal((await f.write(state, 3)).status, 400);
});
test('granular watchlist and position endpoints retain the same account revision contract', async () => {
  const f = await fixture();
  const body = (revision: number) => ({
    ownerId: 'a',
    revision,
    operationId: crypto.randomUUID(),
  });
  assert.equal(
    (await f.call('POST', '/api/me/watchlist', { ...body(0), code: 'SOXX' }))
      .status,
    200,
  );
  assert.equal(
    (await f.call('POST', '/api/me/positions', { ...body(1), position }))
      .status,
    200,
  );
  assert.equal(
    (await f.call('GET', '/api/me/watchlist')).headers.get('Cache-Control'),
    'no-store',
  );
  assert.equal(
    (await f.call('DELETE', '/api/me/positions/SOXX', body(2))).status,
    200,
  );
  assert.equal(
    (await f.call('DELETE', '/api/me/watchlist/SOXX', body(3))).status,
    200,
  );
  assert.deepEqual((await f.get()).state, emptyPersonal());
});
test('invalid origins, content types, extra owner fields and expired sessions never write', async () => {
  const f = await fixture();
  const input = f.input(emptyPersonal());
  assert.equal(
    (
      await f.call('POST', '/api/me/personal', input, f.a, {
        Origin: 'https://evil.test',
      })
    ).status,
    403,
  );
  assert.equal(
    (
      await f.call('POST', '/api/me/personal', input, f.a, {
        'Content-Type': 'text/plain',
      })
    ).status,
    415,
  );
  assert.equal(
    (await f.call('POST', '/api/me/personal', { ...input, userId: 'b' }))
      .status,
    400,
  );
  await f.DB.prepare(
    "UPDATE sessions SET expires_at='2020-01-01T00:00:00.000Z'",
  ).run();
  assert.equal((await f.write(emptyPersonal())).status, 401);
  assert.equal(
    (
      await f.DB.prepare(
        'SELECT COUNT(*) AS n FROM personal_operations',
      ).first()
    )?.n,
    0,
  );
});
test('account disabled between authentication and transaction cannot mutate personal records', async () => {
  const f = await fixture(),
    original = f.DB.batch.bind(f.DB);
  f.env.DB = {
    ...f.DB,
    batch: async (statements) => {
      await f.DB.prepare('UPDATE users SET disabled_at=? WHERE id=?')
        .bind(at, 'a')
        .run();
      return original(statements);
    },
  };
  assert.equal(
    (await f.write({ ...emptyPersonal(), watchlist: ['SOXX'] })).status,
    401,
  );
  assert.equal(
    (await f.DB.prepare('SELECT COUNT(*) AS n FROM watchlist').first())?.n,
    0,
  );
});
test('transaction failure rolls back revision, data and idempotency marker together', async () => {
  const f = await fixture(),
    original = f.DB.batch.bind(f.DB);
  f.env.DB = {
    ...f.DB,
    batch: (statements) =>
      original([
        ...statements,
        f.DB.prepare('INSERT INTO nonexistent_table VALUES (1)'),
      ]),
  };
  assert.equal(
    (await f.write({ ...emptyPersonal(), watchlist: ['SOXX'] })).status,
    503,
  );
  assert.equal(
    (await f.DB.prepare('SELECT COUNT(*) AS n FROM personal_accounts').first())
      ?.n,
    0,
  );
  assert.equal(
    (
      await f.DB.prepare(
        'SELECT COUNT(*) AS n FROM personal_operations',
      ).first()
    )?.n,
    0,
  );
  assert.equal(
    (await f.DB.prepare('SELECT COUNT(*) AS n FROM watchlist').first())?.n,
    0,
  );
});
test('legacy per-unit cost rows are preserved without an imprecise guessed conversion', async () => {
  const db = memoryD1();
  for (const file of [
    '0001_init.sql',
    '0002_auth.sql',
    '0003_admin.sql',
    '0004_ingest.sql',
  ]) {
    // Locate migration filenames without relying on future numbering conventions.
    const { readdirSync } = await import('node:fs');
    const actual = readdirSync(new URL('../migrations/', import.meta.url)).find(
      (name) => name.startsWith(file.slice(0, 4)),
    )!;
    await db.exec(
      readFileSync(new URL(`../migrations/${actual}`, import.meta.url), 'utf8'),
    );
  }
  await db
    .prepare(
      'INSERT INTO users(id,email,created_at,updated_at) VALUES (?,?,?,?)',
    )
    .bind('a', 'a@example.com', at, at)
    .run();
  await db
    .prepare(
      "INSERT INTO instruments(id,kind,symbol,market,name,currency,source,created_at,updated_at) VALUES('fund','fund','000001','CN','test','CNY','test',?,?)",
    )
    .bind(at, at)
    .run();
  await db
    .prepare(
      "INSERT INTO positions(id,user_id,instrument_id,quantity,cost_price,currency,created_at,updated_at) VALUES('old','a','fund','3.0001','0.123456','CNY',?,?)",
    )
    .bind(at, at)
    .run();
  await db.exec(
    readFileSync(
      new URL('../migrations/0005_personal.sql', import.meta.url),
      'utf8',
    ),
  );
  assert.equal(
    (await db.prepare('SELECT cost_price FROM positions_legacy').first())
      ?.cost_price,
    '0.123456',
  );
  assert.equal(
    (await db.prepare('SELECT COUNT(*) AS n FROM positions').first())?.n,
    0,
  );
});

test('units whitespace is normalized before storing a position', async () => {
  const f = await fixture();
  assert.equal(
    (
      await f.write({
        ...emptyPersonal(),
        positions: [{ ...position, units: ' 1.0001 ' }],
      })
    ).status,
    200,
  );
  assert.equal((await f.get()).state.positions[0].units, '1.0001');
});
