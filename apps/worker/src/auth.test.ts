import assert from 'node:assert/strict';
import { test } from 'node:test';
import { handleApi } from './index.ts';
import { bootstrapStatements } from './bootstrap.ts';
import {
  hashPassword,
  randomToken,
  SESSION_COOKIE,
  sha256,
  verifyPassword,
} from './security.ts';
import { migratedD1 } from './test/d1.ts';
import type { Env } from './types.ts';

const at = new Date('2026-11-04T01:00:00.000Z');
const password = 'correct horse battery';
const day = 86400_000;
async function fixture() {
  const env: Env = {
    DB: await migratedD1(),
    ASSETS: { fetch: async () => new Response('asset') },
    TURNSTILE_SITE_KEY: 'live-site-key',
    TURNSTILE_SECRET_KEY: 'live-secret-key',
  };
  const hash = await hashPassword(password);
  await env.DB.batch(
    bootstrapStatements('admin@example.com', hash, at.toISOString()).map((s) =>
      env.DB.prepare(s.sql).bind(...s.params),
    ),
  );
  const admin = (await env.DB.prepare('SELECT id FROM users').first<{
    id: string;
  }>())!;
  const token = randomToken();
  await env.DB.prepare(
    'INSERT INTO sessions (id_hash, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)',
  )
    .bind(
      await sha256(token),
      admin.id,
      new Date(at.getTime() + 30 * day).toISOString(),
      at.toISOString(),
    )
    .run();
  const cookie = `${SESSION_COOKIE}=${token}`;
  const call = (
    path: string,
    body?: unknown,
    auth?: string,
    headers: Record<string, string> = {},
    options: { date?: Date; verify?: typeof fetch; method?: string } = {},
  ) =>
    handleApi(
      new Request(`https://sora.test${path}`, {
        method: options.method ?? (body === undefined ? 'GET' : 'POST'),
        headers: {
          Origin: 'https://sora.test',
          'Content-Type': 'application/json',
          'CF-Connecting-IP': '192.0.2.1',
          ...(auth ? { Cookie: auth } : {}),
          ...headers,
        },
        body:
          body === undefined
            ? undefined
            : typeof body === 'string'
              ? body
              : JSON.stringify(body),
      }),
      env,
      {
        now: () => options.date ?? at,
        fetch:
          options.verify ??
          (async () =>
            Response.json({
              success: true,
              hostname: 'sora.test',
              action: path.endsWith('/register') ? 'register' : 'login',
            })),
      },
    );
  const invite = async () => {
    const response = await call('/api/admin/invites', {}, cookie);
    assert.equal(response.status, 201);
    return ((await response.json()) as { invite: string }).invite;
  };
  return { env, call, cookie, invite, admin, hash, token };
}
const registration = (invite: string, email = 'user@example.com') => ({
  email,
  password,
  invite,
  turnstileToken: 'challenge',
  displayName: '测试用户',
  role: 'admin',
});

test('password hashes have unique salts and verify only the correct password', async () => {
  const a = await hashPassword(password),
    b = await hashPassword(password);
  assert.notEqual(a, b);
  assert.equal(await verifyPassword(password, a), true);
  assert.equal(await verifyPassword('wrong password', a), false);
  assert.equal(await verifyPassword(password, 'malformed'), false);
});

test('bootstrap creates only one administrator and does not overwrite existing accounts', async () => {
  const { env, hash } = await fixture();
  await env.DB.batch(
    bootstrapStatements('second@example.com', hash, at.toISOString()).map((s) =>
      env.DB.prepare(s.sql).bind(...s.params),
    ),
  );
  assert.equal(
    (
      await env.DB.prepare('SELECT COUNT(*) AS n FROM users').first<{
        n: number;
      }>()
    )?.n,
    1,
  );
  assert.equal(
    (
      await env.DB.prepare(
        "SELECT COUNT(*) AS n FROM audit_log WHERE action='admin.bootstrap'",
      ).first<{ n: number }>()
    )?.n,
    1,
  );
});

test('registration consumes one invite, normalizes email, stores hashes, and cannot grant admin', async () => {
  const { env, call, invite } = await fixture();
  const code = await invite();
  const response = await call(
    '/api/auth/register',
    registration(code, ' USER@EXAMPLE.COM '),
  );
  assert.equal(response.status, 201);
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  const identity = (await response.json()) as {
    user: { role: string; email: string };
  };
  assert.equal(identity.user.role, 'user');
  assert.equal(identity.user.email, 'user@example.com');
  const cookie = response.headers.get('Set-Cookie')!;
  for (const attribute of [
    'HttpOnly',
    'Secure',
    'SameSite=Lax',
    'Path=/',
    'Max-Age=2592000',
  ])
    assert.ok(cookie.includes(attribute));
  const raw = cookie.split(';')[0].split('=')[1];
  assert.ok(
    await env.DB.prepare('SELECT id_hash FROM sessions WHERE id_hash = ?')
      .bind(await sha256(raw))
      .first(),
  );
  assert.equal(
    await env.DB.prepare('SELECT id_hash FROM sessions WHERE id_hash = ?')
      .bind(raw)
      .first(),
    null,
  );
  const saved = await env.DB.prepare(
    'SELECT * FROM invites WHERE code_hash = ?',
  )
    .bind(await sha256(code))
    .first<{ used_by: string }>();
  assert.ok(saved?.used_by);
  assert.equal(
    await env.DB.prepare('SELECT * FROM invites WHERE code_hash = ?')
      .bind(code)
      .first(),
    null,
  );
  assert.equal((await call('/api/admin/invites', {}, cookie)).status, 403);
  assert.equal(
    (await call('/api/auth/register', registration(code, 'other@example.com')))
      .status,
    400,
  );
});

test('concurrent registrations cannot reuse an invitation', async () => {
  const { env, call, invite } = await fixture();
  const code = await invite();
  const results = await Promise.all([
    call('/api/auth/register', registration(code, 'a@example.com')),
    call('/api/auth/register', registration(code, 'b@example.com')),
  ]);
  assert.deepEqual(results.map((r) => r.status).sort(), [201, 400]);
  assert.equal(
    (
      await env.DB.prepare('SELECT COUNT(*) AS n FROM users').first<{
        n: number;
      }>()
    )?.n,
    2,
  );
});

test('duplicate email and expired invitation leave no partial user/session/consumption', async () => {
  const { env, call, invite } = await fixture();
  const code = await invite();
  assert.equal(
    (await call('/api/auth/register', registration(code, 'admin@example.com')))
      .status,
    400,
  );
  assert.equal(
    (
      await env.DB.prepare('SELECT used_by FROM invites WHERE code_hash = ?')
        .bind(await sha256(code))
        .first<{ used_by: string | null }>()
    )?.used_by,
    null,
  );
  assert.equal(
    (
      await call(
        '/api/auth/register',
        registration(code),
        undefined,
        {},
        { date: new Date(at.getTime() + 8 * day) },
      )
    ).status,
    400,
  );
  assert.equal(
    (
      await env.DB.prepare('SELECT COUNT(*) AS n FROM users').first<{
        n: number;
      }>()
    )?.n,
    1,
  );
});

test('login, identity restoration, logout and token revocation form a complete session cycle', async () => {
  const { call, env } = await fixture();
  const response = await call('/api/auth/login', {
    email: 'admin@example.com',
    password,
    turnstileToken: 'challenge',
  });
  assert.equal(response.status, 200);
  const cookie = response.headers.get('Set-Cookie')!;
  const me = await call('/api/auth/me', undefined, cookie);
  assert.equal(me.headers.get('Cache-Control'), 'no-store');
  assert.equal(
    ((await me.json()) as { user: { role: string } }).user.role,
    'admin',
  );
  assert.equal(
    (await call('/api/auth/logout', {}, cookie)).headers
      .get('Set-Cookie')
      ?.includes('Max-Age=0'),
    true,
  );
  assert.deepEqual(
    await (await call('/api/auth/me', undefined, cookie)).json(),
    { user: null },
  );
  assert.equal((await call('/api/admin/invites', {}, cookie)).status, 401);
  assert.equal(
    (
      await env.DB.prepare('SELECT COUNT(*) AS n FROM sessions').first<{
        n: number;
      }>()
    )?.n,
    1,
  );
});

test('unknown, wrong-password and disabled logins share a failure message', async () => {
  const { call, env, admin } = await fixture();
  const wrong = await call('/api/auth/login', {
    email: 'admin@example.com',
    password: 'wrong password',
    turnstileToken: 'challenge',
  });
  const missing = await call('/api/auth/login', {
    email: 'unknown@example.com',
    password,
    turnstileToken: 'challenge',
  });
  await env.DB.prepare(
    `INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
    SELECT 'backup', 'backup@example.com', password_hash, 'admin', created_at, updated_at FROM users WHERE id = ?`,
  )
    .bind(admin.id)
    .run();
  await env.DB.prepare('UPDATE users SET disabled_at = ? WHERE id = ?')
    .bind(at.toISOString(), admin.id)
    .run();
  const disabled = await call('/api/auth/login', {
    email: 'admin@example.com',
    password,
    turnstileToken: 'challenge',
  });
  assert.equal(wrong.status, 401);
  assert.equal(missing.status, 401);
  assert.equal(disabled.status, 401);
  assert.deepEqual(await wrong.json(), await missing.json());
  assert.deepEqual(await disabled.json(), { error: '邮箱或密码不正确' });
});

test('expired, deleted and disabled sessions cannot authenticate', async () => {
  const { call, env, cookie, admin } = await fixture();
  await env.DB.prepare(
    `INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
    SELECT 'backup', 'backup@example.com', password_hash, 'admin', created_at, updated_at FROM users WHERE id = ?`,
  )
    .bind(admin.id)
    .run();
  assert.deepEqual(
    await (
      await call(
        '/api/auth/me',
        undefined,
        cookie,
        {},
        { date: new Date(at.getTime() + 31 * day) },
      )
    ).json(),
    { user: null },
  );
  await env.DB.prepare('UPDATE users SET disabled_at = ? WHERE id = ?')
    .bind(at.toISOString(), admin.id)
    .run();
  assert.equal((await call('/api/admin/invites', {}, cookie)).status, 401);
});

test('sliding expiry renews only after a day and updates both DB and cookie', async () => {
  const { call, cookie, env, token } = await fixture();
  assert.equal(
    (await call('/api/auth/me', undefined, cookie)).headers.has('Set-Cookie'),
    false,
  );
  const later = new Date(at.getTime() + 2 * day);
  const response = await call(
    '/api/auth/me',
    undefined,
    cookie,
    {},
    { date: later },
  );
  assert.ok(response.headers.get('Set-Cookie')?.includes('Max-Age=2592000'));
  assert.equal(
    (
      await env.DB.prepare('SELECT expires_at FROM sessions WHERE id_hash = ?')
        .bind(await sha256(token))
        .first<{ expires_at: string }>()
    )?.expires_at,
    new Date(later.getTime() + 30 * day).toISOString(),
  );
});

test('write routes enforce Origin, JSON, body bounds, and methods', async () => {
  const { call, cookie } = await fixture();
  assert.equal(
    (
      await call('/api/auth/logout', {}, cookie, {
        Origin: 'https://attacker.test',
      })
    ).status,
    403,
  );
  assert.equal(
    (await call('/api/auth/logout', {}, cookie, { Origin: '' })).status,
    403,
  );
  assert.equal(
    (
      await call('/api/auth/logout', {}, cookie, {
        'Content-Type': 'text/plain',
      })
    ).status,
    415,
  );
  assert.equal((await call('/api/auth/logout', '{broken', cookie)).status, 400);
  assert.equal(
    (await call('/api/auth/logout', 'x'.repeat(8193), cookie)).status,
    413,
  );
  assert.equal((await call('/api/auth/login')).status, 405);
  assert.equal((await call('/api/admin/invites', {})).status, 401);
});

test('missing configuration and rejected, replayed, wrong-host/action or unavailable Turnstile fail closed', async () => {
  const { call, env } = await fixture();
  const body = {
    email: 'admin@example.com',
    password,
    turnstileToken: 'challenge',
  };
  assert.equal(
    (await call('/api/auth/login', { ...body, turnstileToken: '' })).status,
    400,
  );
  for (const result of [
    { success: false, 'error-codes': ['timeout-or-duplicate'] },
    { success: true, hostname: 'attacker.test', action: 'login' },
    { success: true, hostname: 'sora.test', action: 'register' },
  ])
    assert.equal(
      (
        await call(
          '/api/auth/login',
          body,
          undefined,
          {},
          { verify: async () => Response.json(result) },
        )
      ).status,
      400,
    );
  assert.equal(
    (
      await call(
        '/api/auth/login',
        body,
        undefined,
        {},
        {
          verify: async () => {
            throw new Error('offline');
          },
        },
      )
    ).status,
    503,
  );
  env.TURNSTILE_SECRET_KEY = undefined;
  assert.equal((await call('/api/auth/config')).status, 503);
});

test('production rejects dummy Turnstile keys and never exposes the secret', async () => {
  const { call, env } = await fixture();
  const config = await (await call('/api/auth/config')).text();
  assert.ok(config.includes('live-site-key'));
  assert.ok(!config.includes('live-secret-key'));
  env.TURNSTILE_SECRET_KEY = '1x0000000000000000000000000000000AA';
  assert.equal((await call('/api/auth/config')).status, 503);
});

test('account login rate limiting is atomic, returns Retry-After and resets next window', async () => {
  const { call } = await fixture();
  const body = {
    email: 'admin@example.com',
    password: 'wrong password',
    turnstileToken: 'challenge',
  };
  for (let i = 0; i < 10; i++)
    assert.equal((await call('/api/auth/login', body)).status, 401);
  const limited = await call('/api/auth/login', body);
  assert.equal(limited.status, 429);
  assert.ok(limited.headers.has('Retry-After'));
  assert.equal(
    (
      await call(
        '/api/auth/login',
        body,
        undefined,
        {},
        { date: new Date(at.getTime() + 16 * 60_000) },
      )
    ).status,
    401,
  );
});
