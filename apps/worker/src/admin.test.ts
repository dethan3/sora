import assert from 'node:assert/strict';
import { test } from 'node:test';
import { handleApi } from './index.ts';
import { bootstrapStatements } from './bootstrap.ts';
import {
  hashPassword,
  randomToken,
  SESSION_COOKIE,
  sha256,
} from './security.ts';
import { migratedD1 } from './test/d1.ts';
import type { Env } from './types.ts';

const at = '2026-11-04T02:00:00.000Z';
const password = 'original test password';
async function fixture() {
  const db = await migratedD1();
  const env: Env = {
    DB: db,
    ASSETS: { fetch: async () => new Response('asset') },
    TURNSTILE_SITE_KEY: 'live-site',
    TURNSTILE_SECRET_KEY: 'live-secret',
  };
  const hash = await hashPassword(password);
  await db.batch(
    bootstrapStatements('admin@example.com', hash, at).map((s) =>
      db.prepare(s.sql).bind(...s.params),
    ),
  );
  const admin = (await db
    .prepare('SELECT id FROM users')
    .first<{ id: string }>())!.id;
  async function session(id: string) {
    const token = randomToken();
    await db
      .prepare(
        'INSERT INTO sessions (id_hash, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)',
      )
      .bind(await sha256(token), id, at, '2026-12-01T00:00:00.000Z')
      .run();
    return `${SESSION_COOKIE}=${token}`;
  }
  const cookie = await session(admin);
  async function add(id: string, role = 'user', disabled = false) {
    await db
      .prepare(
        'INSERT INTO users (id, email, display_name, password_hash, role, disabled_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      )
      .bind(
        id,
        `${id}@example.com`,
        id,
        hash,
        role,
        disabled ? at : null,
        at,
        at,
      )
      .run();
    return session(id);
  }
  const call = (
    method: string,
    path: string,
    body?: unknown,
    auth: string | null = cookie,
    headers: Record<string, string> = {},
  ) =>
    handleApi(
      new Request(`https://sora.test${path}`, {
        method,
        headers: {
          Origin: 'https://sora.test',
          'Content-Type': 'application/json',
          ...(auth ? { Cookie: auth } : {}),
          ...headers,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
      env,
      {
        now: () => new Date(at),
        fetch: async () =>
          Response.json({
            success: true,
            hostname: 'sora.test',
            action: path.endsWith('register') ? 'register' : 'login',
          }),
      },
    );
  const patch = (id: string, action: Record<string, unknown>, auth = cookie) =>
    call('PATCH', `/api/admin/users/${id}`, { revision: 0, ...action }, auth);
  const audits = async () =>
    (
      await db
        .prepare(
          "SELECT action, detail_json, target FROM audit_log WHERE action <> 'admin.bootstrap'",
        )
        .all()
    ).results;
  return { db, env, admin, cookie, add, call, patch, session, audits };
}

test('all admin routes require a live administrator, including direct API access', async () => {
  const { call, patch, add } = await fixture();
  const user = await add('ordinary');
  for (const path of [
    '/api/admin/users',
    '/api/admin/invites',
    '/api/admin/audit',
    '/api/admin/missing',
  ]) {
    assert.equal((await call('GET', path, undefined, null)).status, 401);
    assert.equal((await call('GET', path, undefined, user)).status, 403);
  }
  assert.equal(
    (await patch('ordinary', { action: 'setRole', role: 'admin' }, user))
      .status,
    403,
  );
  assert.equal(
    (
      await call(
        'POST',
        `/api/admin/invites/${'a'.repeat(64)}/revoke`,
        {},
        user,
      )
    ).status,
    403,
  );
});

test('user lists paginate, escape search wildcards, and never expose password/session data', async () => {
  const { db, call, add } = await fixture();
  for (let i = 0; i < 22; i++)
    await add(`person-${String(i).padStart(2, '0')}`);
  const response = await call('GET', '/api/admin/users');
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  const body = (await response.json()) as {
    items: { revision: number }[];
    total: number;
    activeAdminCount: number;
  };
  assert.equal(body.items.length, 20);
  assert.equal(body.total, 23);
  assert.equal(body.activeAdminCount, 1);
  assert.ok(body.items.every((row) => row.revision === 0));
  assert.ok(!JSON.stringify(body).match(/password_hash|id_hash|pbkdf2/));
  assert.equal(
    (
      (await (await call('GET', '/api/admin/users?page=2')).json()) as {
        items: unknown[];
      }
    ).items.length,
    3,
  );
  await db
    .prepare(
      "UPDATE users SET display_name = 'percent%_name' WHERE id = 'person-00'",
    )
    .run();
  assert.equal(
    (
      (await (await call('GET', '/api/admin/users?q=%25_')).json()) as {
        total: number;
      }
    ).total,
    1,
  );
  assert.equal((await call('GET', '/api/admin/users?page=-1')).status, 400);
  assert.equal((await call('GET', '/api/admin/users?page=1.5')).status, 400);
});

test('disable and enable revoke sessions, increment revision, and append matching audits', async () => {
  const { call, add, patch, audits } = await fixture();
  const userCookie = await add('person');
  const response = await patch('person', {
    action: 'setDisabled',
    disabled: true,
  });
  assert.equal(response.status, 200);
  assert.equal(
    ((await response.json()) as { user: { revision: number } }).user.revision,
    1,
  );
  assert.deepEqual(
    await (await call('GET', '/api/auth/me', undefined, userCookie)).json(),
    { user: null },
  );
  assert.equal(
    (
      await patch('person', {
        action: 'setDisabled',
        disabled: false,
        revision: 1,
      })
    ).status,
    200,
  );
  assert.deepEqual(
    await (await call('GET', '/api/auth/me', undefined, userCookie)).json(),
    { user: null },
  );
  assert.deepEqual(
    (await audits()).map((row) => row.action),
    ['user.disable', 'user.enable'],
  );
});

test('promote/demote require re-login and stale edits cannot overwrite a newer revision', async () => {
  const { call, add, patch, audits } = await fixture();
  const userCookie = await add('person');
  assert.equal(
    (await patch('person', { action: 'setRole', role: 'admin' })).status,
    200,
  );
  assert.equal(
    (await call('GET', '/api/admin/users', undefined, userCookie)).status,
    401,
  );
  assert.equal(
    (await patch('person', { action: 'setRole', role: 'user' })).status,
    409,
  );
  assert.equal((await audits()).length, 1);
  assert.equal(
    (await patch('person', { action: 'setRole', role: 'user', revision: 1 }))
      .status,
    200,
  );
  assert.deepEqual(JSON.parse(String((await audits())[1].detail_json)).after, {
    role: 'user',
  });
});

test('password reset returns a new password once, revokes sessions and never stores it in audit', async () => {
  const { add, patch, call, audits, db } = await fixture();
  const userCookie = await add('person');
  const response = await patch('person', { action: 'resetPassword' });
  assert.equal(response.status, 200);
  const result = (await response.json()) as {
    newPassword: string;
    signedOut: boolean;
  };
  assert.equal(result.signedOut, false);
  assert.match(result.newPassword, /^[a-f0-9]{32}$/);
  assert.deepEqual(
    await (await call('GET', '/api/auth/me', undefined, userCookie)).json(),
    { user: null },
  );
  assert.equal(
    (
      await call(
        'POST',
        '/api/auth/login',
        { email: 'person@example.com', password, turnstileToken: 'test' },
        null,
      )
    ).status,
    401,
  );
  assert.equal(
    (
      await call(
        'POST',
        '/api/auth/login',
        {
          email: 'person@example.com',
          password: result.newPassword,
          turnstileToken: 'test',
        },
        null,
      )
    ).status,
    200,
  );
  assert.ok(!JSON.stringify(await audits()).includes(result.newPassword));
  assert.ok(
    !JSON.stringify(
      (await db.prepare('SELECT password_hash FROM users').all()).results,
    ).includes(result.newPassword),
  );
  assert.ok(
    !(await (await call('GET', '/api/admin/users')).text()).includes(
      result.newPassword,
    ),
  );
});

test('resetting own password clears the current cookie while preserving one-time result', async () => {
  const { patch, admin, call, cookie } = await fixture();
  const response = await patch(admin, { action: 'resetPassword' });
  assert.equal(response.status, 200);
  assert.ok(response.headers.get('Set-Cookie')?.includes('Max-Age=0'));
  assert.equal(
    ((await response.json()) as { signedOut: boolean }).signedOut,
    true,
  );
  assert.equal(
    (await call('GET', '/api/admin/users', undefined, cookie)).status,
    401,
  );
});

test('the last available admin cannot be demoted, disabled or deleted, and failed writes leave no audit', async () => {
  const { admin, patch, db, audits, add } = await fixture();
  await add('disabled-admin', 'admin', true);
  for (const action of [
    { action: 'setRole', role: 'user' },
    { action: 'setDisabled', disabled: true },
  ]) {
    const response = await patch(admin, action);
    assert.equal(response.status, 409);
    assert.match(await response.text(), /至少保留一个可用管理员/);
  }
  await assert.rejects(
    db.prepare('DELETE FROM users WHERE id = ?').bind(admin).run(),
    /LAST_ACTIVE_ADMIN/,
  );
  assert.equal((await audits()).length, 0);
});

test('concurrent self-demotions leave exactly one available administrator', async () => {
  const { admin, add, patch, db, audits } = await fixture();
  const second = await add('second-admin', 'admin');
  const results = await Promise.all([
    patch(admin, { action: 'setRole', role: 'user' }),
    patch('second-admin', { action: 'setRole', role: 'user' }, second),
  ]);
  assert.deepEqual(results.map((r) => r.status).sort(), [200, 409]);
  assert.equal(
    (
      await db
        .prepare(
          "SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND disabled_at IS NULL",
        )
        .first<{ n: number }>()
    )?.n,
    1,
  );
  assert.equal((await audits()).length, 1);
});

test('concurrent administrators cannot disable each other and leave zero available admins', async () => {
  const { admin, add, patch, db, audits } = await fixture();
  const second = await add('second-admin', 'admin');
  const results = await Promise.all([
    patch('second-admin', { action: 'setDisabled', disabled: true }),
    patch(admin, { action: 'setDisabled', disabled: true }, second),
  ]);
  assert.equal(results.filter((result) => result.status === 200).length, 1);
  assert.equal(
    (
      await db
        .prepare(
          "SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND disabled_at IS NULL",
        )
        .first<{ n: number }>()
    )?.n,
    1,
  );
  assert.equal((await audits()).length, 1);
});

test('a failure after the audit insert rolls the whole mutation back', async () => {
  const { db, add, patch, audits } = await fixture();
  await add('person');
  await db.exec(
    "CREATE TRIGGER reject_test_update BEFORE UPDATE ON users WHEN OLD.id = 'person' BEGIN SELECT RAISE(ABORT, 'TEST_FAILURE'); END;",
  );
  assert.equal(
    (await patch('person', { action: 'setRole', role: 'admin' })).status,
    503,
  );
  assert.equal((await audits()).length, 0);
  assert.equal(
    (
      await db
        .prepare("SELECT role FROM users WHERE id = 'person'")
        .first<{ role: string }>()
    )?.role,
    'user',
  );
});

test('invites can be listed and revoked without exposing raw codes; revoked invites cannot register', async () => {
  const { call, audits } = await fixture();
  const code = (
    (await (await call('POST', '/api/admin/invites', {})).json()) as {
      invite: string;
    }
  ).invite;
  const response = await call('GET', '/api/admin/invites');
  const text = await response.text();
  assert.ok(!text.includes(code));
  const list = JSON.parse(text) as { items: { id: string; status: string }[] };
  assert.equal(list.items[0].status, 'available');
  assert.equal(
    (await call('POST', `/api/admin/invites/${list.items[0].id}/revoke`, {}))
      .status,
    200,
  );
  assert.equal(
    (
      await call(
        'POST',
        '/api/auth/register',
        {
          email: 'new@example.com',
          password,
          invite: code,
          turnstileToken: 'test',
        },
        null,
      )
    ).status,
    400,
  );
  assert.equal(
    (await call('POST', `/api/admin/invites/${list.items[0].id}/revoke`, {}))
      .status,
    409,
  );
  const filtered = (await (
    await call('GET', '/api/admin/audit?action=invite.revoke')
  ).json()) as { total: number; items: { action: string }[] };
  assert.equal(filtered.total, 1);
  assert.equal(filtered.items[0].action, 'invite.revoke');
  assert.equal((await audits()).length, 2);
});

test('concurrent registration/revocation can succeed in only one direction', async () => {
  const { call } = await fixture();
  const code = (
    (await (await call('POST', '/api/admin/invites', {})).json()) as {
      invite: string;
    }
  ).invite;
  const hash = await sha256(code);
  const results = await Promise.all([
    call('POST', `/api/admin/invites/${hash}/revoke`, {}),
    call(
      'POST',
      '/api/auth/register',
      {
        email: 'race@example.com',
        password,
        invite: code,
        turnstileToken: 'test',
      },
      null,
    ),
  ]);
  assert.equal(
    results.filter((result) => [200, 201].includes(result.status)).length,
    1,
  );
});

test('management writes validate Origin, JSON, versions and action names', async () => {
  const { call, add, patch } = await fixture();
  await add('person');
  assert.equal(
    (
      await call('PATCH', '/api/admin/users/person', {}, undefined, {
        Origin: 'https://attacker.test',
      })
    ).status,
    403,
  );
  assert.equal(
    (
      await call('PATCH', '/api/admin/users/person', {}, undefined, {
        'Content-Type': 'text/plain',
      })
    ).status,
    415,
  );
  assert.equal(
    (
      await call('PATCH', '/api/admin/users/person', {
        action: 'resetPassword',
      })
    ).status,
    400,
  );
  assert.equal((await patch('person', { action: 'unknown' })).status, 400);
  assert.equal(
    (await patch('missing', { action: 'resetPassword' })).status,
    404,
  );
  assert.equal((await call('POST', '/api/admin/users', {})).status, 405);
});
