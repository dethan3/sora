import {
  personalEnvelopeSchema,
  importPreviewSchema,
  emptyPersonal,
} from '@sora/market-data/personal';
import { workerClient, runJobs } from '../../data-service/src/ingest.ts';
import { publicInstrumentSchema } from '@sora/market-data/ingest';
import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

// Exercise the actual workerd/D1 runtime without touching developer DB state.
const root = fileURLToPath(new URL('../', import.meta.url));
const require = createRequire(import.meta.url);
const wrangler = resolve(
  dirname(require.resolve('wrangler')),
  '../bin/wrangler.js',
);
const scratch = await mkdtemp(join(tmpdir(), 'sora-worker-check-'));
const state = join(scratch, 'state');
const machineToken = crypto.randomUUID() + crypto.randomUUID();
const env = {
  ...process.env,
  CI: 'true',
  WRANGLER_SEND_METRICS: 'false',
  WRANGLER_LOG_PATH: join(scratch, 'wrangler.log'),
};
const exec = promisify(execFile);
const run = (args: string[]) =>
  exec(process.execPath, [wrangler, ...args], {
    cwd: root,
    env,
    timeout: 60_000,
  });

// Ask the OS for an available loopback port rather than claiming a fixed one.
const socket = createServer();
socket.listen(0, '127.0.0.1');
await once(socket, 'listening');
const address = socket.address();
assert.ok(address && typeof address !== 'string');
const port = address.port;
socket.close();
await once(socket, 'close');

let worker: ReturnType<typeof spawn> | undefined;
let output = '';
try {
  await run([
    'd1',
    'migrations',
    'apply',
    'sora-db',
    '--local',
    '--persist-to',
    state,
  ]);
  await exec(process.execPath, ['scripts/seed.ts'], { cwd: root, env });
  for (let i = 0; i < 2; i++)
    await run([
      'd1',
      'execute',
      'sora-db',
      '--local',
      '--persist-to',
      state,
      '--file',
      '.wrangler/seed.sql',
    ]);

  worker = spawn(
    process.execPath,
    [
      wrangler,
      'dev',
      'src/test/runtime.ts',
      '--local',
      '--ip',
      '127.0.0.1',
      '--port',
      String(port),
      '--persist-to',
      state,
      '--var',
      'TURNSTILE_SITE_KEY:1x00000000000000000000AA',
      '--var',
      'TURNSTILE_SECRET_KEY:1x0000000000000000000000000000000AA',
      '--var',
      `INGEST_TOKEN:${machineToken}`,
    ],
    {
      cwd: root,
      env,
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  worker.stdout?.on('data', (chunk) => {
    output += chunk;
  });
  worker.stderr?.on('data', (chunk) => {
    output += chunk;
  });
  worker.on('error', (error) => {
    output += error.message;
  });
  const base = `http://127.0.0.1:${port}`;
  const get = (path: string, init?: RequestInit) =>
    fetch(base + path, { ...init, signal: AbortSignal.timeout(2000) });
  let ready = false;
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (worker.exitCode !== null || worker.signalCode !== null) break;
    try {
      const response = await get('/api/health');
      if (
        response.ok &&
        ((await response.json()) as { status: string }).status === 'ready'
      ) {
        ready = true;
        break;
      }
    } catch {
      /* Wait for workerd to bind the port. */
    }
    await setTimeout(250);
  }
  assert.ok(ready, `Local Worker did not become ready.\n${output}`);

  const response = await get('/api/catalog');
  assert.equal(response.status, 200);
  assert.match(response.headers.get('Content-Type') ?? '', /application\/json/);
  const reference = JSON.parse(
    await readFile(
      new URL('../../web/public/data/catalog.json', import.meta.url),
      'utf8',
    ),
  );
  assert.deepEqual(await response.json(), reference);
  const etag = response.headers.get('ETag');
  assert.ok(etag);
  assert.equal(
    (await get('/api/catalog', { headers: { 'If-None-Match': etag } })).status,
    304,
  );
  const head = await get('/api/catalog', { method: 'HEAD' });
  assert.equal(head.status, 200);
  assert.equal(await head.text(), '');
  assert.equal(head.headers.get('ETag'), etag);
  assert.equal((await get('/api/catalog', { method: 'POST' })).status, 405);
  for (const path of ['/api', '/api/missing']) {
    const missing = await get(path);
    assert.equal(missing.status, 404);
    assert.match(
      missing.headers.get('Content-Type') ?? '',
      /application\/json/,
    );
  }
  const page = await get('/funds/SOXX', {
    headers: { 'Sec-Fetch-Mode': 'navigate' },
  });
  assert.equal(page.status, 200);
  assert.match(await page.text(), /<div id="root"><\/div>/);
  const fallback = await get('/data/catalog.json');
  assert.deepEqual(await fallback.json(), reference);

  // Bootstrap credentials go over stdin; the CLI never receives/logs plaintext.
  const credentials = {
    email: 'runtime-admin@example.com',
    password: 'runtime-' + crypto.randomUUID(),
  };
  const bootstrap = () =>
    new Promise<void>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        ['scripts/bootstrap-admin.ts', '--local', '--persist-to', state],
        { cwd: root, env, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      let report = '';
      child.stdout.on('data', (chunk) => {
        report += chunk;
      });
      child.stderr.on('data', (chunk) => {
        report += chunk;
      });
      child.on('error', reject);
      child.on('exit', (code) =>
        code === 0 ? resolve() : reject(new Error(report)),
      );
      child.stdin.end(JSON.stringify(credentials));
    });
  await bootstrap();
  await assert.rejects(bootstrap(), /已有管理员/);
  const post = (path: string, body: unknown, cookie?: string) =>
    get(path, {
      method: 'POST',
      headers: {
        Origin: base,
        'Content-Type': 'application/json',
        ...(cookie ? { Cookie: cookie } : {}),
      },
      body: JSON.stringify(body),
    });
  const login = await post('/api/auth/login', {
    ...credentials,
    turnstileToken: 'runtime-' + crypto.randomUUID(),
  });
  assert.equal(login.status, 200, await login.clone().text());
  const adminCookie = login.headers.get('Set-Cookie')!;
  assert.match(adminCookie, /HttpOnly; Secure; SameSite=Lax/);
  const createInvite = await post('/api/admin/invites', {}, adminCookie);
  assert.equal(createInvite.status, 201);
  const invite = ((await createInvite.json()) as { invite: string }).invite;
  const registrations = await Promise.all(
    ['a', 'b'].map((name) =>
      post('/api/auth/register', {
        email: `${name}@example.com`,
        password: credentials.password,
        invite,
        turnstileToken: 'runtime-' + crypto.randomUUID(),
      }),
    ),
  );
  assert.deepEqual(
    registrations.map((response) => response.status).sort(),
    [201, 400],
  );
  const userCookie = registrations
    .find((response) => response.status === 201)!
    .headers.get('Set-Cookie')!;
  assert.equal((await post('/api/admin/invites', {}, userCookie)).status, 403);
  const me = await get('/api/auth/me', { headers: { Cookie: userCookie } });
  assert.equal(me.headers.get('Cache-Control'), 'no-store');
  const registered = (
    (await me.json()) as { user: { id: string; email: string; role: string } }
  ).user;
  assert.equal(registered.role, 'user');

  // Real D1 + actual executor, with only public upstream responses supplied as fixtures.
  const machine = workerClient(base, machineToken);
  const instrument = {
    id: 'CN:etf:510300',
    market: 'CN',
    kind: 'etf',
    symbol: '510300',
    name: 'CI 测试 ETF',
    currency: 'CNY',
    pinyin: 'ceshi',
    sourceName: 'CI fixture',
    sourceUrl: 'https://example.com/ci-fixture',
  };
  const started = await machine('/api/internal/directory/start', {
    scope: 'CN:etf',
    count: 1,
  });
  await machine(`/api/internal/directory/${started.id}/batch`, {
    entries: [instrument],
  });
  await machine(`/api/internal/directory/${started.id}/publish`);
  const search = await get('/api/search?q=510300');
  assert.equal(((await search.json()) as { items: unknown[] }).items.length, 1);
  assert.equal(
    (
      await post(
        '/api/admin/instruments',
        { directoryId: instrument.id },
        userCookie,
      )
    ).status,
    403,
  );
  assert.equal(
    (
      await post(
        '/api/me/requests',
        { directoryId: instrument.id, reason: 'CI' },
        userCookie,
      )
    ).status,
    201,
  );
  const approved = await post(
    `/api/admin/requests/${instrument.id}/approve`,
    {},
    adminCookie,
  );
  assert.equal(approved.status, 202, await approved.clone().text());
  const fixtureDay = (days: number) =>
    new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
  const reportYear = new Date().getUTCFullYear() - 1;
  const jobs = await runJobs(
    machine,
    async (api) =>
      api === 'stock_info_a_code_name'
        ? [{ 代码: '600519', 名称: 'CI 测试股票' }]
        : api === 'fund_portfolio_hold_em'
          ? [
              {
                股票代码: '600519',
                股票名称: 'CI 测试股票',
                '占净值比例(%)': 8.12,
                季度: `${reportYear}年4季度股票投资明细`,
              },
            ]
          : [
              { 日期: fixtureDay(3), 收盘: 3.1 },
              { 日期: fixtureDay(2), 收盘: 3.2 },
            ],
    { log: () => {} },
  );
  assert.deepEqual(jobs, { completed: 1, failed: 0 });
  const detail = publicInstrumentSchema.parse(
    await (await get(`/api/instruments/${instrument.id}`)).json(),
  );
  assert.equal(detail.status, 'ready');
  assert.equal(detail.holdings?.holdings[0].weightBp, 812);
  assert.equal(detail.series?.history.length, 2);
  assert.deepEqual(detail.companies[0].impactTags, []);
  const onlineCatalog = (await (await get('/api/catalog')).json()) as {
    funds: { code: string }[];
  };
  assert.ok(onlineCatalog.funds.some((fund) => fund.code === instrument.id));
  const onlineMarket = (await (await get('/api/market')).json()) as {
    series: { fundCode: string; unit: string }[];
  };
  assert.equal(
    onlineMarket.series.find((series) => series.fundCode === instrument.id)
      ?.unit,
    'cny-per-share',
  );
  await machine('/api/internal/refresh');
  const refresh = await runJobs(
    machine,
    async () => [
      { 日期: fixtureDay(3), 收盘: 3.1 },
      { 日期: fixtureDay(2), 收盘: 3.2 },
    ],
    { log: () => {} },
  );
  // Seed ETFs also refresh, using the same daily fixture in their exchange timezone.
  assert.ok(refresh.completed >= 1, JSON.stringify(refresh));
  assert.equal(refresh.failed, 0);
  assert.equal(
    publicInstrumentSchema.parse(
      await (await get(`/api/instruments/${instrument.id}`)).json(),
    ).holdings?.holdings[0].weightBp,
    812,
  );

  // Local account sync: real sessions, precision, isolation, CAS and import.
  const readPersonal = async (cookie: string) => {
    const response = await get('/api/me/personal', {
      headers: { Cookie: cookie },
    });
    assert.equal(response.headers.get('Cache-Control'), 'no-store');
    assert.equal(response.status, 200, await response.clone().text());
    return personalEnvelopeSchema.parse(await response.json());
  };
  const initialPersonal = await readPersonal(userCookie);
  assert.deepEqual(initialPersonal.state, emptyPersonal());
  const personalState = {
    ...emptyPersonal(),
    watchlist: ['SOXX'],
    positions: [
      {
        code: 'SOXX',
        units: '123.4567',
        costMinor: 123456789012,
        currency: 'USD' as const,
      },
    ],
  };
  const saved = await post(
    '/api/me/personal',
    {
      ownerId: registered.id,
      revision: 0,
      operationId: crypto.randomUUID(),
      state: personalState,
    },
    userCookie,
  );
  assert.equal(saved.status, 200, await saved.clone().text());
  const secondLogin = await post('/api/auth/login', {
    email: registered.email,
    password: credentials.password,
    turnstileToken: 'runtime-' + crypto.randomUUID(),
  });
  assert.equal(secondLogin.status, 200);
  const personalSecondCookie = secondLogin.headers.get('Set-Cookie')!;
  assert.deepEqual(
    (await readPersonal(personalSecondCookie)).state,
    personalState,
  );
  assert.deepEqual((await readPersonal(adminCookie)).state, emptyPersonal());
  const competing = await Promise.all(
    ['SOXX', 'AIQ'].map((code) =>
      post(
        '/api/me/personal',
        {
          ownerId: registered.id,
          revision: 1,
          operationId: crypto.randomUUID(),
          state: { ...personalState, watchlist: [code] },
        },
        userCookie,
      ),
    ),
  );
  assert.deepEqual(
    competing.map((response) => response.status).sort(),
    [200, 409],
  );
  const previewResponse = await post(
    '/api/me/import/preview',
    {
      ownerId: registered.id,
      localState: {
        ...personalState,
        positions: [{ ...personalState.positions[0], units: '9.0000' }],
      },
    },
    userCookie,
  );
  assert.equal(previewResponse.status, 200);
  const preview = importPreviewSchema.parse(await previewResponse.json());
  assert.equal(preview.conflicts.length, 1);
  const importBody = {
    ownerId: registered.id,
    revision: preview.revision,
    operationId: crypto.randomUUID(),
    localState: preview.eligible,
    choices: { SOXX: 'cloud' },
  };
  assert.equal(
    (await post('/api/me/import', importBody, userCookie)).status,
    200,
  );
  assert.equal(
    (await post('/api/me/import', importBody, userCookie)).status,
    200,
  );
  const finalPersonal = await readPersonal(personalSecondCookie);
  assert.equal(finalPersonal.revision, 3);
  assert.equal(finalPersonal.state.positions[0].units, '123.4567');
  assert.equal(finalPersonal.state.positions[0].costMinor, 123456789012);
  assert.equal((await post('/api/auth/logout', {}, userCookie)).status, 200);
  assert.deepEqual(
    await (
      await get('/api/auth/me', { headers: { Cookie: userCookie } })
    ).json(),
    { user: null },
  );
  const patch = (id: string, body: unknown) =>
    get(`/api/admin/users/${id}`, {
      method: 'PATCH',
      headers: {
        Origin: base,
        'Content-Type': 'application/json',
        Cookie: adminCookie,
      },
      body: JSON.stringify(body),
    });
  const adminIdentity = (
    (await (
      await get('/api/auth/me', { headers: { Cookie: adminCookie } })
    ).json()) as { user: { id: string } }
  ).user;
  const lastAdmin = await patch(adminIdentity.id, {
    action: 'setRole',
    role: 'user',
    revision: 0,
  });
  assert.equal(lastAdmin.status, 409, await lastAdmin.clone().text());
  const users = await get('/api/admin/users', {
    headers: { Cookie: adminCookie },
  });
  assert.equal(((await users.json()) as { total: number }).total, 2);
  const reset = await patch(registered.id, {
    action: 'resetPassword',
    revision: 0,
  });
  assert.equal(reset.status, 200, await reset.clone().text());
  const newPassword = ((await reset.json()) as { newPassword: string })
    .newPassword;
  const signIn = (value: string) =>
    post('/api/auth/login', {
      email: registered.email,
      password: value,
      turnstileToken: 'runtime-' + crypto.randomUUID(),
    });
  assert.equal((await signIn(credentials.password)).status, 401);
  const renewed = await signIn(newPassword);
  assert.equal(renewed.status, 200);
  const renewedCookie = renewed.headers.get('Set-Cookie')!;
  assert.equal(
    (
      await patch(registered.id, {
        action: 'setDisabled',
        disabled: true,
        revision: 1,
      })
    ).status,
    200,
  );
  assert.deepEqual(
    await (
      await get('/api/auth/me', { headers: { Cookie: renewedCookie } })
    ).json(),
    { user: null },
  );
  assert.equal(
    (
      await patch(registered.id, {
        action: 'setDisabled',
        disabled: false,
        revision: 2,
      })
    ).status,
    200,
  );
  assert.equal(
    (
      await patch(registered.id, {
        action: 'setRole',
        role: 'admin',
        revision: 3,
      })
    ).status,
    200,
  );
  const secondAdmin = await signIn(newPassword);
  assert.equal(secondAdmin.status, 200);
  const secondCookie = secondAdmin.headers.get('Set-Cookie')!;
  const demotions = await Promise.all([
    patch(adminIdentity.id, { action: 'setRole', role: 'user', revision: 0 }),
    get(`/api/admin/users/${registered.id}`, {
      method: 'PATCH',
      headers: {
        Origin: base,
        'Content-Type': 'application/json',
        Cookie: secondCookie,
      },
      body: JSON.stringify({ action: 'setRole', role: 'user', revision: 4 }),
    }),
  ]);
  assert.deepEqual(
    demotions.map((response) => response.status).sort(),
    [200, 409],
  );
  const survivingCookie =
    demotions[0].status === 200 ? secondCookie : adminCookie;
  const extra = await post('/api/admin/invites', {}, survivingCookie);
  const extraCode = ((await extra.json()) as { invite: string }).invite;
  const { sha256 } = await import('../src/security.ts');
  assert.equal(
    (
      await post(
        `/api/admin/invites/${await sha256(extraCode)}/revoke`,
        {},
        survivingCookie,
      )
    ).status,
    200,
  );
  assert.equal(
    (
      await post('/api/auth/register', {
        email: 'revoked@example.com',
        password: credentials.password,
        invite: extraCode,
        turnstileToken: 'runtime-' + crypto.randomUUID(),
      })
    ).status,
    400,
  );
  const audit = await get('/api/admin/audit?action=user.password_reset', {
    headers: { Cookie: survivingCookie },
  });
  const auditText = await audit.text();
  assert.ok(!auditText.includes(newPassword));
  assert.equal(JSON.parse(auditText).total, 1);
  console.log(
    'Worker local check passed: migrations/catalog/assets, ingestion directory/request/approval/executor/holdings/series/refresh, personal account isolation/precision/concurrency/import/replay, auth, admin password/role/disable changes, revocation, concurrent last-admin protection, invite revocation and audit (Siteverify mocked).',
  );
} finally {
  if (worker?.pid) {
    const exited = once(worker, 'exit');
    try {
      if (process.platform === 'win32') worker.kill('SIGTERM');
      else process.kill(-worker.pid, 'SIGTERM');
    } catch {
      /* The process may already have exited. */
    }
    await Promise.race([exited, setTimeout(3000)]);
    try {
      if (process.platform !== 'win32') process.kill(-worker.pid, 'SIGKILL');
      else if (worker.exitCode === null) worker.kill('SIGKILL');
    } catch {
      /* All child processes have exited. */
    }
  }
  await rm(scratch, { recursive: true, force: true });
}
