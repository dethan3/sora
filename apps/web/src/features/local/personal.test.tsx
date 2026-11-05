import { App } from '../../App';
import catalog from '../../../public/data/catalog.json';
import { StrictMode } from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, afterEach, it, expect, vi } from 'vitest';
import {
  personalSchema,
  emptyPersonal,
  mergePersonal,
  importPreviewSchema,
  type PersonalState,
} from '@sora/market-data/personal';
import { AuthProvider, useAuth } from '../auth/AuthProvider';
import { PersonalProvider, usePersonal } from './PersonalProvider';
import { LocalNotice, BackupTools } from './BackupTools';
import { STORAGE_KEY } from './state';
import type { AuthUser } from '../auth/api';
const userA: AuthUser = {
    id: 'a',
    email: 'a@example.com',
    displayName: 'A',
    role: 'user',
    baseCurrency: 'CNY',
  },
  userB = { ...userA, id: 'b', email: 'b@example.com', displayName: 'B' };
let identity: AuthUser | null;
let accounts: Record<string, { revision: number; state: PersonalState }>;
let mode: 'ok' | 'offline' | 'uncertain' | 'conflict' | 'deferred';
let writes: { path: string; body: Record<string, unknown> }[];
let deferredResolve: (value: Response) => void;
let initialRead: (() => Promise<Response>) | undefined;
const position = {
  code: 'SOXX',
  units: '1.0000',
  costMinor: 10001,
  currency: 'USD',
} as const;
function IdentityControls() {
  const auth = useAuth();
  return (
    <>
      <button
        onClick={() =>
          void auth.authenticate('login', {
            email: userA.email,
            password: 'test',
          })
        }
      >
        切换 A
      </button>
      <button
        onClick={() =>
          void auth.authenticate('login', {
            email: userB.email,
            password: 'test',
          })
        }
      >
        切换 B
      </button>
      <button onClick={() => void auth.logout()}>退出账号</button>
    </>
  );
}
function Probe() {
  const personal = usePersonal();
  return (
    <>
      <output data-testid="personal-state">
        {JSON.stringify(personal.state)}
      </output>
      <output data-testid="mode">{personal.mode}</output>
      <button
        disabled={!personal.canWrite}
        onClick={() =>
          void personal.commit({
            ...personal.state,
            watchlist: [...new Set([...personal.state.watchlist, 'SOXX'])],
          })
        }
      >
        保存关注
      </button>
      <button
        disabled={!personal.canWrite}
        onClick={() =>
          void personal.commit({ ...personal.state, positions: [position] })
        }
      >
        保存持仓测试
      </button>
      <LocalNotice />
      <BackupTools />
    </>
  );
}
function open() {
  const router = createMemoryRouter(
    [
      {
        path: '*',
        element: (
          <AuthProvider enabled>
            <IdentityControls />
            <PersonalProvider>
              <Probe />
            </PersonalProvider>
          </AuthProvider>
        ),
      },
    ],
    { initialEntries: ['/funds'] },
  );
  return render(
    <StrictMode>
      <RouterProvider router={router} />
    </StrictMode>,
  );
}
const state = () =>
  JSON.parse(
    screen.getByTestId('personal-state').textContent!,
  ) as PersonalState;
beforeEach(() => {
  identity = userA;
  mode = 'ok';
  writes = [];
  initialRead = undefined;
  deferredResolve = () => {};
  accounts = {
    a: { revision: 0, state: emptyPersonal() },
    b: { revision: 0, state: { ...emptyPersonal(), watchlist: ['AIQ'] } },
  };
  localStorage.clear();
  sessionStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const path = String(input);
    if (path === '/data/catalog.json' || path === '/api/catalog')
      return Response.json(catalog);
    if (path === '/api/auth/me') return Response.json({ user: identity });
    if (path === '/api/auth/login') {
      identity =
        JSON.parse(String(init?.body)).email === userB.email ? userB : userA;
      return Response.json({ user: identity });
    }
    if (path === '/api/auth/logout') {
      identity = null;
      return Response.json({ user: null });
    }
    if (path === '/api/me/personal' && init?.method === 'GET') {
      if (initialRead) {
        const read = initialRead;
        initialRead = undefined;
        return read();
      }
      if (mode === 'offline') throw new TypeError('Failed to fetch');
      const current = accounts[identity!.id];
      return Response.json({ ownerId: identity!.id, ...current });
    }
    const body = JSON.parse(String(init?.body));
    if (path === '/api/me/import/preview') {
      const local = personalSchema.parse(body.localState),
        current = accounts[identity!.id];
      const conflicts = local.positions.flatMap((row) => {
        const cloud = current.state.positions.find((p) => p.code === row.code);
        return cloud && JSON.stringify(cloud) !== JSON.stringify(row)
          ? [{ code: row.code, local: row, cloud }]
          : [];
      });
      return Response.json(
        importPreviewSchema.parse({
          ownerId: identity!.id,
          revision: current.revision,
          current: current.state,
          eligible: local,
          conflicts,
          unavailable: [],
        }),
      );
    }
    if (path === '/api/me/personal' || path === '/api/me/import') {
      writes.push({ path, body });
      if (mode === 'offline') throw new TypeError('Failed to fetch');
      if (mode === 'conflict')
        return Response.json(
          { error: '账号数据已变化，请重新读取' },
          { status: 409 },
        );
      const current = accounts[identity!.id];
      const desired = path.endsWith('/import')
        ? mergePersonal(current.state, body.localState, body.choices)
        : personalSchema.parse(body.state);
      if (mode === 'deferred')
        return new Promise((resolve) => {
          deferredResolve = resolve;
        });
      current.state = desired;
      current.revision++;
      if (mode === 'uncertain') {
        mode = 'ok';
        throw new TypeError('lost response');
      }
      return Response.json({ ownerId: identity!.id, ...current });
    }
    throw new Error(`Unexpected ${path}`);
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});
it('signed-in changes are confirmed before rendering, survive StrictMode and never touch the device backup', async () => {
  const original = { ...emptyPersonal(), watchlist: ['LOCAL'] };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(original));
  open();
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled(),
  );
  mode = 'deferred';
  await userEvent.click(screen.getByRole('button', { name: '保存关注' }));
  expect(state().watchlist).toEqual([]);
  expect(screen.getByRole('button', { name: '保存关注' })).toBeDisabled();
  await act(async () =>
    deferredResolve(
      Response.json({
        ownerId: 'a',
        revision: 1,
        state: { ...emptyPersonal(), watchlist: ['SOXX'] },
      }),
    ),
  );
  expect(state().watchlist).toEqual(['SOXX']);
  expect(localStorage.getItem(STORAGE_KEY)).toBe(JSON.stringify(original));
  expect(writes[0].body.ownerId).toBe('a');
  expect(screen.getByText('已保存到当前账号。')).toBeInTheDocument();
});
it('an unconfirmed response retries the same operation id instead of inventing a successful local save', async () => {
  open();
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存持仓测试' })).toBeEnabled(),
  );
  mode = 'uncertain';
  await userEvent.click(screen.getByRole('button', { name: '保存持仓测试' }));
  await screen.findByText(/保存结果尚未确认/);
  expect(state().positions).toEqual([]);
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  await userEvent.click(screen.getByRole('button', { name: '保存持仓测试' }));
  await waitFor(() => expect(state().positions).toEqual([position]));
  expect(writes).toHaveLength(2);
  expect(writes[0].body.operationId).toBe(writes[1].body.operationId);
});
it('server conflicts require a fresh read before further writes', async () => {
  open();
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled(),
  );
  mode = 'conflict';
  await userEvent.click(screen.getByRole('button', { name: '保存关注' }));
  await screen.findByText(/其他页面或设备已修改/);
  expect(screen.getByRole('button', { name: '保存关注' })).toBeDisabled();
  expect(state().watchlist).toEqual([]);
  mode = 'ok';
  accounts.a = {
    revision: 1,
    state: { ...emptyPersonal(), watchlist: ['QQQ'] },
  };
  await userEvent.click(
    screen.getByRole('button', { name: '重新读取账号数据' }),
  );
  await waitFor(() => expect(state().watchlist).toEqual(['QQQ']));
  expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled();
});
it('a delayed response from A cannot populate B or the guest state after account changes', async () => {
  let resolveOld: (value: Response) => void = () => {};
  initialRead = () =>
    new Promise((resolve) => {
      resolveOld = resolve;
    });
  open();
  // StrictMode aborts the first read; the second read is independent.
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled(),
  );
  mode = 'deferred';
  await userEvent.click(screen.getByRole('button', { name: '保存关注' }));
  await userEvent.click(screen.getByRole('button', { name: '切换 B' }));
  await waitFor(() => expect(state().watchlist).toEqual(['AIQ']));
  await act(async () => {
    resolveOld(
      Response.json({
        ownerId: 'a',
        revision: 5,
        state: { ...emptyPersonal(), watchlist: ['PRIVATE_A'] },
      }),
    );
    deferredResolve(
      Response.json({
        ownerId: 'a',
        revision: 6,
        state: { ...emptyPersonal(), watchlist: ['PRIVATE_A'] },
      }),
    );
  });
  expect(state().watchlist).toEqual(['AIQ']);
  await userEvent.click(screen.getByRole('button', { name: '退出账号' }));
  await waitFor(() =>
    expect(screen.getByTestId('mode')).toHaveTextContent('local'),
  );
  expect(state()).toEqual(emptyPersonal());
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
});
it('an account read failure never falls back to guest data', async () => {
  localStorage.setItem(
    STORAGE_KEY,
    JSON.stringify({ ...emptyPersonal(), watchlist: ['GUEST'] }),
  );
  mode = 'offline';
  open();
  await screen.findByText(/账号数据暂不可用，未切换为本地保存/);
  expect(state().watchlist).toEqual([]);
  expect(screen.getByRole('button', { name: '保存关注' })).toBeDisabled();
  expect(screen.getByTestId('mode')).toHaveTextContent('account');
});
it('device import is opt-in, resolves conflicts explicitly and retains the original backup', async () => {
  const device = {
    ...emptyPersonal(),
    watchlist: ['SOXX'],
    positions: [{ ...position, units: '2.0000', costMinor: 20002 }],
  };
  const original = JSON.stringify(device);
  localStorage.setItem(STORAGE_KEY, original);
  accounts.a.state = { ...emptyPersonal(), positions: [position] };
  open();
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled(),
  );
  expect(writes).toHaveLength(0);
  await userEvent.click(
    screen.getByRole('button', { name: '查看并导入本地数据' }),
  );
  const dialog = await screen.findByRole('dialog');
  await within(dialog).findByText('SOXX 持仓冲突');
  expect(
    within(dialog).getByRole('button', { name: '确认导入可用记录' }),
  ).toBeDisabled();
  await userEvent.selectOptions(
    within(dialog).getByLabelText('保留 SOXX 的哪份持仓'),
    'local',
  );
  await userEvent.click(
    within(dialog).getByRole('button', { name: '确认导入可用记录' }),
  );
  await waitFor(() =>
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument(),
  );
  expect(state().positions[0].units).toBe('2.0000');
  expect(writes[0].path).toBe('/api/me/import');
  expect(localStorage.getItem(STORAGE_KEY)).toBe(original);
});
it('a mismatched server owner cannot expose another account through the provider', async () => {
  initialRead = async () =>
    Response.json({
      ownerId: 'b',
      revision: 0,
      state: { ...emptyPersonal(), watchlist: ['PRIVATE_B'] },
    });
  open();
  await waitFor(() =>
    expect(screen.getByRole('button', { name: '保存关注' })).toBeEnabled(),
  );
  expect(state().watchlist).not.toContain('PRIVATE_B');
});

it('a failed account position save keeps the form draft until the retry is confirmed', async () => {
  vi.stubEnv('VITE_AUTH_ENABLED', 'true');
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: ['/portfolio?add=SOXX'],
  });
  render(<RouterProvider router={router} />);
  const units = await screen.findByLabelText('持有份额');
  await waitFor(() => expect(units).toBeEnabled());
  await userEvent.type(units, '123.4567');
  await userEvent.type(screen.getByLabelText('持仓成本总额（USD）'), '1234.56');
  mode = 'offline';
  await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
  await screen.findAllByText(/保存结果尚未确认/);
  expect(screen.getByLabelText('持有份额')).toHaveValue('123.4567');
  expect(screen.getByLabelText('持仓成本总额（USD）')).toHaveValue('1234.56');
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  mode = 'ok';
  await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
  await waitFor(() =>
    expect(screen.queryByLabelText('持有份额')).not.toBeInTheDocument(),
  );
  expect(accounts.a.state.positions[0]).toEqual({
    ...position,
    units: '123.4567',
    costMinor: 123456,
  });
  expect(writes[0].body.operationId).toBe(writes[1].body.operationId);
});
