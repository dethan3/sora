import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import { App } from '../../App';
import { loginReturn } from './navigation';
import type { AuthUser } from './api';

const admin: AuthUser = {
  id: 'admin',
  email: 'admin@example.com',
  displayName: '管理员',
  role: 'admin',
  baseCurrency: 'CNY',
};
let identity: AuthUser | null;
let failedLogin: boolean;
let configFailure: boolean;
let options: Record<string, unknown>;
let counter: number;
const remove = vi.fn();
beforeEach(() => {
  identity = null;
  failedLogin = false;
  configFailure = false;
  counter = 0;
  localStorage.clear();
  vi.stubEnv('VITE_AUTH_ENABLED', 'true');
  window.turnstile = {
    remove,
    render: (container, settings) => {
      options = settings;
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = '完成验证';
      button.onclick = () =>
        (settings.callback as (token: string) => void)(`token-${++counter}`);
      container.append(button);
      return String(counter);
    },
  };
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const path = String(input);
    if (path === '/data/catalog.json') return Response.json(catalog);
    if (path === '/api/auth/config')
      return configFailure
        ? Response.json({ error: 'unavailable' }, { status: 503 })
        : Response.json({ siteKey: 'site-key' });
    if (path === '/api/auth/me') return Response.json({ user: identity });
    if (path === '/api/auth/login' || path === '/api/auth/register') {
      if (failedLogin)
        return Response.json({ error: '邮箱或密码不正确' }, { status: 401 });
      const body = JSON.parse(String(init?.body));
      identity = path.endsWith('register')
        ? {
            ...admin,
            id: 'user',
            email: body.email,
            displayName: body.displayName || '用户',
            role: 'user',
          }
        : admin;
      return Response.json({ user: identity });
    }
    if (path === '/api/auth/logout') {
      identity = null;
      return Response.json({ user: null });
    }
    if (path === '/api/admin/invites')
      return Response.json({
        invite: 'a'.repeat(64),
        expiresAt: '2026-11-11T01:00:00.000Z',
        registerPath: `/register?invite=${'a'.repeat(64)}`,
      });
    throw new Error(`Unexpected request ${path}`);
  });
});
afterEach(() => {
  vi.unstubAllEnvs();
  delete window.turnstile;
});
function open(path: string) {
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: [path],
  });
  render(<RouterProvider router={router} />);
  return router;
}
async function fillLogin() {
  await userEvent.type(await screen.findByLabelText('邮箱'), admin.email);
  await userEvent.type(screen.getByLabelText('密码'), 'correct horse battery');
  await userEvent.click(
    await screen.findByRole('button', { name: '完成验证' }),
  );
}

describe('account authentication', () => {
  it('requires verification, logs in, returns to an allowed page, and never stores credentials locally', async () => {
    open('/login?next=%2Fportfolio');
    const submit = await screen.findByRole('button', {
      name: '登录',
    });
    expect(submit).toBeDisabled();
    await fillLogin();
    expect(submit).toBeEnabled();
    await userEvent.click(submit);
    expect(
      await screen.findByRole('heading', { name: '我的组合', level: 1 }),
    ).toBeInTheDocument();
    expect(screen.getByRole('group', { name: '账号菜单' })).toBeInTheDocument();
    expect(JSON.stringify(localStorage)).not.toMatch(/correct horse|token-/);
    const request = vi
      .mocked(fetch)
      .mock.calls.find(([path]) => path === '/api/auth/login')!;
    expect(request[1]).toMatchObject({
      credentials: 'same-origin',
      cache: 'no-store',
      headers: { 'Content-Type': 'application/json' },
    });
  });

  it('shows a failed login and requires a new challenge before retrying', async () => {
    failedLogin = true;
    open('/login');
    await fillLogin();
    await userEvent.click(screen.getByRole('button', { name: '登录' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '邮箱或密码不正确',
    );
    expect(screen.getByRole('button', { name: '登录' })).toBeDisabled();
    failedLogin = false;
    await userEvent.click(
      await screen.findByRole('button', { name: '完成验证' }),
    );
    await userEvent.click(screen.getByRole('button', { name: '登录' }));
    expect(
      await screen.findByRole('heading', { name: '基金', level: 1 }),
    ).toBeInTheDocument();
  });

  it('prefills an invitation, registers and strips invite credentials from the destination', async () => {
    const router = open(`/register?invite=${'a'.repeat(64)}&next=%2Faccount`);
    expect(await screen.findByLabelText('邀请码')).toHaveValue('a'.repeat(64));
    await userEvent.type(screen.getByLabelText('邮箱'), 'new@example.com');
    await userEvent.type(
      screen.getByLabelText('密码（12～128 个字符）'),
      'correct horse battery',
    );
    await userEvent.click(
      await screen.findByRole('button', { name: '完成验证' }),
    );
    await userEvent.click(screen.getByRole('button', { name: '创建账号' }));
    expect(
      await screen.findByRole('heading', { name: '我的账号', level: 1 }),
    ).toBeInTheDocument();
    expect(router.state.location.pathname).toBe('/account');
    expect(router.state.location.search).toBe('');
    expect(
      screen.queryByRole('button', { name: '生成邀请码' }),
    ).not.toBeInTheDocument();
  });

  it('protects the account route and blocks external return destinations', async () => {
    const router = open('/account');
    expect(
      await screen.findByRole('heading', { name: '登录 Sora' }),
    ).toBeInTheDocument();
    expect(router.state.location.pathname).toBe('/login');
    for (const value of [
      '//attacker.test',
      'https://attacker.test',
      '/\\attacker.test',
      '/login',
      '/register',
    ])
      expect(loginReturn(value)).toBe('/funds');
    expect(loginReturn('/funds/SOXX?period=1M')).toBe('/funds/SOXX?period=1M');
  });

  it('restores identity, creates an admin invitation, and revokes the UI identity on logout', async () => {
    identity = admin;
    open('/account');
    await userEvent.click(
      await screen.findByRole('button', { name: '生成邀请码' }),
    );
    expect(await screen.findByLabelText('邀请链接（仅本次显示）')).toHaveValue(
      `${window.location.origin}/register?invite=${'a'.repeat(64)}`,
    );
    await userEvent.click(screen.getByRole('button', { name: '退出登录' }));
    expect(
      await screen.findByRole('heading', { name: '登录 Sora' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('group', { name: '账号菜单' }),
    ).not.toBeInTheDocument();
  });

  it('keeps the form usable when catalog loading fails and retries failed challenge configuration', async () => {
    configFailure = true;
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((input, init) =>
      String(input) === '/data/catalog.json'
        ? Promise.resolve(new Response('', { status: 503 }))
        : original(input, init),
    );
    open('/login');
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '登录验证暂不可用',
    );
    expect(screen.getByLabelText('邮箱')).toBeInTheDocument();
    configFailure = false;
    await userEvent.click(screen.getByRole('button', { name: '重试人机验证' }));
    expect(
      await screen.findByRole('button', { name: '完成验证' }),
    ).toBeInTheDocument();
  });

  it('clears the challenge token when it expires', async () => {
    open('/login');
    await fillLogin();
    expect(screen.getByRole('button', { name: '登录' })).toBeEnabled();
    act(() => (options['expired-callback'] as () => void)());
    expect(screen.getByRole('button', { name: '登录' })).toBeDisabled();
  });

  it('does not let a late identity refresh overwrite successful login', async () => {
    let resolveMe!: (response: Response) => void;
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((input, init) =>
      String(input) === '/api/auth/me'
        ? new Promise((resolve) => {
            resolveMe = resolve;
          })
        : original(input, init),
    );
    open('/login');
    await fillLogin();
    await userEvent.click(screen.getByRole('button', { name: '登录' }));
    expect(
      await screen.findByRole('group', { name: '账号菜单' }),
    ).toBeInTheDocument();
    await act(async () => resolveMe(Response.json({ user: null })));
    expect(screen.getByRole('group', { name: '账号菜单' })).toBeInTheDocument();
  });
});
