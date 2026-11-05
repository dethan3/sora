import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import { App } from '../../App';
import type { AuthUser } from '../auth/api';
import type { AdminUser } from './api';

const admin: AdminUser = {
  id: 'admin',
  email: 'admin@example.com',
  displayName: '管理员',
  role: 'admin',
  baseCurrency: 'CNY',
  disabledAt: null,
  revision: 0,
  createdAt: '2026-11-04T01:00:00.000Z',
  updatedAt: '2026-11-04T01:00:00.000Z',
};
const person: AdminUser = {
  ...admin,
  id: 'person',
  email: 'person@example.com',
  displayName: '用户甲',
  role: 'user',
};
const invite = {
  id: 'a'.repeat(64),
  createdAt: admin.createdAt,
  expiresAt: '2026-11-11T01:00:00.000Z',
  createdBy: admin.email,
  usedBy: null,
  usedAt: null,
  revokedAt: null,
  status: 'available',
};
let identity: AuthUser | null;
let people: AdminUser[];
let patchFailure: boolean;
let nextCount: number;

beforeEach(() => {
  identity = admin;
  people = [{ ...admin }, { ...person }];
  patchFailure = false;
  nextCount = 0;
  vi.stubEnv('VITE_AUTH_ENABLED', 'true');
  localStorage.clear();
  window.turnstile = { render: () => 'widget', remove: () => {} };
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const raw = String(input),
      url = new URL(raw, 'http://localhost');
    if (url.pathname === '/data/catalog.json') return Response.json(catalog);
    if (url.pathname === '/api/auth/me')
      return Response.json({ user: identity });
    if (url.pathname === '/api/auth/config')
      return Response.json({ siteKey: 'test' });
    if (url.pathname === '/api/admin/users' && init?.method === 'GET') {
      const q = url.searchParams.get('q') ?? '';
      const page = Number(url.searchParams.get('page') ?? '1');
      const filtered = people.filter(
        (user) => user.email.includes(q) || user.displayName?.includes(q),
      );
      return Response.json({
        items: filtered,
        total: nextCount || filtered.length,
        activeAdminCount: people.filter(
          (user) => user.role === 'admin' && !user.disabledAt,
        ).length,
        page,
        pageSize: 20,
      });
    }
    if (
      url.pathname.startsWith('/api/admin/users/') &&
      init?.method === 'PATCH'
    ) {
      if (patchFailure)
        return Response.json(
          { error: '账号状态已变化，请刷新后重试' },
          { status: 409 },
        );
      const id = url.pathname.split('/').at(-1)!;
      const target = people.find((user) => user.id === id)!;
      const body = JSON.parse(String(init.body));
      if (body.action === 'setDisabled')
        target.disabledAt = body.disabled ? admin.createdAt : null;
      if (body.action === 'setRole') target.role = body.role;
      target.revision++;
      const self = id === identity?.id;
      if (self) identity = null;
      return Response.json({
        user: target,
        signedOut: self,
        ...(body.action === 'resetPassword'
          ? { newPassword: 'new-password-for-test-only' }
          : {}),
      });
    }
    if (url.pathname === '/api/admin/invites') {
      if (init?.method === 'POST')
        return Response.json({
          invite: 'b'.repeat(64),
          expiresAt: invite.expiresAt,
          registerPath: `/register?invite=${'b'.repeat(64)}`,
        });
      return Response.json({
        items: [invite],
        total: 1,
        page: 1,
        pageSize: 20,
      });
    }
    if (url.pathname.endsWith('/revoke'))
      return Response.json({ revoked: true });
    if (url.pathname === '/api/admin/audit')
      return Response.json({
        items: [
          {
            id: 'event',
            action: 'user.role',
            target: 'person',
            actorEmail: admin.email,
            createdAt: admin.createdAt,
            detail: { after: { role: 'admin' } },
          },
        ],
        total: 1,
        page: 1,
        pageSize: 20,
      });
    throw new Error(`Unexpected ${raw}`);
  });
});
afterEach(() => {
  vi.unstubAllEnvs();
  delete window.turnstile;
});
function open(path = '/admin') {
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: [path],
  });
  render(<RouterProvider router={router} />);
  return router;
}
const userCard = () => screen.findByRole('article', { name: person.email });
async function confirm() {
  await userEvent.click(
    within(screen.getByRole('dialog')).getByRole('button', {
      name: '确认操作',
    }),
  );
}

describe('admin workspace', () => {
  it('redirects guests to login and preserves the admin destination', async () => {
    identity = null;
    const router = open();
    expect(
      await screen.findByRole('heading', { name: '登录 Sora' }),
    ).toBeInTheDocument();
    expect(router.state.location.search).toBe('?next=%2Fadmin');
    expect(
      vi
        .mocked(fetch)
        .mock.calls.some(([path]) => String(path).startsWith('/api/admin')),
    ).toBe(false);
  });

  it('shows 403 for a normal user and hides the admin account-menu entry', async () => {
    identity = person;
    open();
    expect(
      await screen.findByRole('heading', { name: '403 · 需要管理员权限' }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByText('我的账号', { selector: 'summary' }));
    expect(
      screen.queryByRole('link', { name: '管理后台' }),
    ).not.toBeInTheDocument();
    expect(
      vi
        .mocked(fetch)
        .mock.calls.some(([path]) => String(path).startsWith('/api/admin')),
    ).toBe(false);
  });

  it('opens from the account menu and protects the last admin controls', async () => {
    open('/account');
    await screen.findByRole('heading', { name: '我的账号', level: 1 });
    await userEvent.click(
      screen.getByText('我的账号', { selector: 'summary' }),
    );
    await userEvent.click(screen.getByRole('link', { name: '管理后台' }));
    const card = await screen.findByRole('article', { name: admin.email });
    expect(
      within(card).getByRole('button', { name: '禁用账号' }),
    ).toBeDisabled();
    expect(
      within(card).getByRole('button', { name: '取消管理员' }),
    ).toBeDisabled();
    expect(
      within(card).getByRole('button', { name: '重置密码' }),
    ).toBeEnabled();
  });

  it('requires confirmation before disabling a user and submits the original revision', async () => {
    open();
    await userEvent.click(
      within(await userCard()).getByRole('button', { name: '禁用账号' }),
    );
    expect(screen.getByRole('dialog')).toHaveTextContent(person.email);
    await userEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(
      vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === 'PATCH'),
    ).toBe(false);
    await userEvent.click(
      within(await userCard()).getByRole('button', { name: '禁用账号' }),
    );
    await confirm();
    expect(
      await screen.findByText('操作已完成，已记入审计日志。'),
    ).toBeInTheDocument();
    expect(
      within(await userCard()).getByRole('button', { name: '启用账号' }),
    ).toBeInTheDocument();
    const call = vi
      .mocked(fetch)
      .mock.calls.find(([, init]) => init?.method === 'PATCH')!;
    expect(JSON.parse(String(call[1]?.body))).toEqual({
      action: 'setDisabled',
      disabled: true,
      revision: 0,
    });
  });

  it('handles a stale edit by showing the error and reloading instead of changing the visible role', async () => {
    patchFailure = true;
    open();
    await userEvent.click(
      within(await userCard()).getByRole('button', { name: '设为管理员' }),
    );
    await confirm();
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '账号状态已变化',
    );
    await waitFor(() =>
      expect(
        vi
          .mocked(fetch)
          .mock.calls.filter(([path]) =>
            String(path).startsWith('/api/admin/users?'),
          ).length,
      ).toBe(2),
    );
    expect(
      within(await userCard()).getByRole('button', { name: '设为管理员' }),
    ).toBeInTheDocument();
  });

  it('shows a reset password only in the immediate result and clears it on dismissal', async () => {
    open();
    await userEvent.click(
      within(await userCard()).getByRole('button', { name: '重置密码' }),
    );
    await confirm();
    expect(await screen.findByLabelText('新密码（仅本次显示）')).toHaveValue(
      'new-password-for-test-only',
    );
    expect(localStorage.length).toBe(0);
    await userEvent.click(screen.getByRole('button', { name: '已保存，关闭' }));
    expect(
      screen.queryByDisplayValue('new-password-for-test-only'),
    ).not.toBeInTheDocument();
  });

  it('preserves the self-reset result after the old session is cleared, then returns to login', async () => {
    open();
    const card = await screen.findByRole('article', { name: admin.email });
    await userEvent.click(
      within(card).getByRole('button', { name: '重置密码' }),
    );
    await confirm();
    expect(
      await screen.findByRole('heading', { name: '当前账号已更新' }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText('新密码（仅本次显示）')).toHaveValue(
      'new-password-for-test-only',
    );
    await userEvent.click(screen.getByRole('button', { name: '重新登录' }));
    expect(
      await screen.findByRole('heading', { name: '登录 Sora' }),
    ).toBeInTheDocument();
  });

  it('searches users and paginates with explicit page parameters', async () => {
    nextCount = 41;
    open();
    await userCard();
    await userEvent.click(screen.getByRole('button', { name: '下一页' }));
    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        '/api/admin/users?page=2',
        expect.anything(),
      ),
    );
    await userEvent.type(
      screen.getByLabelText('按邮箱或名称搜索用户'),
      'person',
    );
    await userEvent.click(screen.getByRole('button', { name: '搜索用户' }));
    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        '/api/admin/users?page=1&q=person',
        expect.anything(),
      ),
    );
  });

  it('lists and revokes an invitation after confirmation', async () => {
    open();
    await userCard();
    await userEvent.click(screen.getByRole('button', { name: '邀请码管理' }));
    await userEvent.click(
      await screen.findByRole('button', { name: '撤销邀请码' }),
    );
    await confirm();
    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        `/api/admin/invites/${invite.id}/revoke`,
        expect.objectContaining({ method: 'POST' }),
      ),
    );
    expect(
      await screen.findByText('操作已完成，已记入审计日志。'),
    ).toBeInTheDocument();
  });

  it('filters audit logs and displays structured details', async () => {
    open();
    await userCard();
    await userEvent.click(screen.getByRole('button', { name: '审计日志' }));
    expect(
      await screen.findByRole('heading', { name: '修改角色' }),
    ).toBeInTheDocument();
    await userEvent.selectOptions(
      screen.getByLabelText('操作类型'),
      'user.role',
    );
    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        '/api/admin/audit?page=1&action=user.role',
        expect.anything(),
      ),
    );
  });

  it('removes the workspace when the server reports revoked administrator permission', async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    vi.mocked(fetch).mockImplementation((input, init) => {
      if (init?.method === 'PATCH') {
        identity = { ...admin, role: 'user' };
        return Promise.resolve(
          Response.json({ error: '需要管理员权限' }, { status: 403 }),
        );
      }
      return original(input, init);
    });
    open();
    await userEvent.click(
      within(await userCard()).getByRole('button', { name: '禁用账号' }),
    );
    await confirm();
    expect(
      await screen.findByRole('heading', { name: '403 · 需要管理员权限' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('article', { name: person.email }),
    ).not.toBeInTheDocument();
  });

  it('offers retry when loading the users list fails', async () => {
    const original = vi.mocked(fetch).getMockImplementation()!;
    let fail = true;
    vi.mocked(fetch).mockImplementation((input, init) =>
      String(input).startsWith('/api/admin/users?') && fail
        ? Promise.resolve(
            Response.json({ error: '暂时不可用' }, { status: 503 }),
          )
        : original(input, init),
    );
    open();
    expect(await screen.findByRole('alert')).toHaveTextContent('暂时不可用');
    fail = false;
    await userEvent.click(screen.getByRole('button', { name: '重新读取' }));
    expect(await userCard()).toBeInTheDocument();
  });
});
