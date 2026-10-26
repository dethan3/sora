import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import fixture from '../../test/fixtures/demo.json';
import { App } from '../../App';

beforeEach(() => {
  localStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockResolvedValue(
    new Response(JSON.stringify(fixture)),
  );
});

function open() {
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: ['/'],
  });
  render(<RouterProvider router={router} />);
  return router;
}

describe('Landing Page product introduction', () => {
  it('carries a chosen research direction into the actual fund library', async () => {
    const router = open();
    const directions = await screen.findByRole('group', {
      name: '观察方向',
    });
    const china = await within(directions).findByRole('button', {
      name: '中国市场',
    });
    await userEvent.click(china);
    expect(china).toHaveAttribute('aria-pressed', 'true');
    expect(
      within(directions).getByRole('button', { name: '科技主题' }),
    ).toHaveAttribute('aria-pressed', 'false');
    const related = screen.getByRole('list', {
      name: '中国市场关联的基金',
    });
    expect(within(related).getByText('宽基观察示例')).toBeInTheDocument();
    expect(within(related).getByText('暂无净值')).toBeInTheDocument();
    expect(within(related).queryByText('全球科技示例')).not.toBeInTheDocument();
    expect(localStorage.getItem('sora.personal.v1')).toBeNull();
    await userEvent.click(
      screen.getByRole('link', { name: '查看这个方向的基金' }),
    );
    expect(router.state.location.pathname).toBe('/funds');
    expect(router.state.location.search).toBe('?module=china');
    const table = await screen.findByRole('table', { name: '基金列表' });
    expect(
      within(table).getByRole('link', { name: '宽基观察示例' }),
    ).toBeInTheDocument();
    expect(
      within(table).queryByRole('link', { name: '全球科技示例' }),
    ).not.toBeInTheDocument();
  });

  it('offers product explanations and a working start when sample loading fails', async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError('Failed to fetch'));
    const router = open();
    expect(
      screen.getByRole('heading', { name: /给市场观察/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { name: '清晰，也体现在细节里。' }),
    ).toBeInTheDocument();
    const previewLink = screen.getByRole('link', { name: '前往发现页' });
    expect(previewLink).toHaveAttribute('href', '/discover');
    expect(
      screen.getByText('从观察方向，继续了解关联基金。'),
    ).toBeInTheDocument();
    const start = screen.getByRole('region', { name: '从你关心的市场开始。' });
    await userEvent.click(
      within(start).getByRole('link', { name: '探索市场' }),
    );
    expect(router.state.location.pathname).toBe('/discover');
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '暂时无法读取数据',
    );
  });

  it('reveals data and account limits before starting', async () => {
    open();
    const question = screen.getByText('现在可以查看真实行情吗？');
    await userEvent.click(question);
    const details = question.closest('details')!;
    expect(details).toHaveAttribute('open');
    expect(within(details).getByText(/行情尚未接入/)).toBeVisible();
    await userEvent.click(question);
    expect(details).not.toHaveAttribute('open');
    await userEvent.click(screen.getByText('需要先注册账户吗？'));
    expect(screen.getByText(/登录与注册暂未开放/)).toBeVisible();
    await userEvent.click(screen.getByText('我的关注和持仓会保存在哪里？'));
    expect(screen.getByText(/记录不会自动跟随/)).toBeVisible();
  });
});
