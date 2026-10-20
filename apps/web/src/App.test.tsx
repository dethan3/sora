import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import fixture from '../public/data/demo.json';
import { App } from './App';

beforeEach(() => {
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(fixture)),
  );
});
function open(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
}

describe('M0 routes', () => {
  it.each<[string, string | RegExp]>([
    ['/', /让每一份投资/],
    ['/discover', '发现市场'],
    ['/funds', '基金'],
    ['/watchlist', '我的关注'],
    ['/portfolio', '我的组合'],
  ])('opens %s directly', async (path, title) => {
    open(path);
    expect(
      await screen.findByRole('heading', { level: 1, name: title }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('link', { name: navigationLabel(path) }),
    ).toHaveAttribute('aria-current', 'page');
  });
  it('navigates from Welcome to discovery', async () => {
    open('/');
    await userEvent.click(screen.getByRole('link', { name: '探索 Sora' }));
    expect(
      await screen.findByRole('heading', {
        level: 1,
        name: '发现市场',
      }),
    ).toBeInTheDocument();
    expect(screen.getByRole('main')).toHaveFocus();
  });
  it('filters funds and shows an empty state', async () => {
    open('/funds');
    const search = await screen.findByRole('searchbox', {
      name: '搜索示例基金',
    });
    await userEvent.type(search, 'DEMO-KR');
    expect(screen.getByText('韩国市场示例')).toBeInTheDocument();
    expect(screen.queryByText('宽基观察示例')).not.toBeInTheDocument();
    await userEvent.clear(search);
    await userEvent.type(search, '不存在');
    expect(
      screen.getByRole('heading', { name: '暂无基金' }),
    ).toBeInTheDocument();
  });
  it('shows only watched funds and calculates portfolio totals', async () => {
    open('/watchlist');
    expect(await screen.findByText('宽基观察示例')).toBeInTheDocument();
    expect(screen.queryByText('韩国市场示例')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '我的组合' }));
    expect(await screen.findByText('¥15,240.00')).toBeInTheDocument();
    expect(screen.getByText('+1.60%')).toBeInTheDocument();
  });
  it('shows loading, recovers from errors and supports retry', async () => {
    vi.mocked(fetch).mockResolvedValueOnce(new Response('', { status: 500 }));
    open('/funds');
    expect(screen.getByRole('status')).toBeInTheDocument();
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '暂时无法读取示例',
    );
    await userEvent.click(screen.getByRole('button', { name: '重新加载' }));
    expect(await screen.findByRole('searchbox')).toBeInTheDocument();
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
  });
  it('handles unknown URLs independently of data availability', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response('', { status: 503 }));
    open('/missing');
    expect(
      screen.getByRole('heading', { name: '没有找到这个页面' }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '返回首页' }));
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
      '让每一份投资，清晰可见。',
    );
  });
  it('shows a recoverable state when the network is offline', async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError('Failed to fetch'));
    open('/funds');
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '暂时无法读取示例',
    );
    expect(screen.getByRole('button', { name: '重新加载' })).toBeEnabled();
  });
});

function navigationLabel(path: string) {
  return (
    {
      '/': 'Sora 首页',
      '/discover': '发现',
      '/funds': '基金',
      '/watchlist': '关注',
      '/portfolio': '我的组合',
    } as Record<string, string>
  )[path];
}
