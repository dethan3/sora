import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import { App } from '../../App';
import { demoSchema } from '../../data/demo';
import { DIRECTION_KEY, readDirections } from './DirectionProvider';
import { STORAGE_KEY } from '../local/state';
import { listSource, returnPoint, rememberList } from '../../navigation/flow';

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(catalog)),
  );
});
function open(path = '/discover') {
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: [path],
  });
  const view = render(<RouterProvider router={router} />);
  return { router, ...view };
}
const follow = (title: string) =>
  screen.getByRole('button', { name: '关注方向：' + title });

describe('direction discovery', () => {
  it('ships six sourced directions without published fictional quotes or seeded user data', () => {
    const data = demoSchema.parse(catalog);
    expect(data.modules.map((item) => item.id)).toEqual([
      'ai',
      'semiconductors',
      'gold',
      'nasdaq100',
      'sp500',
      'csi300',
    ]);
    expect(data.funds).toHaveLength(6);
    expect(
      data.modules.every(
        (item) =>
          item.sourceUrl &&
          item.fundCodes.every((code) => item.fundReasons[code]),
      ),
    ).toBe(true);
    expect(
      data.funds.every(
        (fund) => fund.nav === null && fund.history.length === 0,
      ),
    ).toBe(true);
    expect(JSON.stringify(catalog)).not.toMatch(/DEMO|示例|虚构/);
    expect(
      demoSchema.safeParse({
        ...catalog,
        funds: catalog.funds.map((fund) => ({ ...fund, nav: '1.05' })),
      }).success,
    ).toBe(false);
  });
  it('groups interests separately from indices and preserves independent follows through refresh', async () => {
    const first = open();
    const themes = await screen.findByRole('region', { name: '主题与资产' });
    expect(
      within(themes).getByRole('link', { name: '半导体' }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('region', { name: '宽基与市场' }),
    ).toHaveTextContent('纳斯达克100');
    expect(screen.queryByText(/示例快照|虚构样本/)).not.toBeInTheDocument();
    await userEvent.click(follow('半导体'));
    expect(
      within(screen.getByRole('region', { name: /我的观察/ })).getByRole(
        'link',
        { name: '半导体' },
      ),
    ).toBeInTheDocument();
    expect(JSON.parse(localStorage.getItem(DIRECTION_KEY)!).ids).toEqual([
      'semiconductors',
    ]);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    first.unmount();
    open();
    const savedRegion = await screen.findByRole('region', { name: /我的观察/ });
    expect(
      await within(savedRegion).findByRole('link', { name: '半导体' }),
    ).toBeInTheDocument();
  });
  it('undoes a removal and synchronizes other tabs without changing fund records', async () => {
    localStorage.setItem(
      DIRECTION_KEY,
      JSON.stringify({ schemaVersion: 1, ids: ['gold'] }),
    );
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ schemaVersion: 1, watchlist: ['IAU'], positions: [] }),
    );
    open();
    await screen.findByRole('heading', { name: '发现市场' });
    await userEvent.click(
      within(screen.getByRole('region', { name: /我的观察/ })).getByRole(
        'button',
        { name: '取消关注方向：黄金' },
      ),
    );
    await userEvent.click(screen.getByRole('button', { name: '撤销' }));
    expect(readDirections().ids).toEqual(['gold']);
    const raw = JSON.stringify({ schemaVersion: 1, ids: ['ai', 'nasdaq100'] });
    localStorage.setItem(DIRECTION_KEY, raw);
    act(() =>
      window.dispatchEvent(
        new StorageEvent('storage', { key: DIRECTION_KEY, newValue: raw }),
      ),
    );
    expect(
      within(screen.getByRole('region', { name: /我的观察/ })).getByRole(
        'link',
        { name: '人工智能 AI' },
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: '撤销' }),
    ).not.toBeInTheDocument();
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!).watchlist).toEqual([
      'IAU',
    ]);
  });
  it('filters by keyword, listing market and type, with a recoverable empty state', async () => {
    const { router } = open();
    const search = await screen.findByRole('searchbox', { name: '搜索方向' });
    await userEvent.type(search, '芯片');
    expect(
      screen.getByRole('region', { name: '主题与资产' }),
    ).toHaveTextContent('半导体');
    expect(
      screen.queryByRole('link', { name: '黄金' }),
    ).not.toBeInTheDocument();
    await userEvent.clear(search);
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: '市场' }),
      '中国市场',
    );
    expect(screen.getByRole('link', { name: '沪深300' })).toBeInTheDocument();
    expect(router.state.location.search).toContain('market=');
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: '方向类型' }),
      'theme',
    );
    expect(
      screen.getByRole('heading', { name: '没有找到这个方向' }),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole('button', { name: '重新浏览全部方向' }),
    );
    expect(search).toHaveFocus();
    expect(router.state.location.search).toBe('');
    await userEvent.type(search, '纳指100ETF');
    expect(
      screen.getByRole('link', { name: '纳斯达克100' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('link', { name: '标普500' }),
    ).not.toBeInTheDocument();
  });
  it('connects filtered direction, fund detail and the original direction with valid return links', async () => {
    const { router } = open('/discover?q=芯片');
    await userEvent.click(await screen.findByRole('link', { name: '半导体' }));
    expect(
      await screen.findByRole('heading', { level: 1, name: '半导体' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('link', { name: '发现' })).toHaveAttribute(
      'aria-current',
      'page',
    );
    expect(screen.getByText('暂无走势数据')).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole('link', { name: 'iShares 半导体 ETF' }),
    );
    expect(
      await screen.findByRole('heading', {
        level: 1,
        name: 'iShares 半导体 ETF',
      }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '← 观察方向' }));
    expect(
      await screen.findByRole('heading', { level: 1, name: '半导体' }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '← 返回发现市场' }));
    expect(router.state.location.search).toContain('q=');
    expect(screen.getByRole('searchbox', { name: '搜索方向' })).toHaveValue(
      '芯片',
    );
    expect(screen.getByRole('link', { name: '半导体' })).toHaveFocus();
  });
  it('keeps a direction context through the filtered fund directory', async () => {
    const { router } = open('/discover/gold?from=%2Fdiscover');
    await userEvent.click(
      await screen.findByRole('link', { name: '在基金目录查看 →' }),
    );
    expect(router.state.location.search).toContain('module=gold');
    await userEvent.click(
      screen.getByRole('link', { name: 'iShares 黄金信托' }),
    );
    await userEvent.click(
      await screen.findByRole('link', { name: '← 基金目录' }),
    );
    await userEvent.click(screen.getByRole('link', { name: '← 返回观察方向' }));
    expect(
      await screen.findByRole('heading', { name: '黄金', level: 1 }),
    ).toBeInTheDocument();
  });
  it('supports deep links, unknown directions and load-error retry', async () => {
    vi.mocked(fetch).mockResolvedValueOnce(new Response('', { status: 503 }));
    open('/discover/missing');
    expect(
      screen.getByRole('status', { name: '正在载入内容' }),
    ).toBeInTheDocument();
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '暂时无法读取数据',
    );
    await userEvent.click(screen.getByRole('button', { name: '重新加载' }));
    expect(
      await screen.findByRole('heading', { name: '没有找到这个观察方向' }),
    ).toBeInTheDocument();
  });
  it('preserves corrupt storage until explicit reset and permits retry after write failure', async () => {
    localStorage.setItem(DIRECTION_KEY, 'broken');
    open();
    await screen.findByRole('heading', { name: '发现市场' });
    await userEvent.click(follow('黄金'));
    expect(localStorage.getItem(DIRECTION_KEY)).toBe('broken');
    await userEvent.click(screen.getByRole('button', { name: '重置观察方向' }));
    await userEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(localStorage.getItem(DIRECTION_KEY)).toBe('broken');
    await userEvent.click(screen.getByRole('button', { name: '重置观察方向' }));
    await userEvent.click(
      within(screen.getByRole('dialog')).getByRole('button', {
        name: '重置',
      }),
    );
    const fail = vi
      .spyOn(Storage.prototype, 'setItem')
      .mockImplementation(() => {
        throw new Error('quota');
      });
    await userEvent.click(follow('黄金'));
    expect(readDirections().ids).toEqual([]);
    expect(screen.getByText(/无法保存观察方向/)).toBeInTheDocument();
    fail.mockRestore();
    await userEvent.click(follow('黄金'));
    expect(readDirections().ids).toEqual(['gold']);
  });
  it('rejects stale writes and retains unavailable saved directions', async () => {
    localStorage.setItem(
      DIRECTION_KEY,
      JSON.stringify({ schemaVersion: 1, ids: ['old-direction'] }),
    );
    open();
    await screen.findByRole('heading', { name: '发现市场' });
    expect(
      screen.getByText(/old-direction · 方向暂不可用/),
    ).toBeInTheDocument();
    const newer = JSON.stringify({ schemaVersion: 1, ids: ['gold'] });
    localStorage.setItem(DIRECTION_KEY, newer);
    await userEvent.click(follow('半导体'));
    expect(localStorage.getItem(DIRECTION_KEY)).toBe(newer);
    expect(screen.getByText(/其他页面已更新观察方向/)).toBeInTheDocument();
  });
  it('isolates return destinations and keeps multiple list return points', () => {
    expect(listSource('/discover/gold?from=https://outside.test')).toBe(
      '/discover/gold',
    );
    expect(listSource('/discover/gold?from=%2Fdiscover%3Fq%3Dgold')).toBe(
      '/discover/gold?from=%2Fdiscover%3Fq%3Dgold',
    );
    expect(listSource('/discover/gold/extra')).toBeNull();
    rememberList('/discover?q=芯片', 'direction:semiconductors');
    rememberList('/discover/semiconductors', 'SOXX');
    expect(returnPoint('/discover?q=芯片')?.code).toBe(
      'direction:semiconductors',
    );
    expect(returnPoint('/discover/semiconductors')?.code).toBe('SOXX');
  });
});
