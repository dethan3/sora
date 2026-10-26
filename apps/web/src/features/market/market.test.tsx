import { act, fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  createMemoryRouter,
  RouterProvider,
  Route,
  Routes,
} from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import { demoSchema } from '../../data/demo';
import {
  DirectionProvider,
  DIRECTION_KEY,
} from '../discover/DirectionProvider';
import { DiscoverPage } from '../discover/DiscoverPage';
import { DirectionDetailPage } from '../discover/DirectionDetailPage';
import { FundDetailPage } from '../funds/FundDetailPage';
import { PortfolioPage } from '../portfolio/PortfolioPage';
import { PersonalProvider } from '../local/PersonalProvider';
import { STORAGE_KEY } from '../local/state';
import { parsePersonal } from '../local/state';
import { money, summarizePositions } from '../portfolio/model';
import {
  MarketProvider,
  MarketNotice,
  useMarket,
  type MarketLoader,
} from './MarketProvider';
import { MarketQuote, MarketChange } from './MarketQuote';
import { mockMarket } from './mock';
import {
  changePercent,
  marketSnapshotSchema,
  observationTime,
  periodHistory,
} from './model';
import { listSource } from '../../navigation/flow';

const data = demoSchema.parse(catalog);
const loader = async () => mockMarket;
beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});
function open(path = '/discover', load: MarketLoader = loader) {
  const router = createMemoryRouter(
    [
      {
        path: '*',
        element: (
          <PersonalProvider>
            <DirectionProvider>
              <MarketProvider enabled loader={load}>
                <Routes>
                  <Route
                    path="/discover"
                    element={<DiscoverPage data={data} />}
                  />
                  <Route
                    path="/discover/:id"
                    element={<DirectionDetailPage data={data} />}
                  />
                  <Route
                    path="/funds/:code"
                    element={<FundDetailPage data={data} />}
                  />
                  <Route
                    path="/portfolio"
                    element={<PortfolioPage data={data} />}
                  />
                </Routes>
              </MarketProvider>
            </DirectionProvider>
          </PersonalProvider>
        ),
      },
    ],
    { initialEntries: [path] },
  );
  return { router, ...render(<RouterProvider router={router} />) };
}
function Refresh() {
  const market = useMarket();
  return <button onClick={market.retry}>刷新行情</button>;
}

describe('market observations and boundaries', () => {
  it('keeps synthetic market observations separate from unpriced fund metadata and positions', () => {
    expect(marketSnapshotSchema.parse(mockMarket).series).toHaveLength(6);
    expect(mockMarket.series.map((item) => item.id)).toEqual(
      data.modules.map((item) => item.id),
    );
    expect(
      data.funds.every(
        (fund) =>
          fund.nav === null && fund.changePct === null && !fund.history.length,
      ),
    ).toBe(true);
    expect(data.funds.find((fund) => fund.code === '2800.HK')?.currency).toBe(
      'HKD',
    );
    expect(JSON.stringify(catalog)).not.toMatch(/沪深300|510300|中国市场/);
  });
  it('rejects duplicate, unordered, future and inconsistent observations', () => {
    const duplicate = structuredClone(mockMarket);
    duplicate.series.push(duplicate.series[0]);
    expect(marketSnapshotSchema.safeParse(duplicate).success).toBe(false);
    const badHistory = structuredClone(mockMarket);
    badHistory.series[0].history.reverse();
    expect(marketSnapshotSchema.safeParse(badHistory).success).toBe(false);
    const badQuote = structuredClone(mockMarket);
    badQuote.series[0].quote!.value += 1;
    expect(marketSnapshotSchema.safeParse(badQuote).success).toBe(false);
    expect(
      marketSnapshotSchema.safeParse({
        ...mockMarket,
        generatedAt: '2025-01-01T00:00:00Z',
      }).success,
    ).toBe(false);
  });
  it('uses calendar windows anchored to the observation date and clamps month ends', () => {
    const points = [
      { date: '2024-02-28', value: 1 },
      { date: '2024-02-29', value: 2 },
      { date: '2024-03-24', value: 3 },
      { date: '2024-03-31', value: 4 },
    ];
    expect(periodHistory(points, '1M').map((point) => point.date)).toEqual([
      '2024-02-29',
      '2024-03-24',
      '2024-03-31',
    ]);
    expect(periodHistory(points, '1W')).toHaveLength(2);
    expect(periodHistory([], '1Y')).toEqual([]);
  });
  it('keeps missing change unknown, treats flat as neutral and renders exchange-local timestamps', () => {
    expect(changePercent(10, null)).toBeNull();
    expect(changePercent(10, 0)).toBeNull();
    expect(changePercent(11, 10)).toBeCloseTo(10);
    const view = render(<MarketChange value={0} />);
    expect(screen.getByText('0.00%')).toHaveClass('market-flat');
    view.rerender(<MarketChange value={null} />);
    expect(screen.getByText('—')).toHaveClass('market-flat');
    view.rerender(<MarketChange value={0.001} />);
    expect(screen.getByText('0.00%')).toHaveClass('market-flat');
    expect(observationTime(mockMarket.series[5])).toContain('16:00 香港时间');
    expect(observationTime(mockMarket.series[0])).toContain('16:00 纽约时间');
  });
  it('does not load or display synthetic observations when disabled', async () => {
    const load = vi.fn(loader);
    render(
      <MarketProvider enabled={false} loader={load}>
        <MarketNotice />
        <MarketQuote id="ai" />
      </MarketProvider>,
    );
    expect(load).not.toHaveBeenCalled();
    expect(screen.queryByText('演示行情')).not.toBeInTheDocument();
    expect(screen.getByText('暂无数据')).toBeInTheDocument();
  });
  it('offers retry after a loading failure without blocking direction navigation', async () => {
    let reject!: (reason?: unknown) => void;
    const load = vi
      .fn<MarketLoader>()
      .mockImplementationOnce(
        () =>
          new Promise((_, fail) => {
            reject = fail;
          }),
      )
      .mockImplementation(loader);
    open('/discover/semiconductors', load);
    expect(
      screen.getByRole('status', { name: '正在载入行情' }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('link', { name: 'iShares 半导体 ETF' }),
    ).toBeInTheDocument();
    await act(async () => {
      reject(new Error('offline'));
    });
    expect(await screen.findByRole('alert')).toHaveTextContent(
      '行情暂时无法载入',
    );
    await userEvent.click(screen.getByRole('button', { name: '重试行情' }));
    expect(
      await screen.findByText('8,120.40', {
        selector: '.market-quote-value strong',
      }),
    ).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
  it('retains previous observations and timestamps when a refresh fails', async () => {
    const load = vi
      .fn<MarketLoader>()
      .mockImplementationOnce(loader)
      .mockRejectedValue(new Error('offline'));
    render(
      <MarketProvider enabled loader={load}>
        <MarketNotice />
        <MarketQuote id="ai" />
        <Refresh />
      </MarketProvider>,
    );
    expect(await screen.findByText('1,542.75')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: '刷新行情' }));
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(screen.getByText('1,542.75')).toBeInTheDocument();
    expect(
      screen.getByText('本次更新失败，正在显示上次记录。'),
    ).toBeInTheDocument();
  });
  it('keeps missing quotes and insufficient history empty instead of fabricating zero or a curve', async () => {
    const empty = structuredClone(mockMarket);
    empty.series[0].quote = null;
    empty.series[0].history = [];
    open('/discover/ai', async () => empty);
    expect(await screen.findByText('暂无数据')).toBeInTheDocument();
    expect(screen.getByText('暂无走势数据')).toBeInTheDocument();
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    expect(screen.queryByText('0.00%')).not.toBeInTheDocument();
  });
  it('shows rise, fall, flat, delay, stale and holiday states with explicit mock provenance', async () => {
    open();
    const chips = (await screen.findByRole('link', { name: '半导体' })).closest(
      'article',
    )!;
    expect(await within(chips).findByText('-1.36%')).toHaveClass('market-down');
    expect(within(chips).getByText('延迟 15 分钟')).toBeInTheDocument();
    expect(screen.getByText('+1.24%')).toHaveClass('market-up');
    expect(screen.getByText('0.00%')).toHaveClass('market-flat');
    expect(screen.getByText('更新滞后')).toBeInTheDocument();
    expect(screen.getByText('休市')).toBeInTheDocument();
    expect(screen.getByText('演示行情')).toBeInTheDocument();
    expect(screen.getByText('USD / 盎司')).toBeInTheDocument();
  });
  it('switches chart ranges without changing daily change and supports keyboard point inspection', async () => {
    const { router } = open(
      '/discover/semiconductors?from=%2Fdiscover%3Fq%3D芯片',
    );
    const month = await screen.findByRole('button', { name: '1月' });
    expect(month).toHaveAttribute('aria-pressed', 'true');
    await screen.findByRole('slider');
    const monthMax = Number(screen.getByRole('slider').getAttribute('max'));
    await userEvent.click(screen.getByRole('button', { name: '1周' }));
    expect(router.state.location.search).toContain('period=1W');
    expect(Number(screen.getByRole('slider').getAttribute('max'))).toBeLessThan(
      monthMax,
    );
    expect(screen.getByText('-1.36%')).toBeInTheDocument();
    expect(screen.getByText('1周区间表现')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('slider'), { target: { value: '0' } });
    expect(screen.getByRole('slider')).toHaveAttribute(
      'aria-valuetext',
      expect.stringContaining('2026-10-16'),
    );
    await userEvent.click(screen.getByRole('button', { name: '1年' }));
    expect(
      Number(screen.getByRole('slider').getAttribute('max')),
    ).toBeGreaterThan(250);
    await userEvent.click(screen.getByText(/查看 .* 条历史记录/));
    expect(
      screen.getByRole('table', { name: /演示历史记录/ }),
    ).toBeInTheDocument();
  });
  it('keeps period and discovery source after fund navigation without turning index points into NAV', async () => {
    const { router } = open(
      '/discover/hangseng?period=3M&from=%2Fdiscover%3Fq%3D港股',
    );
    await screen.findByRole('slider');
    expect(screen.getByRole('button', { name: '3月' })).toHaveAttribute(
      'aria-pressed',
      'true',
    );
    await userEvent.click(screen.getByRole('link', { name: '盈富基金' }));
    expect(await screen.findByText('暂无净值数据')).toBeInTheDocument();
    expect(screen.queryByText('24,580.60')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('link', { name: '← 观察方向' }));
    expect(router.state.location.search).toContain('period=3M');
    expect(screen.getByRole('button', { name: '3月' })).toHaveAttribute(
      'aria-pressed',
      'true',
    );
    await userEvent.click(screen.getByRole('link', { name: '← 返回发现市场' }));
    expect(screen.getByRole('searchbox', { name: '搜索方向' })).toHaveValue(
      '港股',
    );
    expect(listSource('/discover/hangseng?period=oops&from=%2Fdiscover')).toBe(
      '/discover/hangseng?from=%2Fdiscover',
    );
  });
  it('saves HKD holdings without using mock index prices for valuation', async () => {
    open('/funds/2800.HK');
    await userEvent.click(screen.getByRole('link', { name: /录入持仓/ }));
    await userEvent.type(
      screen.getByRole('textbox', { name: '持有份额' }),
      '10',
    );
    await userEvent.type(
      screen.getByRole('textbox', { name: '持仓成本总额（HKD）' }),
      '249.50',
    );
    await userEvent.click(screen.getByRole('button', { name: '保存持仓' }));
    expect(
      JSON.parse(localStorage.getItem(STORAGE_KEY)!).positions[0],
    ).toMatchObject({ code: '2800.HK', currency: 'HKD', costMinor: 24950 });
    expect(screen.getAllByText(/待估值/).length).toBeGreaterThan(0);
  });
  it('keeps chart period changes in browser history without adding a back step', async () => {
    const { router } = open('/discover?q=芯片');
    await userEvent.click(await screen.findByRole('link', { name: '半导体' }));
    await screen.findByRole('slider');
    await userEvent.click(screen.getByRole('button', { name: '1周' }));
    await act(async () => {
      await router.navigate(-1);
    });
    expect(router.state.location.pathname).toBe('/discover');
    expect(screen.getByRole('searchbox', { name: '搜索方向' })).toHaveValue(
      '芯片',
    );
  });
  it('does not modify saved mainland holdings when the public catalog changes', async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        schemaVersion: 1,
        watchlist: ['510300'],
        positions: [
          { code: '510300', currency: 'CNY', units: '10', costMinor: 1000 },
        ],
      }),
    );
    localStorage.setItem(
      DIRECTION_KEY,
      JSON.stringify({ schemaVersion: 1, ids: ['csi300'] }),
    );
    const stored = localStorage.getItem(STORAGE_KEY);
    open();
    await screen.findByText(/csi300 · 方向暂不可用/);
    expect(localStorage.getItem(STORAGE_KEY)).toBe(stored);
  });
  it('round-trips HKD backups and keeps USD and HKD summaries separate without invented valuations', () => {
    const personal = parsePersonal(
      JSON.stringify({
        schemaVersion: 1,
        watchlist: ['2800.HK'],
        positions: [
          { code: '2800.HK', currency: 'HKD', units: '10', costMinor: 24950 },
          { code: 'QQQ', currency: 'USD', units: '1', costMinor: 20000 },
        ],
      }),
    );
    const groups = summarizePositions(personal.positions, data.funds);
    expect(groups.map((group) => group.currency)).toEqual(['USD', 'HKD']);
    expect(groups.map((group) => group.cost)).toEqual([20000, 24950]);
    expect(
      groups.every((group) => group.value === null && group.profit === null),
    ).toBe(true);
    expect(money(24950, 'HKD')).toMatch(/249\.50/);
  });
});
