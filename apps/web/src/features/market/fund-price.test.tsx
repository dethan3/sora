import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  createMemoryRouter,
  RouterProvider,
  Routes,
  Route,
} from 'react-router';
import { beforeEach, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import { demoSchema } from '../../data/demo';
import { MarketProvider, type MarketLoader } from './MarketProvider';
import { mockMarket } from './mock';
import { marketSnapshotSchema, type MarketSnapshot } from './model';
import { FundDetailPage } from '../funds/FundDetailPage';
import { FundsPage } from '../funds/FundsPage';
import { WatchlistPage } from '../watchlist/WatchlistPage';
import { PortfolioPage } from '../portfolio/PortfolioPage';
import { PersonalProvider } from '../local/PersonalProvider';
import { STORAGE_KEY } from '../local/state';
import { summarizePositions } from '../portfolio/model';
import { detailSource } from '../../navigation/flow';

const data = demoSchema.parse(catalog);
// Synthetic test-only values in a collected-data shape; never exported or saved as supplier records.
const fixture = marketSnapshotSchema.parse({
  ...mockMarket,
  source: 'market-snapshot',
  series: mockMarket.series
    .filter((item) => item.instrumentType === 'fund-price')
    .map((item) => ({
      ...item,
      provenance: {
        provider: 'aktools',
        upstream: '新浪财经',
        symbol: item.fundCode === '2800.HK' ? '02800' : item.fundCode,
        sourceUrl: 'https://example.test/quotes/' + item.fundCode,
        adjustment: 'unadjusted',
        retrievedAt: mockMarket.generatedAt,
        attemptedAt: mockMarket.generatedAt,
        updateStatus: 'ok',
      },
    })),
});
beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});
function open(path: string, loader: MarketLoader = async () => fixture) {
  const router = createMemoryRouter(
    [
      {
        path: '*',
        element: (
          <PersonalProvider>
            <MarketProvider enabled loader={loader}>
              <Routes>
                <Route path="/funds" element={<FundsPage data={data} />} />
                <Route
                  path="/funds/:code"
                  element={<FundDetailPage data={data} />}
                />
                <Route
                  path="/watchlist"
                  element={<WatchlistPage data={data} />}
                />
                <Route
                  path="/portfolio"
                  element={<PortfolioPage data={data} />}
                />
              </Routes>
            </MarketProvider>
          </PersonalProvider>
        ),
      },
    ],
    { initialEntries: [path] },
  );
  render(<RouterProvider router={router} />);
  return router;
}
it('shows HKD product closes with source and price-only chart semantics, leaving NAV and valuations untouched', async () => {
  open('/funds/2800.HK');
  await screen.findByRole('slider');
  expect(
    screen.getByRole('heading', { name: '交易收盘价' }),
  ).toBeInTheDocument();
  expect(screen.getAllByText('HKD / 份').length).toBeGreaterThan(0);
  expect(screen.getByText('2026-10-23 收盘 · 香港交易日')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /新浪财经/ })).toHaveAttribute(
    'href',
    'https://example.test/quotes/2800.HK',
  );
  expect(screen.getByText(/拆分、合并及除息/)).toBeInTheDocument();
  expect(screen.getByText('暂无净值数据')).toBeInTheDocument();
  expect(
    data.funds.every((fund) => fund.nav === null && fund.history.length === 0),
  ).toBe(true);
  const groups = summarizePositions(
    [{ code: '2800.HK', currency: 'HKD', units: '10', costMinor: 24000 }],
    data.funds,
  );
  expect(groups[0].value).toBeNull();
});
it('adds a product to persistent watchlist and shows the same dated price on the watchlist', async () => {
  const router = open('/funds/AIQ');
  await screen.findByRole('slider');
  await userEvent.click(
    screen.getByRole('button', { name: '关注Global X 人工智能与科技 ETF' }),
  );
  expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!).watchlist).toEqual([
    'AIQ',
  ]);
  await act(async () => {
    await router.navigate('/watchlist');
  });
  const table = screen.getByRole('table', { name: '基金列表' });
  expect(within(table).getByText(/42.60/)).toBeInTheDocument();
  expect(
    within(table).getByText('2026-10-23 收盘 · 纽约交易日'),
  ).toBeInTheDocument();
  await userEvent.click(
    within(table).getByRole('link', { name: 'Global X 人工智能与科技 ETF' }),
  );
  expect(router.state.location.search).toContain('from=%2Fwatchlist');
  await userEvent.click(
    screen.getByRole('button', { name: '取消关注Global X 人工智能与科技 ETF' }),
  );
  await userEvent.click(screen.getByRole('link', { name: '← 我的关注' }));
  expect(screen.getByText('还没有关注的基金')).toBeInTheDocument();
});
it('preserves product period and list source when returning from the holdings editor', async () => {
  const router = open('/funds/QQQ?from=%2Ffunds%3Fq%3DQQQ');
  await screen.findByRole('slider');
  await userEvent.click(screen.getByRole('button', { name: '3月' }));
  await userEvent.click(screen.getByRole('link', { name: /录入持仓/ }));
  await userEvent.click(screen.getByRole('link', { name: /返回基金详情/ }));
  expect(router.state.location.search).toContain('period=3M');
  expect(screen.getByRole('button', { name: '3月' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  await userEvent.click(screen.getByRole('link', { name: '← 基金目录' }));
  expect(screen.getByRole('searchbox', { name: '搜索基金' })).toHaveValue(
    'QQQ',
  );
  expect(detailSource('/funds/QQQ?period=invalid&from=https://evil.test')).toBe(
    '/funds/QQQ',
  );
});
it('does not display another currency or an index as a product quote', async () => {
  const wrong: MarketSnapshot = structuredClone(fixture);
  const hk = wrong.series.find((item) => item.fundCode === '2800.HK')!;
  hk.unit = 'usd-per-share';
  hk.timeZone = 'America/New_York';
  open('/funds/2800.HK', async () => wrong);
  expect(
    await screen.findByText('暂无可用的交易价格记录。'),
  ).toBeInTheDocument();
  expect(screen.queryByRole('slider')).not.toBeInTheDocument();
});
it('keeps the original product quote after a failed refresh and recovers on retry', async () => {
  const load = vi
    .fn()
    .mockResolvedValueOnce(fixture)
    .mockRejectedValueOnce(new Error('offline'))
    .mockResolvedValue(fixture);
  open('/funds/AIQ', load);
  await screen.findByRole('slider');
  await userEvent.click(screen.getByRole('button', { name: '刷新行情' }));
  expect(await screen.findByRole('alert')).toHaveTextContent(
    '行情暂时无法载入',
  );
  expect(
    screen.getByText('42.60', { selector: '.market-quote-value strong' }),
  ).toBeInTheDocument();
  expect(
    screen.getByText('本次更新失败，正在显示上次记录。'),
  ).toBeInTheDocument();
  await userEvent.click(screen.getByRole('button', { name: '重试行情' }));
  await screen.findByRole('button', { name: '刷新行情' });
  expect(screen.queryByRole('alert')).not.toBeInTheDocument();
});
it('retains the fund entry and retry affordance when the initial price request fails', async () => {
  open('/funds/SOXX', async () => {
    throw new Error('offline');
  });
  await screen.findByRole('alert');
  expect(
    screen.getByRole('heading', { name: 'iShares 半导体 ETF', level: 1 }),
  ).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '重试行情' })).toBeInTheDocument();
  expect(screen.getByText('行情暂不可用')).toBeInTheDocument();
  expect(screen.queryByRole('slider')).not.toBeInTheDocument();
});
