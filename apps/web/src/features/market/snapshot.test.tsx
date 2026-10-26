import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, expect, it, vi } from 'vitest';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { marketSnapshotSchema, observationTime } from './model';
import { mockMarket } from './mock';
import {
  MARKET_CACHE_KEY,
  loadMarketSnapshot,
  readMarketCache,
  saveMarketCache,
} from './snapshot';
import { MarketNotice, MarketProvider } from './MarketProvider';
import { MarketPanel } from './MarketPanel';

// Test-only observations; never exported to the collection cache or release artifacts.
const realShape = marketSnapshotSchema.parse({
  ...mockMarket,
  source: 'market-snapshot',
  series: [
    {
      ...mockMarket.series[3],
      quote: {
        ...mockMarket.series[3].quote,
        asOf: '2026-10-23',
        kind: 'daily-close',
      },
      provenance: {
        provider: 'aktools',
        upstream: '新浪财经',
        symbol: '.NDX',
        sourceUrl: 'https://stock.finance.sina.com.cn/usstock/quotes/.NDX.html',
        adjustment: 'unadjusted',
        retrievedAt: '2026-10-24T00:00:00Z',
        attemptedAt: '2026-10-24T00:00:00Z',
        updateStatus: 'ok',
      },
    },
  ],
});
beforeEach(() => {
  localStorage.clear();
});
it('validates fetched snapshots and rejects a mock payload even if the endpoint returns 200', async () => {
  const fetcher = vi
    .spyOn(globalThis, 'fetch')
    .mockResolvedValue(new Response(JSON.stringify(realShape)));
  expect((await loadMarketSnapshot()).source).toBe('market-snapshot');
  expect(fetcher).toHaveBeenCalledWith(
    expect.stringContaining('market.json'),
    expect.objectContaining({ cache: 'no-cache' }),
  );
  fetcher.mockResolvedValue(new Response(JSON.stringify(mockMarket)));
  await expect(loadMarketSnapshot()).rejects.toThrow('mock');
});
it('rejects HTTP failures and invalid payloads without replacing saved records', async () => {
  saveMarketCache(realShape);
  const saved = localStorage.getItem(MARKET_CACHE_KEY);
  const fetcher = vi
    .spyOn(globalThis, 'fetch')
    .mockResolvedValue(new Response('', { status: 503 }));
  await expect(loadMarketSnapshot()).rejects.toThrow();
  fetcher.mockResolvedValue(new Response('{}'));
  await expect(loadMarketSnapshot()).rejects.toThrow();
  expect(localStorage.getItem(MARKET_CACHE_KEY)).toBe(saved);
});
it('stores only collected data and preserves corrupt cache text for recovery', () => {
  saveMarketCache(mockMarket);
  expect(readMarketCache()).toBeNull();
  saveMarketCache(realShape);
  expect(readMarketCache()).toEqual(realShape);
  localStorage.setItem(MARKET_CACHE_KEY, 'broken');
  expect(readMarketCache()).toBeNull();
  expect(localStorage.getItem(MARKET_CACHE_KEY)).toBe('broken');
});
it('does not let a storage quota error block the market data view', () => {
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
    throw new Error('quota');
  });
  expect(() => saveMarketCache(realShape)).not.toThrow();
});
it('shows date-only close provenance, real chart labels and no mock attribution', async () => {
  const load = async () => realShape;
  const router = createMemoryRouter(
    [
      {
        path: '*',
        element: (
          <MarketProvider enabled loader={load}>
            <MarketNotice />
            <MarketPanel
              id="nasdaq100"
              benchmark="Nasdaq-100 Index"
              note="参考指标"
            />
          </MarketProvider>
        ),
      },
    ],
    { initialEntries: ['/discover/nasdaq100'] },
  );
  render(<RouterProvider router={router} />);
  await screen.findByRole('slider');
  expect(screen.getByText('收盘行情')).toBeInTheDocument();
  expect(screen.queryByText('演示行情')).not.toBeInTheDocument();
  expect(screen.getByRole('link', { name: /新浪财经/ })).toHaveAttribute(
    'href',
    realShape.series[0].provenance!.sourceUrl,
  );
  expect(screen.getByRole('img')).toHaveAccessibleName(/^Nasdaq-100 Index走势/);
  expect(observationTime(realShape.series[0])).toBe(
    '2026-10-23 收盘 · 纽约交易日',
  );
  expect(readMarketCache()?.source).toBe('market-snapshot');
  await userEvent.click(screen.getByRole('button', { name: '3月' }));
  expect(router.state.location.search).toBe('?period=3M');
});
it('makes fetch errors visible with retry and leaves the existing quote unchanged', async () => {
  const load = vi
    .fn()
    .mockResolvedValueOnce(realShape)
    .mockRejectedValueOnce(new Error('offline'))
    .mockResolvedValue(realShape);
  render(
    <MarketProvider enabled loader={load}>
      <MarketNotice />
    </MarketProvider>,
  );
  await screen.findByText(/1 个方向有可用记录/);
  await userEvent.click(screen.getByRole('button', { name: '刷新行情' }));
  expect(await screen.findByRole('alert')).toHaveTextContent(
    '行情暂时无法载入',
  );
  expect(readMarketCache()).toEqual(realShape);
  await userEvent.click(screen.getByRole('button', { name: '重试行情' }));
  await act(async () => {});
  expect(screen.queryByRole('alert')).not.toBeInTheDocument();
});
