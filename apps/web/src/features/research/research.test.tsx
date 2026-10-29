import { render, screen, within, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, expect, it, vi } from 'vitest';
import catalog from '../../../public/data/catalog.json';
import raw from './research.json';
import { App } from '../../App';
import { relatedSignals, research, researchSchema } from './model';
import { RESEARCH_KEY, readResearch } from './saved';
import { DIRECTION_KEY } from '../discover/DirectionProvider';
import { STORAGE_KEY } from '../local/state';
import { listSource } from '../../navigation/flow';

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  vi.spyOn(globalThis, 'fetch').mockImplementation(
    async () => new Response(JSON.stringify(catalog)),
  );
});
function open(path = '/research/semiconductor-equipment') {
  const router = createMemoryRouter([{ path: '*', element: <App /> }], {
    initialEntries: [path],
  });
  return { router, ...render(<RouterProvider router={router} />) };
}
const title = research[0].title;

it('requires dated traceable evidence and does not invent holdings or direct ownership', () => {
  expect(researchSchema.safeParse(raw).success).toBe(true);
  for (const signal of research) {
    expect(
      signal.directionIds.every((id) =>
        catalog.modules.some((item) => item.id === id),
      ),
    ).toBe(true);
    expect(
      signal.funds.every((fund) =>
        catalog.funds.some((item) => item.code === fund.code),
      ),
    ).toBe(true);
    expect(
      signal.funds.every(
        (fund) => fund.weight === null && fund.holdingsAsOf === null,
      ),
    ).toBe(true);
  }
  const invalid = structuredClone(raw);
  invalid.signals[0].facts[0].sourceId = 'missing';
  expect(researchSchema.safeParse(invalid).success).toBe(false);
  const undated = structuredClone(raw);
  Object.assign(undated.signals[0].funds[0], { weight: 10 });
  expect(researchSchema.safeParse(undated).success).toBe(false);
  const future = structuredClone(raw);
  future.signals[0].eventDate = '2099-01-01';
  expect(researchSchema.safeParse(future).success).toBe(false);
  expect(relatedSignals(['gold'], ['IAU'])).toHaveLength(0);
  expect(relatedSignals(['ai'], ['SOXX'])).toHaveLength(1);
});

it('connects followed interests to research without requiring positions', async () => {
  open('/discover?view=markets');
  await userEvent.click(
    await screen.findByRole('button', { name: /与我相关/ }),
  );
  expect(screen.getByText('还没有与你关注内容相关的线索')).toBeInTheDocument();
  await userEvent.click(
    screen.getByRole('button', { name: '关注方向：半导体' }),
  );
  expect(screen.getByText('关联你关注的方向或自选基金')).toBeInTheDocument();
  await userEvent.click(screen.getByRole('link', { name: '展开研究 →' }));
  expect(
    await screen.findByRole('heading', { level: 1, name: title }),
  ).toBeInTheDocument();
  expect(screen.getByText('研究假设 · 尚待验证')).toBeInTheDocument();
  expect(screen.getByText('哪些情况会削弱这条线索？')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /^发现$/ })).toHaveClass('active');
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
});

it('preserves independent research follows on revisit and allows removal in watchlist', async () => {
  const first = open();
  await userEvent.click(
    (await screen.findAllByRole('button', { name: '关注线索：' + title }))[0],
  );
  expect(readResearch().ids).toEqual(['semiconductor-equipment']);
  expect(localStorage.getItem(DIRECTION_KEY)).toBeNull();
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  first.unmount();
  open('/watchlist');
  const saved = await screen.findByRole('region', { name: '关注的研究线索' });
  expect(
    within(saved).getByRole('link', { name: '展开研究 →' }),
  ).toBeInTheDocument();
  await userEvent.click(
    within(saved).getByRole('button', { name: '取消关注线索：' + title }),
  );
  expect(readResearch().ids).toEqual([]);
});

it('returns from a mapped fund to the original research and keeps fund follows separate', async () => {
  const { router } = open();
  const links = await screen.findAllByRole('link', { name: '研究这只基金 →' });
  await userEvent.click(links[0]);
  expect(router.state.location.pathname).toBe('/funds/SOXX');
  expect(
    screen.getByRole('region', { name: '关联研究线索' }),
  ).toHaveTextContent('已核对发行人持仓');
  await userEvent.click(
    screen.getByRole('button', {
      name: '关注' + catalog.funds.find((fund) => fund.code === 'SOXX')!.name,
    }),
  );
  expect(JSON.parse(localStorage.getItem(STORAGE_KEY)!).watchlist).toContain(
    'SOXX',
  );
  expect(readResearch().ids).toEqual([]);
  await userEvent.click(screen.getByRole('link', { name: '← 研究线索' }));
  expect(router.state.location.pathname).toBe(
    '/research/semiconductor-equipment',
  );
  expect(listSource('//evil.test/research/test')).toBeNull();
  expect(
    listSource('/research/semiconductor-equipment?redirect=https://evil.test'),
  ).toBe('/research/semiconductor-equipment');
});

it('retains corrupt saved records and reports failed writes without a false saved state', async () => {
  localStorage.setItem(RESEARCH_KEY, '{broken');
  const first = open();
  await userEvent.click(
    (await screen.findAllByRole('button', { name: '关注线索：' + title }))[0],
  );
  expect(screen.getAllByRole('alert')[0]).toHaveTextContent('原记录未改动');
  expect(localStorage.getItem(RESEARCH_KEY)).toBe('{broken');
  first.unmount();
  localStorage.clear();
  open();
  const write = vi
    .spyOn(Storage.prototype, 'setItem')
    .mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
  await userEvent.click(
    (await screen.findAllByRole('button', { name: '关注线索：' + title }))[0],
  );
  expect(screen.getAllByRole('alert')[0]).toHaveTextContent('未能保存研究关注');
  expect(
    screen.getAllByRole('button', { name: '关注线索：' + title })[0],
  ).toHaveAttribute('aria-pressed', 'false');
  write.mockRestore();
});

it('syncs other tabs and preserves unavailable signal ids for explicit removal', async () => {
  open('/watchlist');
  await screen.findByRole('region', { name: '关注的研究线索' });
  act(() => {
    localStorage.setItem(
      RESEARCH_KEY,
      JSON.stringify({ schemaVersion: 1, ids: ['removed-signal'] }),
    );
    window.dispatchEvent(new StorageEvent('storage', { key: RESEARCH_KEY }));
  });
  expect(screen.getByText(/线索暂不可用/)).toBeInTheDocument();
  await userEvent.click(
    screen.getByRole('button', { name: '移除线索 removed-signal' }),
  );
  expect(readResearch().ids).toEqual([]);
});

it('handles missing research links without creating data', async () => {
  open('/research/missing');
  expect(await screen.findByText('没有找到这条研究线索')).toBeInTheDocument();
  expect(localStorage.getItem(RESEARCH_KEY)).toBeNull();
});

it('keeps discovery filters across research, fund details and the return journey', async () => {
  const { router } = open('/discover?q=芯片&kind=theme');
  await userEvent.click(
    await screen.findByRole('link', { name: '展开研究 →' }),
  );
  await userEvent.click(
    screen.getAllByRole('link', { name: '研究这只基金 →' })[0],
  );
  await userEvent.click(screen.getByRole('link', { name: '← 研究线索' }));
  await userEvent.click(screen.getByRole('link', { name: '← 发现市场' }));
  expect(router.state.location.pathname).toBe('/discover');
  expect(new URLSearchParams(router.state.location.search).get('q')).toBe(
    '芯片',
  );
  expect(screen.getByRole('searchbox', { name: '搜索方向' })).toHaveValue(
    '芯片',
  );
});

it('returns to the original fund when research is opened from a fund detail', async () => {
  const { router } = open('/funds/SOXX');
  await userEvent.click(
    await screen.findByRole('link', { name: '展开研究 →' }),
  );
  await userEvent.click(screen.getByRole('link', { name: '← 基金详情' }));
  expect(router.state.location.pathname).toBe('/funds/SOXX');
});

it('opens a research chapter without resetting its position to the page start', async () => {
  const scroll = vi.spyOn(HTMLElement.prototype, 'scrollIntoView');
  open('/research/semiconductor-equipment#fund-mapping');
  await screen.findByRole('heading', {
    name: '无法直接买入公司时，如何继续研究基金？',
  });
  expect(
    scroll.mock.instances.some(
      (element) => (element as HTMLElement).id === 'fund-mapping',
    ),
  ).toBe(true);
});
