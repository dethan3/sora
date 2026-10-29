import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, expect, it, vi } from 'vitest';
import { App } from '../../App';
import catalog from '../../../public/data/catalog.json';
import raw from './ai-chips.json';
import {
  aiChips,
  domainSchema,
  equityOverlap,
  fundSnapshot,
  holdingFor,
  topTenWeight,
} from './model';
import { research } from '../research/model';
import { STORAGE_KEY } from '../local/state';
import { listSource, returnPoint } from '../../navigation/flow';

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
  return { router, ...render(<RouterProvider router={router} />) };
}

it('joins one sourced domain to existing products and research without invented quote data', () => {
  expect(domainSchema.safeParse(raw).success).toBe(true);
  expect(
    aiChips.fundSnapshots.every((item) =>
      catalog.funds.some((fund) => fund.code === item.code),
    ),
  ).toBe(true);
  expect(
    aiChips.events.every(
      (item) =>
        !item.researchId ||
        research.some((signal) => signal.id === item.researchId),
    ),
  ).toBe(true);
  expect(
    aiChips.events.find((item) => item.company === 'NVDA')?.period,
  ).toContain('2027 财年');
  expect(fundSnapshot('QQQ')).toBeUndefined();
  expect(topTenWeight(fundSnapshot('SOXX')!)).toBe(62.43);
  expect(topTenWeight(fundSnapshot('AIQ')!)).toBe(32.73);
});

it('distinguishes a partial list from verified absence and blocks incomplete overlap', () => {
  const soxx = fundSnapshot('SOXX')!;
  const aiq = fundSnapshot('AIQ')!;
  expect(holdingFor(soxx, 'NVDA')).toEqual({ status: 'held', weight: 7.85 });
  expect(holdingFor(aiq, 'NVDA')).toEqual({ status: 'held', weight: 2.81 });
  expect(holdingFor(aiq, 'ASML')).toEqual({ status: 'unknown', weight: null });
  expect(holdingFor(soxx, 'MSFT')).toEqual({
    status: 'not-held',
    weight: null,
  });
  expect(equityOverlap(soxx, aiq)).toEqual({
    weight: null,
    reason: '两份持仓日期不同，暂不计算重叠率。',
  });
  expect(equityOverlap(soxx, { ...aiq, asOf: soxx.asOf }).reason).toContain(
    '持仓节选',
  );
  expect(equityOverlap(soxx, soxx).weight).toBeGreaterThan(99);
});

it('rejects duplicated holdings, bad evidence, future records and partial data claimed as complete', () => {
  const duplicate = structuredClone(raw);
  duplicate.fundSnapshots[0].holdings[1].symbol = 'AMD';
  expect(domainSchema.safeParse(duplicate).success).toBe(false);
  const incomplete = structuredClone(raw);
  incomplete.fundSnapshots[1].coverage = 'full-equity';
  incomplete.fundSnapshots[1].reportedEquityCount = 10;
  expect(domainSchema.safeParse(incomplete).success).toBe(false);
  const wrongSource = structuredClone(raw);
  wrongSource.fundSnapshots[0].sourceId = 'nvidia-results';
  expect(domainSchema.safeParse(wrongSource).success).toBe(false);
  const future = structuredClone(raw);
  future.events[0].publishedAt = '2099-01-01';
  expect(domainSchema.safeParse(future).success).toBe(false);
});

it('opens with actual domain information without a question, login or holdings entry', async () => {
  open();
  expect(
    await screen.findByRole('heading', { level: 1, name: 'AI 芯片' }),
  ).toBeInTheDocument();
  expect(
    screen.getByRole('heading', { name: '这个领域，发生了什么' }),
  ).toBeInTheDocument();
  expect(
    screen.getByText('2027 财年第二季度，结束于 2026-07-26', { exact: false }),
  ).toBeInTheDocument();
  expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  expect(
    screen.queryByRole('button', { name: /与我相关/ }),
  ).not.toBeInTheDocument();
  expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
});

it('connects a company selection to dated direct fund weights and preserves the return state', async () => {
  const { router } = open();
  await userEvent.selectOptions(
    await screen.findByRole('combobox', { name: '关联公司' }),
    'NVDA',
  );
  expect(router.state.location.search).toBe('?company=NVDA');
  expect(
    screen.getByRole('heading', { name: 'NVIDIA 的相关变化' }),
  ).toBeInTheDocument();
  const bridge = screen.getByRole('complementary', { name: '公司与基金关联' });
  expect(within(bridge).getByText('7.85%')).toBeInTheDocument();
  expect(within(bridge).getByText('2.81%')).toBeInTheDocument();
  await userEvent.click(
    within(bridge).getByRole('link', { name: '查看 SOXX 持仓结构 →' }),
  );
  expect(router.state.location.pathname).toBe('/funds/SOXX');
  const structure = screen.getByRole('region', { name: '基金持仓结构' });
  expect(within(structure).getByText('62.43%')).toBeInTheDocument();
  await userEvent.click(
    within(structure).getByText('查看 30 项已核对持仓及来源'),
  );
  expect(within(structure).getByRole('table')).toHaveTextContent(
    'ASML ADR · ASML',
  );
  await userEvent.click(screen.getByRole('link', { name: '← 发现市场' }));
  expect(router.state.location.search).toBe('?company=NVDA');
  expect(screen.getByRole('combobox', { name: '关联公司' })).toHaveValue(
    'NVDA',
  );
  expect(
    screen.getByRole('link', { name: '查看 SOXX 持仓结构 →' }),
  ).toHaveFocus();
  expect(returnPoint('/discover?company=NVDA')?.code).toBe('SOXX');
});

it('does not turn a company missing from a top-ten excerpt into zero exposure', async () => {
  open('/discover?company=ASML');
  const bridge = await screen.findByRole('complementary', {
    name: '公司与基金关联',
  });
  expect(within(bridge).getByText('1.47%')).toBeInTheDocument();
  expect(within(bridge).getByText('节选未列出')).toBeInTheDocument();
  expect(
    within(bridge).getByText('未列出不代表未持有，需补充完整持仓核实。'),
  ).toBeInTheDocument();
  expect(within(bridge).queryByText('0.00%')).not.toBeInTheDocument();
  await userEvent.click(
    screen.getByRole('link', { name: '完整证据与传导分析 →' }),
  );
  expect(await screen.findByText('直接持有 ASML')).toBeInTheDocument();
  expect(screen.getByText('1.47%')).toBeInTheDocument();
  await userEvent.click(screen.getByRole('link', { name: '← 发现市场' }));
  expect(screen.getByRole('combobox', { name: '关联公司' })).toHaveValue(
    'ASML',
  );
});

it('takes an event directly to the matching fund bridge instead of leaving the user in the feed', async () => {
  const { router } = open();
  await userEvent.click(
    await screen.findByRole('button', { name: '查看 NVDA 的基金关联 →' }),
  );
  expect(router.state.location.hash).toBe('#company-funds');
  expect(screen.getByRole('combobox', { name: '关联公司' })).toHaveValue(
    'NVDA',
  );
  expect(
    screen.getByRole('complementary', { name: '公司与基金关联' }),
  ).toHaveTextContent('7.85%');
});

it('keeps an independent fund follow through the domain journey without creating a position', async () => {
  open('/discover?company=AMD');
  const bridge = await screen.findByRole('complementary', {
    name: '公司与基金关联',
  });
  await userEvent.click(
    within(bridge).getByRole('button', { name: '关注iShares 半导体 ETF' }),
  );
  await userEvent.click(screen.getByRole('link', { name: /^关注$/ }));
  expect(
    await screen.findByRole('link', { name: 'iShares 半导体 ETF' }),
  ).toBeInTheDocument();
  const state = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
  expect(state.watchlist).toEqual(['SOXX']);
  expect(state.positions).toEqual([]);
});

it('handles unsupported companies explicitly and keeps old market bookmarks available', async () => {
  const { router } = open('/discover?company=UNKNOWN');
  expect(await screen.findByText(/暂未覆盖 UNKNOWN/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole('button', { name: '清除公司筛选' }));
  expect(router.state.location.search).toBe('');
  await userEvent.click(
    screen.getByRole('link', { name: '浏览其他市场资料 →' }),
  );
  expect(
    await screen.findByRole('searchbox', { name: '搜索方向' }),
  ).toBeInTheDocument();
  expect(
    listSource('/discover?company=NVDA&redirect=https://outside.test'),
  ).toBe('/discover?company=NVDA');
});
