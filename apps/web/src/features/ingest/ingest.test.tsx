import {
  render,
  screen,
  waitFor,
  within,
  fireEvent,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, RouterProvider } from 'react-router';
import { beforeEach, afterEach, it, expect, vi } from 'vitest';
import { AuthProvider } from '../auth/AuthProvider';
import { DirectorySearch } from './DirectorySearch';
import { InstrumentPage } from './InstrumentPage';
import { IngestAdmin } from './IngestAdmin';
import { MyRequests } from './MyRequests';
import type { AuthUser } from '../auth/api';
import type { PublicInstrument } from '@sora/market-data/ingest';
const user: AuthUser = {
  id: 'person',
  email: 'person@example.com',
  displayName: '用户',
  role: 'user',
  baseCurrency: 'CNY',
};
const item = {
  id: 'CN:etf:510300',
  symbol: '510300',
  name: '沪深300ETF',
  market: 'CN',
  kind: 'etf',
  currency: 'CNY',
  instrumentId: null,
  code: null,
  status: null,
};
const detail: PublicInstrument = {
  id: item.id,
  code: item.id,
  name: item.name,
  symbol: item.symbol,
  market: 'CN',
  kind: 'etf',
  currency: 'CNY',
  status: 'ready',
  revision: 0,
  impactTags: [],
  chainNode: null,
  lastError: null,
  warnings: [],
  companies: [],
  series: {
    type: 'price',
    currency: 'CNY',
    timeZone: 'Asia/Shanghai',
    adjust: 'unadjusted',
    source: {
      name: '日线来源',
      url: 'https://example.com/prices',
      fetchedAt: '2026-11-04T00:00:00.000Z',
    },
    history: [
      { date: '2026-11-02', close: '3.1' },
      { date: '2026-11-03', close: '3.2' },
    ],
  },
  holdings: {
    asOf: '2026-09-30',
    coverage: 'top10',
    source: {
      name: '持仓来源',
      url: 'https://example.com/holdings',
      fetchedAt: '2026-11-04T00:00:00.000Z',
    },
    holdings: [
      {
        id: 'CN:stock:600519',
        market: 'CN',
        symbol: '600519',
        name: '贵州茅台',
        weightBp: 812,
      },
    ],
  },
};
let identity: AuthUser | null;
let writes: { path: string; body: Record<string, unknown> }[];
let myStatus: string;
let jobStatus: string;
let rejectReason: string;
function open(element: React.ReactNode, path = '/funds') {
  const router = createMemoryRouter(
    [
      { path: '*', element: <AuthProvider enabled>{element}</AuthProvider> },
      { path: '/instruments/:id', element: <InstrumentPage /> },
    ],
    { initialEntries: [path] },
  );
  return render(<RouterProvider router={router} />);
}
beforeEach(() => {
  identity = user;
  writes = [];
  myStatus = 'pending';
  jobStatus = 'failed';
  rejectReason = '';
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = new URL(String(input), 'http://localhost'),
      path = url.pathname;
    if (path === '/api/auth/me') return Response.json({ user: identity });
    if (init?.method === 'POST' || init?.method === 'PATCH') {
      const body = JSON.parse(String(init.body));
      writes.push({ path, body });
      if (path.endsWith('/reject')) {
        rejectReason = body.note;
        return Response.json({ reviewed: true });
      }
      if (path.endsWith('/retry')) jobStatus = 'queued';
      return Response.json({
        dispatch: { status: 'unconfigured' },
        status: 'pending',
        id: 'request',
      });
    }
    if (path === '/api/search')
      return Response.json({
        items: [item],
        directoryUpdatedAt: '2026-11-04T00:00:00.000Z',
      });
    if (path === '/api/me/requests')
      return Response.json({
        items: [
          {
            ...item,
            id: 'request',
            directoryId: item.id,
            status: myStatus,
            reason: '研究',
            reviewNote: myStatus === 'rejected' ? '资料不足' : null,
            createdAt: '2026-11-04T00:00:00.000Z',
            instrumentStatus: null,
          },
        ],
      });
    if (path.startsWith('/api/instruments/')) return Response.json(detail);
    if (path === '/api/admin/instruments')
      return Response.json({
        items: [
          {
            ...item,
            instrumentId: item.id,
            code: item.id,
            status: 'ready',
            impactTags: [],
            chainNode: null,
            revision: 0,
          },
        ],
      });
    if (path === '/api/admin/requests')
      return Response.json({
        items: rejectReason
          ? []
          : [
              {
                ...item,
                requesterCount: 3,
                createdAt: '2026-11-04T00:00:00.000Z',
                reason: '研究',
              },
            ],
      });
    if (path === '/api/admin/jobs')
      return Response.json({
        items: [
          {
            id: 'job',
            instrumentId: item.id,
            name: item.name,
            symbol: item.symbol,
            status: jobStatus,
            mode: 'full',
            attempt: 1,
            steps: [{ step: 'series', status: 'failed' }],
            error: jobStatus === 'failed' ? '上游超时' : null,
            createdAt: '2026-11-04T00:00:00.000Z',
            finishedAt: null,
            leaseUntil: null,
          },
        ],
        dispatch: null,
      });
    throw new Error(`Unexpected request ${path}`);
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});
it('ordinary users request collection and can see rejection reason in their account', async () => {
  const first = open(<DirectorySearch />);
  await userEvent.type(screen.getByRole('searchbox'), '510300');
  await userEvent.click(
    await screen.findByRole('button', { name: '申请收录' }),
  );
  await userEvent.type(screen.getByLabelText('申请理由（选填）'), '观察指数');
  await userEvent.click(screen.getByRole('button', { name: '提交申请' }));
  await screen.findByText(/申请已提交/);
  expect(writes[0]).toEqual({
    path: '/api/me/requests',
    body: { directoryId: item.id, reason: '观察指数' },
  });
  first.unmount();
  myStatus = 'rejected';
  open(<MyRequests />);
  await screen.findByText('驳回理由：资料不足');
  expect(screen.getByText('已驳回')).toBeInTheDocument();
});
it('anonymous directory search offers login; administrators add jobs directly', async () => {
  identity = null;
  const first = open(<DirectorySearch />);
  await userEvent.type(screen.getByRole('searchbox'), '510300');
  expect(
    await screen.findByRole('link', { name: '登录后申请' }),
  ).toHaveAttribute('href', '/login?next=%2Ffunds');
  first.unmount();
  identity = { ...user, role: 'admin' };
  open(<DirectorySearch />);
  await userEvent.type(screen.getByRole('searchbox'), '510300');
  await userEvent.click(
    await screen.findByRole('button', { name: '添加并采集' }),
  );
  expect(await screen.findByText(/已加入采集队列/)).toBeInTheDocument();
  expect(writes[0].path).toBe('/api/admin/instruments');
});
it('a stale search response cannot overwrite a newer query', async () => {
  let resolveOld: (value: Response) => void = () => {};
  const original = vi.mocked(fetch).getMockImplementation()!;
  vi.mocked(fetch).mockImplementation((input, init) =>
    String(input).includes('q=old')
      ? new Promise((resolve) => {
          resolveOld = resolve;
        })
      : original(input, init),
  );
  open(<DirectorySearch />);
  fireEvent.change(screen.getByRole('searchbox'), { target: { value: 'old' } });
  await waitFor(() =>
    expect(fetch).toHaveBeenCalledWith('/api/search?q=old', expect.anything()),
  );
  fireEvent.change(screen.getByRole('searchbox'), { target: { value: 'new' } });
  await screen.findByText(item.name);
  resolveOld(Response.json({ items: [{ ...item, name: '过期搜索结果' }] }));
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(screen.queryByText('过期搜索结果')).not.toBeInTheDocument();
});
it('published detail shows actual dates, source, coverage and unknown company research', async () => {
  const router = createMemoryRouter(
    [{ path: '/instruments/:id', element: <InstrumentPage /> }],
    { initialEntries: [`/instruments/${encodeURIComponent(item.id)}`] },
  );
  render(<RouterProvider router={router} />);
  await screen.findByRole('heading', { name: item.name });
  expect(screen.getByText(/报告期 2026-09-30/)).toHaveTextContent('前十大重仓');
  expect(screen.getByText('贵州茅台')).toBeInTheDocument();
  expect(screen.getByText('8.12%')).toBeInTheDocument();
  expect(screen.getByText('待标注')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: '持仓来源' })).toHaveAttribute(
    'href',
    'https://example.com/holdings',
  );
  expect(
    screen.getByRole('heading', { name: '交易价格日线' }),
  ).toBeInTheDocument();
  expect(screen.getByRole('img', { name: /收盘走势/ })).toBeInTheDocument();
});
it('admin retries failed jobs, rejects grouped requests and saves versioned research edits', async () => {
  identity = { ...user, role: 'admin' };
  open(<IngestAdmin />);
  await screen.findByRole('button', { name: '编辑标的' });
  await userEvent.click(screen.getByRole('button', { name: '任务队列' }));
  await userEvent.click(
    await screen.findByRole('button', { name: '重试任务' }),
  );
  expect(writes[0].path).toBe('/api/admin/jobs/job/retry');
  await screen.findByText(/执行器尚未配置/);
  await userEvent.click(screen.getByRole('button', { name: '收录申请' }));
  await screen.findByText(/3 人申请/);
  await userEvent.click(screen.getByRole('button', { name: '驳回' }));
  const dialog = screen.getByRole('dialog');
  await userEvent.type(
    within(dialog).getByLabelText('驳回理由'),
    '当前数据不完整',
  );
  await userEvent.click(
    within(dialog).getByRole('button', { name: '确认驳回' }),
  );
  await screen.findByText('暂无待审申请。');
  expect(rejectReason).toBe('当前数据不完整');
  await userEvent.click(screen.getByRole('button', { name: '标的管理' }));
  await userEvent.click(
    await screen.findByRole('button', { name: '编辑标的' }),
  );
  await userEvent.click(screen.getByRole('button', { name: '添加标签' }));
  await userEvent.type(screen.getByLabelText('标签 1'), '需求增长');
  await userEvent.click(screen.getByRole('button', { name: '保存标的' }));
  await screen.findByText('标的资料已更新。');
  expect(writes.at(-1)?.body).toMatchObject({
    revision: 0,
    status: 'ready',
    chainNode: null,
    impactTags: [{ label: '需求增长', tone: 'watch' }],
  });
});
