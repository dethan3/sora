import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  normalizeHoldings,
  normalizeSeries,
  collectSeries,
  collectHoldings,
  collectDirectory,
  type Aktools,
} from './collect.ts';
import {
  runJobs,
  workerClient,
  syncDirectory,
  type Machine,
} from './ingest.ts';
import type { ClaimedJob, DirectoryEntry } from '@sora/market-data/ingest';
const now = '2026-11-04T02:00:00.000Z';
const instrument: DirectoryEntry = {
  id: 'CN:etf:510300',
  market: 'CN',
  kind: 'etf',
  symbol: '510300',
  name: '沪深300ETF',
  currency: 'CNY',
  pinyin: '',
  sourceName: '东方财富',
  sourceUrl: 'https://quote.eastmoney.com',
};
const job: ClaimedJob = {
  id: 'job1',
  lease: 'lease',
  leaseUntil: '2026-11-04T02:10:00.000Z',
  mode: 'full',
  attempt: 1,
  instrument,
  instrumentId: instrument.id,
  sinceDate: null,
};
const daily = [
  { 日期: '2026-11-02', 收盘: 3.1 },
  { 日期: '2026-11-03', 收盘: 3.2 },
  { 日期: '2026-11-04', 收盘: 3.3 },
];
const holdings = [
  {
    股票代码: '600519',
    股票名称: '贵州茅台',
    '占净值比例(%)': 8.12,
    季度: '2026年3季度股票投资明细',
  },
  {
    股票代码: '00700',
    股票名称: '腾讯控股',
    '占净值比例 (%)': 7.1,
    季度: '2026年3季度股票投资明细',
  },
  {
    股票代码: '000001',
    股票名称: '旧报告',
    '占净值比例(%)': 15,
    季度: '2026年2季度股票投资明细',
  },
];
const verifiedStocks: DirectoryEntry[] = [
  {
    ...instrument,
    id: 'CN:stock:600519',
    kind: 'stock',
    symbol: '600519',
    name: '贵州茅台',
  },
  {
    ...instrument,
    id: 'HK:stock:700.HK',
    kind: 'stock',
    market: 'HK',
    currency: 'HKD',
    symbol: '700.HK',
    name: '腾讯控股',
  },
];
test('holdings use latest disclosed quarter, integer weights and market-qualified stock ids', () => {
  const parsed = normalizeHoldings(holdings, '510300', now, verifiedStocks);
  assert.equal(parsed.asOf, '2026-09-30');
  assert.equal(parsed.coverage, 'top10');
  assert.equal(parsed.holdings.length, 2);
  assert.equal(parsed.holdings[0].weightBp, 812);
  assert.equal(parsed.holdings[1].id, 'HK:stock:700.HK');
  assert.throws(() =>
    normalizeHoldings(
      [{ ...holdings[0], 季度: '2026年4季度股票投资明细' }],
      '510300',
      now,
    ),
  );
  assert.throws(() =>
    normalizeHoldings([{ ...holdings[0], 股票代码: '123' }], '510300', now),
  );
});
test('series exclude unfinished exchange day, validate duplicates, distinguish NAV and close', () => {
  const series = normalizeSeries(daily, instrument, now);
  assert.equal(series.type, 'price');
  assert.equal(series.history.length, 2);
  const nav = normalizeSeries(
    daily.map((row) => ({ 净值日期: row.日期, 单位净值: row.收盘 })),
    { ...instrument, id: 'CN:fund:510300', kind: 'fund' },
    now,
  );
  assert.equal(nav.type, 'nav');
  assert.equal(nav.history[1].close, '3.2');
  assert.throws(() =>
    normalizeSeries(
      [...daily, { 日期: '2026-11-03', 收盘: 4 }],
      instrument,
      now,
    ),
  );
  assert.throws(() =>
    normalizeSeries(
      [{ 日期: '2026-11-02', 收盘: 0 }, daily[1]],
      instrument,
      now,
    ),
  );
});
test('incremental CN ETF adapter uses fixed API, seven-day overlap and unadjusted prices', async () => {
  await collectSeries(
    { ...job, mode: 'refresh', sinceDate: '2026-11-03' },
    async (api, params) => {
      assert.equal(api, 'fund_etf_hist_em');
      assert.deepEqual(params, {
        symbol: '510300',
        period: 'daily',
        adjust: '',
        start_date: '20261027',
        end_date: '20261104',
      });
      return daily;
    },
    now,
  );
  const hk = {
    ...instrument,
    id: 'HK:etf:2800.HK',
    market: 'HK',
    symbol: '2800.HK',
    currency: 'HKD',
  } as const;
  await collectSeries(
    { ...job, instrument: hk },
    async (api, params) => {
      assert.equal(api, 'stock_hk_daily');
      assert.equal(params?.symbol, '02800');
      return daily;
    },
    now,
  );
});
test('directory adapters preserve code zeros, canonical identity, pinyin and HK Chinese name', async () => {
  const funds = await collectDirectory('CN:fund', async () => [
    {
      基金代码: '000001',
      基金简称: '华夏成长',
      拼音缩写: 'HXCZ',
      拼音全称: 'HUAXIACHENGZHANG',
    },
  ]);
  assert.equal(funds[0].id, 'CN:fund:000001');
  assert.match(funds[0].pinyin, /HXCZ/);
  const hk = await collectDirectory('HK:stock', async () => [
    { 代码: '00700', 中文名称: '腾讯控股' },
  ]);
  assert.equal(hk[0].name, '腾讯控股');
  assert.equal(hk[0].symbol, '700.HK');
  await assert.rejects(() =>
    collectDirectory('CN:fund', async () => [
      { 基金代码: '000001', 基金简称: '测试' },
      { 基金代码: '000001', 基金简称: '重复' },
    ]),
  );
});
test('executor publishes full job once, retries uncertain completion and continues after upstream failure', async () => {
  const calls: { path: string; body: unknown }[] = [];
  let claims = 0,
    complete = 0;
  const machine: Machine = async (path, body) => {
    calls.push({ path, body });
    if (path.endsWith('/claim'))
      return {
        job:
          claims++ < 2
            ? {
                ...job,
                id: `job${claims}`,
                instrument:
                  claims === 2
                    ? { ...instrument, symbol: '510500', id: 'CN:etf:510500' }
                    : instrument,
              }
            : null,
      };
    if (path.endsWith('/complete') && complete++ === 0)
      throw new Error('lost response');
    return {};
  };
  const ak: Aktools = async (api, params) => {
    if (params?.symbol === '510500') throw new Error('network failure');
    if (api === 'stock_info_a_code_name')
      return [{ 代码: '600519', 名称: '贵州茅台' }];
    if (api === 'stock_hk_spot')
      return [{ 代码: '00700', 中文名称: '腾讯控股' }];
    return api === 'fund_portfolio_hold_em' ? holdings : daily;
  };
  const output = await runJobs(machine, ak, { now: () => now, log: () => {} });
  assert.deepEqual(output, { completed: 1, failed: 1 });
  assert.equal(calls.filter((row) => row.path.endsWith('/complete')).length, 2);
  assert.ok(calls.some((row) => row.path === '/api/internal/jobs/job2/fail'));
  assert.equal(calls.at(-1)?.path, '/api/internal/runner/finish');
});
test('refresh skips holdings and directory failure never publishes incomplete scopes', async () => {
  let claims = 0;
  const apis: string[] = [];
  const calls: string[] = [];
  const machine: Machine = async (path) => {
    calls.push(path);
    return path.endsWith('/claim')
      ? { job: claims++ ? null : { ...job, mode: 'refresh' } }
      : path.endsWith('/start')
        ? { id: 'import1' }
        : {};
  };
  await runJobs(
    machine,
    async (api) => {
      apis.push(api);
      return daily;
    },
    { now: () => now, log: () => {} },
  );
  assert.deepEqual(apis, ['fund_etf_hist_em']);
  calls.length = 0;
  const out = await syncDirectory(machine, async () => {
    throw new Error('upstream unavailable');
  }, ['CN:fund']);
  assert.equal(out.failed, 1);
  assert.equal(calls.length, 0);
});
test('machine client refuses remote plaintext and credentials; token is only sent to configured origin', async () => {
  assert.throws(() => workerClient('http://example.com', 'x'.repeat(32)));
  assert.throws(() =>
    workerClient('https://name:pass@example.com', 'x'.repeat(32)),
  );
  const client = workerClient(
    'https://worker.example',
    'x'.repeat(32),
    async (input, init) => {
      assert.equal(
        String(input),
        'https://worker.example/api/internal/jobs/claim',
      );
      assert.equal(init?.redirect, 'error');
      assert.match(
        new Headers(init?.headers).get('Authorization') ?? '',
        /^Bearer /,
      );
      return Response.json({ job: null });
    },
  );
  await client('/api/internal/jobs/claim');
  await assert.rejects(() => client('https://other.example'));
});

test('unsupported currency share classes and HK RMB counters never receive a guessed currency', async () => {
  const rows = await collectDirectory('CN:fund', async () => [
    { 基金代码: '000001', 基金简称: '人民币基金' },
    { 基金代码: '000002', 基金简称: '美元现汇基金' },
  ]);
  assert.equal(rows.length, 1);
  const hk = await collectDirectory('HK:stock', async () => [
    { 代码: '00700', 中文名称: '腾讯控股' },
    { 代码: '80700', 中文名称: '腾讯控股人民币' },
  ]);
  assert.equal(hk.length, 1);
  assert.throws(() =>
    normalizeHoldings([{ ...holdings[0], 股票代码: '900901' }], '510300', now),
  );
});

test('holdings reject unverified numeric markets and code collisions with different company names', () => {
  for (const [code, name] of [
    ['005930', '三星电子'],
    ['600519', '韩国示例公司'],
    ['00700', '其他市场公司'],
  ]) {
    assert.throws(
      () =>
        normalizeHoldings(
          [{ ...holdings[0], 股票代码: code, 股票名称: name }],
          '510300',
          now,
          verifiedStocks,
        ),
      /未经目录确认/,
    );
  }
  assert.throws(
    () => normalizeHoldings(holdings, '510300', now),
    /未经目录确认/,
  );
});
test('holdings verify market directories once per executor and retry a failed lookup', async () => {
  const calls: string[] = [];
  let rejectDirectory = true;
  const ak: Aktools = async (api) => {
    calls.push(api);
    if (api === 'stock_info_a_code_name') {
      if (rejectDirectory) throw new Error('directory unavailable');
      return [{ 代码: '600519', 名称: '贵州茅台' }];
    }
    if (api === 'stock_hk_spot')
      return [{ 代码: '00700', 中文名称: '腾讯控股' }];
    return holdings;
  };
  await assert.rejects(
    () => collectHoldings(job, ak, now),
    /directory unavailable/,
  );
  rejectDirectory = false;
  const result = await collectHoldings(job, ak, now);
  await collectHoldings(job, ak, now);
  assert.equal(result.holdings?.holdings[0].id, 'CN:stock:600519');
  assert.equal(
    calls.filter((api) => api === 'stock_info_a_code_name').length,
    3,
  );
  assert.equal(calls.filter((api) => api === 'stock_hk_spot').length, 1);
});
