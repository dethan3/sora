import {
  collectedSeriesSchema,
  holdingsSchema,
  directoryEntrySchema,
  instrumentKey,
  type ClaimedJob,
  type CollectedHoldings,
  type DirectoryEntry,
} from '@sora/market-data/ingest';
import { fetchJson, type Fetcher } from './providers.ts';

const docs = 'https://akshare.akfamily.xyz/data/';
export function aktoolsClient(
  baseUrl = 'http://127.0.0.1:8080',
  fetcher: Fetcher = fetch,
) {
  const base = new URL(baseUrl);
  if (
    !['http:', 'https:'].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash
  )
    throw new Error('Invalid AKTools URL');
  return async (api: string, params: Record<string, string> = {}) => {
    // API names and parameters are chosen by the adapters, never by directory rows.
    const url = new URL(`/api/public/${api}`, base);
    url.search = new URLSearchParams(params).toString();
    return fetchJson(
      url,
      fetcher,
      api === 'get_us_stock_name' ? 600_000 : 180_000,
    );
  };
}
export type Aktools = ReturnType<typeof aktoolsClient>;
function records(raw: unknown): Record<string, unknown>[] {
  if (
    !Array.isArray(raw) ||
    !raw.length ||
    raw.length > 100000 ||
    raw.some((row) => !row || typeof row !== 'object')
  )
    throw new Error('上游未返回有效记录');
  return raw;
}
const zone = {
  CN: 'Asia/Shanghai',
  US: 'America/New_York',
  HK: 'Asia/Hong_Kong',
} as const;
const currency = { CN: 'CNY', US: 'USD', HK: 'HKD' } as const;
function day(now: string, market: DirectoryEntry['market']) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: zone[market],
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date(now));
}
export function normalizeSeries(
  raw: unknown,
  entry: DirectoryEntry,
  now: string,
) {
  const nav = entry.kind === 'fund',
    today = day(now, entry.market);
  const cutoff = new Date(Date.parse(now) - 380 * 86400000)
    .toISOString()
    .slice(0, 10);
  const rows = new Map<string, string>();
  for (const row of records(raw)) {
    const date = String(row[nav ? '净值日期' : '日期'] ?? row.date ?? '').slice(
      0,
      10,
    );
    const value = row[nav ? '单位净值' : '收盘'] ?? row.close;
    const close = String(value);
    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
      !Number.isFinite(Date.parse(date)) ||
      new Date(date).toISOString().slice(0, 10) !== date ||
      !/^\d{1,12}(?:\.\d{1,8})?$/.test(close) ||
      !(Number(close) > 0) ||
      date > today
    )
      throw new Error('日线包含无效价格或日期');
    if (date >= today || date < cutoff) continue;
    if (rows.has(date) && Number(rows.get(date)) !== Number(close))
      throw new Error('日线同日数据冲突');
    rows.set(date, close);
  }
  return collectedSeriesSchema.parse({
    type: nav ? 'nav' : entry.kind === 'index' ? 'index' : 'price',
    currency: entry.currency,
    timeZone: zone[entry.market],
    adjust: 'unadjusted',
    source: {
      name:
        entry.market === 'US' || entry.market === 'HK'
          ? '新浪财经 / AKTools'
          : '东方财富 / AKTools',
      url: nav
        ? `https://fund.eastmoney.com/${entry.symbol}.html`
        : entry.sourceUrl,
      fetchedAt: now,
    },
    history: [...rows]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, close]) => ({ date, close })),
  });
}
function latestHoldingRows(raw: unknown, now: string) {
  const rows = records(raw)
    .map((row) => {
      const period = /^(\d{4})年([1-4])季度/.exec(String(row['季度']));
      if (!period) throw new Error('持仓报告期无法识别');
      const asOf = new Date(
        Date.UTC(Number(period[1]), Number(period[2]) * 3, 0),
      )
        .toISOString()
        .slice(0, 10);
      return { row, asOf };
    })
    .filter((row) => row.asOf <= day(now, 'CN'));
  const latest = rows
    .map((row) => row.asOf)
    .sort()
    .at(-1);
  if (!latest) throw new Error('没有已披露的持仓报告');
  return { latest, latestRows: rows.filter((row) => row.asOf === latest) };
}
export function normalizeHoldings(
  raw: unknown,
  symbol: string,
  now: string,
  verifiedStocks: readonly DirectoryEntry[] = [],
): CollectedHoldings {
  const { latest, latestRows } = latestHoldingRows(raw, now);
  const verified = new Map(
    verifiedStocks
      .filter((entry) => entry.kind === 'stock')
      .map((entry) => [entry.id, entry]),
  );
  const canonicalName = (value: unknown) =>
    String(value ?? '')
      .normalize('NFKC')
      .replace(/\s/g, '')
      .toUpperCase();
  const holdings = latestRows
    .map(({ row }) => {
      const rawCode = String(row['股票代码']).trim().toUpperCase();
      if (
        /^(200|900)\d{3}$/.test(rawCode) ||
        (/^\d{5}$/.test(rawCode) && Number(rawCode) >= 80000)
      )
        throw new Error('暂不支持非人民币 A 股或港币柜台的持仓');
      // Code shape only selects a candidate market; the market-specific directory
      // must also confirm the code and company name before an identity is published.
      const market = /^\d{6}$/.test(rawCode)
        ? 'CN'
        : /^\d{5}$/.test(rawCode)
          ? 'HK'
          : /^[A-Z][A-Z0-9.-]*$/.test(rawCode)
            ? 'US'
            : null;
      if (!market) throw new Error(`持仓证券代码无法识别：${rawCode}`);
      const symbol = market === 'HK' ? `${Number(rawCode)}.HK` : rawCode;
      const known = verified.get(instrumentKey(market, 'stock', symbol));
      if (
        !known ||
        canonicalName(known.name) !== canonicalName(row['股票名称'])
      )
        throw new Error(`持仓证券市场或名称未经目录确认：${rawCode}`);
      const weight = Number(
        String(
          Object.entries(row).find(
            ([key]) => key.replace(/[\s()（）%]/g, '') === '占净值比例',
          )?.[1],
        ).replace(/%$/, ''),
      );
      if (!Number.isFinite(weight) || weight <= 0)
        throw new Error('持仓权重无效');
      return {
        id: instrumentKey(market, 'stock', symbol),
        market,
        symbol,
        name: String(row['股票名称'] ?? ''),
        weightBp: Math.round(weight * 100),
      };
    })
    .sort((a, b) => b.weightBp - a.weightBp)
    .slice(0, 10);
  return holdingsSchema.parse({
    asOf: latest,
    coverage: 'top10',
    source: {
      name: '东方财富基金持仓 / AKTools',
      url: `https://fundf10.eastmoney.com/ccmx_${symbol}.html`,
      fetchedAt: now,
    },
    holdings,
  });
}
export async function collectSeries(job: ClaimedJob, ak: Aktools, now: string) {
  const entry = job.instrument;
  let api: string,
    params: Record<string, string> = { symbol: entry.symbol };
  if (entry.market === 'CN') {
    if (!/^\d{6}$/.test(entry.symbol))
      throw new Error('暂不支持此中国市场代码');
    if (entry.kind === 'fund') {
      api = 'fund_open_fund_info_em';
      params.indicator = '单位净值走势';
    } else {
      if (!['etf', 'stock'].includes(entry.kind))
        throw new Error('暂不支持此指数采集');
      api = entry.kind === 'etf' ? 'fund_etf_hist_em' : 'stock_zh_a_hist';
      const cutoff = Date.parse(now) - 380 * 86400000;
      const start =
        job.sinceDate && job.mode === 'refresh'
          ? Math.max(cutoff, Date.parse(job.sinceDate) - 7 * 86400000)
          : cutoff;
      params = {
        ...params,
        period: 'daily',
        adjust: '',
        start_date: new Date(start)
          .toISOString()
          .slice(0, 10)
          .replaceAll('-', ''),
        end_date: day(now, 'CN').replaceAll('-', ''),
      };
    }
  } else if (entry.market === 'US') {
    if (entry.kind === 'fund' || entry.kind === 'index')
      throw new Error('暂不支持此美国标的类型');
    api = 'stock_us_daily';
    params.adjust = '';
  } else {
    if (entry.kind === 'fund' || entry.kind === 'index')
      throw new Error('暂不支持此香港标的类型');
    api = 'stock_hk_daily';
    params = {
      symbol: entry.symbol.replace(/\.HK$/, '').padStart(5, '0'),
      adjust: '',
    };
  }
  return normalizeSeries(await ak(api, params), entry, now);
}
// One executor batch may process many funds. Reuse confirmed market directories;
// failed lookups are discarded so a later job can retry instead of caching failure.
const holdingDirectories = new WeakMap<
  Aktools,
  Map<string, Promise<DirectoryEntry[]>>
>();
async function holdingDirectory(ak: Aktools, market: DirectoryEntry['market']) {
  let cache = holdingDirectories.get(ak);
  if (!cache) {
    cache = new Map();
    holdingDirectories.set(ak, cache);
  }
  const scope = `${market}:stock`;
  let pending = cache.get(scope);
  if (!pending) {
    pending = collectDirectory(scope, ak).catch((error) => {
      cache.delete(scope);
      throw error;
    });
    cache.set(scope, pending);
  }
  return pending;
}
export async function collectHoldings(
  job: ClaimedJob,
  ak: Aktools,
  now: string,
) {
  if (!['fund', 'etf'].includes(job.instrument.kind))
    return { holdings: null, warnings: [] };
  if (job.instrument.market !== 'CN')
    return {
      holdings: null,
      warnings: [
        '当前执行器未提供此市场的自动持仓来源，配料表仍以已核对的公开资料为准。',
      ],
    };
  const year = Number(day(now, 'CN').slice(0, 4));
  let failure: unknown;
  for (const date of [String(year), String(year - 1)]) {
    try {
      const raw = await ak('fund_portfolio_hold_em', {
        symbol: job.instrument.symbol,
        date,
      });
      const { latestRows } = latestHoldingRows(raw, now);
      const markets = new Set<DirectoryEntry['market']>();
      for (const { row } of latestRows) {
        const code = String(row['股票代码']).trim().toUpperCase();
        if (/^\d{6}$/.test(code)) markets.add('CN');
        else if (/^\d{5}$/.test(code)) markets.add('HK');
        else if (/^[A-Z][A-Z0-9.-]*$/.test(code)) markets.add('US');
      }
      const verifiedStocks = (
        await Promise.all(
          [...markets].map((market) => holdingDirectory(ak, market)),
        )
      ).flat();
      return {
        holdings: normalizeHoldings(
          raw,
          job.instrument.symbol,
          now,
          verifiedStocks,
        ),
        warnings: [],
      };
    } catch (error) {
      failure = error;
    }
  }
  // A failed holdings fetch must not publish an apparently complete new fund.
  throw new Error(
    `持仓采集失败：${failure instanceof Error ? failure.message : '上游异常'}（无股票持仓的基金暂不支持自动入库）`,
  );
}
export const directoryScopes = [
  'CN:fund',
  'CN:etf',
  'CN:stock',
  'US:stock',
  'HK:stock',
] as const;
export async function collectDirectory(
  scope: string,
  ak: Aktools,
): Promise<DirectoryEntry[]> {
  if (!directoryScopes.includes(scope as (typeof directoryScopes)[number]))
    throw new Error('Unsupported directory scope');
  const [market, kind] = scope.split(':') as [
    DirectoryEntry['market'],
    DirectoryEntry['kind'],
  ];
  const api = (
    {
      'CN:fund': 'fund_name_em',
      'CN:etf': 'fund_etf_spot_em',
      'CN:stock': 'stock_info_a_code_name',
      'US:stock': 'get_us_stock_name',
      'HK:stock': 'stock_hk_spot',
    } as Record<string, string>
  )[scope]!;
  const rows = records(await ak(api)).filter((row) => {
    const name = String(
      row['基金简称'] ?? row['名称'] ?? row['中文名称'] ?? row.name ?? '',
    );
    if (
      market === 'CN' &&
      kind === 'fund' &&
      /美元|港币|港元|英镑|欧元|澳元|加元|日元/.test(name)
    )
      return false;
    if (market === 'HK') {
      const code = String(row['代码'] ?? row.code ?? '');
      return /^\d{5}$/.test(code) && Number(code) > 0 && Number(code) < 80000;
    }
    return true;
  });
  const entries = rows.map((row) => {
    let symbol = String(
      row['基金代码'] ?? row['代码'] ?? row.code ?? row.symbol ?? '',
    )
      .trim()
      .toUpperCase();
    if (market === 'HK') symbol = `${Number(symbol)}.HK`;
    return directoryEntrySchema.parse({
      id: instrumentKey(market, kind, symbol),
      market,
      kind,
      symbol,
      name: String(
        row['基金简称'] ??
          row['名称'] ??
          row['中文名称'] ??
          row.cname ??
          row.name ??
          '',
      ),
      currency: currency[market],
      pinyin: `${row['拼音缩写'] ?? ''} ${row['拼音全称'] ?? ''}`.trim(),
      sourceName: 'AKShare 证券目录',
      sourceUrl: `${docs}${kind === 'fund' || kind === 'etf' ? 'fund/fund_public.html' : 'stock/stock.html'}`,
    });
  });
  if (!entries.length) throw new Error('目录没有受支持的币种记录，保留旧版本');
  const unique = new Map(entries.map((entry) => [entry.id, entry]));
  if (unique.size !== entries.length)
    throw new Error('目录包含重复代码，保留旧版本');
  return entries;
}
