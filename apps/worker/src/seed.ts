// Converts the static reference catalog into idempotent seed statements.
// Used by scripts/seed.ts (renders SQL for `wrangler d1 execute`) and tests.

export interface SeedStatement {
  sql: string;
  params: (string | number | null)[];
}

interface StaticCatalog {
  asOf: string;
  funds: {
    code: string;
    name: string;
    market: string;
    category: string;
    note: string;
    currency: string;
    benchmark?: string;
    sourceName?: string;
    sourceUrl?: string;
  }[];
  modules: { id: string }[];
}

const MARKET_PREFIX: Record<string, string> = {
  美国市场: 'US',
  中国香港: 'HK',
  香港市场: 'HK',
  中国内地: 'CN',
};

export function instrumentId(code: string, market: string) {
  if (code.endsWith('.HK')) return `HK:${code.slice(0, -3)}`;
  return `${MARKET_PREFIX[market] ?? 'XX'}:${code}`;
}

export function catalogSeed(catalog: StaticCatalog, now: string) {
  const statements: SeedStatement[] = [
    {
      sql: `INSERT INTO app_meta (key, value) VALUES ('catalog_as_of', ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
      params: [catalog.asOf],
    },
  ];
  catalog.funds.forEach((fund, position) => {
    const market = MARKET_PREFIX[fund.market] ?? fund.market;
    const directoryId = `${market}:etf:${fund.code}`;
    statements.push({
      sql: `INSERT INTO instruments
              (id, kind, symbol, market, name, currency, category, benchmark, note,
               source, source_name, source_url, status, position, created_at, updated_at, public_code, directory_id)
            VALUES (?, 'etf', ?, ?, ?, ?, ?, ?, ?, 'static-seed', ?, ?, 'ready', ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
              name = excluded.name, market = excluded.market, currency = excluded.currency,
              category = excluded.category, benchmark = excluded.benchmark, note = excluded.note,
              source_name = excluded.source_name, source_url = excluded.source_url,
              position = excluded.position, updated_at = excluded.updated_at
            WHERE instruments.source = 'static-seed'`,
      params: [
        instrumentId(fund.code, fund.market),
        fund.code,
        market,
        fund.name,
        fund.currency,
        fund.category,
        fund.benchmark ?? null,
        fund.note,
        fund.sourceName ?? null,
        fund.sourceUrl ?? null,
        position,
        now,
        now,
        fund.code,
        directoryId,
      ],
    });
    statements.push({
      sql: `INSERT INTO symbol_directory (id,kind,symbol,market,name,pinyin,updated_at,currency,source_name,source_url)
      VALUES (?,'etf',?,?,?,'',?,?,?,?) ON CONFLICT(id) DO NOTHING`,
      params: [
        directoryId,
        fund.code,
        market,
        fund.name,
        now,
        fund.currency,
        fund.sourceName ?? '参考目录',
        fund.sourceUrl ?? 'https://akshare.akfamily.xyz/',
      ],
    });
  });
  catalog.modules.forEach((module, position) => {
    statements.push({
      sql: `INSERT INTO catalog_modules (id, position, data_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
              position = excluded.position, data_json = excluded.data_json,
              updated_at = excluded.updated_at`,
      params: [module.id, position, JSON.stringify(module), now],
    });
  });
  return statements;
}

function literal(value: string | number | null) {
  if (value === null) return 'NULL';
  if (typeof value === 'number') return String(value);
  return `'${value.replaceAll("'", "''")}'`;
}

export function renderSql(statements: SeedStatement[]) {
  return statements
    .map(({ sql, params }) => {
      let index = 0;
      const rendered = sql.replace(/\?/g, () => literal(params[index++]));
      if (index !== params.length) throw new Error('参数数量不匹配');
      return rendered.replace(/\s+/g, ' ').trim() + ';';
    })
    .join('\n');
}
