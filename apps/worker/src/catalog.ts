import type { D1Database } from './types.ts';

// Shape served by GET /api/catalog. Mirrors apps/web/public/data/catalog.json
// so the frontend `demoSchema` validates either source unchanged.
export interface CatalogFund {
  id?: string;
  cataloguedAt?: string;
  code: string;
  name: string;
  market: string;
  category: string;
  changePct: null;
  note: string;
  currency: string;
  nav: null;
  history: [];
  benchmark?: string;
  sourceName?: string;
  sourceUrl?: string;
}

export interface Catalog {
  schemaVersion: 1;
  source: 'reference-catalog';
  asOf: string;
  funds: CatalogFund[];
  positions: [];
  watchlist: [];
  modules: Record<string, unknown>[];
}

interface InstrumentRow {
  id: string;
  public_code: string;
  source: string;
  created_at: string;
  symbol: string;
  name: string;
  market: string;
  category: string | null;
  note: string | null;
  currency: string;
  benchmark: string | null;
  source_name: string | null;
  source_url: string | null;
}

export async function readCatalog(db: D1Database): Promise<Catalog | null> {
  const [asOf, funds, modules] = await Promise.all([
    db
      .prepare("SELECT value FROM app_meta WHERE key = 'catalog_as_of'")
      .first<{ value: string }>(),
    db
      .prepare(
        `SELECT id, public_code, source, created_at, symbol, name, market, category, note, currency, benchmark, source_name, source_url
         FROM instruments
         WHERE kind IN ('fund', 'etf') AND status = 'ready'
         ORDER BY position, symbol`,
      )
      .all<InstrumentRow>(),
    db
      .prepare('SELECT data_json FROM catalog_modules ORDER BY position, id')
      .all<{ data_json: string }>(),
  ]);
  if (!asOf) return null;
  return {
    schemaVersion: 1,
    source: 'reference-catalog',
    asOf: asOf.value,
    funds: funds.results.map((row) => ({
      code: row.public_code ?? row.symbol,
      ...(row.source !== 'static-seed'
        ? { id: row.id, cataloguedAt: row.created_at.slice(0, 10) }
        : {}),
      name: row.name,
      market:
        (
          { CN: '中国内地', US: '美国市场', HK: '香港市场' } as Record<
            string,
            string
          >
        )[row.market] ?? row.market,
      category: row.category ?? '',
      changePct: null,
      note: row.note ?? '',
      currency: row.currency,
      nav: null,
      history: [],
      ...(row.benchmark ? { benchmark: row.benchmark } : {}),
      ...(row.source_name ? { sourceName: row.source_name } : {}),
      ...(row.source_url ? { sourceUrl: row.source_url } : {}),
    })),
    positions: [],
    watchlist: [],
    modules: modules.results.map((row) => {
      const module = JSON.parse(row.data_json);
      module.fundCodes = module.fundCodes.filter((code: string) =>
        funds.results.some(
          (fund) => (fund.public_code ?? fund.symbol) === code,
        ),
      );
      return module;
    }),
  };
}
