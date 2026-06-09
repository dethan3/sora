import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Fund, Index, FundMetricsSnapshot } from '@sora/core'
import type {
  IMarketQuoteSource,
  IFundDataSource,
  IndexQuote,
  IndexHistoricalQuote,
  RawFundDetails,
  RawNavRecord,
  RawFundMetrics,
} from './types.js'

function readJson<T>(seedsDir: string, filename: string): T[] {
  const content = readFileSync(join(seedsDir, filename), 'utf-8')
  return JSON.parse(content) as T[]
}

export class SeedSource implements IMarketQuoteSource, IFundDataSource {
  constructor(private seedsDir: string) {}

  async getIndexQuote(ticker: string): Promise<IndexQuote> {
    const indexes = readJson<Index>(this.seedsDir, 'indexes.json')
    const found = indexes.find((i) => i.ticker === ticker)
    if (!found) throw new Error(`SeedSource: index ticker ${ticker} not found in seeds`)

    return {
      ticker,
      price: 0,
      changePercent: 0,
      volume: null,
      high52w: null,
      low52w: null,
      fetchedAt: new Date().toISOString(),
    }
  }

  async getIndexHistory(ticker: string, days: number): Promise<IndexHistoricalQuote[]> {
    void ticker
    void days

    return []
  }

  async getFundDetails(fundCode: string): Promise<RawFundDetails> {
    const funds = readJson<Fund>(this.seedsDir, 'funds.json')
    const metrics = readJson<FundMetricsSnapshot>(this.seedsDir, 'fund-metrics.json')

    const fund = funds.find((f) => f.fundCode === fundCode)
    const metric = metrics.find((m) => m.fundId === `fund-${fundCode}`)

    if (!fund) throw new Error(`SeedSource: fund ${fundCode} not found in seeds`)

    return {
      fundCode: fund.fundCode,
      fundName: fund.fundName,
      nav: metric?.nav ?? 1,
      navDate: metric?.snapshotDate ?? new Date().toISOString().split('T')[0],
      estimatedNav: null,
      estimatedChangePercent: null,
      dataSource: 'seed',
      fetchedAt: new Date().toISOString(),
    }
  }

  async getFundNavHistory(fundCode: string, days: number): Promise<RawNavRecord[]> {
    void fundCode
    void days

    return []
  }

  async getFundMetrics(fundCode: string): Promise<RawFundMetrics> {
    const metrics = readJson<FundMetricsSnapshot>(this.seedsDir, 'fund-metrics.json')
    const m = metrics.find((x) => x.fundId === `fund-${fundCode}`)

    if (!m) throw new Error(`SeedSource: metrics for fund ${fundCode} not found in seeds`)

    return {
      fundCode,
      nav: m.nav,
      price: m.price,
      premiumRate: m.premiumRate,
      volume: m.volume,
      turnover: m.turnover,
      snapshotDate: m.snapshotDate,
      dataSource: 'seed',
    }
  }
}
