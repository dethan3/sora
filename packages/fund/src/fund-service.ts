import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Fund, FundMapping, FundMetricsSnapshot } from '@sora/core'
import type { IFundDataSource, RawFundMetrics } from '@sora/sources'
import type { FundWithMetrics } from './types.js'
import { scoreFund } from './scoring.js'
import type { FundAnalysis } from '@sora/core'

function readJson<T>(seedsDir: string, filename: string): T[] {
  const content = readFileSync(join(seedsDir, filename), 'utf-8')
  return JSON.parse(content) as T[]
}

function toPartialSnapshot(raw: RawFundMetrics, fundId: string): FundMetricsSnapshot {
  return {
    id: `metrics-${fundId}-live`,
    fundId,
    nav: raw.nav,
    price: raw.price,
    premiumRate: raw.premiumRate,
    volume: raw.volume,
    turnover: raw.turnover,
    sharpeRatio: null,
    maxDrawdown: null,
    volatility: null,
    trackingError: null,
    return1m: null,
    return3m: null,
    return6m: null,
    return1y: null,
    return3y: null,
    snapshotDate: raw.snapshotDate,
    dataSource: raw.dataSource,
  }
}

export class FundService {
  constructor(
    private seedsDir: string,
    private fundSource: IFundDataSource
  ) {}

  private funds(): Fund[] {
    return readJson<Fund>(this.seedsDir, 'funds.json')
  }

  private mappings(): FundMapping[] {
    return readJson<FundMapping>(this.seedsDir, 'mappings.json')
  }

  async getFundsByIndex(indexId: string): Promise<FundWithMetrics[]> {
    const maps = this.mappings().filter((m) => m.indexId === indexId)
    const allFunds = this.funds()

    const results: FundWithMetrics[] = []
    for (const map of maps) {
      const fund = allFunds.find((f) => f.id === map.fundId)
      if (!fund) continue

      let metrics: FundMetricsSnapshot | null = null
      try {
        const raw = await this.fundSource.getFundMetrics(fund.fundCode)
        metrics = toPartialSnapshot(raw, fund.id)
      } catch {
        // source unavailable
      }

      results.push({ fund, metrics })
    }
    return results
  }

  async getFundsByMarket(marketId: string): Promise<FundWithMetrics[]> {
    const allFunds = this.funds().filter((f) => f.marketId === marketId)
    const results: FundWithMetrics[] = []
    for (const fund of allFunds) {
      let metrics: FundMetricsSnapshot | null = null
      try {
        const raw = await this.fundSource.getFundMetrics(fund.fundCode)
        metrics = toPartialSnapshot(raw, fund.id)
      } catch {
        // source unavailable
      }
      results.push({ fund, metrics })
    }
    return results
  }

  async analyzeFunds(funds: FundWithMetrics[]): Promise<FundAnalysis[]> {
    const analyses = funds
      .filter((fw) => fw.metrics !== null)
      .map((fw) => scoreFund(fw.fund, fw.metrics!))

    return analyses.sort((a, b) => b.executionQualityScore - a.executionQualityScore)
  }
}
