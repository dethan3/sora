import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Fund, FundMapping, FundMetricsSnapshot } from '@sora/core'
import type { Market } from '@sora/core'
import type { Index } from '@sora/core'
import type { SoraDb } from './db.js'
import {
  markets,
  indexes,
  funds,
  fundIndexMappings,
  fundMetricsSnapshots,
} from './schema.js'

export interface SeedStats {
  markets: number
  indexes: number
  funds: number
  mappings: number
  metrics: number
}

function readJson<T>(seedsDir: string, filename: string): T[] {
  const content = readFileSync(join(seedsDir, filename), 'utf-8')
  return JSON.parse(content) as T[]
}

export function dbSeed(db: SoraDb, seedsDir: string): SeedStats {
  const marketsData = readJson<Market>(seedsDir, 'markets.json')
  const indexesData = readJson<Index>(seedsDir, 'indexes.json')
  const fundsData = readJson<Fund>(seedsDir, 'funds.json')
  const mappingsData = readJson<FundMapping>(seedsDir, 'mappings.json')
  const metricsData = readJson<FundMetricsSnapshot>(seedsDir, 'fund-metrics.json')

  for (const m of marketsData) {
    db.insert(markets).values({
      id: m.id,
      name: m.name,
      category: m.category,
      description: m.description ?? null,
    }).run()
  }

  for (const i of indexesData) {
    db.insert(indexes).values({
      id: i.id,
      name: i.name,
      marketId: i.marketId,
      ticker: i.ticker,
      description: i.description ?? null,
    }).run()
  }

  for (const f of fundsData) {
    db.insert(funds).values({
      id: f.id,
      fundCode: f.fundCode,
      fundName: f.fundName,
      fundType: f.fundType,
      marketId: f.marketId,
      trackingIndexId: f.trackingIndexId,
      manager: f.manager ?? null,
      fee: f.fee ?? null,
      scale: f.scale ?? null,
      inceptionDate: f.inceptionDate ?? null,
      isEtf: f.isEtf,
      isEtfFeeder: f.isEtfFeeder,
      isQdii: f.isQdii,
      purchaseStatus: f.purchaseStatus,
      purchaseLimit: f.purchaseLimit ?? null,
      dataSource: f.dataSource,
      updatedAt: f.updatedAt,
    }).run()
  }

  for (const map of mappingsData) {
    db.insert(fundIndexMappings).values({
      id: map.id,
      fundId: map.fundId,
      indexId: map.indexId,
      isPrimary: map.isPrimary,
    }).run()
  }

  for (const m of metricsData) {
    db.insert(fundMetricsSnapshots).values({
      id: m.id,
      fundId: m.fundId,
      nav: m.nav ?? null,
      price: m.price ?? null,
      premiumRate: m.premiumRate ?? null,
      volume: m.volume ?? null,
      turnover: m.turnover ?? null,
      sharpeRatio: m.sharpeRatio ?? null,
      maxDrawdown: m.maxDrawdown ?? null,
      volatility: m.volatility ?? null,
      trackingError: m.trackingError ?? null,
      return1m: m.return1m ?? null,
      return3m: m.return3m ?? null,
      return6m: m.return6m ?? null,
      return1y: m.return1y ?? null,
      return3y: m.return3y ?? null,
      snapshotDate: m.snapshotDate,
      dataSource: m.dataSource,
    }).run()
  }

  return {
    markets: marketsData.length,
    indexes: indexesData.length,
    funds: fundsData.length,
    mappings: mappingsData.length,
    metrics: metricsData.length,
  }
}
