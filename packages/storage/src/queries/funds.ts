import { eq, desc } from 'drizzle-orm'
import type { Fund, FundMetricsSnapshot } from '@sora/core'
import type { SoraDb } from '../db.js'
import { funds, fundIndexMappings, fundMetricsSnapshots } from '../schema.js'

function rowToFund(row: typeof funds.$inferSelect): Fund {
  return {
    id: row.id,
    fundCode: row.fundCode,
    fundName: row.fundName,
    fundType: row.fundType as Fund['fundType'],
    marketId: row.marketId,
    trackingIndexId: row.trackingIndexId,
    manager: row.manager ?? undefined,
    fee: row.fee ?? undefined,
    scale: row.scale ?? undefined,
    inceptionDate: row.inceptionDate ?? undefined,
    isEtf: row.isEtf,
    isEtfFeeder: row.isEtfFeeder,
    isQdii: row.isQdii,
    purchaseStatus: row.purchaseStatus as Fund['purchaseStatus'],
    purchaseLimit: row.purchaseLimit ?? null,
    dataSource: row.dataSource,
    updatedAt: row.updatedAt,
  }
}

function rowToMetrics(row: typeof fundMetricsSnapshots.$inferSelect): FundMetricsSnapshot {
  return {
    id: row.id,
    fundId: row.fundId,
    nav: row.nav ?? null,
    price: row.price ?? null,
    premiumRate: row.premiumRate ?? null,
    volume: row.volume ?? null,
    turnover: row.turnover ?? null,
    sharpeRatio: row.sharpeRatio ?? null,
    maxDrawdown: row.maxDrawdown ?? null,
    volatility: row.volatility ?? null,
    trackingError: row.trackingError ?? null,
    return1m: row.return1m ?? null,
    return3m: row.return3m ?? null,
    return6m: row.return6m ?? null,
    return1y: row.return1y ?? null,
    return3y: row.return3y ?? null,
    snapshotDate: row.snapshotDate,
    dataSource: row.dataSource,
  }
}

export function findFundsByIndex(db: SoraDb, indexId: string): Fund[] {
  const mappings = db
    .select()
    .from(fundIndexMappings)
    .where(eq(fundIndexMappings.indexId, indexId))
    .all()

  const result: Fund[] = []
  for (const map of mappings) {
    const row = db.select().from(funds).where(eq(funds.id, map.fundId)).get()
    if (row) result.push(rowToFund(row))
  }
  return result
}

export function findFundsByMarket(db: SoraDb, marketId: string): Fund[] {
  return db.select().from(funds).where(eq(funds.marketId, marketId)).all().map(rowToFund)
}

export function getLatestFundMetrics(db: SoraDb, fundId: string): FundMetricsSnapshot | null {
  const row = db
    .select()
    .from(fundMetricsSnapshots)
    .where(eq(fundMetricsSnapshots.fundId, fundId))
    .orderBy(desc(fundMetricsSnapshots.snapshotDate))
    .get()
  return row ? rowToMetrics(row) : null
}
