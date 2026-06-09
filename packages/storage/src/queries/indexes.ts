import { eq } from 'drizzle-orm'
import type { Index } from '@sora/core'
import type { SoraDb } from '../db.js'
import { indexes } from '../schema.js'

function rowToIndex(row: typeof indexes.$inferSelect): Index {
  return {
    id: row.id,
    name: row.name,
    marketId: row.marketId,
    ticker: row.ticker,
    description: row.description ?? undefined,
  }
}

export function listAllIndexes(db: SoraDb): Index[] {
  return db.select().from(indexes).all().map(rowToIndex)
}

export function listIndexesByMarket(db: SoraDb, marketId: string): Index[] {
  return db.select().from(indexes).where(eq(indexes.marketId, marketId)).all().map(rowToIndex)
}

export function findIndexById(db: SoraDb, id: string): Index | null {
  const row = db.select().from(indexes).where(eq(indexes.id, id)).get()
  return row ? rowToIndex(row) : null
}
