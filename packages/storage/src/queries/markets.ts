import { eq } from 'drizzle-orm'
import type { Market } from '@sora/core'
import type { SoraDb } from '../db.js'
import { markets } from '../schema.js'

function rowToMarket(row: typeof markets.$inferSelect): Market {
  return {
    id: row.id,
    name: row.name,
    category: row.category as Market['category'],
    description: row.description ?? undefined,
  }
}

export function findMarketById(db: SoraDb, id: string): Market | null {
  const row = db.select().from(markets).where(eq(markets.id, id)).get()
  return row ? rowToMarket(row) : null
}

export function listAllMarkets(db: SoraDb): Market[] {
  return db.select().from(markets).all().map(rowToMarket)
}
