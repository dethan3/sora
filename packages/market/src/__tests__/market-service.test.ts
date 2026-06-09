import { describe, it, expect } from 'vitest'
import { join } from 'node:path'
import { SeedSource } from '@sora/sources'
import { MarketService } from '../market-service.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../../data/seeds')

function makeService() {
  const seedSource = new SeedSource(SEEDS_DIR)
  return new MarketService(SEEDS_DIR, seedSource)
}

describe('MarketService.listMarkets', () => {
  it('returns 6 markets', async () => {
    const svc = makeService()
    const markets = await svc.listMarkets()
    expect(markets).toHaveLength(6)
  })

  it('includes us-tech and global markets', async () => {
    const svc = makeService()
    const markets = await svc.listMarkets()
    const ids = markets.map((m) => m.id)
    expect(ids).toContain('us-tech')
    expect(ids).toContain('global')
  })
})

describe('MarketService.getMarket', () => {
  it('returns correct market for known id', async () => {
    const svc = makeService()
    const m = await svc.getMarket('us-tech')
    expect(m).not.toBeNull()
    expect(m!.name).toBe('美国科技')
    expect(m!.category).toBe('us')
  })

  it('returns null for unknown id', async () => {
    const svc = makeService()
    const m = await svc.getMarket('nonexistent')
    expect(m).toBeNull()
  })
})

describe('MarketService.listIndexesByMarket', () => {
  it('returns Nasdaq 100 for us-tech', async () => {
    const svc = makeService()
    const indexes = await svc.listIndexesByMarket('us-tech')
    expect(indexes).toHaveLength(1)
    expect(indexes[0].id).toBe('nasdaq-100')
    expect(indexes[0].ticker).toBe('^NDX')
  })

  it('returns empty array for market with no indexes', async () => {
    const svc = makeService()
    const indexes = await svc.listIndexesByMarket('no-such-market')
    expect(indexes).toHaveLength(0)
  })
})

describe('MarketService.getIndexWithQuote', () => {
  it('returns index with synthetic quote from SeedSource', async () => {
    const svc = makeService()
    const result = await svc.getIndexWithQuote('nasdaq-100')
    expect(result).not.toBeNull()
    expect(result!.id).toBe('nasdaq-100')
    expect(result!.ticker).toBe('^NDX')
    expect(result!.quote).not.toBeNull()
    expect(result!.quote!.ticker).toBe('^NDX')
  })

  it('returns null for unknown index', async () => {
    const svc = makeService()
    const result = await svc.getIndexWithQuote('unknown-index')
    expect(result).toBeNull()
  })
})
