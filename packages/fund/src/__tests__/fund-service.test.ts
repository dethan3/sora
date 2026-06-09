import { describe, it, expect } from 'vitest'
import { join } from 'node:path'
import { SeedSource } from '@sora/sources'
import { FundService } from '../fund-service.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../../data/seeds')

function makeService() {
  return new FundService(SEEDS_DIR, new SeedSource(SEEDS_DIR))
}

describe('FundService.getFundsByIndex', () => {
  it('returns all funds mapped to nasdaq-100', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByIndex('nasdaq-100')
    expect(funds.length).toBeGreaterThanOrEqual(3)
    expect(funds.every((fw) => fw.fund.trackingIndexId === 'nasdaq-100')).toBe(true)
  })

  it('each fund has metrics attached', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByIndex('nasdaq-100')
    expect(funds.every((fw) => fw.metrics !== null)).toBe(true)
    expect(funds[0].metrics!.nav).toBeGreaterThan(0)
  })

  it('returns empty array for unknown indexId', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByIndex('unknown-index')
    expect(funds).toHaveLength(0)
  })
})

describe('FundService.getFundsByMarket', () => {
  it('returns funds for hk-tech market', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByMarket('hk-tech')
    expect(funds.length).toBeGreaterThanOrEqual(2)
    expect(funds.every((fw) => fw.fund.marketId === 'hk-tech')).toBe(true)
  })
})

describe('FundService.analyzeFunds', () => {
  it('returns sorted FundAnalysis array', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByIndex('nasdaq-100')
    const analyses = await svc.analyzeFunds(funds)

    expect(analyses.length).toBeGreaterThanOrEqual(1)
    for (let i = 1; i < analyses.length; i++) {
      expect(analyses[i - 1].executionQualityScore).toBeGreaterThanOrEqual(
        analyses[i].executionQualityScore
      )
    }
  })

  it('includes all score sub-fields in [0,100]', async () => {
    const svc = makeService()
    const funds = await svc.getFundsByIndex('nasdaq-100')
    const analyses = await svc.analyzeFunds(funds)

    for (const a of analyses) {
      expect(a.costScore).toBeGreaterThanOrEqual(0)
      expect(a.costScore).toBeLessThanOrEqual(100)
      expect(a.liquidityScore).toBeGreaterThanOrEqual(0)
      expect(a.premiumRiskScore).toBeGreaterThanOrEqual(0)
      expect(a.executionQualityScore).toBeGreaterThanOrEqual(0)
      expect(a.executionQualityScore).toBeLessThanOrEqual(100)
    }
  })

  it('returns empty array if no funds have metrics', async () => {
    const svc = makeService()
    const analyses = await svc.analyzeFunds([])
    expect(analyses).toHaveLength(0)
  })
})
