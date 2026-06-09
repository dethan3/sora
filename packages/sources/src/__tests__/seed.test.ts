import { describe, it, expect } from 'vitest'
import { join } from 'node:path'
import { SeedSource } from '../seed.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../../data/seeds')

describe('SeedSource.getIndexQuote', () => {
  it('returns a synthetic quote for a known ticker', async () => {
    const source = new SeedSource(SEEDS_DIR)
    const result = await source.getIndexQuote('^NDX')

    expect(result.ticker).toBe('^NDX')
    expect(typeof result.price).toBe('number')
    expect(result.fetchedAt).toBeTruthy()
  })
})

describe('SeedSource.getFundDetails', () => {
  it('returns fund details for a known fund code', async () => {
    const source = new SeedSource(SEEDS_DIR)
    const result = await source.getFundDetails('159941')

    expect(result.fundCode).toBe('159941')
    expect(result.fundName).toContain('易方达')
    expect(result.nav).toBeGreaterThan(0)
    expect(result.dataSource).toBe('seed')
  })

  it('throws for unknown fund code', async () => {
    const source = new SeedSource(SEEDS_DIR)
    await expect(source.getFundDetails('000000')).rejects.toThrow('SeedSource: fund 000000 not found')
  })
})

describe('SeedSource.getFundNavHistory', () => {
  it('returns empty array (no history in seeds)', async () => {
    const source = new SeedSource(SEEDS_DIR)
    const result = await source.getFundNavHistory('159941', 30)
    expect(result).toEqual([])
  })
})

describe('SeedSource.getFundMetrics', () => {
  it('returns metrics for a known fund code', async () => {
    const source = new SeedSource(SEEDS_DIR)
    const result = await source.getFundMetrics('159941')

    expect(result.fundCode).toBe('159941')
    expect(result.nav).toBeGreaterThan(0)
    expect(result.snapshotDate).toBe('2024-06-01')
    expect(result.dataSource).toBe('seed')
  })

  it('throws for unknown fund code', async () => {
    const source = new SeedSource(SEEDS_DIR)
    await expect(source.getFundMetrics('000000')).rejects.toThrow('SeedSource: metrics for fund 000000 not found')
  })
})
