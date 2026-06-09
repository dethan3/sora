import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdirSync, rmSync, existsSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { readCache, writeCache, isCacheStale, ONE_DAY_MS } from '../cache.js'

const TEST_CACHE_DIR = join(tmpdir(), `sora-test-cache-${Date.now()}`)

beforeEach(() => {
  mkdirSync(TEST_CACHE_DIR, { recursive: true })
})

afterEach(() => {
  rmSync(TEST_CACHE_DIR, { recursive: true, force: true })
})

describe('writeCache / readCache', () => {
  it('round-trips a simple object', () => {
    const data = { ticker: '^NDX', price: 19800.5 }
    writeCache(TEST_CACHE_DIR, 'market-quotes/^NDX', 'yahoo-finance', data)

    const entry = readCache<typeof data>(TEST_CACHE_DIR, 'market-quotes/^NDX')
    expect(entry).not.toBeNull()
    expect(entry!.source).toBe('yahoo-finance')
    expect(entry!.data.ticker).toBe('^NDX')
    expect(entry!.data.price).toBe(19800.5)
  })

  it('creates nested directories automatically', () => {
    writeCache(TEST_CACHE_DIR, 'fund-details/159941', 'eastmoney', { name: '易方达纳指ETF' })
    expect(existsSync(join(TEST_CACHE_DIR, 'fund-details/159941.json'))).toBe(true)
  })

  it('overwrites existing cache entry', () => {
    writeCache(TEST_CACHE_DIR, 'test-key', 'src', { v: 1 })
    writeCache(TEST_CACHE_DIR, 'test-key', 'src', { v: 2 })
    const entry = readCache<{ v: number }>(TEST_CACHE_DIR, 'test-key')
    expect(entry!.data.v).toBe(2)
  })

  it('returns null for missing key', () => {
    const entry = readCache(TEST_CACHE_DIR, 'no-such-key')
    expect(entry).toBeNull()
  })

  it('returns null for corrupted JSON', () => {
    writeFileSync(join(TEST_CACHE_DIR, 'bad.json'), '{ not valid json }')
    const entry = readCache(TEST_CACHE_DIR, 'bad')
    expect(entry).toBeNull()
  })
})

describe('isCacheStale', () => {
  it('returns false for a fresh entry', () => {
    const entry = { fetchedAt: new Date().toISOString(), source: 'test', data: {} }
    expect(isCacheStale(entry, ONE_DAY_MS)).toBe(false)
  })

  it('returns true for an entry older than maxAgeMs', () => {
    const twoDaysAgo = new Date(Date.now() - 2 * ONE_DAY_MS).toISOString()
    const entry = { fetchedAt: twoDaysAgo, source: 'test', data: {} }
    expect(isCacheStale(entry, ONE_DAY_MS)).toBe(true)
  })
})
