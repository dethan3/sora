import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { EastMoneyFundSource } from '../eastmoney.js'

const FIXTURES = join(import.meta.dirname, 'fixtures')
const TEST_CACHE_DIR = join(tmpdir(), `sora-eastmoney-test-${Date.now()}`)

beforeEach(() => {
  mkdirSync(TEST_CACHE_DIR, { recursive: true })
  vi.restoreAllMocks()
})

afterEach(() => {
  rmSync(TEST_CACHE_DIR, { recursive: true, force: true })
})

function mockFetch(responseText: string, asJson = false) {
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({
      text: () => Promise.resolve(responseText),
      json: () => Promise.resolve(JSON.parse(responseText)),
    })
  )
}

describe('EastMoneyFundSource.getFundDetails', () => {
  it('fetches and parses NAV JSONP response', async () => {
    const navText = readFileSync(join(FIXTURES, 'eastmoney-nav-159941.txt'), 'utf-8')
    mockFetch(navText)

    const source = new EastMoneyFundSource({ cacheDir: TEST_CACHE_DIR, requestDelayMs: 0 })
    const result = await source.getFundDetails('159941')

    expect(result.fundCode).toBe('159941')
    expect(result.fundName).toBe('易方达纳斯达克100ETF')
    expect(result.nav).toBeCloseTo(1.523)
    expect(result.navDate).toBe('2024-06-01')
    expect(result.estimatedNav).toBeCloseTo(1.536)
    expect(result.estimatedChangePercent).toBeCloseTo(0.86)
    expect(result.dataSource).toBe('eastmoney')
  })

  it('returns cached result without calling fetch again', async () => {
    const navText = readFileSync(join(FIXTURES, 'eastmoney-nav-159941.txt'), 'utf-8')
    const fetchMock = vi.fn().mockResolvedValue({ text: () => Promise.resolve(navText) })
    vi.stubGlobal('fetch', fetchMock)

    const source = new EastMoneyFundSource({ cacheDir: TEST_CACHE_DIR, requestDelayMs: 0 })
    await source.getFundDetails('159941')
    await source.getFundDetails('159941')

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})

describe('EastMoneyFundSource.getFundNavHistory', () => {
  it('fetches and parses history response', async () => {
    const historyJson = readFileSync(join(FIXTURES, 'eastmoney-history-159941.json'), 'utf-8')
    mockFetch(historyJson, true)

    const source = new EastMoneyFundSource({ cacheDir: TEST_CACHE_DIR, requestDelayMs: 0 })
    const result = await source.getFundNavHistory('159941', 5)

    expect(result).toHaveLength(3)
    expect(result[0].date).toBe('2024-06-01')
    expect(result[0].nav).toBeCloseTo(1.523)
    expect(result[0].changePercent).toBeCloseTo(0.86)
    expect(result[1].date).toBe('2024-05-31')
  })

  it('returns cached history without calling fetch again', async () => {
    const historyJson = readFileSync(join(FIXTURES, 'eastmoney-history-159941.json'), 'utf-8')
    const fetchMock = vi.fn().mockResolvedValue({ json: () => Promise.resolve(JSON.parse(historyJson)) })
    vi.stubGlobal('fetch', fetchMock)

    const source = new EastMoneyFundSource({ cacheDir: TEST_CACHE_DIR, requestDelayMs: 0 })
    await source.getFundNavHistory('159941', 5)
    await source.getFundNavHistory('159941', 5)

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})

describe('EastMoneyFundSource.getFundMetrics', () => {
  it('returns metrics derived from NAV details', async () => {
    const navText = readFileSync(join(FIXTURES, 'eastmoney-nav-159941.txt'), 'utf-8')
    mockFetch(navText)

    const source = new EastMoneyFundSource({ cacheDir: TEST_CACHE_DIR, requestDelayMs: 0 })
    const result = await source.getFundMetrics('159941')

    expect(result.fundCode).toBe('159941')
    expect(result.nav).toBeCloseTo(1.523)
    expect(result.price).toBeNull()
    expect(result.premiumRate).toBeNull()
    expect(result.dataSource).toBe('eastmoney')
  })
})
