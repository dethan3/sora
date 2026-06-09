import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

vi.mock('yahoo-finance2', () => ({
  default: {
    quote: vi.fn(),
    historical: vi.fn(),
  },
}))

import yahooFinance from 'yahoo-finance2'
import { YahooFinanceSource } from '../yahoo.js'

const TEST_CACHE_DIR = join(tmpdir(), `sora-yahoo-test-${Date.now()}`)

beforeEach(() => {
  mkdirSync(TEST_CACHE_DIR, { recursive: true })
  vi.restoreAllMocks()
})

afterEach(() => {
  rmSync(TEST_CACHE_DIR, { recursive: true, force: true })
})

describe('YahooFinanceSource.getIndexQuote', () => {
  it('fetches and maps yahoo-finance2 quote', async () => {
    vi.mocked(yahooFinance.quote).mockResolvedValue({
      regularMarketPrice: 19800.5,
      regularMarketChangePercent: 1.23,
      regularMarketVolume: 5000000,
      fiftyTwoWeekHigh: 21000,
      fiftyTwoWeekLow: 14000,
    } as Awaited<ReturnType<typeof yahooFinance.quote>>)

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })
    const result = await source.getIndexQuote('^NDX')

    expect(result.ticker).toBe('^NDX')
    expect(result.price).toBeCloseTo(19800.5)
    expect(result.changePercent).toBeCloseTo(1.23)
    expect(result.volume).toBe(5000000)
    expect(result.high52w).toBe(21000)
    expect(result.low52w).toBe(14000)
  })

  it('returns cached result without calling yahoo-finance2 again', async () => {
    vi.mocked(yahooFinance.quote).mockResolvedValue({
      regularMarketPrice: 19800.5,
      regularMarketChangePercent: 1.23,
    } as Awaited<ReturnType<typeof yahooFinance.quote>>)

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })
    await source.getIndexQuote('^NDX')
    await source.getIndexQuote('^NDX')

    expect(yahooFinance.quote).toHaveBeenCalledTimes(1)
  })

  it('handles null fields from yahoo-finance2', async () => {
    vi.mocked(yahooFinance.quote).mockResolvedValue({
      regularMarketPrice: 3000,
    } as Awaited<ReturnType<typeof yahooFinance.quote>>)

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })
    const result = await source.getIndexQuote('^HSI')

    expect(result.changePercent).toBe(0)
    expect(result.volume).toBeNull()
    expect(result.high52w).toBeNull()
  })

  it('wraps Yahoo request failures with offline fallback guidance', async () => {
    vi.mocked(yahooFinance.quote).mockRejectedValue(new Error('getaddrinfo EAI_AGAIN'))

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })

    await expect(source.getIndexQuote('^NDX')).rejects.toThrow(
      'Yahoo Finance request failed; check network access or use USE_SEED_DATA=true for offline demos. ticker=^NDX.'
    )
  })

  it('wraps Yahoo rate limits with retry guidance', async () => {
    vi.mocked(yahooFinance.quote).mockRejectedValue(new Error('HTTP 429 Too Many Requests'))

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })

    await expect(source.getIndexQuote('^NDX')).rejects.toThrow(
      'Yahoo Finance rate limit may be active; retry later or use USE_SEED_DATA=true for offline demos. ticker=^NDX.'
    )
  })
})

describe('YahooFinanceSource.getIndexHistory', () => {
  it('fetches and maps historical data', async () => {
    vi.mocked(yahooFinance.historical).mockResolvedValue([
      { date: new Date('2024-06-01'), open: 19700, high: 19900, low: 19600, close: 19800, volume: 5000000, adjClose: 19800 },
      { date: new Date('2024-05-31'), open: 19500, high: 19750, low: 19400, close: 19700, volume: 4800000, adjClose: 19700 },
    ])

    const source = new YahooFinanceSource({ cacheDir: TEST_CACHE_DIR })
    const result = await source.getIndexHistory('^NDX', 7)

    expect(result).toHaveLength(2)
    expect(result[0].date).toBe('2024-06-01')
    expect(result[0].close).toBe(19800)
    expect(result[0].volume).toBe(5000000)
  })
})
