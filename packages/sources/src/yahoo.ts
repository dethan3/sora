import yahooFinance from 'yahoo-finance2'
import { readCache, writeCache, isCacheStale, ONE_DAY_MS } from '@sora/shared'
import type { IMarketQuoteSource, IndexQuote, IndexHistoricalQuote } from './types.js'

interface Config {
  cacheDir: string
  cacheTtlMs?: number
}

function safeTicker(ticker: string): string {
  return ticker.replace(/[^a-zA-Z0-9_-]/g, '_')
}

export class YahooFinanceSource implements IMarketQuoteSource {
  private ttl: number

  constructor(private config: Config) {
    this.ttl = config.cacheTtlMs ?? ONE_DAY_MS
  }

  async getIndexQuote(ticker: string): Promise<IndexQuote> {
    const cacheKey = `market-quotes/${safeTicker(ticker)}`
    const cached = readCache<IndexQuote>(this.config.cacheDir, cacheKey)
    if (cached && !isCacheStale(cached, this.ttl)) {
      return cached.data
    }

    const quote = await yahooFinance.quote(ticker)
    const result: IndexQuote = {
      ticker,
      price: quote.regularMarketPrice ?? 0,
      changePercent: quote.regularMarketChangePercent ?? 0,
      volume: quote.regularMarketVolume ?? null,
      high52w: quote.fiftyTwoWeekHigh ?? null,
      low52w: quote.fiftyTwoWeekLow ?? null,
      fetchedAt: new Date().toISOString(),
    }

    writeCache(this.config.cacheDir, cacheKey, 'yahoo-finance', result)
    return result
  }

  async getIndexHistory(ticker: string, days: number): Promise<IndexHistoricalQuote[]> {
    const cacheKey = `market-quotes/${safeTicker(ticker)}-history-${days}d`
    const cached = readCache<IndexHistoricalQuote[]>(this.config.cacheDir, cacheKey)
    if (cached && !isCacheStale(cached, this.ttl)) {
      return cached.data
    }

    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000)
    const history = await yahooFinance.historical(ticker, {
      period1: startDate.toISOString().split('T')[0],
    })

    const result: IndexHistoricalQuote[] = history.map((h) => ({
      date: h.date.toISOString().split('T')[0],
      open: h.open ?? 0,
      high: h.high ?? 0,
      low: h.low ?? 0,
      close: h.close ?? 0,
      volume: h.volume ?? null,
    }))

    writeCache(this.config.cacheDir, cacheKey, 'yahoo-finance', result)
    return result
  }
}
