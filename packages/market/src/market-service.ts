import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Market, Index } from '@sora/core'
import type { IMarketQuoteSource, IndexQuote } from '@sora/sources'

export interface IndexWithQuote extends Index {
  quote: IndexQuote | null
}

export class MarketService {
  constructor(
    private seedsDir: string,
    private marketSource: IMarketQuoteSource
  ) {}

  private readSeeds<T>(filename: string): T[] {
    const content = readFileSync(join(this.seedsDir, filename), 'utf-8')
    return JSON.parse(content) as T[]
  }

  async listMarkets(): Promise<Market[]> {
    return this.readSeeds<Market>('markets.json')
  }

  async getMarket(id: string): Promise<Market | null> {
    const markets = await this.listMarkets()
    return markets.find((m) => m.id === id) ?? null
  }

  async listIndexes(): Promise<Index[]> {
    return this.readSeeds<Index>('indexes.json')
  }

  async listIndexesByMarket(marketId: string): Promise<Index[]> {
    const indexes = await this.listIndexes()
    return indexes.filter((i) => i.marketId === marketId)
  }

  async getIndexWithQuote(indexId: string): Promise<IndexWithQuote | null> {
    const indexes = await this.listIndexes()
    const index = indexes.find((i) => i.id === indexId)
    if (!index) return null

    let quote: IndexQuote | null = null
    try {
      quote = await this.marketSource.getIndexQuote(index.ticker)
    } catch {
      // source unavailable — return index without live quote
    }

    return { ...index, quote }
  }
}
