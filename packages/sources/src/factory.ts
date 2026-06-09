import { YahooFinanceSource } from './yahoo.js'
import { EastMoneyFundSource } from './eastmoney.js'
import { TavilySearchSource } from './tavily.js'
import { SeedSource } from './seed.js'
import type { IMarketQuoteSource, IFundDataSource, ISearchSource } from './types.js'

interface Env {
  SORA_CACHE_DIR?: string
  SORA_SEEDS_DIR?: string
  TAVILY_API_KEY?: string
  USE_SEED_DATA?: string
}

function resolveCacheDir(env: Env): string {
  return env.SORA_CACHE_DIR ?? './data/cache'
}

function resolveSeedsDir(env: Env): string {
  return env.SORA_SEEDS_DIR ?? './data/seeds'
}

export function createMarketSource(env: Env = process.env): IMarketQuoteSource {
  if (env.USE_SEED_DATA === 'true') {
    return new SeedSource(resolveSeedsDir(env))
  }
  return new YahooFinanceSource({ cacheDir: resolveCacheDir(env) })
}

export function createFundSource(env: Env = process.env): IFundDataSource {
  if (env.USE_SEED_DATA === 'true') {
    return new SeedSource(resolveSeedsDir(env))
  }
  return new EastMoneyFundSource({ cacheDir: resolveCacheDir(env) })
}

export function createSearchSource(env: Env = process.env): ISearchSource {
  const apiKey = env.TAVILY_API_KEY
  if (!apiKey) throw new Error('TAVILY_API_KEY is required for TavilySearchSource')
  return new TavilySearchSource({ apiKey, cacheDir: resolveCacheDir(env) })
}
