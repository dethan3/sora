import { tavily } from '@tavily/core'
import type { TavilyClient } from '@tavily/core'
import { writeCache } from '@sora/shared'
import type { ISearchSource, SearchResult } from './types.js'

interface Config {
  apiKey: string
  cacheDir: string
}

export class TavilySearchSource implements ISearchSource {
  private client: TavilyClient

  constructor(private config: Config) {
    this.client = tavily({ apiKey: config.apiKey })
  }

  async search(query: string, maxResults = 5): Promise<SearchResult[]> {
    const response = await this.client.search(query, { maxResults })

    const results: SearchResult[] = response.results.map((r) => ({
      title: r.title,
      url: r.url,
      content: r.content,
      score: r.score ?? null,
      publishedDate: r.publishedDate ?? null,
    }))

    const date = new Date().toISOString().split('T')[0]
    const safeQuery = query.slice(0, 40).replace(/[^\w\u4e00-\u9fff]/g, '-')
    writeCache(this.config.cacheDir, `search-results/${date}-${safeQuery}`, 'tavily', results)

    return results
  }
}
