import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'

const mockSearch = vi.fn()
vi.mock('@tavily/core', () => ({
  tavily: () => ({ search: mockSearch }),
}))

import { TavilySearchSource } from '../tavily.js'

const FIXTURES = join(import.meta.dirname, 'fixtures')
const TEST_CACHE_DIR = join(tmpdir(), `sora-tavily-test-${Date.now()}`)

beforeEach(() => {
  mkdirSync(TEST_CACHE_DIR, { recursive: true })
  vi.clearAllMocks()
})

afterEach(() => {
  rmSync(TEST_CACHE_DIR, { recursive: true, force: true })
})

describe('TavilySearchSource.search', () => {
  it('returns mapped search results', async () => {
    const fixture = JSON.parse(readFileSync(join(FIXTURES, 'tavily-search.json'), 'utf-8'))
    mockSearch.mockResolvedValue(fixture)

    const source = new TavilySearchSource({ apiKey: 'test-key', cacheDir: TEST_CACHE_DIR })
    const results = await source.search('纳指100 国内ETF 申购限制', 5)

    expect(results).toHaveLength(2)
    expect(results[0].title).toBe('易方达纳指ETF限购公告')
    expect(results[0].url).toBe('https://example.com/news/1')
    expect(results[0].score).toBeCloseTo(0.92)
    expect(results[0].publishedDate).toBe('2024-06-01')
    expect(results[1].title).toBe('华夏纳指ETF最新动态')
  })

  it('writes result to cache', async () => {
    const fixture = JSON.parse(readFileSync(join(FIXTURES, 'tavily-search.json'), 'utf-8'))
    mockSearch.mockResolvedValue(fixture)

    const source = new TavilySearchSource({ apiKey: 'test-key', cacheDir: TEST_CACHE_DIR })
    await source.search('test query', 3)

    expect(mockSearch).toHaveBeenCalledWith('test query', { maxResults: 3 })
  })

  it('handles missing score gracefully', async () => {
    mockSearch.mockResolvedValue({
      results: [{ title: 'T', url: 'http://x', content: 'c', publishedDate: null }],
    })

    const source = new TavilySearchSource({ apiKey: 'test-key', cacheDir: TEST_CACHE_DIR })
    const results = await source.search('q')

    expect(results[0].score).toBeNull()
    expect(results[0].publishedDate).toBeNull()
  })
})
