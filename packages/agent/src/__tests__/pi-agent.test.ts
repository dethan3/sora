import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { PiResearchAgent } from '../pi-agent.js'
import type { ResearchCardInput } from '../types.js'

const TEST_CACHE_DIR = join(tmpdir(), `sora-pi-test-${Date.now()}`)

const SAMPLE_INPUT: ResearchCardInput = {
  market: { id: 'us-tech', name: '美国科技', category: 'us', description: '' },
  indexes: [{ id: 'nasdaq-100', name: 'Nasdaq 100', marketId: 'us-tech', ticker: '^NDX' }],
  fundAnalyses: [
    {
      fundId: 'fund-159941',
      executionQualityScore: 72,
      costScore: 90,
      liquidityScore: 80,
      premiumRiskScore: 50,
      trackingScore: 85,
      riskScore: 70,
      warnings: [{ level: 'watch', code: 'ELEVATED_PREMIUM', message: '存在一定溢价' }],
      summary: '执行质量评分 72/100，共 1 条风险提示',
      analyzedAt: '2024-06-01T10:00:00Z',
    },
  ],
}

const VALID_CARD_BODY = {
  title: '美国科技 国内基金执行质量观察',
  marketId: 'us-tech',
  relatedIndexIds: ['nasdaq-100'],
  relatedFundIds: ['fund-159941'],
  summary: '测试摘要',
  keyEvidence: ['评分 72/100'],
  fundExecutionRisks: ['存在一定溢价'],
  marketImplication: '美国科技板块对国内基金执行质量影响显著',
  risks: ['汇率波动风险'],
  invalidationConditions: ['溢价率回落'],
  status: 'watch',
}

beforeEach(() => {
  mkdirSync(TEST_CACHE_DIR, { recursive: true })
  vi.restoreAllMocks()
})

afterEach(() => {
  rmSync(TEST_CACHE_DIR, { recursive: true, force: true })
})

function mockPiFetch(responseBody: object, ok = true) {
  vi.stubGlobal(
    'fetch',
    vi.fn().mockResolvedValue({
      ok,
      status: ok ? 200 : 500,
      statusText: ok ? 'OK' : 'Internal Server Error',
      json: () => Promise.resolve(responseBody),
    })
  )
}

describe('PiResearchAgent.generateResearchCard', () => {
  it('returns validated ResearchCard from Pi response', async () => {
    mockPiFetch({
      choices: [{ message: { content: JSON.stringify(VALID_CARD_BODY) } }],
    })

    const agent = new PiResearchAgent({
      apiKey: 'test-key',
      baseUrl: 'https://api.pi.ai/v1',
      cacheDir: TEST_CACHE_DIR,
    })

    const card = await agent.generateResearchCard(SAMPLE_INPUT)
    expect(card.title).toBe('美国科技 国内基金执行质量观察')
    expect(card.marketId).toBe('us-tech')
    expect(card.status).toBe('watch')
    expect(card.id).toContain('card-us-tech')
    expect(card.generatedAt).toBeTruthy()
  })

  it('throws when Pi returns HTTP error', async () => {
    mockPiFetch({}, false)

    const agent = new PiResearchAgent({
      apiKey: 'test-key',
      baseUrl: 'https://api.pi.ai/v1',
      cacheDir: TEST_CACHE_DIR,
    })

    await expect(agent.generateResearchCard(SAMPLE_INPUT)).rejects.toThrow('Pi API error')
  })

  it('throws when Pi response fails schema validation', async () => {
    mockPiFetch({
      choices: [{ message: { content: JSON.stringify({ title: 'incomplete' }) } }],
    })

    const agent = new PiResearchAgent({
      apiKey: 'test-key',
      baseUrl: 'https://api.pi.ai/v1',
      cacheDir: TEST_CACHE_DIR,
    })

    await expect(agent.generateResearchCard(SAMPLE_INPUT)).rejects.toThrow()
  })
})
