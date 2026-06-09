import { describe, it, expect } from 'vitest'
import type { ResearchCardInput } from '@sora/agent'
import { generateDeterministicCard } from '../deterministic.js'

function makeInput(overrides: Partial<ResearchCardInput> = {}): ResearchCardInput {
  return {
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
        warnings: [],
        summary: '',
        analyzedAt: '2024-06-01T10:00:00Z',
      },
    ],
    ...overrides,
  }
}

describe('generateDeterministicCard — structure', () => {
  it('produces a ResearchCard with all required fields', () => {
    const card = generateDeterministicCard(makeInput())
    expect(card.id).toContain('card-us-tech')
    expect(card.title).toBe('美国科技 国内基金执行质量观察')
    expect(card.marketId).toBe('us-tech')
    expect(card.relatedIndexIds).toContain('nasdaq-100')
    expect(card.relatedFundIds).toContain('fund-159941')
    expect(card.summary).toContain('美国科技')
    expect(card.keyEvidence.length).toBeGreaterThan(0)
    expect(card.risks.length).toBeGreaterThan(0)
    expect(card.invalidationConditions.length).toBeGreaterThan(0)
    expect(card.generatedAt).toBeTruthy()
  })
})

describe('generateDeterministicCard — status logic', () => {
  it('status is "ignore" when no warnings', () => {
    const card = generateDeterministicCard(makeInput())
    expect(card.status).toBe('ignore')
  })

  it('status is "watch" when only watch-level warnings', () => {
    const input = makeInput()
    input.fundAnalyses[0].warnings = [{ level: 'watch', code: 'ELEVATED_PREMIUM', message: '一定溢价' }]
    const card = generateDeterministicCard(input)
    expect(card.status).toBe('watch')
  })

  it('status is "active_watch" when any warning-level warning', () => {
    const input = makeInput()
    input.fundAnalyses[0].warnings = [{ level: 'warning', code: 'HIGH_PREMIUM', message: '高溢价' }]
    const card = generateDeterministicCard(input)
    expect(card.status).toBe('active_watch')
  })
})

describe('generateDeterministicCard — key evidence', () => {
  it('mentions average score in keyEvidence', () => {
    const card = generateDeterministicCard(makeInput())
    expect(card.keyEvidence.some((e) => e.includes('72'))).toBe(true)
  })

  it('mentions warning count when warnings present', () => {
    const input = makeInput()
    input.fundAnalyses[0].warnings = [{ level: 'warning', code: 'HIGH_PREMIUM', message: '高溢价' }]
    const card = generateDeterministicCard(input)
    expect(card.keyEvidence.some((e) => e.includes('warning'))).toBe(true)
  })
})

describe('generateDeterministicCard — fund execution risks', () => {
  it('includes warning messages in fundExecutionRisks', () => {
    const input = makeInput()
    input.fundAnalyses[0].warnings = [
      { level: 'warning', code: 'HIGH_PREMIUM', message: '高溢价风险（溢价率 4.00%）' },
    ]
    const card = generateDeterministicCard(input)
    expect(card.fundExecutionRisks.some((r) => r.includes('高溢价'))).toBe(true)
  })

  it('returns empty fundExecutionRisks when no warnings', () => {
    const card = generateDeterministicCard(makeInput())
    expect(card.fundExecutionRisks).toHaveLength(0)
  })
})
