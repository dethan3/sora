import { describe, it, expect } from 'vitest'
import type { FundAnalysis, ResearchCard } from '@sora/core'
import { fromFundAnalysis, fromResearchCard, collectEvents } from '../event-generator.js'

function makeAnalysis(overrides: Partial<FundAnalysis> = {}): FundAnalysis {
  return {
    fundId: 'fund-test',
    executionQualityScore: 75,
    costScore: 90,
    liquidityScore: 80,
    premiumRiskScore: 60,
    trackingScore: 85,
    riskScore: 70,
    warnings: [],
    summary: 'test',
    analyzedAt: '2024-06-01T10:00:00Z',
    ...overrides,
  }
}

function makeCard(overrides: Partial<ResearchCard> = {}): ResearchCard {
  return {
    id: 'card-test',
    title: '美国科技 国内基金执行质量观察',
    marketId: 'us-tech',
    relatedIndexIds: ['nasdaq-100'],
    relatedFundIds: ['fund-test'],
    summary: '测试摘要',
    keyEvidence: [],
    fundExecutionRisks: [],
    marketImplication: '影响分析',
    risks: [],
    invalidationConditions: [],
    status: 'watch',
    generatedAt: '2024-06-01T10:00:00Z',
    ...overrides,
  }
}

describe('fromFundAnalysis', () => {
  it('returns empty array when no warnings', () => {
    const events = fromFundAnalysis(makeAnalysis())
    expect(events).toHaveLength(0)
  })

  it('maps each warning to a NotificationEvent', () => {
    const analysis = makeAnalysis({
      warnings: [
        { level: 'warning', code: 'HIGH_PREMIUM', message: '高溢价风险' },
        { level: 'watch', code: 'PURCHASE_LIMITED', message: '限购' },
      ],
    })
    const events = fromFundAnalysis(analysis)
    expect(events).toHaveLength(2)
    expect(events[0].level).toBe('warning')
    expect(events[0].type).toBe('high_premium')
    expect(events[0].relatedEntityType).toBe('fund')
    expect(events[1].level).toBe('watch')
  })

  it('sets source to "sora"', () => {
    const analysis = makeAnalysis({
      warnings: [{ level: 'info', code: 'HIGH_VOLATILITY', message: '波动率较高' }],
    })
    const events = fromFundAnalysis(analysis)
    expect(events[0].source).toBe('sora')
  })
})

describe('fromResearchCard', () => {
  it('returns empty for "ignore" status', () => {
    expect(fromResearchCard(makeCard({ status: 'ignore' }))).toHaveLength(0)
  })

  it('returns empty for "invalidated" status', () => {
    expect(fromResearchCard(makeCard({ status: 'invalidated' }))).toHaveLength(0)
  })

  it('returns watch event for "watch" status', () => {
    const events = fromResearchCard(makeCard({ status: 'watch' }))
    expect(events).toHaveLength(1)
    expect(events[0].level).toBe('watch')
    expect(events[0].type).toBe('research_card')
  })

  it('returns warning event for "confirmed" status', () => {
    const events = fromResearchCard(makeCard({ status: 'confirmed' }))
    expect(events).toHaveLength(1)
    expect(events[0].level).toBe('warning')
  })

  it('returns watch event for "active_watch" status', () => {
    const events = fromResearchCard(makeCard({ status: 'active_watch' }))
    expect(events[0].level).toBe('watch')
  })
})

describe('collectEvents', () => {
  it('combines fund and card events', () => {
    const analysis = makeAnalysis({
      warnings: [{ level: 'warning', code: 'HIGH_PREMIUM', message: '高溢价' }],
    })
    const card = makeCard({ status: 'active_watch' })
    const events = collectEvents([analysis], card)
    expect(events.length).toBe(2)
    expect(events.some((e) => e.type === 'research_card')).toBe(true)
    expect(events.some((e) => e.type === 'high_premium')).toBe(true)
  })

  it('returns only fund events when card is null', () => {
    const analysis = makeAnalysis({
      warnings: [{ level: 'watch', code: 'SMALL_SCALE', message: '规模偏小' }],
    })
    const events = collectEvents([analysis], null)
    expect(events.every((e) => e.type !== 'research_card')).toBe(true)
  })
})
