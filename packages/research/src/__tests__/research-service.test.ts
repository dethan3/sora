import { describe, it, expect, vi } from 'vitest'
import type { ResearchCardInput } from '@sora/agent'
import { ResearchService } from '../research-service.js'

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
      warnings: [],
      summary: '',
      analyzedAt: '2024-06-01T10:00:00Z',
    },
  ],
}

describe('ResearchService — deterministic mode (no agent)', () => {
  it('generates card without agent', async () => {
    const svc = new ResearchService(null)
    const card = await svc.generateCard(SAMPLE_INPUT)
    expect(card.marketId).toBe('us-tech')
    expect(card.title).toContain('美国科技')
  })
})

describe('ResearchService — Pi mode', () => {
  it('uses agent when provided', async () => {
    const mockCard = {
      id: 'card-pi-test',
      title: 'Pi generated card',
      marketId: 'us-tech',
      relatedIndexIds: [],
      relatedFundIds: [],
      summary: 'Pi summary',
      keyEvidence: [],
      fundExecutionRisks: [],
      marketImplication: 'test',
      risks: [],
      invalidationConditions: [],
      status: 'watch' as const,
      generatedAt: new Date().toISOString(),
    }

    const mockAgent = {
      generateResearchCard: vi.fn().mockResolvedValue(mockCard),
      summarizeMarketSignal: vi.fn(),
      explainTransmissionPath: vi.fn(),
      generateFollowUpTasks: vi.fn(),
    }

    const svc = new ResearchService(mockAgent)
    const card = await svc.generateCard(SAMPLE_INPUT)

    expect(mockAgent.generateResearchCard).toHaveBeenCalledOnce()
    expect(card.title).toBe('Pi generated card')
  })

  it('falls back to deterministic when agent throws', async () => {
    const mockAgent = {
      generateResearchCard: vi.fn().mockRejectedValue(new Error('Pi API error: 503')),
      summarizeMarketSignal: vi.fn(),
      explainTransmissionPath: vi.fn(),
      generateFollowUpTasks: vi.fn(),
    }

    const svc = new ResearchService(mockAgent)
    const card = await svc.generateCard(SAMPLE_INPUT)

    expect(card.title).toBe('美国科技 国内基金执行质量观察')
  })
})
