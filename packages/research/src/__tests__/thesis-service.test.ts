import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { join } from 'node:path'
import type { SoraDb } from '@sora/storage'
import { closeDb, dbInit, dbSeed } from '@sora/storage'
import {
  addEvidence,
  calculateConfidenceDelta,
  clampConfidence,
  getAssetExposure,
  getContradictions,
  getEvidenceGroups,
  getEvidenceTimeline,
  getThesis,
  listTheses,
  reviewTheses,
} from '../thesis-service.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../../data/seeds')

let sqlite: ReturnType<typeof dbInit>['sqlite']
let db: SoraDb

beforeEach(() => {
  const result = dbInit(':memory:')
  sqlite = result.sqlite
  db = result.db
  dbSeed(db, SEEDS_DIR)
})

afterEach(() => {
  closeDb(sqlite)
})

describe('thesis workflow confidence rules', () => {
  it('maps evidence direction and strength to deterministic deltas', () => {
    expect(calculateConfidenceDelta('support', 'strong')).toBe(8)
    expect(calculateConfidenceDelta('support', 'medium')).toBe(4)
    expect(calculateConfidenceDelta('support', 'weak')).toBe(2)
    expect(calculateConfidenceDelta('neutral', 'strong')).toBe(0)
    expect(calculateConfidenceDelta('against', 'weak')).toBe(-2)
    expect(calculateConfidenceDelta('against', 'medium')).toBe(-4)
    expect(calculateConfidenceDelta('against', 'strong')).toBe(-8)
  })

  it('clamps confidence to the valid range', () => {
    expect(clampConfidence(-1)).toBe(0)
    expect(clampConfidence(50)).toBe(50)
    expect(clampConfidence(101)).toBe(100)
  })
})

describe('thesis workflow service', () => {
  it('lists and gets theses through the workflow API', () => {
    const all = listTheses(db)
    const thesis = getThesis(db, 'ai-infra')

    expect(all).toHaveLength(5)
    expect(thesis?.title).toContain('AI')
  })

  it('adds support evidence and records the confidence update', () => {
    const result = addEvidence(db, {
      id: 'evidence-ai-infra-phase2-support',
      thesisId: 'ai-infra',
      source: 'phase2-test',
      title: 'AI capex guidance remained resilient',
      summary: 'Hyperscaler guidance continues to support AI infrastructure demand.',
      direction: 'support',
      strength: 'medium',
      rationale: 'Medium support evidence should add four confidence points.',
      observedAt: '2024-06-03T10:00:00Z',
      createdAt: '2024-06-03T10:00:00Z',
    })

    expect(result.evidence.confidenceDelta).toBe(4)
    expect(result.update.previousConfidence).toBe(72)
    expect(result.update.newConfidence).toBe(76)
    expect(result.thesis.confidence).toBe(76)
    expect(getEvidenceTimeline(db, 'ai-infra')[0].id).toBe('evidence-ai-infra-phase2-support')
  })

  it('records neutral evidence without changing confidence', () => {
    const result = addEvidence(db, {
      id: 'evidence-ai-infra-phase2-neutral',
      thesisId: 'ai-infra',
      source: 'phase2-test',
      title: 'AI ETF flows were mixed',
      summary: 'Fund flow data is not directional enough to move the thesis.',
      direction: 'neutral',
      strength: 'strong',
      rationale: 'Neutral evidence should be stored but have zero delta.',
      observedAt: '2024-06-04T10:00:00Z',
      createdAt: '2024-06-04T10:00:00Z',
    })

    expect(result.evidence.confidenceDelta).toBe(0)
    expect(result.update.previousConfidence).toBe(result.update.newConfidence)
    expect(result.thesis.confidence).toBe(72)
  })

  it('marks a thesis as challenged after strong contradicting evidence', () => {
    const result = addEvidence(db, {
      id: 'evidence-ai-infra-phase2-against',
      thesisId: 'ai-infra',
      source: 'phase2-test',
      title: 'AI hardware backlog weakened',
      summary: 'Backlog indicators weakened enough to challenge the thesis.',
      direction: 'against',
      strength: 'strong',
      rationale: 'Strong against evidence should challenge the thesis.',
      observedAt: '2024-06-05T10:00:00Z',
      createdAt: '2024-06-05T10:00:00Z',
    })

    const contradictions = getContradictions(db, 'ai-infra')

    expect(result.evidence.confidenceDelta).toBe(-8)
    expect(result.thesis.status).toBe('challenged')
    expect(contradictions.isChallenged).toBe(true)
    expect(contradictions.strongestAgainstEvidence[0].id).toBe('evidence-ai-infra-phase2-against')
  })

  it('groups support, against, and neutral evidence separately', () => {
    addEvidence(db, {
      id: 'evidence-ai-infra-phase2-neutral-group',
      thesisId: 'ai-infra',
      source: 'phase2-test',
      title: 'Mixed AI survey data',
      summary: 'Survey data is mixed.',
      direction: 'neutral',
      strength: 'weak',
      rationale: 'Used to verify neutral grouping.',
      observedAt: '2024-06-06T10:00:00Z',
      createdAt: '2024-06-06T10:00:00Z',
    })

    const groups = getEvidenceGroups(db, 'ai-infra')

    expect(groups.support.every((item) => item.direction === 'support')).toBe(true)
    expect(groups.against.every((item) => item.direction === 'against')).toBe(true)
    expect(groups.neutral.map((item) => item.id)).toContain('evidence-ai-infra-phase2-neutral-group')
  })

  it('reviews seed data and returns the MVP thesis workbench slices', () => {
    const review = reviewTheses(db)

    expect(review.changedTheses.length).toBeGreaterThan(0)
    expect(review.strongestTheses[0].confidence).toBeGreaterThanOrEqual(
      review.strongestTheses[1].confidence
    )
    expect(review.challengedTheses.map((thesis) => thesis.id)).toContain('us-tech-valuation')
    expect(review.latestSupportEvidence.every((item) => item.direction === 'support')).toBe(true)
    expect(review.latestAgainstEvidence.every((item) => item.direction === 'against')).toBe(true)
  })

  it('returns thesis exposure sorted by exposure score', () => {
    const exposure = getAssetExposure(db, 'ai-infra')

    expect(exposure.length).toBeGreaterThanOrEqual(3)
    expect(exposure[0].exposureScore).toBeGreaterThanOrEqual(exposure[1].exposureScore)
  })
})
