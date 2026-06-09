import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { dbInit } from '../init.js'
import { dbSeed } from '../seed.js'
import { closeDb } from '../db.js'
import {
  findMarketById,
  listAllMarkets,
  listAllIndexes,
  listIndexesByMarket,
  findIndexById,
  findFundsByIndex,
  findFundsByMarket,
  getLatestFundMetrics,
  saveResearchCard,
  saveNotificationEvents,
  getThesisById,
  insertThesisEvidence,
  insertThesisUpdate,
  listAssetExposuresByThesis,
  listChallengedTheses,
  listTheses,
  listThesisEvidence,
  listThesisEvidenceByDirection,
  listThesisUpdates,
  updateThesisConfidence,
} from '../index.js'
import type { SoraDb } from '../db.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../../data/seeds')

let sqlite: Database.Database
let db: SoraDb

beforeEach(() => {
  const result = dbInit(':memory:')
  sqlite = result.sqlite
  db = result.db
})

afterEach(() => {
  closeDb(sqlite)
})

describe('dbInit', () => {
  it('creates all 12 tables', () => {
    const tables = sqlite
      .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
      .all() as Array<{ name: string }>
    const names = tables.map((t) => t.name)
    expect(names).toContain('asset_exposures')
    expect(names).toContain('markets')
    expect(names).toContain('indexes')
    expect(names).toContain('funds')
    expect(names).toContain('fund_index_mappings')
    expect(names).toContain('fund_metrics_snapshots')
    expect(names).toContain('research_cards')
    expect(names).toContain('alerts')
    expect(names).toContain('notification_events')
    expect(names).toContain('theses')
    expect(names).toContain('thesis_evidence')
    expect(names).toContain('thesis_updates')
    expect(names).toHaveLength(12)
  })

  it('is idempotent — calling twice does not throw', () => {
    sqlite.exec("CREATE TABLE IF NOT EXISTS markets (id TEXT PRIMARY KEY)")
    expect(() => dbInit(':memory:')).not.toThrow()
  })
})

describe('dbSeed', () => {
  it('imports all seed files and returns correct counts', () => {
    const stats = dbSeed(db, SEEDS_DIR)
    expect(stats.markets).toBe(6)
    expect(stats.indexes).toBeGreaterThanOrEqual(6)
    expect(stats.funds).toBeGreaterThanOrEqual(7)
    expect(stats.mappings).toBeGreaterThanOrEqual(7)
    expect(stats.metrics).toBeGreaterThanOrEqual(7)
    expect(stats.theses).toBe(5)
    expect(stats.thesisEvidence).toBeGreaterThanOrEqual(6)
    expect(stats.thesisUpdates).toBe(5)
    expect(stats.assetExposures).toBeGreaterThanOrEqual(10)
  })

  it('can be run repeatedly without duplicating rows', () => {
    dbSeed(db, SEEDS_DIR)
    expect(() => dbSeed(db, SEEDS_DIR)).not.toThrow()

    const marketCount = (
      sqlite.prepare('SELECT COUNT(*) as c FROM markets').get() as { c: number }
    ).c
    const fundCount = (
      sqlite.prepare('SELECT COUNT(*) as c FROM funds').get() as { c: number }
    ).c
    const thesisCount = (
      sqlite.prepare('SELECT COUNT(*) as c FROM theses').get() as { c: number }
    ).c

    expect(marketCount).toBe(6)
    expect(fundCount).toBeGreaterThanOrEqual(7)
    expect(thesisCount).toBe(5)
  })
})

describe('thesis queries', () => {
  beforeEach(() => dbSeed(db, SEEDS_DIR))

  it('lists and gets seeded theses', () => {
    const all = listTheses(db)
    expect(all).toHaveLength(5)

    const thesis = getThesisById(db, 'ai-infra')
    expect(thesis).not.toBeNull()
    expect(thesis!.confidence).toBe(72)
    expect(thesis!.affectedIndexIds).toContain('nasdaq-100')
  })

  it('lists evidence and filters by direction', () => {
    const timeline = listThesisEvidence(db, 'ai-infra')
    expect(timeline.length).toBeGreaterThanOrEqual(2)

    const support = listThesisEvidenceByDirection(db, 'ai-infra', 'support')
    const against = listThesisEvidenceByDirection(db, 'ai-infra', 'against')

    expect(support.every((e) => e.direction === 'support')).toBe(true)
    expect(against.every((e) => e.direction === 'against')).toBe(true)
  })

  it('inserts evidence and update, then updates thesis confidence', () => {
    insertThesisEvidence(db, {
      id: 'evidence-ai-infra-test',
      thesisId: 'ai-infra',
      source: 'test',
      title: 'Test evidence',
      summary: 'Test summary',
      direction: 'support',
      strength: 'weak',
      confidenceDelta: 2,
      rationale: 'Test rationale',
      observedAt: '2024-06-02T10:00:00Z',
      createdAt: '2024-06-02T10:00:00Z',
    })

    insertThesisUpdate(db, {
      id: 'update-ai-infra-test',
      thesisId: 'ai-infra',
      previousConfidence: 72,
      newConfidence: 74,
      evidenceIds: ['evidence-ai-infra-test'],
      rationale: 'Weak support evidence increased confidence.',
      createdAt: '2024-06-02T10:00:00Z',
    })

    updateThesisConfidence(db, 'ai-infra', 74, '2024-06-02T10:00:00Z')

    const thesis = getThesisById(db, 'ai-infra')
    const updates = listThesisUpdates(db, 'ai-infra')

    expect(thesis!.confidence).toBe(74)
    expect(updates[0].id).toBe('update-ai-infra-test')
    expect(updates[0].evidenceIds).toEqual(['evidence-ai-infra-test'])
  })

  it('lists asset exposures sorted by score', () => {
    const exposures = listAssetExposuresByThesis(db, 'ai-infra')
    expect(exposures.length).toBeGreaterThanOrEqual(3)
    expect(exposures[0].exposureScore).toBeGreaterThanOrEqual(exposures[1].exposureScore)
  })

  it('lists challenged theses', () => {
    const challenged = listChallengedTheses(db)
    expect(challenged.map((t) => t.id)).toContain('us-tech-valuation')
  })
})

describe('market queries', () => {
  beforeEach(() => dbSeed(db, SEEDS_DIR))

  it('listAllMarkets returns 6 markets', () => {
    expect(listAllMarkets(db)).toHaveLength(6)
  })

  it('findMarketById returns correct market', () => {
    const m = findMarketById(db, 'us-tech')
    expect(m).not.toBeNull()
    expect(m!.name).toBe('美国科技')
    expect(m!.category).toBe('us')
  })

  it('findMarketById returns null for unknown id', () => {
    expect(findMarketById(db, 'nope')).toBeNull()
  })
})

describe('index queries', () => {
  beforeEach(() => dbSeed(db, SEEDS_DIR))

  it('listAllIndexes returns all indexes', () => {
    expect(listAllIndexes(db).length).toBeGreaterThanOrEqual(6)
  })

  it('listIndexesByMarket returns Nasdaq 100 for us-tech', () => {
    const idx = listIndexesByMarket(db, 'us-tech')
    expect(idx).toHaveLength(1)
    expect(idx[0].id).toBe('nasdaq-100')
    expect(idx[0].ticker).toBe('^NDX')
  })

  it('findIndexById returns correct index', () => {
    const idx = findIndexById(db, 'nasdaq-100')
    expect(idx).not.toBeNull()
    expect(idx!.marketId).toBe('us-tech')
  })
})

describe('fund queries', () => {
  beforeEach(() => dbSeed(db, SEEDS_DIR))

  it('findFundsByIndex returns funds for nasdaq-100', () => {
    const fs = findFundsByIndex(db, 'nasdaq-100')
    expect(fs.length).toBeGreaterThanOrEqual(3)
    expect(fs.every((f) => f.trackingIndexId === 'nasdaq-100')).toBe(true)
  })

  it('findFundsByMarket returns funds for hk-tech', () => {
    const fs = findFundsByMarket(db, 'hk-tech')
    expect(fs.length).toBeGreaterThanOrEqual(2)
  })

  it('getLatestFundMetrics returns metrics for known fund', () => {
    const m = getLatestFundMetrics(db, 'fund-159941')
    expect(m).not.toBeNull()
    expect(m!.nav).toBeGreaterThan(0)
  })

  it('getLatestFundMetrics returns null for unknown fund', () => {
    expect(getLatestFundMetrics(db, 'fund-000000')).toBeNull()
  })
})

describe('research & notification queries', () => {
  it('saveResearchCard inserts and is retrievable via raw SQL', () => {
    saveResearchCard(db, {
      id: 'card-test-001',
      title: '美国科技 基金执行质量观察',
      marketId: 'us-tech',
      relatedIndexIds: ['nasdaq-100'],
      relatedFundIds: ['fund-159941'],
      summary: '测试摘要',
      keyEvidence: ['评分 75/100'],
      fundExecutionRisks: [],
      marketImplication: '汇率波动',
      risks: ['数据延迟'],
      invalidationConditions: ['溢价回落'],
      status: 'watch',
      generatedAt: '2024-06-01T10:00:00Z',
    })

    const row = sqlite
      .prepare('SELECT * FROM research_cards WHERE id = ?')
      .get('card-test-001') as { id: string; title: string; related_index_ids: string } | undefined
    expect(row).not.toBeUndefined()
    expect(row!.title).toBe('美国科技 基金执行质量观察')
    expect(JSON.parse(row!.related_index_ids)).toContain('nasdaq-100')
  })

  it('saveNotificationEvents inserts multiple events', () => {
    saveNotificationEvents(db, [
      {
        id: 'evt-001',
        level: 'warning',
        title: '高溢价风险',
        summary: '溢价率 4.2%',
        source: 'sora',
        type: 'high_premium',
        relatedEntityType: 'fund',
        relatedEntityId: 'fund-159941',
        payload: { premiumRate: 0.042 },
        createdAt: '2024-06-01T10:00:00Z',
      },
      {
        id: 'evt-002',
        level: 'watch',
        title: '限购提示',
        summary: '限购中',
        source: 'sora',
        type: 'purchase_limited',
        createdAt: '2024-06-01T10:00:00Z',
      },
    ])

    const count = (
      sqlite.prepare('SELECT COUNT(*) as c FROM notification_events').get() as { c: number }
    ).c
    expect(count).toBe(2)
  })
})
