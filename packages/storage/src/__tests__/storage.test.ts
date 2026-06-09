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
  it('creates all 8 tables', () => {
    const tables = sqlite
      .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
      .all() as Array<{ name: string }>
    const names = tables.map((t) => t.name)
    expect(names).toContain('markets')
    expect(names).toContain('indexes')
    expect(names).toContain('funds')
    expect(names).toContain('fund_index_mappings')
    expect(names).toContain('fund_metrics_snapshots')
    expect(names).toContain('research_cards')
    expect(names).toContain('alerts')
    expect(names).toContain('notification_events')
    expect(names).toHaveLength(8)
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
