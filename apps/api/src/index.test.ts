import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { join } from 'node:path'
import { closeDb, dbInit, dbSeed } from '@sora/storage'
import type { SoraDb } from '@sora/storage'
import { handleApiRequest } from './index.js'

const SEEDS_DIR = join(import.meta.dirname, '../../../data/seeds')

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

describe('Sora API', () => {
  it('returns health status with disclaimer', () => {
    const res = handleApiRequest(db, { method: 'GET', path: '/health' })
    const body = res.body as { data: { ok: boolean }; disclaimer: string }

    expect(res.status).toBe(200)
    expect(body.data.ok).toBe(true)
    expect(body.disclaimer).toContain('不构成任何投资建议')
  })

  it('lists seeded theses as JSON', () => {
    const res = handleApiRequest(db, { method: 'GET', path: '/api/theses' })
    const body = res.body as { data: Array<{ id: string; confidence: number }> }

    expect(res.status).toBe(200)
    expect(body.data.map((item) => item.id)).toContain('ai-infra')
  })

  it('returns validation errors for invalid evidence input', () => {
    const res = handleApiRequest(db, {
      method: 'POST',
      path: '/api/theses/ai-infra/evidence',
      body: { title: 'bad', direction: 'wrong', strength: 'medium' },
    })
    const body = res.body as { error: { details: string[] } }

    expect(res.status).toBe(400)
    expect(body.error.details).toContain('direction must be support, against, or neutral.')
  })

  it('adds evidence through the workflow service', () => {
    const res = handleApiRequest(db, {
      method: 'POST',
      path: '/api/theses/ai-infra/evidence',
      body: {
        title: 'API test evidence',
        direction: 'support',
        strength: 'medium',
      },
    })
    const body = res.body as {
      data: {
        evidence: { confidenceDelta: number }
        update: { previousConfidence: number; newConfidence: number }
      }
    }

    expect(res.status).toBe(201)
    expect(body.data.evidence.confidenceDelta).toBe(4)
    expect(body.data.update.previousConfidence).toBe(72)
    expect(body.data.update.newConfidence).toBe(76)
  })
})
