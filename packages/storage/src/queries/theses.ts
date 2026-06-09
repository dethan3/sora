import { desc, eq } from 'drizzle-orm'
import type { AssetExposure, EvidenceDirection, Thesis, ThesisEvidence, ThesisUpdate } from '@sora/core'
import type { SoraDb } from '../db.js'
import { assetExposures, theses, thesisEvidence, thesisUpdates } from '../schema.js'

function parseJsonArray(value: string): string[] {
  return JSON.parse(value) as string[]
}

function rowToThesis(row: typeof theses.$inferSelect): Thesis {
  return {
    id: row.id,
    title: row.title,
    summary: row.summary,
    timeHorizon: row.timeHorizon as Thesis['timeHorizon'],
    status: row.status as Thesis['status'],
    confidence: row.confidence,
    causalChain: parseJsonArray(row.causalChain),
    keyAssumptions: parseJsonArray(row.keyAssumptions),
    affectedMarketIds: parseJsonArray(row.affectedMarketIds),
    affectedIndexIds: parseJsonArray(row.affectedIndexIds),
    affectedFundIds: parseJsonArray(row.affectedFundIds),
    invalidationConditions: parseJsonArray(row.invalidationConditions),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  }
}

function rowToEvidence(row: typeof thesisEvidence.$inferSelect): ThesisEvidence {
  return {
    id: row.id,
    thesisId: row.thesisId,
    source: row.source,
    title: row.title,
    summary: row.summary,
    url: row.url ?? undefined,
    direction: row.direction as ThesisEvidence['direction'],
    strength: row.strength as ThesisEvidence['strength'],
    confidenceDelta: row.confidenceDelta,
    rationale: row.rationale,
    observedAt: row.observedAt,
    createdAt: row.createdAt,
  }
}

function rowToUpdate(row: typeof thesisUpdates.$inferSelect): ThesisUpdate {
  return {
    id: row.id,
    thesisId: row.thesisId,
    previousConfidence: row.previousConfidence,
    newConfidence: row.newConfidence,
    evidenceIds: parseJsonArray(row.evidenceIds),
    rationale: row.rationale,
    createdAt: row.createdAt,
  }
}

function rowToExposure(row: typeof assetExposures.$inferSelect): AssetExposure {
  return {
    id: row.id,
    thesisId: row.thesisId,
    assetType: row.assetType as AssetExposure['assetType'],
    assetId: row.assetId,
    exposureScore: row.exposureScore,
    rationale: row.rationale,
    updatedAt: row.updatedAt,
  }
}

export function listTheses(db: SoraDb): Thesis[] {
  return db.select().from(theses).orderBy(desc(theses.updatedAt)).all().map(rowToThesis)
}

export function getThesisById(db: SoraDb, id: string): Thesis | null {
  const row = db.select().from(theses).where(eq(theses.id, id)).get()
  return row ? rowToThesis(row) : null
}

export function listThesisEvidence(db: SoraDb, thesisId: string): ThesisEvidence[] {
  return db
    .select()
    .from(thesisEvidence)
    .where(eq(thesisEvidence.thesisId, thesisId))
    .orderBy(desc(thesisEvidence.observedAt))
    .all()
    .map(rowToEvidence)
}

export function listThesisEvidenceByDirection(
  db: SoraDb,
  thesisId: string,
  direction: EvidenceDirection
): ThesisEvidence[] {
  return listThesisEvidence(db, thesisId).filter((evidence) => evidence.direction === direction)
}

export function insertThesisEvidence(db: SoraDb, evidence: ThesisEvidence): void {
  db.insert(thesisEvidence).values({
    id: evidence.id,
    thesisId: evidence.thesisId,
    source: evidence.source,
    title: evidence.title,
    summary: evidence.summary,
    url: evidence.url ?? null,
    direction: evidence.direction,
    strength: evidence.strength,
    confidenceDelta: evidence.confidenceDelta,
    rationale: evidence.rationale,
    observedAt: evidence.observedAt,
    createdAt: evidence.createdAt,
  }).run()
}

export function insertThesisUpdate(db: SoraDb, update: ThesisUpdate): void {
  db.insert(thesisUpdates).values({
    id: update.id,
    thesisId: update.thesisId,
    previousConfidence: update.previousConfidence,
    newConfidence: update.newConfidence,
    evidenceIds: JSON.stringify(update.evidenceIds),
    rationale: update.rationale,
    createdAt: update.createdAt,
  }).run()
}

export function updateThesisConfidence(
  db: SoraDb,
  thesisId: string,
  confidence: number,
  updatedAt: string,
  status?: Thesis['status']
): void {
  db.update(theses)
    .set({
      confidence,
      updatedAt,
      ...(status ? { status } : {}),
    })
    .where(eq(theses.id, thesisId))
    .run()
}

export function listThesisUpdates(db: SoraDb, thesisId: string): ThesisUpdate[] {
  return db
    .select()
    .from(thesisUpdates)
    .where(eq(thesisUpdates.thesisId, thesisId))
    .orderBy(desc(thesisUpdates.createdAt))
    .all()
    .map(rowToUpdate)
}

export function listAssetExposuresByThesis(db: SoraDb, thesisId: string): AssetExposure[] {
  return db
    .select()
    .from(assetExposures)
    .where(eq(assetExposures.thesisId, thesisId))
    .orderBy(desc(assetExposures.exposureScore))
    .all()
    .map(rowToExposure)
}

export function listChallengedTheses(db: SoraDb): Thesis[] {
  return db
    .select()
    .from(theses)
    .where(eq(theses.status, 'challenged'))
    .orderBy(desc(theses.updatedAt))
    .all()
    .map(rowToThesis)
}
