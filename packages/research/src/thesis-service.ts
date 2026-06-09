import type {
  AssetExposure,
  EvidenceDirection,
  EvidenceStrength,
  Thesis,
  ThesisEvidence,
  ThesisStatus,
  ThesisUpdate,
} from '@sora/core'
import {
  getThesisById,
  insertThesisEvidence,
  insertThesisUpdate,
  listAssetExposuresByThesis,
  listChallengedTheses,
  listTheses as queryListTheses,
  listThesisEvidence,
  listThesisEvidenceByDirection,
  listThesisUpdates,
  updateThesisConfidence,
  type SoraDb,
} from '@sora/storage'
import { randomUUID } from 'node:crypto'

const CHALLENGED_CONFIDENCE_THRESHOLD = 50

const CONFIDENCE_DELTAS: Record<EvidenceDirection, Record<EvidenceStrength, number>> = {
  support: {
    weak: 2,
    medium: 4,
    strong: 8,
  },
  neutral: {
    weak: 0,
    medium: 0,
    strong: 0,
  },
  against: {
    weak: -2,
    medium: -4,
    strong: -8,
  },
}

const STRENGTH_RANK: Record<EvidenceStrength, number> = {
  weak: 1,
  medium: 2,
  strong: 3,
}

export type AddEvidenceInput = Omit<ThesisEvidence, 'id' | 'confidenceDelta' | 'createdAt'> & {
  id?: string
  createdAt?: string
}

export interface AddEvidenceResult {
  thesis: Thesis
  evidence: ThesisEvidence
  update: ThesisUpdate
}

export interface EvidenceGroups {
  support: ThesisEvidence[]
  against: ThesisEvidence[]
  neutral: ThesisEvidence[]
}

export interface ContradictionSummary {
  thesis: Thesis
  supportCount: number
  againstCount: number
  neutralCount: number
  strongestAgainstEvidence: ThesisEvidence[]
  isChallenged: boolean
}

export interface ThesisReview {
  changedTheses: Thesis[]
  strongestTheses: Thesis[]
  challengedTheses: Thesis[]
  recentlyUpdatedTheses: Thesis[]
  latestSupportEvidence: ThesisEvidence[]
  latestAgainstEvidence: ThesisEvidence[]
}

export function calculateConfidenceDelta(
  direction: EvidenceDirection,
  strength: EvidenceStrength
): number {
  return CONFIDENCE_DELTAS[direction][strength]
}

export function clampConfidence(confidence: number): number {
  return Math.max(0, Math.min(100, confidence))
}

export function listTheses(db: SoraDb): Thesis[] {
  return queryListTheses(db)
}

export function getThesis(db: SoraDb, thesisId: string): Thesis | null {
  return getThesisById(db, thesisId)
}

export function addEvidence(db: SoraDb, input: AddEvidenceInput): AddEvidenceResult {
  const thesis = getThesisById(db, input.thesisId)
  if (!thesis) {
    throw new Error(`Thesis not found: ${input.thesisId}`)
  }

  const now = input.createdAt ?? new Date().toISOString()
  const evidence: ThesisEvidence = {
    ...input,
    id: input.id ?? `evidence-${randomUUID()}`,
    confidenceDelta: calculateConfidenceDelta(input.direction, input.strength),
    createdAt: now,
  }
  const newConfidence = clampConfidence(thesis.confidence + evidence.confidenceDelta)
  const status = nextThesisStatus(thesis.status, evidence, newConfidence)
  const update: ThesisUpdate = {
    id: `update-${evidence.id}`,
    thesisId: thesis.id,
    previousConfidence: thesis.confidence,
    newConfidence,
    evidenceIds: [evidence.id],
    rationale: buildUpdateRationale(evidence, thesis.confidence, newConfidence),
    createdAt: now,
  }

  insertThesisEvidence(db, evidence)
  insertThesisUpdate(db, update)
  updateThesisConfidence(db, thesis.id, newConfidence, now, status)

  const updatedThesis = getThesisById(db, thesis.id)
  if (!updatedThesis) {
    throw new Error(`Thesis disappeared after update: ${thesis.id}`)
  }

  return {
    thesis: updatedThesis,
    evidence,
    update,
  }
}

export function getEvidenceTimeline(db: SoraDb, thesisId: string): ThesisEvidence[] {
  return listThesisEvidence(db, thesisId)
}

export function getEvidenceGroups(db: SoraDb, thesisId: string): EvidenceGroups {
  return {
    support: listThesisEvidenceByDirection(db, thesisId, 'support'),
    against: listThesisEvidenceByDirection(db, thesisId, 'against'),
    neutral: listThesisEvidenceByDirection(db, thesisId, 'neutral'),
  }
}

export function getContradictions(db: SoraDb, thesisId: string): ContradictionSummary {
  const thesis = getThesisById(db, thesisId)
  if (!thesis) {
    throw new Error(`Thesis not found: ${thesisId}`)
  }

  const groups = getEvidenceGroups(db, thesisId)
  const strongestAgainstEvidence = sortEvidenceByStrength(groups.against)
  const hasStrongAgainst = strongestAgainstEvidence.some((evidence) => evidence.strength === 'strong')

  return {
    thesis,
    supportCount: groups.support.length,
    againstCount: groups.against.length,
    neutralCount: groups.neutral.length,
    strongestAgainstEvidence,
    isChallenged:
      thesis.status === 'challenged' ||
      thesis.confidence < CHALLENGED_CONFIDENCE_THRESHOLD ||
      hasStrongAgainst,
  }
}

export function getAssetExposure(db: SoraDb, thesisId: string): AssetExposure[] {
  return listAssetExposuresByThesis(db, thesisId)
}

export function reviewTheses(db: SoraDb): ThesisReview {
  const theses = queryListTheses(db)
  const changedTheses = theses
    .filter((thesis) => listThesisUpdates(db, thesis.id).length > 0)
    .sort(compareUpdatedDesc)
  const allEvidence = theses.flatMap((thesis) => listThesisEvidence(db, thesis.id))

  return {
    changedTheses,
    strongestTheses: [...theses].sort((a, b) => b.confidence - a.confidence).slice(0, 5),
    challengedTheses: listChallengedTheses(db),
    recentlyUpdatedTheses: [...theses].sort(compareUpdatedDesc).slice(0, 5),
    latestSupportEvidence: latestEvidenceByDirection(allEvidence, 'support'),
    latestAgainstEvidence: latestEvidenceByDirection(allEvidence, 'against'),
  }
}

function nextThesisStatus(
  currentStatus: ThesisStatus,
  evidence: ThesisEvidence,
  confidence: number
): ThesisStatus {
  if (currentStatus === 'invalidated' || currentStatus === 'archived') {
    return currentStatus
  }
  if (evidence.direction === 'against' && evidence.strength === 'strong') {
    return 'challenged'
  }
  if (confidence < CHALLENGED_CONFIDENCE_THRESHOLD) {
    return 'challenged'
  }
  return currentStatus
}

function buildUpdateRationale(
  evidence: ThesisEvidence,
  previousConfidence: number,
  newConfidence: number
): string {
  const directionText = {
    support: 'supporting',
    against: 'contradicting',
    neutral: 'neutral',
  } satisfies Record<EvidenceDirection, string>

  return `${evidence.strength} ${directionText[evidence.direction]} evidence changed confidence from ${previousConfidence} to ${newConfidence}.`
}

function sortEvidenceByStrength(evidence: ThesisEvidence[]): ThesisEvidence[] {
  return [...evidence].sort((a, b) => {
    const rankDiff = STRENGTH_RANK[b.strength] - STRENGTH_RANK[a.strength]
    if (rankDiff !== 0) return rankDiff
    return b.observedAt.localeCompare(a.observedAt)
  })
}

function latestEvidenceByDirection(
  evidence: ThesisEvidence[],
  direction: EvidenceDirection
): ThesisEvidence[] {
  return evidence
    .filter((item) => item.direction === direction)
    .sort((a, b) => b.observedAt.localeCompare(a.observedAt))
    .slice(0, 5)
}

function compareUpdatedDesc(a: Thesis, b: Thesis): number {
  return b.updatedAt.localeCompare(a.updatedAt)
}
