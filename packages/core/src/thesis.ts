import { z } from 'zod'

export const TimeHorizonSchema = z.enum(['3m', '6m', '1y', '3y', '5y'])
export type TimeHorizon = z.infer<typeof TimeHorizonSchema>

export const ThesisStatusSchema = z.enum([
  'draft',
  'watch',
  'active',
  'challenged',
  'invalidated',
  'archived',
])
export type ThesisStatus = z.infer<typeof ThesisStatusSchema>

export const EvidenceDirectionSchema = z.enum(['support', 'against', 'neutral'])
export type EvidenceDirection = z.infer<typeof EvidenceDirectionSchema>

export const EvidenceStrengthSchema = z.enum(['weak', 'medium', 'strong'])
export type EvidenceStrength = z.infer<typeof EvidenceStrengthSchema>

export const ExposureAssetTypeSchema = z.enum(['market', 'index', 'fund'])
export type ExposureAssetType = z.infer<typeof ExposureAssetTypeSchema>

export const ThesisSchema = z.object({
  id: z.string(),
  title: z.string(),
  summary: z.string(),
  timeHorizon: TimeHorizonSchema,
  status: ThesisStatusSchema,
  confidence: z.number().min(0).max(100),
  causalChain: z.array(z.string()),
  keyAssumptions: z.array(z.string()),
  affectedMarketIds: z.array(z.string()),
  affectedIndexIds: z.array(z.string()),
  affectedFundIds: z.array(z.string()),
  invalidationConditions: z.array(z.string()),
  createdAt: z.string(),
  updatedAt: z.string(),
})
export type Thesis = z.infer<typeof ThesisSchema>

export const ThesisEvidenceSchema = z.object({
  id: z.string(),
  thesisId: z.string(),
  source: z.string(),
  title: z.string(),
  summary: z.string(),
  url: z.string().url().optional(),
  direction: EvidenceDirectionSchema,
  strength: EvidenceStrengthSchema,
  confidenceDelta: z.number(),
  rationale: z.string(),
  observedAt: z.string(),
  createdAt: z.string(),
})
export type ThesisEvidence = z.infer<typeof ThesisEvidenceSchema>

export const ThesisUpdateSchema = z.object({
  id: z.string(),
  thesisId: z.string(),
  previousConfidence: z.number().min(0).max(100),
  newConfidence: z.number().min(0).max(100),
  evidenceIds: z.array(z.string()).min(1),
  rationale: z.string(),
  createdAt: z.string(),
})
export type ThesisUpdate = z.infer<typeof ThesisUpdateSchema>

export const AssetExposureSchema = z.object({
  id: z.string(),
  thesisId: z.string(),
  assetType: ExposureAssetTypeSchema,
  assetId: z.string(),
  exposureScore: z.number().min(0).max(100),
  rationale: z.string(),
  updatedAt: z.string(),
})
export type AssetExposure = z.infer<typeof AssetExposureSchema>
