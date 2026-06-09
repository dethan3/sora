import { z } from 'zod'

export const ResearchSignalSchema = z.object({
  id: z.string(),
  marketId: z.string(),
  indexId: z.string().optional(),
  content: z.string(),
  source: z.string(),
  createdAt: z.string(),  // ISO datetime string
})
export type ResearchSignal = z.infer<typeof ResearchSignalSchema>

export const ResearchCardStatusSchema = z.enum([
  'ignore',
  'watch',
  'active_watch',
  'confirmed',
  'invalidated',
])
export type ResearchCardStatus = z.infer<typeof ResearchCardStatusSchema>

export const ResearchCardSchema = z.object({
  id: z.string(),
  title: z.string(),
  marketId: z.string(),
  relatedIndexIds: z.array(z.string()),
  relatedFundIds: z.array(z.string()),
  summary: z.string(),
  keyEvidence: z.array(z.string()),
  fundExecutionRisks: z.array(z.string()),
  marketImplication: z.string(),
  risks: z.array(z.string()),
  invalidationConditions: z.array(z.string()),
  status: ResearchCardStatusSchema,
  generatedAt: z.string(),  // ISO datetime string
})
export type ResearchCard = z.infer<typeof ResearchCardSchema>
