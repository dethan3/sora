import { z } from 'zod'

export const FundWarningLevelSchema = z.enum(['info', 'watch', 'warning'])
export type FundWarningLevel = z.infer<typeof FundWarningLevelSchema>

export const FundWarningSchema = z.object({
  level: FundWarningLevelSchema,
  code: z.string(),    // e.g. 'HIGH_PREMIUM', 'PURCHASE_SUSPENDED', 'SMALL_SCALE'
  message: z.string(),
})
export type FundWarning = z.infer<typeof FundWarningSchema>

export const FundAnalysisSchema = z.object({
  fundId: z.string(),
  executionQualityScore: z.number().min(0).max(100),
  costScore: z.number().min(0).max(100),
  liquidityScore: z.number().min(0).max(100),
  premiumRiskScore: z.number().min(0).max(100),
  trackingScore: z.number().min(0).max(100),
  riskScore: z.number().min(0).max(100),
  warnings: z.array(FundWarningSchema),
  summary: z.string(),
  analyzedAt: z.string(),  // ISO datetime string
})
export type FundAnalysis = z.infer<typeof FundAnalysisSchema>
