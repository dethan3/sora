import { z } from 'zod'

export const FundTypeSchema = z.enum(['etf', 'etf_feeder', 'qdii', 'lof', 'mutual_fund'])
export type FundType = z.infer<typeof FundTypeSchema>

export const PurchaseStatusSchema = z.enum(['open', 'limited', 'suspended', 'unknown'])
export type PurchaseStatus = z.infer<typeof PurchaseStatusSchema>

export const FundSchema = z.object({
  id: z.string(),
  fundCode: z.string(),
  fundName: z.string(),
  fundType: FundTypeSchema,
  marketId: z.string(),
  trackingIndexId: z.string(),
  manager: z.string().optional(),
  fee: z.number().optional(),            // annual management fee ratio, e.g. 0.005 = 0.5%
  scale: z.number().optional(),          // AUM in 亿 CNY
  inceptionDate: z.string().optional(),  // ISO date string YYYY-MM-DD
  isEtf: z.boolean(),
  isEtfFeeder: z.boolean(),
  isQdii: z.boolean(),
  purchaseStatus: PurchaseStatusSchema,
  purchaseLimit: z.number().nullable().optional(),  // daily purchase limit in CNY, null = unlimited
  dataSource: z.string(),
  updatedAt: z.string(),                 // ISO datetime string
})
export type Fund = z.infer<typeof FundSchema>

export const FundMappingSchema = z.object({
  id: z.string(),
  fundId: z.string(),
  indexId: z.string(),
  isPrimary: z.boolean(),
})
export type FundMapping = z.infer<typeof FundMappingSchema>
