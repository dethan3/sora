import { z } from 'zod'

export const FundMetricsSnapshotSchema = z.object({
  id: z.string(),
  fundId: z.string(),
  nav: z.number().nullable(),            // net asset value (场外申购净值)
  price: z.number().nullable(),          // market price (场内 ETF 价格)
  premiumRate: z.number().nullable(),    // (price - nav) / nav, e.g. 0.03 = 3%
  volume: z.number().nullable(),         // daily trading volume (shares)
  turnover: z.number().nullable(),       // daily turnover in CNY
  sharpeRatio: z.number().nullable(),
  maxDrawdown: z.number().nullable(),    // e.g. -0.35 = -35%
  volatility: z.number().nullable(),     // annualized volatility
  trackingError: z.number().nullable(),  // annualized tracking error vs benchmark
  return1m: z.number().nullable(),
  return3m: z.number().nullable(),
  return6m: z.number().nullable(),
  return1y: z.number().nullable(),
  return3y: z.number().nullable(),
  snapshotDate: z.string(),              // ISO date string YYYY-MM-DD
  dataSource: z.string(),
})
export type FundMetricsSnapshot = z.infer<typeof FundMetricsSnapshotSchema>
