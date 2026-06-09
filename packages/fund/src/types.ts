import type { Fund, FundMetricsSnapshot } from '@sora/core'

export interface FundWithMetrics {
  fund: Fund
  metrics: FundMetricsSnapshot | null
}
