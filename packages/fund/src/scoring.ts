import type { Fund, FundMetricsSnapshot, FundAnalysis, FundWarning } from '@sora/core'

function clamp(v: number): number {
  return Math.max(0, Math.min(100, v))
}

export function calcCostScore(fee: number | undefined | null): number {
  if (fee == null) return 50
  if (fee <= 0.001) return 100
  if (fee >= 0.015) return 0
  return clamp(Math.round(100 - (fee / 0.015) * 100))
}

export function calcLiquidityScore(
  scale: number | undefined | null,
  volume: number | undefined | null
): number {
  let score = 50
  if (scale != null) {
    if (scale >= 100) score += 25
    else if (scale >= 20) score += 10
    else if (scale < 2) score -= 20
  }
  if (volume != null) {
    if (volume >= 10_000_000) score += 25
    else if (volume >= 1_000_000) score += 15
    else if (volume >= 100_000) score += 5
    else score -= 10
  }
  return clamp(score)
}

export function calcPremiumRiskScore(premiumRate: number | undefined | null): number {
  if (premiumRate == null) return 80
  const pct = Math.abs(premiumRate) * 100
  if (pct <= 0.3) return 100
  if (pct <= 1) return 90
  if (pct <= 2) return 70
  if (pct <= 3) return 50
  if (pct <= 5) return 20
  return 0
}

export function calcTrackingScore(trackingError: number | undefined | null): number {
  if (trackingError == null) return 70
  const pct = trackingError * 100
  if (pct <= 0.3) return 100
  if (pct <= 0.8) return 90
  if (pct <= 1.5) return 75
  if (pct <= 2) return 60
  if (pct <= 3) return 40
  return 20
}

export function calcRiskScore(
  sharpeRatio: number | undefined | null,
  maxDrawdown: number | undefined | null,
  volatility: number | undefined | null
): number {
  let score = 60
  if (sharpeRatio != null) {
    if (sharpeRatio >= 1.5) score += 20
    else if (sharpeRatio >= 1.0) score += 10
    else if (sharpeRatio < 0.5) score -= 10
  }
  if (maxDrawdown != null) {
    const dd = Math.abs(maxDrawdown) * 100
    if (dd <= 20) score += 10
    else if (dd <= 50) score += 0
    else score -= 15
  }
  if (volatility != null) {
    const vol = volatility * 100
    if (vol <= 15) score += 10
    else if (vol <= 25) score += 0
    else if (vol <= 35) score -= 5
    else score -= 15
  }
  return clamp(score)
}

export function scoreFund(fund: Fund, metrics: FundMetricsSnapshot): FundAnalysis {
  const costScore = calcCostScore(fund.fee)
  const liquidityScore = calcLiquidityScore(fund.scale, metrics.volume)
  const premiumRiskScore = calcPremiumRiskScore(metrics.premiumRate)
  const trackingScore = calcTrackingScore(metrics.trackingError)
  const riskScore = calcRiskScore(metrics.sharpeRatio, metrics.maxDrawdown, metrics.volatility)

  const executionQualityScore = Math.round(
    costScore * 0.2 + liquidityScore * 0.2 + premiumRiskScore * 0.25 + trackingScore * 0.2 + riskScore * 0.15
  )

  const warnings = generateWarnings(fund, metrics)

  return {
    fundId: fund.id,
    executionQualityScore,
    costScore,
    liquidityScore,
    premiumRiskScore,
    trackingScore,
    riskScore,
    warnings,
    summary: `综合执行质量评分 ${executionQualityScore}/100，共 ${warnings.length} 条风险提示`,
    analyzedAt: new Date().toISOString(),
  }
}

export function generateWarnings(fund: Fund, metrics: FundMetricsSnapshot): FundWarning[] {
  const warnings: FundWarning[] = []

  const premiumPct =
    metrics.premiumRate != null ? Math.abs(metrics.premiumRate) * 100 : null

  if (premiumPct != null && premiumPct > 3) {
    warnings.push({
      level: 'warning',
      code: 'HIGH_PREMIUM',
      message: `高溢价风险（溢价率 ${premiumPct.toFixed(2)}%），场内买入需谨慎`,
    })
  } else if (premiumPct != null && premiumPct > 1) {
    warnings.push({
      level: 'watch',
      code: 'ELEVATED_PREMIUM',
      message: `存在一定溢价（${premiumPct.toFixed(2)}%），注意场内价格`,
    })
  }

  if (fund.purchaseStatus === 'suspended') {
    warnings.push({
      level: 'warning',
      code: 'PURCHASE_SUSPENDED',
      message: '暂停申购，当前无法场外买入',
    })
  } else if (fund.purchaseStatus === 'limited') {
    const limit = fund.purchaseLimit != null ? `（上限 ${fund.purchaseLimit.toLocaleString()} 元）` : ''
    warnings.push({
      level: 'watch',
      code: 'PURCHASE_LIMITED',
      message: `限制申购${limit}`,
    })
  }

  if (fund.scale != null && fund.scale < 2) {
    warnings.push({
      level: 'warning',
      code: 'SMALL_SCALE',
      message: `规模偏小（${fund.scale} 亿元），存在清盘风险`,
    })
  }

  if (metrics.trackingError != null && metrics.trackingError * 100 > 2) {
    warnings.push({
      level: 'watch',
      code: 'HIGH_TRACKING_ERROR',
      message: `跟踪误差偏高（${(metrics.trackingError * 100).toFixed(2)}%）`,
    })
  }

  if (metrics.maxDrawdown != null && Math.abs(metrics.maxDrawdown) * 100 > 50) {
    warnings.push({
      level: 'info',
      code: 'LARGE_DRAWDOWN',
      message: `历史最大回撤较大（${(Math.abs(metrics.maxDrawdown) * 100).toFixed(1)}%）`,
    })
  }

  if (metrics.volatility != null && metrics.volatility * 100 > 30) {
    warnings.push({
      level: 'info',
      code: 'HIGH_VOLATILITY',
      message: `波动率较高（${(metrics.volatility * 100).toFixed(1)}%）`,
    })
  }

  return warnings
}
