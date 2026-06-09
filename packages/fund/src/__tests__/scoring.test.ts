import { describe, it, expect } from 'vitest'
import type { Fund, FundMetricsSnapshot } from '@sora/core'
import { scoreFund, generateWarnings } from '../scoring.js'

function makeFund(overrides: Partial<Fund> = {}): Fund {
  return {
    id: 'fund-test',
    fundCode: '159941',
    fundName: '测试基金',
    fundType: 'etf',
    marketId: 'us-tech',
    trackingIndexId: 'nasdaq-100',
    manager: '测试基金公司',
    fee: 0.002,
    scale: 100,
    inceptionDate: '2020-01-01',
    isEtf: true,
    isEtfFeeder: false,
    isQdii: false,
    purchaseStatus: 'open',
    purchaseLimit: null,
    dataSource: 'test',
    updatedAt: '2024-06-01T10:00:00Z',
    ...overrides,
  }
}

function makeMetrics(overrides: Partial<FundMetricsSnapshot> = {}): FundMetricsSnapshot {
  return {
    id: 'metrics-test',
    fundId: 'fund-test',
    nav: 1.5,
    price: 1.5,
    premiumRate: 0.001,
    volume: 5_000_000,
    turnover: 7_500_000,
    sharpeRatio: 1.2,
    maxDrawdown: -0.3,
    volatility: 0.2,
    trackingError: 0.005,
    return1m: 0.02,
    return3m: 0.06,
    return6m: 0.12,
    return1y: 0.22,
    return3y: 0.55,
    snapshotDate: '2024-06-01',
    dataSource: 'test',
    ...overrides,
  }
}

describe('scoreFund — normal case', () => {
  it('returns a valid FundAnalysis with score in [0,100]', () => {
    const analysis = scoreFund(makeFund(), makeMetrics())
    expect(analysis.fundId).toBe('fund-test')
    expect(analysis.executionQualityScore).toBeGreaterThanOrEqual(0)
    expect(analysis.executionQualityScore).toBeLessThanOrEqual(100)
    expect(analysis.warnings).toBeInstanceOf(Array)
    expect(analysis.summary).toContain('100')
  })
})

describe('generateWarnings — risk scenarios', () => {
  it('HIGH_PREMIUM: premiumRate > 3% triggers warning', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ premiumRate: 0.035 }))
    expect(warnings.some((w) => w.code === 'HIGH_PREMIUM')).toBe(true)
    expect(warnings.find((w) => w.code === 'HIGH_PREMIUM')!.level).toBe('warning')
  })

  it('ELEVATED_PREMIUM: premiumRate between 1-3% triggers watch', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ premiumRate: 0.018 }))
    expect(warnings.some((w) => w.code === 'ELEVATED_PREMIUM')).toBe(true)
    expect(warnings.find((w) => w.code === 'ELEVATED_PREMIUM')!.level).toBe('watch')
  })

  it('PURCHASE_SUSPENDED: suspended status triggers warning', () => {
    const warnings = generateWarnings(makeFund({ purchaseStatus: 'suspended' }), makeMetrics())
    expect(warnings.some((w) => w.code === 'PURCHASE_SUSPENDED')).toBe(true)
    expect(warnings.find((w) => w.code === 'PURCHASE_SUSPENDED')!.level).toBe('warning')
  })

  it('PURCHASE_LIMITED: limited status triggers watch', () => {
    const warnings = generateWarnings(
      makeFund({ purchaseStatus: 'limited', purchaseLimit: 10000 }),
      makeMetrics()
    )
    expect(warnings.some((w) => w.code === 'PURCHASE_LIMITED')).toBe(true)
    expect(warnings.find((w) => w.code === 'PURCHASE_LIMITED')!.level).toBe('watch')
    expect(warnings.find((w) => w.code === 'PURCHASE_LIMITED')!.message).toContain('10,000')
  })

  it('SMALL_SCALE: scale < 2亿 triggers warning', () => {
    const warnings = generateWarnings(makeFund({ scale: 1.5 }), makeMetrics())
    expect(warnings.some((w) => w.code === 'SMALL_SCALE')).toBe(true)
    expect(warnings.find((w) => w.code === 'SMALL_SCALE')!.level).toBe('warning')
  })

  it('HIGH_TRACKING_ERROR: trackingError > 2% triggers watch', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ trackingError: 0.025 }))
    expect(warnings.some((w) => w.code === 'HIGH_TRACKING_ERROR')).toBe(true)
    expect(warnings.find((w) => w.code === 'HIGH_TRACKING_ERROR')!.level).toBe('watch')
  })

  it('LARGE_DRAWDOWN: maxDrawdown > 50% triggers info', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ maxDrawdown: -0.55 }))
    expect(warnings.some((w) => w.code === 'LARGE_DRAWDOWN')).toBe(true)
    expect(warnings.find((w) => w.code === 'LARGE_DRAWDOWN')!.level).toBe('info')
  })

  it('HIGH_VOLATILITY: volatility > 30% triggers info', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ volatility: 0.35 }))
    expect(warnings.some((w) => w.code === 'HIGH_VOLATILITY')).toBe(true)
    expect(warnings.find((w) => w.code === 'HIGH_VOLATILITY')!.level).toBe('info')
  })

  it('no warnings for a healthy fund', () => {
    const warnings = generateWarnings(makeFund(), makeMetrics({ premiumRate: 0.001 }))
    expect(warnings).toHaveLength(0)
  })
})

describe('scoreFund — score ordering', () => {
  it('lower fee produces higher costScore', () => {
    const highFee = scoreFund(makeFund({ fee: 0.01 }), makeMetrics())
    const lowFee = scoreFund(makeFund({ fee: 0.001 }), makeMetrics())
    expect(lowFee.costScore).toBeGreaterThan(highFee.costScore)
  })

  it('higher premiumRate produces lower premiumRiskScore', () => {
    const low = scoreFund(makeFund(), makeMetrics({ premiumRate: 0.001 }))
    const high = scoreFund(makeFund(), makeMetrics({ premiumRate: 0.04 }))
    expect(low.premiumRiskScore).toBeGreaterThan(high.premiumRiskScore)
  })
})
