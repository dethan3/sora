import { describe, it, expect } from 'vitest'
import { MarketCategorySchema, MarketSchema } from '../market.js'
import { IndexSchema } from '../index-schema.js'
import {
  FundTypeSchema,
  PurchaseStatusSchema,
  FundSchema,
  FundMappingSchema,
} from '../fund.js'
import { FundMetricsSnapshotSchema } from '../fund-metrics.js'
import { FundWarningSchema, FundAnalysisSchema } from '../fund-analysis.js'
import { ResearchSignalSchema, ResearchCardStatusSchema, ResearchCardSchema } from '../research.js'
import { AlertLevelSchema, AlertSchema } from '../alert.js'
import { NotificationLevelSchema, NotificationEventSchema } from '../notification.js'

describe('MarketCategorySchema', () => {
  it('parses valid category', () => {
    expect(MarketCategorySchema.parse('us')).toBe('us')
    expect(MarketCategorySchema.parse('hk')).toBe('hk')
    expect(MarketCategorySchema.parse('cn')).toBe('cn')
    expect(MarketCategorySchema.parse('commodity')).toBe('commodity')
    expect(MarketCategorySchema.parse('global')).toBe('global')
  })

  it('rejects invalid category', () => {
    expect(() => MarketCategorySchema.parse('europe')).toThrow()
    expect(() => MarketCategorySchema.parse('')).toThrow()
  })
})

describe('MarketSchema', () => {
  it('parses valid market', () => {
    const result = MarketSchema.parse({
      id: 'us-tech',
      name: '美国科技',
      category: 'us',
      description: 'Nasdaq 100 代表的美国科技成长市场',
    })
    expect(result.id).toBe('us-tech')
    expect(result.category).toBe('us')
  })

  it('parses market without optional description', () => {
    const result = MarketSchema.parse({ id: 'gold', name: '黄金', category: 'commodity' })
    expect(result.description).toBeUndefined()
  })

  it('rejects market with invalid category', () => {
    expect(() =>
      MarketSchema.parse({ id: 'x', name: 'X', category: 'invalid' })
    ).toThrow()
  })

  it('rejects market missing required fields', () => {
    expect(() => MarketSchema.parse({ id: 'us-tech' })).toThrow()
  })
})

describe('IndexSchema', () => {
  it('parses valid index', () => {
    const result = IndexSchema.parse({
      id: 'nasdaq-100',
      name: 'Nasdaq 100',
      marketId: 'us-tech',
      ticker: '^NDX',
    })
    expect(result.ticker).toBe('^NDX')
  })

  it('rejects index missing ticker', () => {
    expect(() =>
      IndexSchema.parse({ id: 'nasdaq-100', name: 'Nasdaq 100', marketId: 'us-tech' })
    ).toThrow()
  })
})

describe('FundTypeSchema', () => {
  it('parses all valid fund types', () => {
    for (const t of ['etf', 'etf_feeder', 'qdii', 'lof', 'mutual_fund']) {
      expect(FundTypeSchema.parse(t)).toBe(t)
    }
  })

  it('rejects invalid fund type', () => {
    expect(() => FundTypeSchema.parse('hedge_fund')).toThrow()
  })
})

describe('PurchaseStatusSchema', () => {
  it('parses all valid statuses', () => {
    for (const s of ['open', 'limited', 'suspended', 'unknown']) {
      expect(PurchaseStatusSchema.parse(s)).toBe(s)
    }
  })

  it('rejects invalid status', () => {
    expect(() => PurchaseStatusSchema.parse('closed')).toThrow()
  })
})

describe('FundSchema', () => {
  const validFund = {
    id: 'fund-159941',
    fundCode: '159941',
    fundName: '易方达中证科技创新ETF',
    fundType: 'etf',
    marketId: 'us-tech',
    trackingIndexId: 'nasdaq-100',
    manager: '易方达基金',
    fee: 0.002,
    scale: 220.5,
    inceptionDate: '2020-09-11',
    isEtf: true,
    isEtfFeeder: false,
    isQdii: false,
    purchaseStatus: 'open',
    purchaseLimit: null,
    dataSource: 'eastmoney',
    updatedAt: '2024-06-01T10:00:00Z',
  }

  it('parses valid fund', () => {
    const result = FundSchema.parse(validFund)
    expect(result.fundCode).toBe('159941')
    expect(result.isEtf).toBe(true)
  })

  it('parses fund with limited purchase', () => {
    const result = FundSchema.parse({ ...validFund, purchaseStatus: 'limited', purchaseLimit: 10000 })
    expect(result.purchaseStatus).toBe('limited')
    expect(result.purchaseLimit).toBe(10000)
  })

  it('rejects fund with invalid fundType', () => {
    expect(() => FundSchema.parse({ ...validFund, fundType: 'invalid' })).toThrow()
  })

  it('rejects fund missing required boolean fields', () => {
    const withoutIsEtf = { ...validFund }
    delete (withoutIsEtf as Partial<typeof validFund>).isEtf
    expect(() => FundSchema.parse(withoutIsEtf)).toThrow()
  })
})

describe('FundMappingSchema', () => {
  it('parses valid mapping', () => {
    const result = FundMappingSchema.parse({
      id: 'map-001',
      fundId: 'fund-159941',
      indexId: 'nasdaq-100',
      isPrimary: true,
    })
    expect(result.isPrimary).toBe(true)
  })

  it('rejects mapping missing indexId', () => {
    expect(() =>
      FundMappingSchema.parse({ id: 'map-001', fundId: 'fund-159941', isPrimary: true })
    ).toThrow()
  })
})

describe('FundMetricsSnapshotSchema', () => {
  const validMetrics = {
    id: 'metrics-001',
    fundId: 'fund-159941',
    nav: 1.523,
    price: 1.531,
    premiumRate: 0.0053,
    volume: 12000000,
    turnover: 18000000,
    sharpeRatio: 1.42,
    maxDrawdown: -0.35,
    volatility: 0.28,
    trackingError: 0.008,
    return1m: 0.03,
    return3m: 0.08,
    return6m: 0.15,
    return1y: 0.25,
    return3y: 0.6,
    snapshotDate: '2024-06-01',
    dataSource: 'eastmoney',
  }

  it('parses valid metrics snapshot', () => {
    const result = FundMetricsSnapshotSchema.parse(validMetrics)
    expect(result.premiumRate).toBeCloseTo(0.0053)
  })

  it('parses metrics with null values', () => {
    const result = FundMetricsSnapshotSchema.parse({
      ...validMetrics,
      price: null,
      premiumRate: null,
      volume: null,
    })
    expect(result.price).toBeNull()
    expect(result.premiumRate).toBeNull()
  })

  it('rejects metrics missing snapshotDate', () => {
    const without = { ...validMetrics }
    delete (without as Partial<typeof validMetrics>).snapshotDate
    expect(() => FundMetricsSnapshotSchema.parse(without)).toThrow()
  })
})

describe('FundWarningSchema', () => {
  it('parses valid warning', () => {
    const result = FundWarningSchema.parse({
      level: 'warning',
      code: 'HIGH_PREMIUM',
      message: '溢价率超过 3%，场内买入需谨慎',
    })
    expect(result.code).toBe('HIGH_PREMIUM')
  })

  it('rejects warning with invalid level', () => {
    expect(() =>
      FundWarningSchema.parse({ level: 'critical', code: 'X', message: 'X' })
    ).toThrow()
  })
})

describe('FundAnalysisSchema', () => {
  const validAnalysis = {
    fundId: 'fund-159941',
    executionQualityScore: 82,
    costScore: 90,
    liquidityScore: 85,
    premiumRiskScore: 78,
    trackingScore: 80,
    riskScore: 75,
    warnings: [],
    summary: '执行质量良好，溢价率正常，建议持续观察。',
    analyzedAt: '2024-06-01T10:00:00Z',
  }

  it('parses valid analysis', () => {
    const result = FundAnalysisSchema.parse(validAnalysis)
    expect(result.executionQualityScore).toBe(82)
  })

  it('rejects score out of range', () => {
    expect(() => FundAnalysisSchema.parse({ ...validAnalysis, costScore: 101 })).toThrow()
    expect(() => FundAnalysisSchema.parse({ ...validAnalysis, riskScore: -1 })).toThrow()
  })
})

describe('ResearchSignalSchema', () => {
  it('parses valid signal', () => {
    const result = ResearchSignalSchema.parse({
      id: 'sig-001',
      marketId: 'us-tech',
      content: 'Nasdaq 100 近期波动加大',
      source: 'tavily',
      createdAt: '2024-06-01T10:00:00Z',
    })
    expect(result.indexId).toBeUndefined()
  })

  it('rejects signal missing marketId', () => {
    expect(() =>
      ResearchSignalSchema.parse({ id: 'sig-001', content: 'X', source: 'X', createdAt: 'X' })
    ).toThrow()
  })
})

describe('ResearchCardStatusSchema', () => {
  it('parses all valid statuses', () => {
    for (const s of ['ignore', 'watch', 'active_watch', 'confirmed', 'invalidated']) {
      expect(ResearchCardStatusSchema.parse(s)).toBe(s)
    }
  })

  it('rejects invalid status', () => {
    expect(() => ResearchCardStatusSchema.parse('pending')).toThrow()
  })
})

describe('ResearchCardSchema', () => {
  const validCard = {
    id: 'card-001',
    title: 'Nasdaq 100 国内基金执行质量观察',
    marketId: 'us-tech',
    relatedIndexIds: ['nasdaq-100'],
    relatedFundIds: ['fund-159941', 'fund-513100'],
    summary: '当前国内可通过 ETF 和 QDII 获得 Nasdaq 100 暴露。',
    keyEvidence: ['相关基金数量充足', '溢价率正常'],
    fundExecutionRisks: ['部分 QDII 存在限购'],
    marketImplication: '需重点关注 QDII 申购状态和 ETF 溢价率。',
    risks: ['流动性风险', '汇率风险'],
    invalidationConditions: ['QDII 全面限购', '长期高溢价'],
    status: 'watch',
    generatedAt: '2024-06-01T10:00:00Z',
  }

  it('parses valid research card', () => {
    const result = ResearchCardSchema.parse(validCard)
    expect(result.status).toBe('watch')
    expect(result.relatedFundIds).toHaveLength(2)
  })

  it('rejects card with invalid status', () => {
    expect(() => ResearchCardSchema.parse({ ...validCard, status: 'pending' })).toThrow()
  })
})

describe('AlertSchema', () => {
  it('parses valid alert', () => {
    const result = AlertSchema.parse({
      id: 'alert-001',
      level: 'warning',
      title: '高溢价风险',
      fundId: 'fund-159941',
      message: '该基金当前溢价率为 4.2%，超过警戒线。',
      createdAt: '2024-06-01T10:00:00Z',
    })
    expect(result.level).toBe('warning')
  })

  it('parses alert without optional fundId', () => {
    const result = AlertSchema.parse({
      id: 'alert-002',
      level: 'info',
      title: '市场信息',
      message: '美股今日收涨。',
      createdAt: '2024-06-01T10:00:00Z',
    })
    expect(result.fundId).toBeUndefined()
  })

  it('rejects alert with invalid level', () => {
    expect(() =>
      AlertSchema.parse({ id: 'x', level: 'low', title: 'x', message: 'x', createdAt: 'x' })
    ).toThrow()
  })
})

describe('AlertLevelSchema', () => {
  it('parses all valid levels', () => {
    for (const l of ['info', 'watch', 'warning', 'critical']) {
      expect(AlertLevelSchema.parse(l)).toBe(l)
    }
  })
})

describe('NotificationLevelSchema', () => {
  it('parses all valid levels', () => {
    for (const l of ['info', 'watch', 'warning', 'critical']) {
      expect(NotificationLevelSchema.parse(l)).toBe(l)
    }
  })
})

describe('NotificationEventSchema', () => {
  const validEvent = {
    id: 'evt-001',
    level: 'warning',
    title: '159941 存在高溢价风险',
    summary: '易方达纳指 ETF 当前溢价率为 4.2%，超过 3% 警戒线。',
    source: 'sora',
    type: 'premium_risk',
    relatedEntityType: 'fund',
    relatedEntityId: '159941',
    payload: { premiumRate: 0.042 },
    createdAt: '2024-06-01T10:00:00Z',
  }

  it('parses valid notification event', () => {
    const result = NotificationEventSchema.parse(validEvent)
    expect(result.source).toBe('sora')
    expect(result.type).toBe('premium_risk')
  })

  it('parses event without optional fields', () => {
    const result = NotificationEventSchema.parse({
      id: 'evt-002',
      level: 'info',
      title: '市场更新',
      summary: '恒生科技指数今日上涨 2%。',
      source: 'sora',
      type: 'market_update',
      createdAt: '2024-06-01T10:00:00Z',
    })
    expect(result.relatedEntityType).toBeUndefined()
    expect(result.payload).toBeUndefined()
  })

  it('rejects event with wrong source', () => {
    expect(() =>
      NotificationEventSchema.parse({ ...validEvent, source: 'tickeye' })
    ).toThrow()
  })

  it('rejects event with invalid level', () => {
    expect(() =>
      NotificationEventSchema.parse({ ...validEvent, level: 'debug' })
    ).toThrow()
  })
})
