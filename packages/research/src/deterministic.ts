import type { Market, Index, FundAnalysis, ResearchCard, ResearchCardStatus } from '@sora/core'
import type { ResearchCardInput } from '@sora/agent'

const DEFAULT_INVALIDATION_CONDITIONS = [
  '指数成分股发生重大调整',
  '基金申购状态在 2 周内恢复正常',
  '溢价率回落至 0.5% 以内并持续 5 个交易日',
  '监管政策出现重大变化',
]

const MARKET_IMPLICATIONS: Record<string, string> = {
  'us-tech':  '美国科技板块对国内 QDII/ETF 基金的执行质量影响显著；汇率波动（CNY/USD）和申购限制是主要关注点。',
  'us-broad': '美国宽基指数产品是中国投资者配置海外资产的核心工具；流动性溢价率和费率差异直接影响持有成本。',
  'hk-tech':  '港股科技板块与 A 股联动性较强；港股通机制使部分 ETF 可通过场内渠道买卖，需关注溢价率。',
  'hk-broad': '恒生指数 ETF 以场内为主，溢价率和成交量是判断执行成本的关键指标。',
  'cn-broad': '沪深 300 指数基金竞争充分，费率分化明显；优先选择规模大、费率低的产品。',
  'global':   'MSCI World 系列产品在国内可投标的有限；QDII 配额和汇率是额外风险来源。',
}

function determineStatus(analyses: FundAnalysis[]): ResearchCardStatus {
  const allWarnings = analyses.flatMap((a) => a.warnings)
  if (allWarnings.some((w) => w.level === 'warning')) return 'active_watch'
  if (allWarnings.some((w) => w.level === 'watch')) return 'watch'
  return 'ignore'
}

function buildKeyEvidence(analyses: FundAnalysis[]): string[] {
  const evidence: string[] = []
  const sorted = [...analyses].sort((a, b) => b.executionQualityScore - a.executionQualityScore)

  if (sorted.length > 0) {
    evidence.push(`最高执行质量评分：${sorted[0].executionQualityScore}/100（${sorted[0].fundId}）`)
  }
  if (sorted.length > 1) {
    const last = sorted[sorted.length - 1]
    evidence.push(`最低执行质量评分：${last.executionQualityScore}/100（${last.fundId}）`)
  }

  const avgScore = Math.round(
    analyses.reduce((s, a) => s + a.executionQualityScore, 0) / analyses.length
  )
  evidence.push(`${analyses.length} 只基金平均执行质量评分 ${avgScore}/100`)

  const warned = analyses.filter((a) => a.warnings.some((w) => w.level === 'warning'))
  if (warned.length > 0) {
    evidence.push(`${warned.length} 只基金存在 warning 级别风险提示`)
  }
  return evidence
}

function buildFundExecutionRisks(analyses: FundAnalysis[]): string[] {
  const risks = new Set<string>()
  for (const a of analyses) {
    for (const w of a.warnings) {
      if (w.level === 'warning' || w.level === 'watch') {
        risks.add(`[${a.fundId}] ${w.message}`)
      }
    }
  }
  return [...risks].slice(0, 8)
}

function buildSummary(market: Market, indexes: Index[], analyses: FundAnalysis[]): string {
  const avgScore = analyses.length
    ? Math.round(analyses.reduce((s, a) => s + a.executionQualityScore, 0) / analyses.length)
    : 0
  const indexNames = indexes.map((i) => i.name).join('、')
  const warningCount = analyses.flatMap((a) => a.warnings).filter((w) => w.level === 'warning').length
  return `${market.name}（${indexNames}）共追踪 ${analyses.length} 只国内基金，` +
    `平均执行质量评分 ${avgScore}/100。` +
    (warningCount > 0 ? `当前有 ${warningCount} 条 warning 级别风险提示，需关注。` : '当前无 warning 级别风险提示。')
}

export function generateDeterministicCard(input: ResearchCardInput): ResearchCard {
  const { market, indexes, fundAnalyses } = input
  const now = new Date().toISOString()

  return {
    id: `card-${market.id}-${now.split('T')[0]}`,
    title: `${market.name} 国内基金执行质量观察`,
    marketId: market.id,
    relatedIndexIds: indexes.map((i) => i.id),
    relatedFundIds: fundAnalyses.map((a) => a.fundId),
    summary: buildSummary(market, indexes, fundAnalyses),
    keyEvidence: buildKeyEvidence(fundAnalyses),
    fundExecutionRisks: buildFundExecutionRisks(fundAnalyses),
    marketImplication: MARKET_IMPLICATIONS[market.id] ?? `${market.name} 市场对国内基金执行质量的影响待进一步分析。`,
    risks: [
      '数据来源为东方财富非官方接口，存在延迟风险',
      '汇率波动可能影响 QDII 基金净值的实际换算',
      '境外市场休市日与 A 股存在错位，可能导致净值偏差',
    ],
    invalidationConditions: DEFAULT_INVALIDATION_CONDITIONS,
    status: determineStatus(fundAnalyses),
    generatedAt: now,
  }
}
