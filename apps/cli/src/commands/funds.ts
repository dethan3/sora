import { Command } from 'commander'
import { FundService } from '@sora/fund'
import { createFundSource } from '@sora/sources'
import { SEEDS_DIR } from '../utils/env.js'
import { printTable, printDisclaimer, fmtPct, fmtNum, warnIcon } from '../utils/format.js'
import type { FundWithMetrics } from '@sora/fund'
import type { FundAnalysis } from '@sora/core'

function makeService() {
  return new FundService(SEEDS_DIR, createFundSource())
}

function purchaseLabel(status: string, limit?: number | null): string {
  if (status === 'suspended') return '暂停'
  if (status === 'limited') return limit != null ? `限购(${(limit / 10000).toFixed(0)}万)` : '限购'
  return '正常'
}

function warningsSummary(analysis: FundAnalysis): string {
  const warns = analysis.warnings
  if (warns.length === 0) return '-'
  return warns.map((w) => `${warnIcon(w.level)}${w.code}`).join(' ')
}

function printFundsTable(items: Array<{ fw: FundWithMetrics; analysis: FundAnalysis }>): void {
  const headers = ['代码', '名称', '类型', '费率', '规模(亿)', '溢价率', '申购', '夏普', '最大回撤', '跟踪误差', '执行质量', '风险提示']
  const rows = items.map(({ fw, analysis }) => {
    const f = fw.fund
    const m = fw.metrics
    return [
      f.fundCode,
      f.fundName.slice(0, 12),
      f.isEtf ? 'ETF' : f.isEtfFeeder ? '联接' : f.isQdii ? 'QDII' : f.fundType,
      f.fee != null ? fmtPct(f.fee, 2) : 'N/A',
      f.scale != null ? String(f.scale) : 'N/A',
      m ? fmtPct(m.premiumRate, 2) : 'N/A',
      purchaseLabel(f.purchaseStatus, f.purchaseLimit),
      m ? fmtNum(m.sharpeRatio, 2) : 'N/A',
      m ? fmtPct(m.maxDrawdown, 1) : 'N/A',
      m ? fmtPct(m.trackingError, 2) : 'N/A',
      String(analysis.executionQualityScore),
      warningsSummary(analysis),
    ]
  })
  printTable(headers, rows)
}

export function makeFundsCommand(): Command {
  const funds = new Command('funds').description('基金映射与分析')

  funds
    .command('map')
    .description('列出追踪某指数/市场的国内基金')
    .option('--index <indexId>', '指数 ID')
    .option('--market <marketId>', '市场 ID')
    .action(async (opts: { index?: string; market?: string }) => {
      const svc = makeService()
      let items: FundWithMetrics[]

      if (opts.index) {
        items = await svc.getFundsByIndex(opts.index)
      } else if (opts.market) {
        items = await svc.getFundsByMarket(opts.market)
      } else {
        console.error('❌ 请指定 --index 或 --market')
        process.exit(1)
      }

      if (items.length === 0) {
        console.log('暂无数据')
        return
      }

      printTable(
        ['代码', '名称', '类型', '费率', '规模(亿)', '申购状态', '溢价率'],
        items.map(({ fund: f, metrics: m }) => [
          f.fundCode,
          f.fundName.slice(0, 16),
          f.isEtf ? 'ETF' : f.isEtfFeeder ? '联接' : f.isQdii ? 'QDII' : f.fundType,
          f.fee != null ? fmtPct(f.fee, 2) : 'N/A',
          f.scale != null ? String(f.scale) : 'N/A',
          purchaseLabel(f.purchaseStatus, f.purchaseLimit),
          m ? fmtPct(m.premiumRate, 2) : 'N/A',
        ])
      )
      printDisclaimer()
    })

  funds
    .command('analyze')
    .description('横向分析同一指数下的基金，输出评分与风险提示')
    .option('--index <indexId>', '指数 ID')
    .option('--market <marketId>', '市场 ID')
    .action(async (opts: { index?: string; market?: string }) => {
      const svc = makeService()
      let items: FundWithMetrics[]

      if (opts.index) {
        items = await svc.getFundsByIndex(opts.index)
      } else if (opts.market) {
        items = await svc.getFundsByMarket(opts.market)
      } else {
        console.error('❌ 请指定 --index 或 --market')
        process.exit(1)
      }

      if (items.length === 0) {
        console.log('暂无数据')
        return
      }

      const analyses = await svc.analyzeFunds(items)
      const analysisMap = new Map(analyses.map((a) => [a.fundId, a]))

      const tableItems = items
        .filter((fw) => analysisMap.has(fw.fund.id))
        .map((fw) => ({ fw, analysis: analysisMap.get(fw.fund.id)! }))
        .sort((a, b) => b.analysis.executionQualityScore - a.analysis.executionQualityScore)

      printFundsTable(tableItems)
      printDisclaimer()
    })

  return funds
}
