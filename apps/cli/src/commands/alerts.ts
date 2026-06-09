import { Command } from 'commander'
import { FundService } from '@sora/fund'
import { createFundSource } from '@sora/sources'
import { SEEDS_DIR } from '../utils/env.js'
import { printTable, printDisclaimer, warnIcon } from '../utils/format.js'

export function makeAlertsCommand(): Command {
  const alerts = new Command('alerts').description('风险提醒管理')

  alerts
    .command('list')
    .description('列出当前所有风险提醒（实时分析）')
    .option('--level <level>', '过滤级别：info | watch | warning')
    .action(async (opts: { level?: string }) => {
      const svc = new FundService(SEEDS_DIR, createFundSource())
      const allMarketIds = ['us-tech', 'us-broad', 'hk-tech', 'hk-broad', 'cn-broad', 'global']
      const analyses = []

      for (const mId of allMarketIds) {
        const items = await svc.getFundsByMarket(mId)
        const result = await svc.analyzeFunds(items)
        analyses.push(...result)
      }

      const rows: string[][] = []
      for (const a of analyses) {
        for (const w of a.warnings) {
          if (opts.level && w.level !== opts.level) continue
          rows.push([warnIcon(w.level), w.level, a.fundId, w.code, w.message.slice(0, 40)])
        }
      }

      if (rows.length === 0) {
        console.log('✅ 当前无风险提醒')
      } else {
        printTable(['', '级别', '基金', '代码', '提示'], rows)
      }
      printDisclaimer()
    })

  return alerts
}
