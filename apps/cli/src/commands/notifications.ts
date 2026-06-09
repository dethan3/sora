import { Command } from 'commander'
import { FundService } from '@sora/fund'
import { createFundSource } from '@sora/sources'
import { collectEvents, exportEvents } from '@sora/notifier'
import { SEEDS_DIR } from '../utils/env.js'
import { printDisclaimer } from '../utils/format.js'
import { join } from 'node:path'

export function makeNotificationsCommand(): Command {
  const notifications = new Command('notifications').description('通知事件导出')

  notifications
    .command('export')
    .description('导出所有 NotificationEvent 为 JSON')
    .option('--output <path>', '输出文件路径（默认打印到 stdout）')
    .action(async (opts: { output?: string }) => {
      const svc = new FundService(SEEDS_DIR, createFundSource())
      const allMarketIds = ['us-tech', 'us-broad', 'hk-tech', 'hk-broad', 'cn-broad', 'global']
      const analyses = []

      for (const mId of allMarketIds) {
        const items = await svc.getFundsByMarket(mId)
        const result = await svc.analyzeFunds(items)
        analyses.push(...result)
      }

      const events = collectEvents(analyses, null)

      if (opts.output) {
        const outputPath = opts.output.startsWith('/')
          ? opts.output
          : join(process.cwd(), opts.output)
        const result = exportEvents(events, outputPath)
        console.log(`✅ 已导出 ${result.events.length} 个事件到 ${outputPath}`)
      } else {
        const result = {
          exportedAt: new Date().toISOString(),
          events,
        }
        console.log(JSON.stringify(result, null, 2))
      }
      printDisclaimer()
    })

  return notifications
}
