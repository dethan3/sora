import { Command } from 'commander'
import { MarketService } from '@sora/market'
import { createMarketSource } from '@sora/sources'
import { SEEDS_DIR } from '../utils/env.js'
import { printTable, printDisclaimer } from '../utils/format.js'

function makeService() {
  return new MarketService(SEEDS_DIR, createMarketSource())
}

export function makeMarketsCommand(): Command {
  const markets = new Command('markets').description('市场管理')

  markets
    .command('list')
    .description('列出所有市场')
    .action(async () => {
      const svc = makeService()
      const list = await svc.listMarkets()
      printTable(
        ['ID', '名称', '类别', '描述'],
        list.map((m) => [m.id, m.name, m.category, m.description?.slice(0, 30) ?? ''])
      )
      printDisclaimer()
    })

  markets
    .command('show')
    .description('查看单个市场（含关联指数）')
    .requiredOption('--id <id>', '市场 ID')
    .action(async (opts: { id: string }) => {
      const svc = makeService()
      const m = await svc.getMarket(opts.id)
      if (!m) {
        console.error(`❌ 市场 "${opts.id}" 不存在`)
        process.exit(1)
      }
      console.log(`\n📊 ${m.name} (${m.id})`)
      console.log(`   类别：${m.category}`)
      if (m.description) console.log(`   描述：${m.description}`)
      const indexes = await svc.listIndexesByMarket(opts.id)
      if (indexes.length > 0) {
        console.log('\n关联指数：')
        printTable(
          ['ID', '名称', '代码'],
          indexes.map((i) => [i.id, i.name, i.ticker])
        )
      }
      printDisclaimer()
    })

  return markets
}
