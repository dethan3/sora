import { Command } from 'commander'
import { MarketService } from '@sora/market'
import { createMarketSource } from '@sora/sources'
import { SEEDS_DIR } from '../utils/env.js'
import { printTable, printDisclaimer, fmtNum, fmtPct } from '../utils/format.js'

function makeService() {
  return new MarketService(SEEDS_DIR, createMarketSource())
}

export function makeIndexesCommand(): Command {
  const indexes = new Command('indexes').description('指数管理')

  indexes
    .command('list')
    .description('列出指数（含实时行情）')
    .option('--market <marketId>', '按市场过滤')
    .action(async (opts: { market?: string }) => {
      const svc = makeService()
      const list = opts.market
        ? await svc.listIndexesByMarket(opts.market)
        : await svc.listIndexes()

      if (list.length === 0) {
        console.log('暂无数据')
        return
      }

      const rows = await Promise.all(
        list.map(async (idx) => {
          const result = await svc.getIndexWithQuote(idx.id)
          const q = result?.quote
          const price = q ? fmtNum(q.price, 2) : 'N/A'
          const chg = q ? fmtPct(q.changePercent / 100, 2) : 'N/A'
          return [idx.id, idx.name, idx.ticker, price, chg]
        })
      )

      printTable(['ID', '名称', '代码', '价格', '涨跌幅'], rows)
      printDisclaimer()
    })

  return indexes
}
