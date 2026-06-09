import { Command } from 'commander'
import { createMarketSource, createFundSource, createSearchSource } from '@sora/sources'
import { SEEDS_DIR, TAVILY_API_KEY } from '../utils/env.js'
import { printDisclaimer } from '../utils/format.js'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Index } from '@sora/core'

export function makeDataCommand(): Command {
  const data = new Command('data').description('数据刷新与搜索')

  data
    .command('refresh')
    .description('刷新数据缓存')
    .requiredOption('--type <type>', '刷新类型：market | fund')
    .option('--code <code>', '基金代码（--type fund 时必填）')
    .action(async (opts: { type: string; code?: string }) => {
      if (opts.type === 'market') {
        console.log('🔄 刷新市场行情缓存...')
        const source = createMarketSource()
        const indexes = JSON.parse(
          readFileSync(join(SEEDS_DIR, 'indexes.json'), 'utf-8')
        ) as Index[]
        for (const idx of indexes) {
          try {
            const q = await source.getIndexQuote(idx.ticker)
            console.log(`  ✓ ${idx.name} (${idx.ticker}): ${q.price} (${q.changePercent >= 0 ? '+' : ''}${q.changePercent.toFixed(2)}%)`)
          } catch (e) {
            console.log(`  ✗ ${idx.name} (${idx.ticker}): ${(e as Error).message}`)
          }
        }
        console.log('✅ 市场行情缓存已更新')
      } else if (opts.type === 'fund') {
        if (!opts.code) {
          console.error('❌ --type fund 需要指定 --code <fundCode>')
          process.exit(1)
        }
        console.log(`🔄 刷新基金 ${opts.code} 数据缓存...`)
        const source = createFundSource()
        try {
          const m = await source.getFundMetrics(opts.code)
          console.log(`  ✓ NAV: ${m.nav ?? 'N/A'}, 价格: ${m.price ?? 'N/A'}, 溢价率: ${m.premiumRate != null ? (m.premiumRate * 100).toFixed(2) + '%' : 'N/A'}`)
          console.log('✅ 基金数据缓存已更新')
        } catch (e) {
          console.error(`❌ 刷新失败: ${(e as Error).message}`)
        }
      } else {
        console.error('❌ 未知类型，支持：market | fund')
        process.exit(1)
      }
      printDisclaimer()
    })

  data
    .command('search')
    .description('通过 Tavily 搜索并缓存结果')
    .requiredOption('--query <query>', '搜索关键词')
    .option('--max <n>', '最大结果数', '5')
    .action(async (opts: { query: string; max: string }) => {
      if (!TAVILY_API_KEY) {
        console.error('❌ 未配置 TAVILY_API_KEY，无法执行搜索')
        process.exit(1)
      }
      console.log(`🔍 搜索：${opts.query}`)
      const source = createSearchSource()
      const results = await source.search(opts.query, parseInt(opts.max))
      results.forEach((r, i) => {
        console.log(`\n[${i + 1}] ${r.title}`)
        console.log(`    ${r.url}`)
        console.log(`    ${r.content.slice(0, 100)}...`)
      })
      printDisclaimer()
    })

  return data
}
