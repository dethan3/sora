import { Command } from 'commander'
import { MarketService } from '@sora/market'
import { FundService } from '@sora/fund'
import { ResearchService } from '@sora/research'
import { PiResearchAgent } from '@sora/agent'
import { createMarketSource, createFundSource } from '@sora/sources'
import {
  dbInit,
  closeDb,
  saveResearchCard,
} from '@sora/storage'
import { SEEDS_DIR, CACHE_DIR, DB_PATH, PI_API_KEY, PI_BASE_URL } from '../utils/env.js'
import { printDisclaimer } from '../utils/format.js'

export function makeResearchCommand(): Command {
  const research = new Command('research').description('研究卡片生成与查看')

  research
    .command('create')
    .description('生成研究卡片（调用 Pi Agent 或确定性生成器）')
    .option('--market <marketId>', '市场 ID')
    .option('--index <indexId>', '指数 ID')
    .option('--save', '保存到数据库', false)
    .action(async (opts: { market?: string; index?: string; save: boolean }) => {
      const marketSvc = new MarketService(SEEDS_DIR, createMarketSource())
      const fundSvc = new FundService(SEEDS_DIR, createFundSource())

      let marketId: string
      if (opts.market) {
        marketId = opts.market
      } else if (opts.index) {
        const idx = (await marketSvc.listIndexes()).find((i) => i.id === opts.index)
        if (!idx) {
          console.error(`❌ 指数 "${opts.index}" 不存在`)
          process.exit(1)
        }
        marketId = idx.marketId
      } else {
        console.error('❌ 请指定 --market 或 --index')
        process.exit(1)
      }

      const market = await marketSvc.getMarket(marketId)
      if (!market) {
        console.error(`❌ 市场 "${marketId}" 不存在`)
        process.exit(1)
      }

      const indexes = await marketSvc.listIndexesByMarket(marketId)
      const fundItems = await fundSvc.getFundsByMarket(marketId)
      const fundAnalyses = await fundSvc.analyzeFunds(fundItems)

      let agent: PiResearchAgent | null = null
      if (PI_API_KEY) {
        agent = new PiResearchAgent({ apiKey: PI_API_KEY, baseUrl: PI_BASE_URL, cacheDir: CACHE_DIR })
        console.log('🤖 Pi Agent 已配置，将调用 Pi API...')
      } else {
        console.log('ℹ️  未配置 SORA_PI_API_KEY，使用确定性生成器')
      }

      const resSvc = new ResearchService(agent)
      console.log(`🔬 生成 ${market.name} 研究卡片...`)

      const card = await resSvc.generateCard({ market, indexes, fundAnalyses })

      console.log(`\n📋 ${card.title}`)
      console.log(`   状态：${card.status}`)
      console.log(`   摘要：${card.summary}`)
      console.log('\n关键证据：')
      card.keyEvidence.forEach((e) => console.log(`  • ${e}`))
      if (card.fundExecutionRisks.length > 0) {
        console.log('\n基金执行风险：')
        card.fundExecutionRisks.forEach((r) => console.log(`  ⚠️  ${r}`))
      }
      console.log('\n市场传导：')
      console.log(`  ${card.marketImplication}`)

      if (opts.save) {
        const { sqlite, db } = dbInit(DB_PATH)
        saveResearchCard(db, card)
        closeDb(sqlite)
        console.log(`\n💾 已保存到数据库：${card.id}`)
      }

      printDisclaimer()
    })

  research
    .command('list')
    .description('列出已生成的研究卡片（来自数据库）')
    .action(() => {
      try {
        const { sqlite } = dbInit(DB_PATH)
        const cards = (
          sqlite.prepare('SELECT id, title, market_id, status, generated_at FROM research_cards ORDER BY generated_at DESC').all() as Array<{
            id: string; title: string; market_id: string; status: string; generated_at: string
          }>
        )
        closeDb(sqlite)
        if (cards.length === 0) {
          console.log('暂无数据（使用 research create --save 生成并保存）')
          return
        }
        cards.forEach((c) => {
          console.log(`\n[${c.status}] ${c.title}`)
          console.log(`    ID: ${c.id}  |  市场: ${c.market_id}  |  生成时间: ${c.generated_at}`)
        })
      } catch {
        console.log('暂无数据（数据库未初始化，请先运行 db init）')
      }
      printDisclaimer()
    })

  return research
}
