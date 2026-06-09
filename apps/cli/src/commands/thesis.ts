import { Command } from 'commander'
import {
  getAssetExposure,
  getEvidenceGroups,
  getEvidenceTimeline,
  getThesis,
  listTheses,
  reviewTheses,
} from '@sora/research'
import { closeDb, dbInit } from '@sora/storage'
import type { SoraDb } from '@sora/storage'
import { DB_PATH } from '../utils/env.js'
import { printDisclaimer, printTable } from '../utils/format.js'

function withDb<T>(fn: (db: SoraDb) => T): T {
  const { sqlite, db } = dbInit(DB_PATH)
  try {
    return fn(db)
  } finally {
    closeDb(sqlite)
  }
}

export function makeThesisCommand(): Command {
  const thesis = new Command('thesis').description('Thesis 工作流')

  thesis
    .command('list')
    .description('列出所有 thesis')
    .action(() => {
      withDb((db) => {
        const rows = listTheses(db).map((item) => {
          const latestEvidence = getEvidenceTimeline(db, item.id)[0]
          return [
            item.id,
            item.title.slice(0, 24),
            item.status,
            String(item.confidence),
            latestEvidence?.observedAt ?? '-',
          ]
        })

        if (rows.length === 0) {
          console.log('暂无 thesis 数据，请先运行 db seed')
          return
        }

        printTable(['ID', '标题', '状态', '置信度', '最新证据时间'], rows)
      })
      printDisclaimer()
    })

  thesis
    .command('show')
    .argument('<id>', 'Thesis ID')
    .description('查看 thesis 详情')
    .action((id: string) => {
      withDb((db) => {
        const item = getThesis(db, id)
        if (!item) {
          console.error(`Thesis "${id}" 不存在`)
          process.exit(1)
        }

        const evidence = getEvidenceGroups(db, id)
        const exposure = getAssetExposure(db, id).slice(0, 5)

        console.log(`\n${item.title} (${item.id})`)
        console.log(`摘要：${item.summary}`)
        console.log(`周期：${item.timeHorizon}`)
        console.log(`状态：${item.status}`)
        console.log(`置信度：${item.confidence}`)

        printSection('因果链', item.causalChain)
        printSection('关键假设', item.keyAssumptions)
        printEvidenceSection('支持证据', evidence.support)
        printEvidenceSection('反向证据', evidence.against)
        printSection('失效条件', item.invalidationConditions)

        if (exposure.length > 0) {
          console.log('\nTop Exposure')
          printTable(
            ['类型', '资产 ID', '分数', '说明'],
            exposure.map((asset) => [
              asset.assetType,
              asset.assetId,
              String(asset.exposureScore),
              asset.rationale.slice(0, 36),
            ])
          )
        }
      })
      printDisclaimer()
    })

  thesis
    .command('review')
    .description('复盘 thesis 状态')
    .action(() => {
      withDb((db) => {
        const review = reviewTheses(db)

        printThesisList('有更新记录的 thesis', review.changedTheses)
        printThesisList('被挑战的 thesis', review.challengedTheses)
        printThesisList('置信度最高的 thesis', review.strongestTheses)

        console.log('\n最新支持证据')
        printTable(
          ['Thesis', '强度', '标题', '时间'],
          review.latestSupportEvidence.map((item) => [
            item.thesisId,
            item.strength,
            item.title.slice(0, 28),
            item.observedAt,
          ])
        )

        console.log('\n最新反向证据')
        printTable(
          ['Thesis', '强度', '标题', '时间'],
          review.latestAgainstEvidence.map((item) => [
            item.thesisId,
            item.strength,
            item.title.slice(0, 28),
            item.observedAt,
          ])
        )
      })
      printDisclaimer()
    })

  thesis
    .command('exposure')
    .argument('<id>', 'Thesis ID')
    .description('查看 thesis 关联资产暴露')
    .action((id: string) => {
      withDb((db) => {
        const exposure = getAssetExposure(db, id)
        if (exposure.length === 0) {
          console.log('暂无暴露数据')
          return
        }

        printTable(
          ['类型', '资产 ID', '暴露分数', '说明'],
          exposure.map((asset) => [
            asset.assetType,
            asset.assetId,
            String(asset.exposureScore),
            asset.rationale.slice(0, 48),
          ])
        )
      })
      printDisclaimer()
    })

  return thesis
}

function printSection(title: string, rows: string[]): void {
  if (rows.length === 0) return
  console.log(`\n${title}`)
  rows.forEach((row) => console.log(`- ${row}`))
}

function printEvidenceSection(title: string, rows: ReturnType<typeof getEvidenceTimeline>): void {
  if (rows.length === 0) return
  console.log(`\n${title}`)
  rows.slice(0, 5).forEach((row) => {
    console.log(`- [${row.strength}] ${row.title} (${row.observedAt})`)
  })
}

function printThesisList(title: string, rows: ReturnType<typeof listTheses>): void {
  console.log(`\n${title}`)
  if (rows.length === 0) {
    console.log('暂无数据')
    return
  }

  printTable(
    ['ID', '状态', '置信度', '标题'],
    rows.map((item) => [item.id, item.status, String(item.confidence), item.title.slice(0, 28)])
  )
}
