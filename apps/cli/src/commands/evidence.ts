import { Command } from 'commander'
import { EvidenceDirectionSchema, EvidenceStrengthSchema } from '@sora/core'
import { addEvidence } from '@sora/research'
import { closeDb, dbInit } from '@sora/storage'
import { DB_PATH } from '../utils/env.js'
import { printDisclaimer } from '../utils/format.js'

interface AddEvidenceOptions {
  thesis: string
  direction: string
  strength: string
  title: string
  summary?: string
  source?: string
  rationale?: string
  url?: string
  observedAt?: string
}

export function makeEvidenceCommand(): Command {
  const evidence = new Command('evidence').description('Thesis 证据管理')

  evidence
    .command('add')
    .description('为 thesis 添加证据并更新置信度')
    .requiredOption('--thesis <id>', 'Thesis ID')
    .requiredOption('--direction <direction>', 'support | against | neutral')
    .requiredOption('--strength <strength>', 'weak | medium | strong')
    .requiredOption('--title <title>', '证据标题')
    .option('--summary <summary>', '证据摘要，默认使用标题')
    .option('--source <source>', '证据来源', 'manual')
    .option('--rationale <rationale>', '更新理由', 'Manual evidence entry.')
    .option('--url <url>', '证据链接')
    .option('--observed-at <isoDate>', '观察时间，默认当前时间')
    .action((opts: AddEvidenceOptions) => {
      const direction = EvidenceDirectionSchema.safeParse(opts.direction)
      const strength = EvidenceStrengthSchema.safeParse(opts.strength)

      if (!direction.success) {
        console.error('direction 必须是 support、against 或 neutral')
        process.exit(1)
      }
      if (!strength.success) {
        console.error('strength 必须是 weak、medium 或 strong')
        process.exit(1)
      }

      const { sqlite, db } = dbInit(DB_PATH)
      try {
        const result = addEvidence(db, {
          thesisId: opts.thesis,
          source: opts.source ?? 'manual',
          title: opts.title,
          summary: opts.summary ?? opts.title,
          url: opts.url,
          direction: direction.data,
          strength: strength.data,
          rationale: opts.rationale ?? 'Manual evidence entry.',
          observedAt: opts.observedAt ?? new Date().toISOString(),
        })

        console.log(`证据 ID：${result.evidence.id}`)
        console.log(`原置信度：${result.update.previousConfidence}`)
        console.log(`新置信度：${result.update.newConfidence}`)
        console.log(`置信度变化：${result.evidence.confidenceDelta}`)
        console.log(`当前状态：${result.thesis.status}`)
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        console.error(message)
        process.exit(1)
      } finally {
        closeDb(sqlite)
      }

      printDisclaimer()
    })

  return evidence
}
