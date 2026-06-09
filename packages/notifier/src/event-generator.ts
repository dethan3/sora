import type { FundAnalysis, ResearchCard, NotificationEvent, NotificationLevel } from '@sora/core'

function warningLevelToNotification(level: 'info' | 'watch' | 'warning'): NotificationLevel {
  return level
}

let _seq = 0
function nextId(prefix: string): string {
  return `${prefix}-${Date.now()}-${++_seq}`
}

export function fromFundAnalysis(analysis: FundAnalysis): NotificationEvent[] {
  return analysis.warnings.map((w) => ({
    id: nextId('evt'),
    level: warningLevelToNotification(w.level),
    title: `[${w.code}] ${analysis.fundId}`,
    summary: w.message,
    source: 'sora' as const,
    type: w.code.toLowerCase(),
    relatedEntityType: 'fund',
    relatedEntityId: analysis.fundId,
    payload: { warningCode: w.code },
    createdAt: analysis.analyzedAt,
  }))
}

export function fromResearchCard(card: ResearchCard): NotificationEvent[] {
  if (card.status === 'ignore' || card.status === 'invalidated') return []

  const levelMap: Record<string, NotificationLevel> = {
    watch: 'watch',
    active_watch: 'watch',
    confirmed: 'warning',
  }

  const level: NotificationLevel = levelMap[card.status] ?? 'info'

  return [
    {
      id: nextId('evt-card'),
      level,
      title: card.title,
      summary: card.summary,
      source: 'sora' as const,
      type: 'research_card',
      relatedEntityType: 'research_card',
      relatedEntityId: card.id,
      payload: { status: card.status },
      createdAt: card.generatedAt,
    },
  ]
}

export function collectEvents(
  analyses: FundAnalysis[],
  card: ResearchCard | null
): NotificationEvent[] {
  const events: NotificationEvent[] = analyses.flatMap(fromFundAnalysis)
  if (card) events.push(...fromResearchCard(card))
  return events
}
