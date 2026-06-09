import type { ResearchCard, NotificationEvent } from '@sora/core'
import type { SoraDb } from '../db.js'
import { researchCards, notificationEvents } from '../schema.js'

export function saveResearchCard(db: SoraDb, card: ResearchCard): void {
  db.insert(researchCards).values({
    id: card.id,
    title: card.title,
    marketId: card.marketId,
    relatedIndexIds: JSON.stringify(card.relatedIndexIds),
    relatedFundIds: JSON.stringify(card.relatedFundIds),
    summary: card.summary,
    keyEvidence: JSON.stringify(card.keyEvidence),
    fundExecutionRisks: JSON.stringify(card.fundExecutionRisks),
    marketImplication: card.marketImplication,
    risks: JSON.stringify(card.risks),
    invalidationConditions: JSON.stringify(card.invalidationConditions),
    status: card.status,
    generatedAt: card.generatedAt,
  }).run()
}

export function saveNotificationEvents(db: SoraDb, events: NotificationEvent[]): void {
  for (const evt of events) {
    db.insert(notificationEvents).values({
      id: evt.id,
      level: evt.level,
      title: evt.title,
      summary: evt.summary,
      source: evt.source,
      type: evt.type,
      relatedEntityType: evt.relatedEntityType ?? null,
      relatedEntityId: evt.relatedEntityId ?? null,
      payload: evt.payload ? JSON.stringify(evt.payload) : null,
      createdAt: evt.createdAt,
    }).run()
  }
}
