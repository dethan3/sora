import { sqliteTable, text, real, integer } from 'drizzle-orm/sqlite-core'
import { sql } from 'drizzle-orm'

const nowDefault = sql`(datetime('now'))`

export const markets = sqliteTable('markets', {
  id: text('id').primaryKey(),
  name: text('name').notNull(),
  category: text('category').notNull(),
  description: text('description'),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const indexes = sqliteTable('indexes', {
  id: text('id').primaryKey(),
  name: text('name').notNull(),
  marketId: text('market_id').notNull(),
  ticker: text('ticker').notNull(),
  description: text('description'),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const funds = sqliteTable('funds', {
  id: text('id').primaryKey(),
  fundCode: text('fund_code').notNull(),
  fundName: text('fund_name').notNull(),
  fundType: text('fund_type').notNull(),
  marketId: text('market_id').notNull(),
  trackingIndexId: text('tracking_index_id').notNull(),
  manager: text('manager'),
  fee: real('fee'),
  scale: real('scale'),
  inceptionDate: text('inception_date'),
  isEtf: integer('is_etf', { mode: 'boolean' }).notNull().default(false),
  isEtfFeeder: integer('is_etf_feeder', { mode: 'boolean' }).notNull().default(false),
  isQdii: integer('is_qdii', { mode: 'boolean' }).notNull().default(false),
  purchaseStatus: text('purchase_status').notNull().default('unknown'),
  purchaseLimit: real('purchase_limit'),
  dataSource: text('data_source').notNull(),
  updatedAt: text('updated_at').notNull(),
})

export const fundIndexMappings = sqliteTable('fund_index_mappings', {
  id: text('id').primaryKey(),
  fundId: text('fund_id').notNull(),
  indexId: text('index_id').notNull(),
  isPrimary: integer('is_primary', { mode: 'boolean' }).notNull().default(false),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const fundMetricsSnapshots = sqliteTable('fund_metrics_snapshots', {
  id: text('id').primaryKey(),
  fundId: text('fund_id').notNull(),
  nav: real('nav'),
  price: real('price'),
  premiumRate: real('premium_rate'),
  volume: real('volume'),
  turnover: real('turnover'),
  sharpeRatio: real('sharpe_ratio'),
  maxDrawdown: real('max_drawdown'),
  volatility: real('volatility'),
  trackingError: real('tracking_error'),
  return1m: real('return_1m'),
  return3m: real('return_3m'),
  return6m: real('return_6m'),
  return1y: real('return_1y'),
  return3y: real('return_3y'),
  snapshotDate: text('snapshot_date').notNull(),
  dataSource: text('data_source').notNull(),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const researchCards = sqliteTable('research_cards', {
  id: text('id').primaryKey(),
  title: text('title').notNull(),
  marketId: text('market_id').notNull(),
  relatedIndexIds: text('related_index_ids').notNull().default('[]'),
  relatedFundIds: text('related_fund_ids').notNull().default('[]'),
  summary: text('summary').notNull(),
  keyEvidence: text('key_evidence').notNull().default('[]'),
  fundExecutionRisks: text('fund_execution_risks').notNull().default('[]'),
  marketImplication: text('market_implication').notNull(),
  risks: text('risks').notNull().default('[]'),
  invalidationConditions: text('invalidation_conditions').notNull().default('[]'),
  status: text('status').notNull(),
  generatedAt: text('generated_at').notNull(),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const alerts = sqliteTable('alerts', {
  id: text('id').primaryKey(),
  level: text('level').notNull(),
  title: text('title').notNull(),
  fundId: text('fund_id'),
  message: text('message').notNull(),
  createdAt: text('created_at').notNull().default(nowDefault),
})

export const notificationEvents = sqliteTable('notification_events', {
  id: text('id').primaryKey(),
  level: text('level').notNull(),
  title: text('title').notNull(),
  summary: text('summary').notNull(),
  source: text('source').notNull(),
  type: text('type').notNull(),
  relatedEntityType: text('related_entity_type'),
  relatedEntityId: text('related_entity_id'),
  payload: text('payload'),
  createdAt: text('created_at').notNull(),
})

export const theses = sqliteTable('theses', {
  id: text('id').primaryKey(),
  title: text('title').notNull(),
  summary: text('summary').notNull(),
  timeHorizon: text('time_horizon').notNull(),
  status: text('status').notNull(),
  confidence: real('confidence').notNull(),
  causalChain: text('causal_chain').notNull().default('[]'),
  keyAssumptions: text('key_assumptions').notNull().default('[]'),
  affectedMarketIds: text('affected_market_ids').notNull().default('[]'),
  affectedIndexIds: text('affected_index_ids').notNull().default('[]'),
  affectedFundIds: text('affected_fund_ids').notNull().default('[]'),
  invalidationConditions: text('invalidation_conditions').notNull().default('[]'),
  createdAt: text('created_at').notNull(),
  updatedAt: text('updated_at').notNull(),
})

export const thesisEvidence = sqliteTable('thesis_evidence', {
  id: text('id').primaryKey(),
  thesisId: text('thesis_id').notNull(),
  source: text('source').notNull(),
  title: text('title').notNull(),
  summary: text('summary').notNull(),
  url: text('url'),
  direction: text('direction').notNull(),
  strength: text('strength').notNull(),
  confidenceDelta: real('confidence_delta').notNull(),
  rationale: text('rationale').notNull(),
  observedAt: text('observed_at').notNull(),
  createdAt: text('created_at').notNull(),
})

export const thesisUpdates = sqliteTable('thesis_updates', {
  id: text('id').primaryKey(),
  thesisId: text('thesis_id').notNull(),
  previousConfidence: real('previous_confidence').notNull(),
  newConfidence: real('new_confidence').notNull(),
  evidenceIds: text('evidence_ids').notNull().default('[]'),
  rationale: text('rationale').notNull(),
  createdAt: text('created_at').notNull(),
})

export const assetExposures = sqliteTable('asset_exposures', {
  id: text('id').primaryKey(),
  thesisId: text('thesis_id').notNull(),
  assetType: text('asset_type').notNull(),
  assetId: text('asset_id').notNull(),
  exposureScore: real('exposure_score').notNull(),
  rationale: text('rationale').notNull(),
  updatedAt: text('updated_at').notNull(),
})

export const schema = {
  markets,
  indexes,
  funds,
  fundIndexMappings,
  fundMetricsSnapshots,
  researchCards,
  alerts,
  notificationEvents,
  theses,
  thesisEvidence,
  thesisUpdates,
  assetExposures,
}
