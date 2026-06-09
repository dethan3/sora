import { mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { openDb } from './db.js'
import type { SoraDb } from './db.js'
import type Database from 'better-sqlite3'

const CREATE_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS markets (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS indexes (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  market_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS funds (
  id TEXT PRIMARY KEY,
  fund_code TEXT NOT NULL,
  fund_name TEXT NOT NULL,
  fund_type TEXT NOT NULL,
  market_id TEXT NOT NULL,
  tracking_index_id TEXT NOT NULL,
  manager TEXT,
  fee REAL,
  scale REAL,
  inception_date TEXT,
  is_etf INTEGER NOT NULL DEFAULT 0,
  is_etf_feeder INTEGER NOT NULL DEFAULT 0,
  is_qdii INTEGER NOT NULL DEFAULT 0,
  purchase_status TEXT NOT NULL DEFAULT 'unknown',
  purchase_limit REAL,
  data_source TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fund_index_mappings (
  id TEXT PRIMARY KEY,
  fund_id TEXT NOT NULL,
  index_id TEXT NOT NULL,
  is_primary INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fund_metrics_snapshots (
  id TEXT PRIMARY KEY,
  fund_id TEXT NOT NULL,
  nav REAL,
  price REAL,
  premium_rate REAL,
  volume REAL,
  turnover REAL,
  sharpe_ratio REAL,
  max_drawdown REAL,
  volatility REAL,
  tracking_error REAL,
  return_1m REAL,
  return_3m REAL,
  return_6m REAL,
  return_1y REAL,
  return_3y REAL,
  snapshot_date TEXT NOT NULL,
  data_source TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS research_cards (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  market_id TEXT NOT NULL,
  related_index_ids TEXT NOT NULL DEFAULT '[]',
  related_fund_ids TEXT NOT NULL DEFAULT '[]',
  summary TEXT NOT NULL,
  key_evidence TEXT NOT NULL DEFAULT '[]',
  fund_execution_risks TEXT NOT NULL DEFAULT '[]',
  market_implication TEXT NOT NULL,
  risks TEXT NOT NULL DEFAULT '[]',
  invalidation_conditions TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY,
  level TEXT NOT NULL,
  title TEXT NOT NULL,
  fund_id TEXT,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS notification_events (
  id TEXT PRIMARY KEY,
  level TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  source TEXT NOT NULL,
  type TEXT NOT NULL,
  related_entity_type TEXT,
  related_entity_id TEXT,
  payload TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS theses (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  time_horizon TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL NOT NULL,
  causal_chain TEXT NOT NULL DEFAULT '[]',
  key_assumptions TEXT NOT NULL DEFAULT '[]',
  affected_market_ids TEXT NOT NULL DEFAULT '[]',
  affected_index_ids TEXT NOT NULL DEFAULT '[]',
  affected_fund_ids TEXT NOT NULL DEFAULT '[]',
  invalidation_conditions TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thesis_evidence (
  id TEXT PRIMARY KEY,
  thesis_id TEXT NOT NULL,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  url TEXT,
  direction TEXT NOT NULL,
  strength TEXT NOT NULL,
  confidence_delta REAL NOT NULL,
  rationale TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thesis_updates (
  id TEXT PRIMARY KEY,
  thesis_id TEXT NOT NULL,
  previous_confidence REAL NOT NULL,
  new_confidence REAL NOT NULL,
  evidence_ids TEXT NOT NULL DEFAULT '[]',
  rationale TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_exposures (
  id TEXT PRIMARY KEY,
  thesis_id TEXT NOT NULL,
  asset_type TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  exposure_score REAL NOT NULL,
  rationale TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`

export function dbInit(dbPath: string): { sqlite: Database.Database; db: SoraDb } {
  if (dbPath !== ':memory:') {
    mkdirSync(dirname(dbPath), { recursive: true })
  }
  const { sqlite, db } = openDb(dbPath)
  sqlite.exec(CREATE_TABLES_SQL)
  return { sqlite, db }
}
