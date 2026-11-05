-- Sora Phase 5 initial schema (Cloudflare D1 / SQLite)
-- Money and prices are stored as decimal strings; weights as integer basis points.

-- ===== App metadata =====
CREATE TABLE app_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- ===== Users & auth =====
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE COLLATE NOCASE,
  password_hash TEXT,
  display_name TEXT,
  role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
  base_currency TEXT NOT NULL DEFAULT 'CNY',
  disabled_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE sessions (
  id_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  user_agent TEXT,
  ip TEXT
);
CREATE INDEX sessions_user ON sessions (user_id);

CREATE TABLE invites (
  code_hash TEXT PRIMARY KEY,
  created_by TEXT NOT NULL REFERENCES users (id),
  used_by TEXT REFERENCES users (id),
  used_at TEXT,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- Reserved for v2 (email verification / password reset); unused in v1.
CREATE TABLE email_codes (
  email TEXT NOT NULL COLLATE NOCASE,
  purpose TEXT NOT NULL CHECK (purpose IN ('verify', 'reset')),
  code_hash TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (email, purpose)
);

-- Reserved for future OAuth providers.
CREATE TABLE oauth_accounts (
  provider TEXT NOT NULL,
  provider_user_id TEXT NOT NULL,
  user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  PRIMARY KEY (provider, provider_user_id)
);

CREATE TABLE rate_limits (
  bucket TEXT PRIMARY KEY,
  count INTEGER NOT NULL,
  window_start TEXT NOT NULL
);

-- ===== Public research data (admin-writable only) =====
CREATE TABLE symbol_directory (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('fund', 'etf', 'stock', 'index')),
  symbol TEXT NOT NULL,
  market TEXT NOT NULL,
  name TEXT NOT NULL,
  pinyin TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX symbol_directory_symbol ON symbol_directory (symbol);
CREATE INDEX symbol_directory_name ON symbol_directory (name);

CREATE TABLE instruments (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('fund', 'etf', 'stock', 'index')),
  symbol TEXT NOT NULL,
  market TEXT NOT NULL,
  name TEXT NOT NULL,
  currency TEXT NOT NULL,
  category TEXT,
  benchmark TEXT,
  note TEXT,
  source TEXT NOT NULL,
  source_name TEXT,
  source_url TEXT,
  impact_tags_json TEXT,
  chain_node TEXT,
  status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('pending', 'ready', 'failed', 'archived')),
  position INTEGER NOT NULL DEFAULT 0,
  added_by TEXT REFERENCES users (id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX instruments_symbol ON instruments (symbol);

CREATE TABLE catalog_modules (
  id TEXT PRIMARY KEY,
  position INTEGER NOT NULL,
  data_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE fund_holdings (
  fund_id TEXT NOT NULL REFERENCES instruments (id) ON DELETE CASCADE,
  period TEXT NOT NULL,
  rank INTEGER NOT NULL,
  stock_id TEXT NOT NULL REFERENCES instruments (id),
  weight_bp INTEGER NOT NULL,
  coverage TEXT NOT NULL CHECK (coverage IN ('full', 'top10')),
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (fund_id, period, rank)
);

CREATE TABLE market_series (
  instrument_id TEXT NOT NULL REFERENCES instruments (id) ON DELETE CASCADE,
  date TEXT NOT NULL,
  close TEXT NOT NULL,
  adjust TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (instrument_id, date)
);

CREATE TABLE ingest_jobs (
  id TEXT PRIMARY KEY,
  instrument_id TEXT NOT NULL,
  requested_by TEXT REFERENCES users (id),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed')),
  steps_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE INDEX ingest_jobs_status ON ingest_jobs (status);

CREATE TABLE instrument_requests (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  directory_id TEXT NOT NULL REFERENCES symbol_directory (id),
  reason TEXT,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  review_note TEXT,
  reviewed_by TEXT REFERENCES users (id),
  reviewed_at TEXT,
  created_at TEXT NOT NULL,
  UNIQUE (user_id, directory_id)
);

CREATE TABLE audit_log (
  id TEXT PRIMARY KEY,
  actor_id TEXT REFERENCES users (id),
  action TEXT NOT NULL,
  target TEXT,
  detail_json TEXT,
  created_at TEXT NOT NULL
);

-- ===== User-owned data =====
CREATE TABLE watchlist (
  user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  instrument_id TEXT NOT NULL REFERENCES instruments (id),
  created_at TEXT NOT NULL,
  PRIMARY KEY (user_id, instrument_id)
);

CREATE TABLE positions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  instrument_id TEXT NOT NULL REFERENCES instruments (id),
  quantity TEXT NOT NULL,
  cost_price TEXT NOT NULL,
  currency TEXT NOT NULL,
  opened_at TEXT,
  note TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX positions_user ON positions (user_id);
