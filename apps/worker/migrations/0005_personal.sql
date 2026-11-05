-- The old, unused reservation stored per-unit cost. Preserve any hand-written
-- legacy rows; do not guess a conversion to the frontend's exact total cost.
ALTER TABLE positions RENAME TO positions_legacy;
DROP INDEX positions_user;
CREATE TABLE positions (
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  units TEXT NOT NULL,
  cost_minor INTEGER NOT NULL CHECK(typeof(cost_minor)='integer' AND cost_minor BETWEEN 0 AND 1000000000000),
  currency TEXT NOT NULL CHECK(currency IN ('CNY','USD','HKD','KRW')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(user_id,instrument_id)
);
CREATE TABLE personal_accounts (
  user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  revision INTEGER NOT NULL DEFAULT 0,
  last_mutation TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE personal_operations (
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  operation_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  revision INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(user_id,operation_id)
);
