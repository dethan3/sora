DROP INDEX instruments_symbol;
CREATE UNIQUE INDEX instruments_identity ON instruments (market, kind, symbol);
ALTER TABLE instruments ADD COLUMN public_code TEXT;
ALTER TABLE instruments ADD COLUMN directory_id TEXT;
ALTER TABLE instruments ADD COLUMN ingest_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE instruments ADD COLUMN last_refresh_day TEXT;
UPDATE instruments SET public_code = symbol;
CREATE UNIQUE INDEX instruments_public_code ON instruments (public_code);
CREATE UNIQUE INDEX instruments_directory ON instruments (directory_id);

ALTER TABLE symbol_directory ADD COLUMN currency TEXT NOT NULL DEFAULT 'CNY';
ALTER TABLE symbol_directory ADD COLUMN source_name TEXT NOT NULL DEFAULT 'AKTools';
ALTER TABLE symbol_directory ADD COLUMN source_url TEXT NOT NULL DEFAULT 'https://akshare.akfamily.xyz/';
ALTER TABLE symbol_directory ADD COLUMN active INTEGER NOT NULL DEFAULT 1;
ALTER TABLE symbol_directory ADD COLUMN generation_id TEXT;
CREATE INDEX directory_active_kind ON symbol_directory (active, market, kind, symbol);

-- Preserve legacy public URLs while joining the new market/kind directory IDs.
INSERT INTO symbol_directory (id, kind, symbol, market, name, pinyin, updated_at, currency, source_name, source_url)
SELECT CASE WHEN market = '美国市场' THEN 'US' WHEN market IN ('中国香港','香港市场') THEN 'HK' ELSE 'CN' END || ':' || kind || ':' || symbol,
  kind, symbol, CASE WHEN market = '美国市场' THEN 'US' WHEN market IN ('中国香港','香港市场') THEN 'HK' ELSE 'CN' END,
  name, '', updated_at, currency, COALESCE(source_name,'参考目录'), COALESCE(source_url,'https://akshare.akfamily.xyz/') FROM instruments;
UPDATE instruments SET directory_id = (CASE WHEN market = '美国市场' THEN 'US' WHEN market IN ('中国香港','香港市场') THEN 'HK' ELSE 'CN' END) || ':' || kind || ':' || symbol;
UPDATE instruments SET market = CASE WHEN market = '美国市场' THEN 'US' WHEN market IN ('中国香港','香港市场') THEN 'HK' WHEN market = '中国内地' THEN 'CN' ELSE market END;

CREATE TABLE directory_imports (id TEXT PRIMARY KEY, scope TEXT NOT NULL, expected_count INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'uploading', publish_token TEXT, created_at TEXT NOT NULL);
-- One JSON array per batch; avoid writing one staging row per symbol.
CREATE TABLE directory_staging (import_id TEXT NOT NULL REFERENCES directory_imports(id) ON DELETE CASCADE, id TEXT NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY(import_id,id));
CREATE TABLE instrument_snapshots (instrument_id TEXT PRIMARY KEY REFERENCES instruments(id), holdings_json TEXT, series_meta_json TEXT, warnings_json TEXT NOT NULL DEFAULT '[]', published_at TEXT NOT NULL);

ALTER TABLE ingest_jobs ADD COLUMN mode TEXT NOT NULL DEFAULT 'full';
ALTER TABLE ingest_jobs ADD COLUMN lease_hash TEXT;
ALTER TABLE ingest_jobs ADD COLUMN lease_until TEXT;
ALTER TABLE ingest_jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ingest_jobs ADD COLUMN payload_hash TEXT;
ALTER TABLE ingest_jobs ADD COLUMN started_at TEXT;
CREATE UNIQUE INDEX jobs_one_active ON ingest_jobs (instrument_id) WHERE status IN ('queued','running');
CREATE INDEX jobs_leases ON ingest_jobs (status, lease_until, created_at);
CREATE INDEX requests_pending ON instrument_requests (status, directory_id, created_at);
