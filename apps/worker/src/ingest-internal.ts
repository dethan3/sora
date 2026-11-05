import {
  collectionSchema,
  directoryEntrySchema,
  directoryScopeSchema,
  jobStepsSchema,
  z,
} from '@sora/market-data/ingest';
import { authJson, type AuthServices } from './auth.ts';
import { HttpError, readJson } from './http.ts';
import { entryFromRow } from './ingest-model.ts';
import { randomToken, sha256 } from './security.ts';
import type { Env } from './types.ts';

function validated<T>(schema: z.ZodType<T>, value: unknown): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new HttpError(400, '采集数据未通过校验');
  return parsed.data;
}
async function machine(request: Request, env: Env) {
  if (!env.INGEST_TOKEN || env.INGEST_TOKEN.length < 32)
    throw new HttpError(503, '执行器令牌尚未配置');
  const raw = request.headers.get('Authorization') ?? '';
  const actual = await sha256(raw),
    expected = await sha256(`Bearer ${env.INGEST_TOKEN}`);
  let difference = 0;
  for (let i = 0; i < expected.length; i++)
    difference |= actual.charCodeAt(i) ^ expected.charCodeAt(i);
  if (difference) throw new HttpError(401, '执行器认证失败');
}
const leaseSeconds = 600;
export async function handleInternal(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/api/internal/')) return null;
  await machine(request, env);
  if (request.method !== 'POST') throw new HttpError(405, 'Method not allowed');
  const body = await readJson(request, 512 * 1024),
    now = services.now?.() ?? new Date(),
    at = now.toISOString();
  if (path === '/api/internal/directory/start') {
    const input = validated(
        z
          .object({
            scope: directoryScopeSchema,
            count: z.number().int().min(1).max(100000),
          })
          .strict(),
        body,
      ),
      id = crypto.randomUUID();
    await env.DB.prepare('DELETE FROM directory_imports WHERE created_at < ?')
      .bind(new Date(now.getTime() - 86400_000).toISOString())
      .run();
    await env.DB.prepare(
      'INSERT INTO directory_imports(id,scope,expected_count,created_at) VALUES (?,?,?,?)',
    )
      .bind(id, input.scope, input.count, at)
      .run();
    return authJson({ id }, undefined, 201);
  }
  const directory =
    /^\/api\/internal\/directory\/([^/]+)\/(batch|publish)$/.exec(path);
  if (directory) {
    const upload = await env.DB.prepare(
      "SELECT * FROM directory_imports WHERE id=? AND status='uploading'",
    )
      .bind(directory[1])
      .first<{ scope: string; expected_count: number }>();
    if (!upload) throw new HttpError(409, '目录导入不存在或已结束');
    if (directory[2] === 'batch') {
      const input = validated(
        z
          .object({ entries: z.array(directoryEntrySchema).min(1).max(500) })
          .strict(),
        body,
      );
      if (
        input.entries.some(
          (entry) => `${entry.market}:${entry.kind}` !== upload.scope,
        )
      )
        throw new HttpError(400, '目录范围不匹配');
      const data = JSON.stringify(input.entries);
      await env.DB.prepare(
        'INSERT OR IGNORE INTO directory_staging(import_id,id,data_json) VALUES (?,?,?)',
      )
        .bind(directory[1], await sha256(data), data)
        .run();
      return authJson({ accepted: input.entries.length });
    }
    const counts = await env.DB.prepare(
      "SELECT COUNT(*) AS n,COUNT(DISTINCT json_extract(e.value,'$.id')) AS unique_count FROM directory_staging s,json_each(s.data_json) e WHERE s.import_id=?",
    )
      .bind(directory[1])
      .first<{ n: number; unique_count: number }>();
    const count = counts?.n;
    if (count !== upload.expected_count || count !== counts?.unique_count)
      throw new HttpError(409, '目录条数不完整或存在重复代码，保留上一版目录');
    const [market, kind] = upload.scope.split(':');
    const publishToken = crypto.randomUUID();
    const published = await env.DB.batch([
      env.DB.prepare(
        `UPDATE directory_imports SET status='published',publish_token=? WHERE id=? AND status='uploading'
        AND NOT EXISTS(SELECT 1 FROM directory_imports newer WHERE newer.scope=directory_imports.scope AND newer.status='published' AND newer.rowid>directory_imports.rowid) RETURNING id`,
      ).bind(publishToken, directory[1]),
      env.DB.prepare(
        `INSERT INTO symbol_directory(id,kind,symbol,market,name,pinyin,updated_at,currency,source_name,source_url,active,generation_id)
        SELECT json_extract(e.value,'$.id'),json_extract(e.value,'$.kind'),json_extract(e.value,'$.symbol'),json_extract(e.value,'$.market'),json_extract(e.value,'$.name'),json_extract(e.value,'$.pinyin'),?,json_extract(e.value,'$.currency'),json_extract(e.value,'$.sourceName'),json_extract(e.value,'$.sourceUrl'),1,?
        FROM directory_staging s,json_each(s.data_json) e WHERE s.import_id=? AND EXISTS(SELECT 1 FROM directory_imports WHERE id=? AND status='published' AND publish_token=?)
        ON CONFLICT(id) DO UPDATE SET name=excluded.name,pinyin=excluded.pinyin,updated_at=excluded.updated_at,currency=excluded.currency,source_name=excluded.source_name,source_url=excluded.source_url,active=1,generation_id=excluded.generation_id
        WHERE symbol_directory.active<>1 OR symbol_directory.name<>excluded.name OR COALESCE(symbol_directory.pinyin,'')<>excluded.pinyin OR symbol_directory.currency<>excluded.currency OR symbol_directory.source_name<>excluded.source_name OR symbol_directory.source_url<>excluded.source_url`,
      ).bind(at, directory[1], directory[1], directory[1], publishToken),
      env.DB.prepare(
        "UPDATE symbol_directory SET active=0 WHERE market=? AND kind=? AND active=1 AND id NOT IN (SELECT json_extract(e.value,'$.id') FROM directory_staging s,json_each(s.data_json) e WHERE s.import_id=?) AND EXISTS(SELECT 1 FROM directory_imports WHERE id=? AND status='published' AND publish_token=?)",
      ).bind(market, kind, directory[1], directory[1], publishToken),
    ]);
    if (!published[0].results.length)
      throw new HttpError(409, '更新的目录已发布，本次导入已过期');
    return authJson({ published: true, count });
  }
  if (path === '/api/internal/refresh') {
    const day = at.slice(0, 10);
    await env.DB.batch([
      env.DB.prepare(
        `INSERT OR IGNORE INTO ingest_jobs(id,instrument_id,status,created_at,mode)
        SELECT 'refresh:' || ? || ':' || id,id,'queued',?,'refresh' FROM instruments
        WHERE status='ready' AND (added_by IS NOT NULL OR source='static-seed') AND COALESCE(last_refresh_day,'')<>? AND NOT EXISTS(SELECT 1 FROM ingest_jobs j WHERE j.instrument_id=instruments.id AND j.status IN ('queued','running'))`,
      ).bind(day, at, day),
      env.DB.prepare(
        "UPDATE instruments SET last_refresh_day=? WHERE EXISTS(SELECT 1 FROM ingest_jobs WHERE id='refresh:' || ? || ':' || instruments.id)",
      ).bind(day, day),
    ]);
    return authJson({ queued: true });
  }
  if (path === '/api/internal/runner/finish') {
    await env.DB.prepare(
      "UPDATE app_meta SET value='0' WHERE key='dispatch_lock'",
    ).run();
    return authJson({ finished: true });
  }
  if (path === '/api/internal/jobs/claim') {
    const lease = randomToken(),
      hash = await sha256(lease),
      until = new Date(now.getTime() + leaseSeconds * 1000).toISOString();
    await env.DB.batch([
      env.DB.prepare(
        "UPDATE ingest_jobs SET status='failed',error='执行器多次超时，请检查后重试',finished_at=? WHERE status='running' AND lease_until<=? AND attempt>=3",
      ).bind(at, at),
      env.DB.prepare(
        "UPDATE instruments SET status='failed' WHERE status='pending' AND NOT EXISTS(SELECT 1 FROM ingest_jobs WHERE instrument_id=instruments.id AND status IN ('queued','running')) AND EXISTS(SELECT 1 FROM ingest_jobs WHERE instrument_id=instruments.id AND status='failed')",
      ),
    ]);
    const job = await env.DB.prepare(
      `UPDATE ingest_jobs SET status='running',lease_hash=?,lease_until=?,attempt=attempt+1,started_at=?,error=NULL
      WHERE id=(SELECT id FROM ingest_jobs WHERE status='queued' OR (status='running' AND lease_until<=? AND attempt<3) ORDER BY created_at,id LIMIT 1) RETURNING *`,
    )
      .bind(hash, until, at, at)
      .first();
    if (!job) return authJson({ job: null });
    await env.DB.prepare(
      "INSERT INTO app_meta(key,value) VALUES('dispatch_lock',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
    )
      .bind(String(now.getTime() + 900_000))
      .run();
    const row = await env.DB.prepare(
      `SELECT d.* FROM instruments i JOIN symbol_directory d ON d.id=i.directory_id WHERE i.id=?`,
    )
      .bind(job.instrument_id)
      .first();
    if (!row) throw new HttpError(409, '任务目录信息丢失');
    const last = await env.DB.prepare(
      'SELECT MAX(date) AS date FROM market_series WHERE instrument_id=?',
    )
      .bind(job.instrument_id)
      .first();
    return authJson({
      job: {
        id: job.id,
        lease,
        leaseUntil: until,
        mode: job.mode,
        attempt: job.attempt,
        instrument: entryFromRow(row),
        instrumentId: job.instrument_id,
        sinceDate: last?.date ?? null,
      },
    });
  }
  const operation =
    /^\/api\/internal\/jobs\/([^/]+)\/(heartbeat|complete|fail)$/.exec(path);
  if (!operation || typeof body.lease !== 'string')
    throw new HttpError(404, 'Not found');
  const id = decodeURIComponent(operation[1]),
    hash = await sha256(body.lease);
  const job = await env.DB.prepare(
    'SELECT j.*,i.kind,i.market,i.currency FROM ingest_jobs j JOIN instruments i ON i.id=j.instrument_id WHERE j.id=?',
  )
    .bind(id)
    .first();
  if (!job || job.lease_hash !== hash)
    throw new HttpError(409, '任务租约已失效');
  if (operation[2] === 'complete' && job.status === 'done') {
    const payload = validated(collectionSchema, body.result);
    if (job.payload_hash !== (await sha256(JSON.stringify(payload))))
      throw new HttpError(409, '已完成任务的回写内容不一致');
    return authJson({ completed: true, alreadyCompleted: true });
  }
  if (job.status !== 'running' || String(job.lease_until) <= at)
    throw new HttpError(409, '任务租约已失效');
  if (operation[2] === 'heartbeat') {
    const steps = validated(jobStepsSchema, body.steps ?? []),
      until = new Date(now.getTime() + leaseSeconds * 1000).toISOString();
    const updated = await env.DB.prepare(
      "UPDATE ingest_jobs SET lease_until=?,steps_json=? WHERE id=? AND lease_hash=? AND status='running' AND lease_until>? RETURNING id",
    )
      .bind(until, JSON.stringify(steps), id, hash, at)
      .first();
    if (!updated) throw new HttpError(409, '任务租约已失效');
    await env.DB.prepare(
      "UPDATE app_meta SET value=? WHERE key='dispatch_lock'",
    )
      .bind(String(now.getTime() + 900_000))
      .run();
    return authJson({ leaseUntil: until });
  }
  if (operation[2] === 'fail') {
    const message =
      typeof body.error === 'string' ? body.error.slice(0, 500) : '采集失败';
    const failed = await env.DB.batch([
      env.DB.prepare(
        "UPDATE ingest_jobs SET status='failed',error=?,finished_at=? WHERE id=? AND lease_hash=? AND status='running' AND lease_until>? RETURNING id",
      ).bind(message, at, id, hash, at),
      env.DB.prepare(
        "UPDATE instruments SET status='failed' WHERE id=? AND status='pending' AND EXISTS(SELECT 1 FROM ingest_jobs WHERE id=? AND lease_hash=? AND status='failed')",
      ).bind(job.instrument_id, id, hash),
    ]);
    if (!failed[0].results.length) throw new HttpError(409, '任务租约已失效');
    return authJson({ failed: true });
  }
  const result = validated(collectionSchema, body.result),
    series = result.series;
  const expectedType =
    job.kind === 'fund' ? 'nav' : job.kind === 'index' ? 'index' : 'price';
  const zone = (
    {
      CN: 'Asia/Shanghai',
      HK: 'Asia/Hong_Kong',
      US: 'America/New_York',
    } as Record<string, string>
  )[String(job.market)];
  const today = new Intl.DateTimeFormat('en-CA', {
    timeZone: zone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
  const last = await env.DB.prepare(
    'SELECT MAX(date) AS date FROM market_series WHERE instrument_id=?',
  )
    .bind(job.instrument_id)
    .first();
  const previous = await env.DB.prepare(
    'SELECT holdings_json FROM instrument_snapshots WHERE instrument_id=?',
  )
    .bind(job.instrument_id)
    .first();
  const previousHoldings = previous?.holdings_json
    ? JSON.parse(String(previous.holdings_json))
    : null;
  if (
    (result.holdings !== null &&
      (job.mode === 'refresh' ||
        !['fund', 'etf'].includes(String(job.kind)))) ||
    (result.holdings &&
      previousHoldings &&
      result.holdings.asOf < previousHoldings.asOf) ||
    series.type !== expectedType ||
    series.currency !== job.currency ||
    series.timeZone !== zone ||
    series.history.some((row) => row.date >= today) ||
    (last?.date && series.history.at(-1)!.date < String(last.date)) ||
    Date.parse(series.source.fetchedAt) > now.getTime() ||
    (result.holdings &&
      (result.holdings.asOf > at.slice(0, 10) ||
        Date.parse(result.holdings.source.fetchedAt) > now.getTime()))
  )
    throw new HttpError(400, '采集口径、日期或标的映射不匹配');
  const payloadHash = await sha256(JSON.stringify(result)),
    gate =
      "EXISTS(SELECT 1 FROM ingest_jobs WHERE id=? AND lease_hash=? AND payload_hash=? AND status='done')";
  const gateArgs = [id, hash, payloadHash],
    instrumentId = String(job.instrument_id);
  const { history, ...meta } = series;
  const statements = [
    env.DB.prepare(
      "UPDATE ingest_jobs SET status='done',finished_at=?,payload_hash=?,steps_json=?,error=NULL WHERE id=? AND lease_hash=? AND status='running' AND lease_until>? RETURNING id",
    ).bind(
      at,
      payloadHash,
      JSON.stringify([
        { step: 'metadata', status: 'done' },
        { step: 'holdings', status: 'done' },
        { step: 'series', status: 'done' },
      ]),
      id,
      hash,
      at,
    ),
    env.DB.prepare(
      `INSERT INTO market_series(instrument_id,date,close,adjust) SELECT ?,json_extract(value,'$.date'),json_extract(value,'$.close'),'unadjusted' FROM json_each(?) WHERE ${gate}
      ON CONFLICT(instrument_id,date) DO UPDATE SET close=excluded.close,adjust=excluded.adjust`,
    ).bind(instrumentId, JSON.stringify(history), ...gateArgs),
    env.DB.prepare(
      `DELETE FROM market_series WHERE instrument_id=? AND date<? AND ${gate}`,
    ).bind(
      instrumentId,
      new Date(now.getTime() - 380 * 86400_000).toISOString().slice(0, 10),
      ...gateArgs,
    ),
    env.DB.prepare(
      `INSERT INTO instrument_snapshots(instrument_id,holdings_json,series_meta_json,warnings_json,published_at)
      SELECT ?,?,?,?,? WHERE ${gate} ON CONFLICT(instrument_id) DO UPDATE SET series_meta_json=excluded.series_meta_json,
      holdings_json=CASE WHEN ?='full' THEN excluded.holdings_json ELSE instrument_snapshots.holdings_json END,
      warnings_json=CASE WHEN ?='full' THEN excluded.warnings_json ELSE instrument_snapshots.warnings_json END,published_at=excluded.published_at`,
    ).bind(
      instrumentId,
      result.holdings ? JSON.stringify(result.holdings) : null,
      JSON.stringify(meta),
      JSON.stringify(result.warnings),
      at,
      ...gateArgs,
      job.mode,
      job.mode,
    ),
    env.DB.prepare(
      `UPDATE instruments SET status='ready',updated_at=? WHERE id=? AND status<>'archived' AND ${gate}`,
    ).bind(at, instrumentId, ...gateArgs),
  ];
  if (job.mode === 'full' && result.holdings) {
    const rows = result.holdings.holdings;
    statements.push(
      env.DB.prepare(
        `INSERT INTO instruments(id,kind,symbol,market,name,currency,source,status,created_at,updated_at,public_code,directory_id)
        SELECT json_extract(value,'$.id'),'stock',json_extract(value,'$.symbol'),json_extract(value,'$.market'),json_extract(value,'$.name'),CASE json_extract(value,'$.market') WHEN 'CN' THEN 'CNY' WHEN 'US' THEN 'USD' ELSE 'HKD' END,'aktools-holding','ready',?,?,json_extract(value,'$.id'),json_extract(value,'$.id') FROM json_each(?) WHERE ${gate} ON CONFLICT(id) DO NOTHING`,
      ).bind(at, at, JSON.stringify(rows), ...gateArgs),
      env.DB.prepare(
        `DELETE FROM fund_holdings WHERE fund_id=? AND period=? AND ${gate}`,
      ).bind(instrumentId, result.holdings.asOf, ...gateArgs),
      env.DB.prepare(
        `INSERT INTO fund_holdings(fund_id,period,rank,stock_id,weight_bp,coverage,fetched_at) SELECT ?,?,CAST(key AS INTEGER)+1,json_extract(value,'$.id'),json_extract(value,'$.weightBp'),?,? FROM json_each(?) WHERE ${gate}`,
      ).bind(
        instrumentId,
        result.holdings.asOf,
        result.holdings.coverage === 'full' ? 'full' : 'top10',
        result.holdings.source.fetchedAt,
        JSON.stringify(rows),
        ...gateArgs,
      ),
    );
  }
  const published = await env.DB.batch(statements);
  if (!published[0].results.length)
    throw new HttpError(409, '任务已被其他执行器完成或租约失效');
  return authJson({ completed: true, alreadyCompleted: false });
}
