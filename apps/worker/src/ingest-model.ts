import {
  directoryEntrySchema,
  type DirectoryEntry,
} from '@sora/market-data/ingest';
import { LIVE_ACTOR } from './admin.ts';
import { requireAdmin } from './auth.ts';
import { HttpError } from './http.ts';
import { sessionToken, sha256 } from './security.ts';
import type { Env } from './types.ts';

export function entryFromRow(row: Record<string, unknown>): DirectoryEntry {
  return directoryEntrySchema.parse({
    id: row.id,
    market: row.market,
    kind: row.kind,
    symbol: row.symbol,
    name: row.name,
    currency: row.currency,
    pinyin: row.pinyin ?? '',
    sourceName: row.source_name,
    sourceUrl: row.source_url,
  });
}
export async function adminActor(request: Request, env: Env, now: Date) {
  const session = await requireAdmin(request, env, now);
  return {
    ...session,
    args: [
      session.user.id,
      await sha256(sessionToken(request)!),
      now.toISOString(),
    ],
  };
}
export async function queueInstrument(
  env: Env,
  directoryId: string,
  actor: Awaited<ReturnType<typeof adminActor>>,
  now: Date,
  approve = false,
) {
  const directory = await env.DB.prepare(
    'SELECT * FROM symbol_directory WHERE id = ? AND active = 1',
  )
    .bind(directoryId)
    .first();
  if (!directory) throw new HttpError(404, '目录中没有这只标的，请先同步目录');
  const existing = await env.DB.prepare(
    'SELECT id, status FROM instruments WHERE directory_id = ?',
  )
    .bind(directoryId)
    .first<{ id: string; status: string }>();
  if (existing?.status === 'archived')
    throw new HttpError(409, '标的已下架，请先恢复上架');
  const id = existing?.id ?? directoryId,
    audit = crypto.randomUUID(),
    job = crypto.randomUUID(),
    at = now.toISOString();
  const pending = approve
    ? "AND EXISTS (SELECT 1 FROM instrument_requests WHERE directory_id = d.id AND status = 'pending')"
    : "AND NOT EXISTS (SELECT 1 FROM ingest_jobs WHERE instrument_id = ? AND status IN ('queued','running'))";
  const result = await env.DB.batch([
    env.DB.prepare(
      `INSERT INTO audit_log (id,actor_id,action,target,detail_json,created_at)
      SELECT ?, ?, ?, ?, ?, ? FROM symbol_directory d WHERE d.id = ? AND d.active = 1 AND ${LIVE_ACTOR} ${pending} AND NOT EXISTS(SELECT 1 FROM instruments WHERE directory_id=d.id AND status='archived') RETURNING id`,
    ).bind(
      audit,
      actor.user.id,
      approve ? 'request.approve' : 'instrument.add',
      id,
      JSON.stringify({ directoryId }),
      at,
      directoryId,
      ...actor.args,
      ...(approve ? [] : [id]),
    ),
    env.DB.prepare(
      `INSERT INTO instruments (id,kind,symbol,market,name,currency,category,source,source_name,source_url,status,added_by,created_at,updated_at,public_code,directory_id)
      SELECT ?, d.kind,d.symbol,d.market,d.name,d.currency,d.kind,'aktools',d.source_name,d.source_url,'pending',?,?,?, ?,d.id FROM symbol_directory d
      WHERE d.id = ? AND EXISTS (SELECT 1 FROM audit_log WHERE id = ?)
      ON CONFLICT(directory_id) DO UPDATE SET status = CASE WHEN instruments.status = 'ready' THEN 'ready' ELSE 'pending' END, added_by=COALESCE(instruments.added_by,excluded.added_by), updated_at = excluded.updated_at`,
    ).bind(id, actor.user.id, at, at, id, directoryId, audit),
    env.DB.prepare(
      `INSERT OR IGNORE INTO ingest_jobs (id,instrument_id,requested_by,status,steps_json,created_at,mode)
      SELECT ?,?,?, 'queued', ?,?,'full' WHERE EXISTS (SELECT 1 FROM audit_log WHERE id = ?)`,
    ).bind(
      job,
      id,
      actor.user.id,
      JSON.stringify([
        { step: 'metadata', status: 'pending' },
        { step: 'holdings', status: 'pending' },
        { step: 'series', status: 'pending' },
      ]),
      at,
      audit,
    ),
    env.DB.prepare(
      `UPDATE instrument_requests SET status='approved',reviewed_by=?,reviewed_at=?,review_note=NULL WHERE directory_id=? AND status='pending'
      AND EXISTS (SELECT 1 FROM audit_log WHERE id=?)`,
    ).bind(actor.user.id, at, directoryId, audit),
  ]);
  if (!result[0].results.length && approve)
    throw new HttpError(409, '申请状态已变化，请刷新后重试');
  const queued = await env.DB.prepare(
    "SELECT id FROM ingest_jobs WHERE instrument_id = ? AND status IN ('queued','running') ORDER BY created_at DESC LIMIT 1",
  )
    .bind(id)
    .first<{ id: string }>();
  if (!queued) throw new HttpError(409, '任务状态已变化，请刷新后重试');
  return {
    instrumentId: id,
    jobId: queued.id,
    alreadyQueued: !result[0].results.length,
  };
}

export async function dispatchPending(
  env: Env,
  fetcher: typeof fetch = fetch,
  now = new Date(),
) {
  const at = now.toISOString();
  if (!env.GITHUB_TOKEN || !env.GITHUB_REPOSITORY)
    return { status: 'unconfigured', at };
  if (!/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(env.GITHUB_REPOSITORY))
    return { status: 'failed', at, error: 'GitHub 仓库配置无效' };
  const pending = await env.DB.prepare(
    "SELECT id FROM ingest_jobs WHERE status = 'queued' OR (status = 'running' AND lease_until < ?) LIMIT 1",
  )
    .bind(at)
    .first();
  if (!pending) return { status: 'idle', at };
  const lock = await env.DB.prepare(
    `INSERT INTO app_meta (key,value) VALUES ('dispatch_lock',?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(app_meta.value AS INTEGER)<=? RETURNING key`,
  )
    .bind(String(now.getTime() + 900_000), now.getTime())
    .first();
  if (!lock) return { status: 'pending', at };
  let report: { status: string; at: string; error?: string };
  try {
    const response = await fetcher(
      `https://api.github.com/repos/${env.GITHUB_REPOSITORY}/actions/workflows/ingest.yml/dispatches`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
          'User-Agent': 'sora-ingest',
        },
        body: JSON.stringify({
          ref: env.GITHUB_REF ?? 'main',
          inputs: { mode: 'jobs' },
        }),
        signal: AbortSignal.timeout(5000),
        redirect: 'error',
      },
    );
    report = response.ok
      ? { status: 'sent', at }
      : { status: 'failed', at, error: `GitHub HTTP ${response.status}` };
  } catch {
    report = {
      status: 'failed',
      at,
      error: '无法连接 GitHub，任务仍保留在队列',
    };
  }
  await env.DB.prepare(
    "INSERT INTO app_meta(key,value) VALUES ('dispatch_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
  )
    .bind(JSON.stringify(report))
    .run();
  if (report.status === 'failed')
    await env.DB.prepare(
      "UPDATE app_meta SET value=? WHERE key='dispatch_lock'",
    )
      .bind(String(now.getTime() + 60_000))
      .run();
  return report;
}
