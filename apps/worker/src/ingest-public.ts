import { impactTagsSchema } from '@sora/market-data/ingest';
import { LIVE_ACTOR } from './admin.ts';
import { authJson, requireUser, type AuthServices } from './auth.ts';
import { HttpError, readJson } from './http.ts';
import {
  adminActor,
  dispatchPending,
  queueInstrument,
} from './ingest-model.ts';
import { checkOrigin, rateLimit, sha256, sessionToken } from './security.ts';
import type { Env } from './types.ts';

export async function instrumentDetail(env: Env, id: string) {
  const row = await env.DB.prepare(
    "SELECT * FROM instruments WHERE (id = ? OR public_code = ?) AND status <> 'archived'",
  )
    .bind(id, id)
    .first();
  if (!row) throw new HttpError(404, '标的尚未收录或已下架');
  const snapshot = await env.DB.prepare(
    'SELECT * FROM instrument_snapshots WHERE instrument_id = ?',
  )
    .bind(row.id)
    .first();
  const series = snapshot?.series_meta_json
    ? JSON.parse(String(snapshot.series_meta_json))
    : null;
  if (series)
    series.history = (
      await env.DB.prepare(
        'SELECT date, close FROM market_series WHERE instrument_id = ? ORDER BY date',
      )
        .bind(row.id)
        .all()
    ).results;
  const error = await env.DB.prepare(
    'SELECT error FROM ingest_jobs WHERE instrument_id=? ORDER BY created_at DESC,id DESC LIMIT 1',
  )
    .bind(row.id)
    .first();
  const holdings = snapshot?.holdings_json
    ? JSON.parse(String(snapshot.holdings_json))
    : null;
  const companies = holdings
    ? (
        await env.DB.prepare(
          "SELECT id,impact_tags_json,chain_node FROM instruments WHERE status<>'archived' AND id IN (SELECT json_extract(value,'$.id') FROM json_each(?))",
        )
          .bind(JSON.stringify(holdings.holdings))
          .all()
      ).results.map((company) => ({
        id: company.id,
        impactTags: JSON.parse(String(company.impact_tags_json ?? '[]')),
        chainNode: company.chain_node,
      }))
    : [];
  return {
    id: row.id,
    code: row.public_code,
    market: row.market,
    kind: row.kind,
    symbol: row.symbol,
    name: row.name,
    currency: row.currency,
    status: row.status,
    impactTags: JSON.parse(String(row.impact_tags_json ?? '[]')),
    chainNode: row.chain_node,
    revision: row.ingest_revision,
    holdings,
    series,
    companies,
    warnings: JSON.parse(String(snapshot?.warnings_json ?? '[]')),
    lastError: error?.error ?? null,
  };
}
export async function handleIngestPublic(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const url = new URL(request.url),
    path = url.pathname,
    now = services.now?.() ?? new Date(),
    at = now.toISOString();
  if (path === '/api/search' && request.method === 'GET') {
    const q = (url.searchParams.get('q') ?? '').trim();
    if (!q || q.length > 100) return authJson({ items: [] });
    const escaped = `%${q.replace(/[\\%_]/g, '\\$&')}%`;
    const items = await env.DB.prepare(
      `SELECT d.id,d.kind,d.symbol,d.market,d.name,d.currency,i.id AS instrumentId,i.public_code AS code,i.status
      FROM symbol_directory d LEFT JOIN instruments i ON i.directory_id = d.id
      WHERE d.active=1 AND (d.symbol LIKE ? ESCAPE '\\' OR d.name LIKE ? ESCAPE '\\' OR d.pinyin LIKE ? ESCAPE '\\')
      ORDER BY CASE WHEN d.symbol = ? THEN 0 ELSE 1 END, d.market,d.kind,d.symbol LIMIT 30`,
    )
      .bind(escaped, escaped, escaped, q.toUpperCase())
      .all();
    return authJson({
      items: items.results,
      directoryUpdatedAt:
        (
          await env.DB.prepare(
            `SELECT COALESCE((SELECT MAX(created_at) FROM directory_imports WHERE status='published'),MAX(updated_at)) AS at FROM symbol_directory WHERE active=1`,
          ).first()
        )?.at ?? null,
    });
  }
  const detailMatch = /^\/api\/instruments\/([^/]+)$/.exec(path);
  if (detailMatch && request.method === 'GET')
    return authJson(
      await instrumentDetail(env, decodeURIComponent(detailMatch[1])),
    );
  if (path === '/api/me/requests') {
    const session = await requireUser(request, env, now);
    if (request.method === 'GET') {
      const items = await env.DB.prepare(
        `SELECT r.id,r.directory_id AS directoryId,r.reason,r.status,r.review_note AS reviewNote,r.created_at AS createdAt,
        d.name,d.symbol,d.market,d.kind,i.id AS instrumentId,i.status AS instrumentStatus FROM instrument_requests r JOIN symbol_directory d ON d.id=r.directory_id
        LEFT JOIN instruments i ON i.directory_id=d.id WHERE r.user_id=? ORDER BY r.created_at DESC LIMIT 200`,
      )
        .bind(session.user.id)
        .all();
      return authJson({ items: items.results }, session.cookie);
    }
    if (request.method !== 'POST')
      throw new HttpError(405, 'Method not allowed');
    checkOrigin(request, env);
    const body = await readJson(request);
    if (
      typeof body.directoryId !== 'string' ||
      typeof (body.reason ?? '') !== 'string' ||
      String(body.reason ?? '').length > 300
    )
      throw new HttpError(400, '申请内容无效');
    const existing = await env.DB.prepare(
      'SELECT id,status FROM instrument_requests WHERE user_id=? AND directory_id=?',
    )
      .bind(session.user.id, body.directoryId)
      .first();
    if (existing)
      return authJson({ ...existing, alreadyRequested: true }, session.cookie);
    const localDay = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(now);
    const bucket = await sha256(`request:${session.user.id}:${localDay}`),
      id = crypto.randomUUID();
    const results = await env.DB.batch([
      env.DB.prepare(
        'INSERT OR IGNORE INTO rate_limits (bucket,count,window_start) VALUES (?,0,?)',
      ).bind(bucket, at),
      env.DB.prepare(
        `INSERT INTO instrument_requests (id,user_id,directory_id,reason,status,created_at)
        SELECT ?,?,?,?,'pending',? FROM symbol_directory d WHERE d.id=? AND active=1
        AND NOT EXISTS (SELECT 1 FROM instruments WHERE directory_id=d.id AND status='ready')
        AND NOT EXISTS (SELECT 1 FROM instrument_requests WHERE user_id=? AND directory_id=d.id)
        AND (SELECT count FROM rate_limits WHERE bucket=?)<5
        AND EXISTS(SELECT 1 FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id_hash=? AND s.user_id=? AND s.expires_at>? AND u.disabled_at IS NULL) RETURNING id,status`,
      ).bind(
        id,
        session.user.id,
        body.directoryId,
        String(body.reason ?? '').trim(),
        at,
        body.directoryId,
        session.user.id,
        bucket,
        await sha256(sessionToken(request)!),
        session.user.id,
        at,
      ),
      env.DB.prepare(
        'UPDATE rate_limits SET count=count+1 WHERE bucket=? AND EXISTS (SELECT 1 FROM instrument_requests WHERE id=?)',
      ).bind(bucket, id),
    ]);
    if (!results[1].results.length) {
      const duplicate = await env.DB.prepare(
        'SELECT id,status FROM instrument_requests WHERE user_id=? AND directory_id=?',
      )
        .bind(session.user.id, body.directoryId)
        .first();
      if (duplicate)
        return authJson(
          { ...duplicate, alreadyRequested: true },
          session.cookie,
        );
      const quota = await env.DB.prepare(
        'SELECT count FROM rate_limits WHERE bucket=?',
      )
        .bind(bucket)
        .first<{ count: number }>();
      if (quota && quota.count >= 5)
        throw new HttpError(429, '每天最多申请收录 5 只标的，请明天再试');
      throw new HttpError(409, '目录或收录状态已变化，请刷新后重试');
    }
    return authJson(
      { id, status: 'pending', alreadyRequested: false },
      session.cookie,
      201,
    );
  }
  const review = /^\/api\/admin\/requests\/([^/]+)\/(approve|reject)$/.exec(
    path,
  );
  const retry = /^\/api\/admin\/jobs\/([^/]+)\/retry$/.exec(path);
  const patch = /^\/api\/admin\/instruments\/([^/]+)$/.exec(path);
  if (
    ![
      '/api/admin/instruments',
      '/api/admin/jobs',
      '/api/admin/requests',
      '/api/admin/jobs/dispatch',
    ].includes(path) &&
    !review &&
    !retry &&
    !patch
  )
    return null;
  const actor = await adminActor(request, env, now);
  if (request.method === 'GET') {
    if (path === '/api/admin/jobs') {
      const items = await env.DB.prepare(
        `SELECT j.id,j.instrument_id AS instrumentId,i.name,i.symbol,j.status,j.mode,j.attempt,j.steps_json AS steps,j.error,j.created_at AS createdAt,j.finished_at AS finishedAt,j.lease_until AS leaseUntil
        FROM ingest_jobs j JOIN instruments i ON i.id=j.instrument_id ORDER BY j.created_at DESC,j.id LIMIT 100`,
      ).all();
      return authJson(
        {
          items: items.results.map((row) => ({
            ...row,
            steps: JSON.parse(String(row.steps ?? '[]')),
          })),
          dispatch: JSON.parse(
            String(
              (
                await env.DB.prepare(
                  "SELECT value FROM app_meta WHERE key='dispatch_status'",
                ).first()
              )?.value ??
                (!env.GITHUB_TOKEN || !env.GITHUB_REPOSITORY
                  ? JSON.stringify({ status: 'unconfigured', at })
                  : 'null'),
            ),
          ),
        },
        actor.cookie,
      );
    }
    if (path === '/api/admin/requests') {
      const items = await env.DB.prepare(
        `SELECT d.id,d.name,d.symbol,d.market,d.kind,COUNT(*) AS requesterCount,MIN(r.created_at) AS createdAt,MIN(r.reason) AS reason
        FROM instrument_requests r JOIN symbol_directory d ON d.id=r.directory_id WHERE r.status='pending'
        GROUP BY d.id ORDER BY requesterCount DESC,createdAt,d.id LIMIT 100`,
      ).all();
      return authJson({ items: items.results }, actor.cookie);
    }
    if (path === '/api/admin/instruments') {
      const q = `%${(url.searchParams.get('q') ?? '').slice(0, 100).replace(/[\\%_]/g, '\\$&')}%`;
      return authJson(
        {
          items: (
            await env.DB.prepare(
              "SELECT id,public_code AS code,name,symbol,market,kind,status,impact_tags_json AS impactTags,chain_node AS chainNode,ingest_revision AS revision FROM instruments WHERE name LIKE ? ESCAPE '\\' OR symbol LIKE ? ESCAPE '\\' ORDER BY created_at DESC LIMIT 200",
            )
              .bind(q, q)
              .all()
          ).results.map((row) => ({
            ...row,
            impactTags: JSON.parse(String(row.impactTags ?? '[]')),
          })),
        },
        actor.cookie,
      );
    }
    throw new HttpError(405, 'Method not allowed');
  }
  checkOrigin(request, env);
  const body = await readJson(request);
  await rateLimit(env, `ingest-admin:${actor.user.id}`, 100, 900, now);
  if (
    request.method === 'POST' &&
    (path === '/api/admin/instruments' || (review && review[2] === 'approve'))
  ) {
    const directoryId = review
      ? decodeURIComponent(review[1])
      : body.directoryId;
    if (typeof directoryId !== 'string')
      throw new HttpError(400, '请选择目录中的标的');
    const result = await queueInstrument(
      env,
      directoryId,
      actor,
      now,
      !!review,
    );
    return authJson(
      { ...result, dispatch: await dispatchPending(env, services.fetch, now) },
      actor.cookie,
      202,
    );
  }
  if (request.method === 'POST' && review && review[2] === 'reject') {
    const note = typeof body.note === 'string' ? body.note.trim() : '';
    if (!note || note.length > 300)
      throw new HttpError(400, '请填写 1～300 字的驳回理由');
    const directoryId = decodeURIComponent(review[1]),
      audit = crypto.randomUUID();
    const result = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO audit_log(id,actor_id,action,target,detail_json,created_at)
        SELECT ?,?,'request.reject',?,?,? WHERE ${LIVE_ACTOR} AND EXISTS(SELECT 1 FROM instrument_requests WHERE directory_id=? AND status='pending') RETURNING id`,
      ).bind(
        audit,
        actor.user.id,
        directoryId,
        JSON.stringify({ note }),
        at,
        ...actor.args,
        directoryId,
      ),
      env.DB.prepare(
        "UPDATE instrument_requests SET status='rejected',review_note=?,reviewed_by=?,reviewed_at=? WHERE directory_id=? AND status='pending' AND EXISTS(SELECT 1 FROM audit_log WHERE id=?)",
      ).bind(note, actor.user.id, at, directoryId, audit),
    ]);
    if (!result[0].results.length)
      throw new HttpError(409, '申请已被处理，请刷新后重试');
    return authJson({ reviewed: true }, actor.cookie);
  }
  if (request.method === 'POST' && path === '/api/admin/jobs/dispatch') {
    const audit = await env.DB.prepare(
      `INSERT INTO audit_log(id,actor_id,action,created_at) SELECT ?,?,'job.dispatch',? WHERE ${LIVE_ACTOR} RETURNING id`,
    )
      .bind(crypto.randomUUID(), actor.user.id, at, ...actor.args)
      .first();
    if (!audit) throw new HttpError(403, '管理员会话已失效');
    return authJson(
      { dispatch: await dispatchPending(env, services.fetch, now) },
      actor.cookie,
    );
  }
  if (request.method === 'POST' && retry) {
    const id = decodeURIComponent(retry[1]),
      audit = crypto.randomUUID();
    const results = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO audit_log(id,actor_id,action,target,created_at) SELECT ?,?,'job.retry',id,? FROM ingest_jobs
        WHERE id=? AND status='failed' AND ${LIVE_ACTOR} AND NOT EXISTS(SELECT 1 FROM ingest_jobs other WHERE other.instrument_id=ingest_jobs.instrument_id AND other.status IN ('queued','running')) RETURNING id`,
      ).bind(audit, actor.user.id, at, id, ...actor.args),
      env.DB.prepare(
        "UPDATE ingest_jobs SET status='queued',attempt=0,error=NULL,lease_hash=NULL,lease_until=NULL,finished_at=NULL,started_at=NULL,steps_json='[]' WHERE id=? AND EXISTS(SELECT 1 FROM audit_log WHERE id=?)",
      ).bind(id, audit),
      env.DB.prepare(
        "UPDATE instruments SET status='pending' WHERE id=(SELECT instrument_id FROM ingest_jobs WHERE id=?) AND status='failed' AND EXISTS(SELECT 1 FROM audit_log WHERE id=?)",
      ).bind(id, audit),
    ]);
    if (!results[0].results.length)
      throw new HttpError(409, '当前任务不可重试或已有采集任务');
    return authJson(
      {
        queued: true,
        dispatch: await dispatchPending(env, services.fetch, now),
      },
      actor.cookie,
    );
  }
  if (request.method === 'PATCH' && patch) {
    const tags = impactTagsSchema.safeParse(body.impactTags),
      node = body.chainNode,
      status = body.status;
    if (
      !tags.success ||
      !(node === null || (typeof node === 'string' && node.length <= 80)) ||
      !['ready', 'archived'].includes(String(status)) ||
      !Number.isSafeInteger(body.revision)
    )
      throw new HttpError(400, '标的编辑内容无效');
    const id = decodeURIComponent(patch[1]),
      audit = crypto.randomUUID();
    const result = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO audit_log(id,actor_id,action,target,detail_json,created_at) SELECT ?,?,'instrument.edit',id,?,? FROM instruments
        WHERE id=? AND ingest_revision=? AND ${LIVE_ACTOR} AND (status IN ('ready','archived')) RETURNING id`,
      ).bind(
        audit,
        actor.user.id,
        JSON.stringify({ impactTags: tags.data, chainNode: node, status }),
        at,
        id,
        body.revision,
        ...actor.args,
      ),
      env.DB.prepare(
        'UPDATE instruments SET impact_tags_json=?,chain_node=?,status=?,ingest_revision=ingest_revision+1,updated_at=? WHERE id=? AND EXISTS(SELECT 1 FROM audit_log WHERE id=?)',
      ).bind(JSON.stringify(tags.data), node, status, at, id, audit),
    ]);
    if (!result[0].results.length)
      throw new HttpError(409, '标的状态已变化，请刷新后重试');
    return authJson({ updated: true }, actor.cookie);
  }
  throw new HttpError(405, 'Method not allowed');
}
