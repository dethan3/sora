import {
  z,
  personalSchema,
  personalEnvelopeSchema,
  replacePersonalSchema,
  importPersonalSchema,
  importPreviewSchema,
  mutationBase,
  positionSchema,
  mergePersonal,
  type PersonalState,
  type PersonalEnvelope,
} from '@sora/market-data/personal';
import { authJson, requireUser, type AuthServices } from './auth.ts';
import { HttpError, readJson } from './http.ts';
import { checkOrigin, sessionToken, sha256, rateLimit } from './security.ts';
import type { Env } from './types.ts';

const live = `EXISTS(SELECT 1 FROM users u JOIN sessions s ON s.user_id=u.id WHERE u.id=? AND u.disabled_at IS NULL AND s.id_hash=? AND s.expires_at>?)`;
type Scope = { userId: string; hash: string; at: string };
const args = (s: Scope) => [s.userId, s.hash, s.at];
async function snapshot(env: Env, scope: Scope): Promise<PersonalEnvelope> {
  const row = await env.DB.prepare(
    `SELECT u.id,COALESCE(a.revision,0) AS revision,
  (SELECT json_group_array(code) FROM (SELECT i.public_code AS code FROM watchlist w JOIN instruments i ON i.id=w.instrument_id WHERE w.user_id=u.id ORDER BY w.created_at,i.public_code)) AS watchlist,
  (SELECT json_group_array(json_object('code',code,'units',units,'costMinor',cost_minor,'currency',currency)) FROM (SELECT i.public_code AS code,p.units,p.cost_minor,p.currency FROM positions p JOIN instruments i ON i.id=p.instrument_id WHERE p.user_id=u.id ORDER BY p.created_at,i.public_code)) AS positions
  FROM users u LEFT JOIN personal_accounts a ON a.user_id=u.id WHERE u.id=? AND ${live}`,
  )
    .bind(scope.userId, ...args(scope))
    .first();
  if (!row) throw new HttpError(401, '请重新登录');
  return personalEnvelopeSchema.parse({
    ownerId: scope.userId,
    revision: row.revision,
    state: {
      schemaVersion: 1,
      watchlist: JSON.parse(String(row.watchlist ?? '[]')),
      positions: JSON.parse(String(row.positions ?? '[]')),
    },
  });
}
// Retaining an unchanged archived record is allowed; adding/editing requires a
// ready fund with the matching currency. Recheck this inside the write transaction.
const invalidSql = `SELECT value AS code,'watchlist' AS part FROM json_each(?) w WHERE
 NOT EXISTS(SELECT 1 FROM instruments i WHERE i.public_code=w.value AND i.kind IN ('fund','etf') AND i.status='ready')
 AND NOT EXISTS(SELECT 1 FROM watchlist old JOIN instruments i ON i.id=old.instrument_id WHERE old.user_id=? AND i.public_code=w.value)
 UNION ALL
 SELECT json_extract(p.value,'$.code') AS code,'position' AS part FROM json_each(?) p WHERE
 NOT EXISTS(SELECT 1 FROM instruments i WHERE i.public_code=json_extract(p.value,'$.code') AND i.kind IN ('fund','etf') AND i.status='ready' AND i.currency=json_extract(p.value,'$.currency'))
 AND NOT EXISTS(SELECT 1 FROM positions old JOIN instruments i ON i.id=old.instrument_id WHERE old.user_id=? AND i.public_code=json_extract(p.value,'$.code') AND old.units=json_extract(p.value,'$.units') AND old.cost_minor=json_extract(p.value,'$.costMinor') AND old.currency=json_extract(p.value,'$.currency'))`;
const invalidArgs = (state: PersonalState, s: Scope) => [
  JSON.stringify(state.watchlist),
  s.userId,
  JSON.stringify(state.positions),
  s.userId,
];
function parse<T>(schema: z.ZodType<T>, body: unknown): T {
  const result = schema.safeParse(body);
  if (!result.success)
    throw new HttpError(
      400,
      '关注或持仓数据无效，请检查份额、成本、币种与条数',
    );
  return result.data;
}
async function replay(env: Env, s: Scope, id: string, hash: string) {
  const old = await env.DB.prepare(
    'SELECT payload_hash FROM personal_operations WHERE user_id=? AND operation_id=?',
  )
    .bind(s.userId, id)
    .first();
  if (!old) return null;
  if (old.payload_hash !== hash)
    throw new HttpError(409, '同一操作编号不能提交不同内容');
  return { ...(await snapshot(env, s)), replayed: true };
}
async function replace(
  env: Env,
  s: Scope,
  state: PersonalState,
  revision: number,
  id: string,
  payloadHash: string,
) {
  const existing = await replay(env, s, id, payloadHash);
  if (existing) return existing;
  const invalid = await env.DB.prepare(invalidSql)
    .bind(...invalidArgs(state, s))
    .all();
  if (invalid.results.length)
    throw new HttpError(
      400,
      '包含尚未收录、已下架或币种不匹配的基金；请保留原记录或移除后重试',
    );
  const nonce = crypto.randomUUID(),
    gate =
      'EXISTS(SELECT 1 FROM personal_accounts WHERE user_id=? AND last_mutation=?)',
    gateArgs = [s.userId, nonce];
  const result = await env.DB.batch([
    env.DB.prepare(
      `INSERT OR IGNORE INTO personal_accounts(user_id,updated_at) SELECT ?,? WHERE ${live}`,
    ).bind(s.userId, s.at, ...args(s)),
    env.DB.prepare(
      `UPDATE personal_accounts SET revision=revision+1,last_mutation=?,updated_at=? WHERE user_id=? AND revision=? AND ${live}
    AND NOT EXISTS(SELECT 1 FROM personal_operations WHERE user_id=? AND operation_id=?)
    AND NOT EXISTS(${invalidSql}) RETURNING revision`,
    ).bind(
      nonce,
      s.at,
      s.userId,
      revision,
      ...args(s),
      s.userId,
      id,
      ...invalidArgs(state, s),
    ),
    env.DB.prepare(
      `DELETE FROM watchlist WHERE user_id=? AND instrument_id NOT IN (SELECT i.id FROM instruments i JOIN json_each(?) e ON i.public_code=e.value) AND ${gate}`,
    ).bind(s.userId, JSON.stringify(state.watchlist), ...gateArgs),
    env.DB.prepare(
      `INSERT INTO watchlist(user_id,instrument_id,created_at) SELECT ?,i.id,? FROM json_each(?) e JOIN instruments i ON i.public_code=e.value WHERE ${gate} ON CONFLICT(user_id,instrument_id) DO NOTHING`,
    ).bind(s.userId, s.at, JSON.stringify(state.watchlist), ...gateArgs),
    env.DB.prepare(
      `DELETE FROM positions WHERE user_id=? AND instrument_id NOT IN (SELECT i.id FROM instruments i JOIN json_each(?) e ON i.public_code=json_extract(e.value,'$.code')) AND ${gate}`,
    ).bind(s.userId, JSON.stringify(state.positions), ...gateArgs),
    env.DB.prepare(
      `INSERT INTO positions(user_id,instrument_id,units,cost_minor,currency,created_at,updated_at)
   SELECT ?,i.id,json_extract(e.value,'$.units'),json_extract(e.value,'$.costMinor'),json_extract(e.value,'$.currency'),?,? FROM json_each(?) e JOIN instruments i ON i.public_code=json_extract(e.value,'$.code') WHERE ${gate}
   ON CONFLICT(user_id,instrument_id) DO UPDATE SET units=excluded.units,cost_minor=excluded.cost_minor,currency=excluded.currency,updated_at=excluded.updated_at WHERE positions.units<>excluded.units OR positions.cost_minor<>excluded.cost_minor OR positions.currency<>excluded.currency`,
    ).bind(s.userId, s.at, s.at, JSON.stringify(state.positions), ...gateArgs),
    env.DB.prepare(
      `INSERT INTO personal_operations(user_id,operation_id,payload_hash,revision,created_at) SELECT ?,?,?,revision,? FROM personal_accounts WHERE user_id=? AND last_mutation=?`,
    ).bind(s.userId, id, payloadHash, s.at, ...gateArgs),
  ]);
  if (!result[1].results.length) {
    const old = await replay(env, s, id, payloadHash);
    if (old) return old;
    await snapshot(env, s);
    throw new HttpError(409, '账号数据或基金状态已变化，请重新读取后再修改');
  }
  return snapshot(env, s);
}
async function preview(env: Env, s: Scope, localState: PersonalState) {
  const current = await snapshot(env, s);
  const codes = [
    ...new Set([
      ...localState.watchlist,
      ...localState.positions.map((p) => p.code),
    ]),
  ];
  const instruments = (
    await env.DB.prepare(
      "SELECT public_code,currency FROM instruments WHERE status='ready' AND kind IN ('fund','etf') AND public_code IN (SELECT value FROM json_each(?))",
    )
      .bind(JSON.stringify(codes))
      .all()
  ).results;
  const allowed = new Map(
    instruments.map((row) => [String(row.public_code), row.currency]),
  );
  const unavailable: {
    code: string;
    part: 'watchlist' | 'position';
    reason: string;
  }[] = [];
  const eligible: PersonalState = {
    schemaVersion: 1,
    watchlist: localState.watchlist.filter((code) => {
      if (allowed.has(code)) return true;
      unavailable.push({ code, part: 'watchlist', reason: '尚未收录或已下架' });
      return false;
    }),
    positions: localState.positions.filter((row) => {
      if (allowed.get(row.code) === row.currency) return true;
      unavailable.push({
        code: row.code,
        part: 'position',
        reason: allowed.has(row.code)
          ? '币种与公共目录不一致'
          : '尚未收录或已下架',
      });
      return false;
    }),
  };
  const conflicts = eligible.positions.flatMap((local) => {
    const cloud = current.state.positions.find(
      (row) => row.code === local.code,
    );
    return cloud && JSON.stringify(cloud) !== JSON.stringify(local)
      ? [{ code: local.code, local, cloud }]
      : [];
  });
  return importPreviewSchema.parse({
    ownerId: s.userId,
    revision: current.revision,
    current: current.state,
    eligible,
    unavailable,
    conflicts,
  });
}
export async function handlePersonal(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const path = new URL(request.url).pathname;
  if (!/^\/api\/me\/(personal|watchlist|positions|import)(\/|$)/.test(path))
    return null;
  const now = services.now?.() ?? new Date(),
    session = await requireUser(request, env, now);
  const s: Scope = {
    userId: session.user.id,
    hash: await sha256(sessionToken(request)!),
    at: now.toISOString(),
  };
  if (request.method === 'GET') {
    const result = await snapshot(env, s);
    if (path === '/api/me/personal') return authJson(result, session.cookie);
    if (path === '/api/me/watchlist')
      return authJson(
        {
          ownerId: result.ownerId,
          revision: result.revision,
          watchlist: result.state.watchlist,
        },
        session.cookie,
      );
    if (path === '/api/me/positions')
      return authJson(
        {
          ownerId: result.ownerId,
          revision: result.revision,
          positions: result.state.positions,
        },
        session.cookie,
      );
    throw new HttpError(404, 'Not found');
  }
  if (!['POST', 'DELETE'].includes(request.method))
    throw new HttpError(405, 'Method not allowed');
  checkOrigin(request, env);
  const body = await readJson(request, 96 * 1024);
  if (body.ownerId !== s.userId)
    throw new HttpError(409, '当前登录账号已变化，请重新读取个人数据');
  await rateLimit(env, `personal:${s.userId}`, 180, 900, now);
  if (path === '/api/me/import/preview' && request.method === 'POST') {
    const input = parse(
      z.object({ ownerId: z.string(), localState: personalSchema }).strict(),
      body,
    );
    return authJson(await preview(env, s, input.localState), session.cookie);
  }
  let state: PersonalState,
    base:
      | z.infer<typeof replacePersonalSchema>
      | z.infer<typeof importPersonalSchema>
      | z.infer<z.ZodObject<typeof mutationBase>>;
  let operation: unknown;
  if (path === '/api/me/personal' && request.method === 'POST') {
    const input = parse(replacePersonalSchema, body);
    state = input.state;
    base = input;
    operation = input;
  } else if (path === '/api/me/import' && request.method === 'POST') {
    const input = parse(importPersonalSchema, body);
    base = input;
    operation = input;
    const hash = await sha256(JSON.stringify({ path, input })),
      old = await replay(env, s, input.operationId, hash);
    if (old) return authJson(old, session.cookie);
    const current = await snapshot(env, s);
    try {
      state = mergePersonal(current.state, input.localState, input.choices);
    } catch {
      throw new HttpError(
        400,
        '请处理同一基金的持仓冲突，并检查合并后是否超过 200 条',
      );
    }
  } else if (path === '/api/me/watchlist' && request.method === 'POST') {
    const input = parse(
      z.object({ ...mutationBase, code: z.string().min(1).max(50) }).strict(),
      body,
    );
    base = input;
    operation = input;
    const current = await snapshot(env, s);
    state = {
      ...current.state,
      watchlist: [...new Set([...current.state.watchlist, input.code])],
    };
  } else if (path === '/api/me/positions' && request.method === 'POST') {
    const input = parse(
      z.object({ ...mutationBase, position: positionSchema }).strict(),
      body,
    );
    base = input;
    operation = input;
    const current = await snapshot(env, s);
    state = {
      ...current.state,
      positions: [
        ...current.state.positions.filter(
          (row) => row.code !== input.position.code,
        ),
        input.position,
      ],
    };
  } else if (
    request.method === 'DELETE' &&
    /^\/api\/me\/(watchlist|positions)\/[^/]+$/.test(path)
  ) {
    const input = parse(z.object(mutationBase).strict(), body);
    base = input;
    operation = input;
    let code: string;
    try {
      code = decodeURIComponent(path.split('/').at(-1)!);
    } catch {
      throw new HttpError(400, '基金代码无效');
    }
    const current = await snapshot(env, s);
    state = path.startsWith('/api/me/watchlist/')
      ? {
          ...current.state,
          watchlist: current.state.watchlist.filter((row) => row !== code),
        }
      : {
          ...current.state,
          positions: current.state.positions.filter((row) => row.code !== code),
        };
  } else throw new HttpError(404, 'Not found');
  const hash = await sha256(JSON.stringify({ path, input: operation }));
  const old = await replay(env, s, base.operationId, hash);
  if (old) return authJson(old, session.cookie);
  state = parse(personalSchema, state);
  return authJson(
    await replace(env, s, state, base.revision, base.operationId, hash),
    session.cookie,
  );
}
