import {
  authJson,
  publicUser,
  requireAdmin,
  type AuthServices,
  type UserRow,
} from './auth.ts';
import { HttpError, json, readJson } from './http.ts';
import {
  checkOrigin,
  clearSessionCookie,
  hashPassword,
  randomToken,
  rateLimit,
  sessionToken,
  sha256,
} from './security.ts';
import type { Env } from './types.ts';

interface AdminUserRow extends Omit<UserRow, 'password_hash'> {
  created_at: string;
  updated_at: string;
  admin_revision: number;
}
const USER_COLUMNS =
  'id, email, display_name, role, base_currency, disabled_at, created_at, updated_at, admin_revision';
const adminUser = (row: AdminUserRow) => ({
  ...publicUser(row),
  disabledAt: row.disabled_at,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  revision: row.admin_revision,
});
const PAGE_SIZE = 20;
export const LIVE_ACTOR = `EXISTS (SELECT 1 FROM users actor JOIN sessions actor_session ON actor_session.user_id = actor.id
  WHERE actor.id = ? AND actor.role = 'admin' AND actor.disabled_at IS NULL AND actor_session.id_hash = ? AND actor_session.expires_at > ?)`;
function pagination(url: URL) {
  const raw = url.searchParams.get('page') ?? '1';
  if (!/^\d{1,5}$/.test(raw) || Number(raw) < 1)
    throw new HttpError(400, '页码无效');
  return { page: Number(raw), offset: (Number(raw) - 1) * PAGE_SIZE };
}
function lastAdminError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return (
    error.message.includes('LAST_ACTIVE_ADMIN') || lastAdminError(error.cause)
  );
}

export async function handleAdmin(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/api/admin/')) return null;
  const now = services.now?.() ?? new Date();
  const at = now.toISOString();
  const session = await requireAdmin(request, env, now);
  const cookie = session.cookie;
  const actorHash = await sha256(sessionToken(request)!);
  const actorArgs = [session.user.id, actorHash, at];
  const path = url.pathname;
  const userMatch = /^\/api\/admin\/users\/([a-zA-Z0-9-]{1,100})$/.exec(path);
  const revokeMatch = /^\/api\/admin\/invites\/([a-f0-9]{64})\/revoke$/.exec(
    path,
  );
  const allowed =
    path === '/api/admin/invites'
      ? ['GET', 'POST']
      : ['/api/admin/users', '/api/admin/audit'].includes(path)
        ? ['GET']
        : userMatch
          ? ['PATCH']
          : revokeMatch
            ? ['POST']
            : [];
  if (!allowed.length) return authJson({ error: 'Not found' }, cookie, 404);
  if (!allowed.includes(request.method))
    return json(
      { error: 'Method not allowed' },
      {
        status: 405,
        headers: { Allow: allowed.join(', '), 'Cache-Control': 'no-store' },
      },
    );

  if (request.method === 'GET') {
    const { page, offset } = pagination(url);
    if (path === '/api/admin/users') {
      const q = (url.searchParams.get('q') ?? '').trim();
      if (q.length > 100) throw new HttpError(400, '搜索内容过长');
      const query = `%${q.replace(/[\\%_]/g, '\\$&')}%`;
      const where = `(email LIKE ? ESCAPE '\\' OR COALESCE(display_name, '') LIKE ? ESCAPE '\\')`;
      const [users, count, admins] = await env.DB.batch([
        env.DB.prepare(
          `SELECT ${USER_COLUMNS} FROM users WHERE ${where} ORDER BY created_at DESC, id LIMIT ? OFFSET ?`,
        ).bind(query, query, PAGE_SIZE, offset),
        env.DB.prepare(`SELECT COUNT(*) AS n FROM users WHERE ${where}`).bind(
          query,
          query,
        ),
        env.DB.prepare(
          "SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND disabled_at IS NULL",
        ),
      ]);
      return authJson(
        {
          items: (users.results as unknown as AdminUserRow[]).map(adminUser),
          total: count.results[0].n,
          activeAdminCount: admins.results[0].n,
          page,
          pageSize: PAGE_SIZE,
        },
        cookie,
      );
    }
    if (path === '/api/admin/invites') {
      const [items, count] = await env.DB.batch([
        env.DB.prepare(
          `SELECT i.code_hash AS id, i.created_at AS createdAt, i.expires_at AS expiresAt,
          creator.email AS createdBy, used.email AS usedBy, i.used_at AS usedAt, i.revoked_at AS revokedAt,
          CASE WHEN i.revoked_at IS NOT NULL THEN 'revoked' WHEN i.used_by IS NOT NULL THEN 'used'
            WHEN i.expires_at <= ? THEN 'expired' ELSE 'available' END AS status
          FROM invites i JOIN users creator ON creator.id = i.created_by LEFT JOIN users used ON used.id = i.used_by
          ORDER BY i.created_at DESC, i.code_hash LIMIT ? OFFSET ?`,
        ).bind(at, PAGE_SIZE, offset),
        env.DB.prepare('SELECT COUNT(*) AS n FROM invites'),
      ]);
      return authJson(
        {
          items: items.results,
          total: count.results[0].n,
          page,
          pageSize: PAGE_SIZE,
        },
        cookie,
      );
    }
    const action = url.searchParams.get('action') ?? '';
    if (action && !/^[a-z][a-z0-9_.-]{0,59}$/.test(action))
      throw new HttpError(400, '操作类型无效');
    const [items, count] = await env.DB.batch([
      env.DB.prepare(
        `SELECT log.id, log.action, log.target, log.detail_json AS detail, log.created_at AS createdAt,
        actor.email AS actorEmail, target_user.email AS targetEmail FROM audit_log log
        LEFT JOIN users actor ON actor.id = log.actor_id LEFT JOIN users target_user ON target_user.id = log.target
        WHERE (? = '' OR log.action = ?) ORDER BY log.created_at DESC, log.id LIMIT ? OFFSET ?`,
      ).bind(action, action, PAGE_SIZE, offset),
      env.DB.prepare(
        "SELECT COUNT(*) AS n FROM audit_log WHERE (? = '' OR action = ?)",
      ).bind(action, action),
    ]);
    return authJson(
      {
        items: items.results.map((row) => ({
          ...row,
          detail: row.detail ? JSON.parse(String(row.detail)) : null,
        })),
        total: count.results[0].n,
        page,
        pageSize: PAGE_SIZE,
      },
      cookie,
    );
  }

  checkOrigin(request, env);
  const body = await readJson(request);
  if (path === '/api/admin/invites') {
    await rateLimit(env, `invites:${session.user.id}`, 30, 86400, now);
    const invite = randomToken(),
      hash = await sha256(invite);
    const expiresAt = new Date(now.getTime() + 7 * 86400_000).toISOString();
    const result = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO invites (code_hash, created_by, expires_at, created_at)
        SELECT ?, ?, ?, ? WHERE ${LIVE_ACTOR} RETURNING code_hash`,
      ).bind(hash, session.user.id, expiresAt, at, ...actorArgs),
      env.DB.prepare(
        `INSERT INTO audit_log (id, actor_id, action, target, detail_json, created_at)
        SELECT ?, ?, 'invite.create', ?, ?, ? WHERE EXISTS (SELECT 1 FROM invites WHERE code_hash = ?)`,
      ).bind(
        crypto.randomUUID(),
        session.user.id,
        hash,
        JSON.stringify({ expiresAt }),
        at,
        hash,
      ),
    ]);
    if (!result[0].results.length)
      throw new HttpError(403, '管理员权限已失效，请重新登录');
    return authJson(
      { invite, expiresAt, registerPath: `/register?invite=${invite}` },
      cookie,
      201,
    );
  }
  await rateLimit(env, `admin-write:${session.user.id}`, 100, 900, now);
  const auditId = crypto.randomUUID();
  if (revokeMatch) {
    const id = revokeMatch[1];
    const result = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO audit_log (id, actor_id, action, target, created_at)
        SELECT ?, ?, 'invite.revoke', code_hash, ? FROM invites
        WHERE code_hash = ? AND used_by IS NULL AND revoked_at IS NULL AND expires_at > ? AND ${LIVE_ACTOR} RETURNING id`,
      ).bind(auditId, session.user.id, at, id, at, ...actorArgs),
      env.DB.prepare(
        `UPDATE invites SET revoked_at = ?, revoked_by = ? WHERE code_hash = ?
        AND EXISTS (SELECT 1 FROM audit_log WHERE id = ?)`,
      ).bind(at, session.user.id, id, auditId),
    ]);
    if (!result[0].results.length) {
      await requireAdmin(request, env, now);
      throw new HttpError(409, '邀请码已使用、过期或撤销，请刷新后重试');
    }
    return authJson({ revoked: true }, cookie);
  }

  const id = userMatch![1];
  if (!Number.isSafeInteger(body.revision) || Number(body.revision) < 0)
    throw new HttpError(400, '请提供有效的用户版本');
  const target = await env.DB.prepare(
    `SELECT ${USER_COLUMNS} FROM users WHERE id = ?`,
  )
    .bind(id)
    .first<AdminUserRow>();
  if (!target) throw new HttpError(404, '用户不存在');
  if (target.admin_revision !== body.revision)
    throw new HttpError(409, '账号状态已变化，请刷新后重试');
  const action = body.action;
  let assignment: string, value: string | null, auditAction: string;
  let newPassword: string | undefined;
  let detail: Record<string, unknown>;
  if (action === 'setDisabled' && typeof body.disabled === 'boolean') {
    if (!!target.disabled_at === body.disabled)
      throw new HttpError(409, '账号状态已变化，请刷新后重试');
    assignment = 'disabled_at';
    value = body.disabled ? at : null;
    auditAction = body.disabled ? 'user.disable' : 'user.enable';
    detail = {
      before: { disabled: !!target.disabled_at },
      after: { disabled: body.disabled },
    };
  } else if (
    action === 'setRole' &&
    ['user', 'admin'].includes(String(body.role))
  ) {
    if (target.role === body.role)
      throw new HttpError(409, '账号角色已变化，请刷新后重试');
    assignment = 'role';
    value = String(body.role);
    auditAction = 'user.role';
    detail = { before: { role: target.role }, after: { role: value } };
  } else if (action === 'resetPassword') {
    assignment = 'password_hash';
    newPassword = randomToken().slice(0, 32);
    value = await hashPassword(newPassword);
    auditAction = 'user.password_reset';
    detail = { sessionsRevoked: true };
  } else throw new HttpError(400, '用户管理操作无效');
  const detailJson = JSON.stringify({
    ...detail,
    revision: target.admin_revision + 1,
  });
  let result;
  try {
    result = await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO audit_log (id, actor_id, action, target, detail_json, created_at)
        SELECT ?, ?, ?, id, ?, ? FROM users WHERE id = ? AND admin_revision = ? AND ${LIVE_ACTOR} RETURNING id`,
      ).bind(
        auditId,
        session.user.id,
        auditAction,
        detailJson,
        at,
        id,
        target.admin_revision,
        ...actorArgs,
      ),
      env.DB.prepare(
        `UPDATE users SET ${assignment} = ?, updated_at = ?, admin_revision = admin_revision + 1
        WHERE id = ? AND EXISTS (SELECT 1 FROM audit_log WHERE id = ?) RETURNING ${USER_COLUMNS}`,
      ).bind(value, at, id, auditId),
      env.DB.prepare(
        'DELETE FROM sessions WHERE user_id = ? AND EXISTS (SELECT 1 FROM audit_log WHERE id = ?)',
      ).bind(id, auditId),
    ]);
  } catch (error) {
    if (lastAdminError(error))
      throw new HttpError(409, '至少保留一个可用管理员，请先设置其他管理员');
    throw error;
  }
  if (!result[1].results.length) {
    await requireAdmin(request, env, now);
    throw new HttpError(409, '账号状态已变化，请刷新后重试');
  }
  const signedOut = id === session.user.id;
  return authJson(
    {
      user: adminUser(result[1].results[0] as unknown as AdminUserRow),
      signedOut,
      ...(newPassword ? { newPassword } : {}),
    },
    signedOut ? clearSessionCookie() : cookie,
  );
}
