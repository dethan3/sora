import { HttpError, json, readJson } from './http.ts';
import {
  checkOrigin,
  clearSessionCookie,
  DUMMY_HASH,
  hashPassword,
  normalizeEmail,
  randomToken,
  rateLimit,
  SESSION_SECONDS,
  sessionCookie,
  sessionToken,
  sha256,
  turnstileConfig,
  validPassword,
  verifyPassword,
  verifyTurnstile,
} from './security.ts';
import type { Env } from './types.ts';

export interface UserRow {
  id: string;
  email: string;
  display_name: string | null;
  role: 'user' | 'admin';
  base_currency: string;
  disabled_at: string | null;
  password_hash: string | null;
}
export const publicUser = (
  user: Pick<
    UserRow,
    'id' | 'email' | 'display_name' | 'role' | 'base_currency'
  >,
) => ({
  id: user.id,
  email: user.email,
  displayName: user.display_name,
  role: user.role,
  baseCurrency: user.base_currency,
});
export const authJson = (body: unknown, cookie?: string, status = 200) =>
  json(body, {
    status,
    headers: {
      'Cache-Control': 'no-store',
      ...(cookie ? { 'Set-Cookie': cookie } : {}),
    },
  });

export async function readSession(
  request: Request,
  env: Env,
  now = new Date(),
) {
  const token = sessionToken(request);
  if (!token) return null;
  const idHash = await sha256(token);
  const user = await env.DB.prepare(
    `SELECT u.id, u.email, u.display_name, u.role, u.base_currency,
      s.expires_at AS session_expires_at FROM sessions s JOIN users u ON u.id = s.user_id
      WHERE s.id_hash = ? AND s.expires_at > ? AND u.disabled_at IS NULL`,
  )
    .bind(idHash, now.toISOString())
    .first<
      Pick<
        UserRow,
        'id' | 'email' | 'display_name' | 'role' | 'base_currency'
      > & { session_expires_at: string }
    >();
  if (!user) return null;
  let cookie: string | undefined;
  const expiry = new Date(now.getTime() + SESSION_SECONDS * 1000).toISOString();
  // Renew at most once per day; the DB expiry and browser expiry move together.
  if (
    Date.parse(user.session_expires_at) <
    now.getTime() + (SESSION_SECONDS - 86400) * 1000
  ) {
    const renewed = await env.DB.prepare(
      `UPDATE sessions SET expires_at = ? WHERE id_hash = ? AND expires_at > ?
      AND EXISTS (SELECT 1 FROM users WHERE id = sessions.user_id AND disabled_at IS NULL) RETURNING id_hash`,
    )
      .bind(expiry, idHash, now.toISOString())
      .first();
    if (!renewed) return null;
    cookie = sessionCookie(token);
  }
  return { user: publicUser(user), cookie };
}

export async function requireUser(
  request: Request,
  env: Env,
  now = new Date(),
) {
  const session = await readSession(request, env, now);
  if (!session) throw new HttpError(401, '请先登录');
  return session;
}
export async function requireAdmin(
  request: Request,
  env: Env,
  now = new Date(),
) {
  const session = await requireUser(request, env, now);
  if (session.user.role !== 'admin') throw new HttpError(403, '需要管理员权限');
  return session;
}

export interface AuthServices {
  fetch?: typeof fetch;
  now?: () => Date;
}

export async function handleAuth(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const path = new URL(request.url).pathname;
  const read = ['/api/auth/me', '/api/auth/config'].includes(path);
  const write = [
    '/api/auth/register',
    '/api/auth/login',
    '/api/auth/logout',
  ].includes(path);
  if (!read && !write) return null;
  const method = read ? 'GET' : 'POST';
  if (request.method !== method)
    return json(
      { error: 'Method not allowed' },
      {
        status: 405,
        headers: { Allow: method, 'Cache-Control': 'no-store' },
      },
    );
  const now = services.now?.() ?? new Date();
  const at = now.toISOString();
  if (path === '/api/auth/config') {
    const { siteKey } = turnstileConfig(request, env);
    return authJson({ siteKey });
  }
  if (path === '/api/auth/me') {
    const session = await readSession(request, env, now);
    return authJson(
      { user: session?.user ?? null },
      session?.cookie ??
        (!session && sessionToken(request) ? clearSessionCookie() : undefined),
    );
  }
  checkOrigin(request, env);
  const body = await readJson(request);
  if (path === '/api/auth/logout') {
    const token = sessionToken(request);
    if (token)
      await env.DB.prepare('DELETE FROM sessions WHERE id_hash = ?')
        .bind(await sha256(token))
        .run();
    return authJson({ user: null }, clearSessionCookie());
  }
  const action = path.endsWith('/login') ? 'login' : 'register';
  const email = normalizeEmail(body.email);
  const password = typeof body.password === 'string' ? body.password : '';
  const invalid =
    action === 'login'
      ? '邮箱或密码不正确'
      : '无法注册，请检查邮箱、密码和邀请码';
  const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
  await rateLimit(
    env,
    `auth-ip:${action}:${ip}`,
    action === 'login' ? 60 : 10,
    action === 'login' ? 900 : 3600,
    now,
  );
  if (!email || !password || password.length > 128)
    throw new HttpError(400, invalid);
  await rateLimit(env, `auth-email:${action}:${email}`, 10, 900, now);
  await verifyTurnstile(
    request,
    env,
    body.turnstileToken,
    action,
    services.fetch,
  );
  // Bound expiry cleanup, retaining only the current/recent rate-limit buckets.
  await env.DB.batch([
    env.DB.prepare(
      'DELETE FROM sessions WHERE id_hash IN (SELECT id_hash FROM sessions WHERE expires_at <= ? LIMIT 100)',
    ).bind(at),
    env.DB.prepare(
      'DELETE FROM rate_limits WHERE bucket IN (SELECT bucket FROM rate_limits WHERE window_start < ? LIMIT 100)',
    ).bind(new Date(now.getTime() - 2 * 86400_000).toISOString()),
  ]);
  const token = randomToken();
  const tokenHash = await sha256(token);
  const expiresAt = new Date(
    now.getTime() + SESSION_SECONDS * 1000,
  ).toISOString();
  const ua = request.headers.get('User-Agent')?.slice(0, 256) ?? null;

  if (action === 'login') {
    const user = await env.DB.prepare('SELECT * FROM users WHERE email = ?')
      .bind(email)
      .first<UserRow>();
    const valid = await verifyPassword(
      password,
      user?.password_hash ?? DUMMY_HASH,
    );
    if (!valid || !user || user.disabled_at) throw new HttpError(401, invalid);
    // A disable/reset racing with password verification must not issue a session.
    const issued = await env.DB.prepare(
      `INSERT INTO sessions (id_hash, user_id, expires_at, created_at, user_agent, ip)
      SELECT ?, id, ?, ?, ?, ? FROM users WHERE id = ? AND password_hash = ? AND disabled_at IS NULL RETURNING id_hash`,
    )
      .bind(
        tokenHash,
        expiresAt,
        at,
        ua,
        ip.slice(0, 64),
        user.id,
        user.password_hash,
      )
      .first();
    if (!issued) throw new HttpError(401, invalid);
    return authJson({ user: publicUser(user) }, sessionCookie(token));
  }

  const invite = typeof body.invite === 'string' ? body.invite.trim() : '';
  const name =
    typeof body.displayName === 'string' ? body.displayName.trim() : '';
  if (
    !validPassword(password) ||
    !/^[a-f0-9]{64}$/.test(invite) ||
    name.length > 60
  )
    throw new HttpError(400, invalid);
  const userId = crypto.randomUUID();
  const passwordHash = await hashPassword(password);
  const inviteHash = await sha256(invite);
  // D1 batch is one transaction: each following statement is conditional on
  // THIS new user, so a spent invite or duplicate email leaves no partial state.
  const result = await env.DB.batch([
    env.DB.prepare(
      `INSERT INTO users (id, email, password_hash, display_name, role, created_at, updated_at)
      SELECT ?, ?, ?, ?, 'user', ?, ? WHERE EXISTS
      (SELECT 1 FROM invites WHERE code_hash = ? AND used_by IS NULL AND revoked_at IS NULL AND expires_at > ?)
      AND NOT EXISTS (SELECT 1 FROM users WHERE email = ?)`,
    ).bind(
      userId,
      email,
      passwordHash,
      name || email.split('@')[0],
      at,
      at,
      inviteHash,
      at,
      email,
    ),
    env.DB.prepare(
      `UPDATE invites SET used_by = ?, used_at = ? WHERE code_hash = ? AND used_by IS NULL
      AND EXISTS (SELECT 1 FROM users WHERE id = ?)`,
    ).bind(userId, at, inviteHash, userId),
    env.DB.prepare(
      `INSERT INTO sessions (id_hash, user_id, expires_at, created_at, user_agent, ip)
      SELECT ?, id, ?, ?, ?, ? FROM users WHERE id = ? RETURNING id_hash`,
    ).bind(tokenHash, expiresAt, at, ua, ip.slice(0, 64), userId),
  ]);
  if (!result[2].results.length) throw new HttpError(400, invalid);
  return authJson(
    {
      user: publicUser({
        id: userId,
        email,
        display_name: name || email.split('@')[0],
        role: 'user',
        base_currency: 'CNY',
      }),
    },
    sessionCookie(token),
    201,
  );
}
