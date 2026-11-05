import { HttpError } from './http.ts';
import type { Env } from './types.ts';

const encoder = new TextEncoder();
const ITERATIONS = 100_000;
export const SESSION_SECONDS = 30 * 24 * 60 * 60;
export const SESSION_COOKIE = '__Host-sora_session';
const hex = (bytes: Uint8Array) =>
  [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
const unhex = (text: string) =>
  new Uint8Array(text.match(/../g)!.map((byte) => parseInt(byte, 16)));
export const randomToken = () =>
  hex(crypto.getRandomValues(new Uint8Array(32)));
export async function sha256(text: string) {
  return hex(
    new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(text))),
  );
}

async function derive(password: string, salt: Uint8Array) {
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(password),
    'PBKDF2',
    false,
    ['deriveBits'],
  );
  return hex(
    new Uint8Array(
      await crypto.subtle.deriveBits(
        {
          name: 'PBKDF2',
          hash: 'SHA-256',
          salt: new Uint8Array(salt),
          iterations: ITERATIONS,
        },
        key,
        256,
      ),
    ),
  );
}
export function validPassword(value: unknown): value is string {
  return typeof value === 'string' && value.length >= 12 && value.length <= 128;
}
export function normalizeEmail(value: unknown) {
  if (typeof value !== 'string') return null;
  const email = value.trim().toLowerCase();
  return email.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
    ? email
    : null;
}
export async function hashPassword(password: string) {
  if (!validPassword(password)) throw new Error('密码须为 12～128 个字符');
  const salt = crypto.getRandomValues(new Uint8Array(16));
  return `pbkdf2-sha256$${ITERATIONS}$${hex(salt)}$${await derive(password, salt)}`;
}
// The dummy hash keeps unknown/disabled accounts on the same derivation path.
export const DUMMY_HASH = `pbkdf2-sha256$${ITERATIONS}$${'00'.repeat(16)}$${'00'.repeat(32)}`;
export async function verifyPassword(password: string, encoded: string) {
  const parts = /^pbkdf2-sha256\$100000\$([a-f0-9]{32})\$([a-f0-9]{64})$/.exec(
    encoded,
  );
  const digest = await derive(password, unhex(parts?.[1] ?? '00'.repeat(16)));
  const expected = parts?.[2] ?? '00'.repeat(32);
  let different = 0;
  for (let i = 0; i < digest.length; i++)
    different |= digest.charCodeAt(i) ^ expected.charCodeAt(i);
  return !!parts && different === 0;
}

export function sessionToken(request: Request) {
  const value = request.headers
    .get('Cookie')
    ?.split(';')
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${SESSION_COOKIE}=`))
    ?.slice(SESSION_COOKIE.length + 1);
  return value && /^[a-f0-9]{64}$/.test(value) ? value : null;
}
export const sessionCookie = (token: string, maxAge = SESSION_SECONDS) =>
  `${SESSION_COOKIE}=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`;
export const clearSessionCookie = () => sessionCookie('', 0);

export function checkOrigin(request: Request, env: Env) {
  const origin = request.headers.get('Origin');
  const current = new URL(request.url).origin;
  if (!origin || (origin !== current && origin !== env.AUTH_TRUSTED_ORIGIN))
    throw new HttpError(403, '请求来源无效');
}

export async function rateLimit(
  env: Env,
  identity: string,
  limit: number,
  seconds: number,
  now: Date,
) {
  const start = new Date(
    Math.floor(now.getTime() / (seconds * 1000)) * seconds * 1000,
  ).toISOString();
  const bucket = await sha256(identity);
  const row = await env.DB.prepare(
    `INSERT INTO rate_limits (bucket, count, window_start) VALUES (?, 1, ?)
    ON CONFLICT (bucket) DO UPDATE SET
      count = CASE WHEN rate_limits.window_start = excluded.window_start THEN MIN(rate_limits.count + 1, 1000000) ELSE 1 END,
      window_start = excluded.window_start RETURNING count`,
  )
    .bind(bucket, start)
    .first<{ count: number }>();
  if (!row || row.count > limit)
    throw new HttpError(
      429,
      '尝试过于频繁，请稍后再试',
      Math.ceil((Date.parse(start) + seconds * 1000 - now.getTime()) / 1000),
    );
}

export function turnstileConfig(request: Request, env: Env) {
  if (!env.TURNSTILE_SITE_KEY || !env.TURNSTILE_SECRET_KEY)
    throw new HttpError(503, '登录验证尚未配置，请联系管理员');
  const testKey =
    /^[123]x0+/.test(env.TURNSTILE_SITE_KEY) ||
    /^[123]x0+/.test(env.TURNSTILE_SECRET_KEY);
  const local = ['localhost', '127.0.0.1', '[::1]'].includes(
    new URL(request.url).hostname,
  );
  if (testKey && !local) throw new HttpError(503, '登录验证配置无效');
  return { siteKey: env.TURNSTILE_SITE_KEY, testKey };
}

export async function verifyTurnstile(
  request: Request,
  env: Env,
  token: unknown,
  action: string,
  fetcher: typeof fetch = fetch,
) {
  const config = turnstileConfig(request, env);
  if (typeof token !== 'string' || !token || token.length > 2048)
    throw new HttpError(400, '请完成人机验证后重试');
  const body = new URLSearchParams({
    secret: env.TURNSTILE_SECRET_KEY!,
    response: token,
  });
  const ip = request.headers.get('CF-Connecting-IP');
  if (ip) body.set('remoteip', ip);
  let result: { success?: boolean; hostname?: string; action?: string };
  try {
    const response = await fetcher(
      'https://challenges.cloudflare.com/turnstile/v0/siteverify',
      {
        method: 'POST',
        body,
        signal: AbortSignal.timeout(8000),
      },
    );
    if (!response.ok) throw new Error();
    result = (await response.json()) as typeof result;
  } catch {
    throw new HttpError(503, '人机验证暂不可用，请稍后重试');
  }
  const expectedHost = new URL(request.headers.get('Origin') ?? request.url)
    .hostname;
  if (
    !result.success ||
    (!config.testKey &&
      (result.action !== action || result.hostname !== expectedHost))
  )
    throw new HttpError(400, '人机验证已失效，请重新验证');
}
