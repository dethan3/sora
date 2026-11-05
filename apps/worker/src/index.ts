import { handlePersonal } from './personal.ts';
import { readMarket } from './market.ts';
import { readCatalog } from './catalog.ts';
import type { Env } from './types.ts';
import { HttpError, json, JSON_HEADERS } from './http.ts';
import { handleAuth, type AuthServices } from './auth.ts';
import { handleAdmin } from './admin.ts';
import { handleInternal } from './ingest-internal.ts';
import { handleIngestPublic } from './ingest-public.ts';
import { dispatchPending } from './ingest-model.ts';

async function etagged(request: Request, body: string) {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(body),
  );
  const etag =
    '"' +
    [...new Uint8Array(digest)]
      .map((byte) => byte.toString(16).padStart(2, '0'))
      .join('') +
    '"';
  const headers = { ...JSON_HEADERS, ETag: etag, 'Cache-Control': 'no-cache' };
  if (request.headers.get('If-None-Match') === etag)
    return new Response(null, { status: 304, headers });
  return new Response(request.method === 'HEAD' ? null : body, { headers });
}

export async function handleApi(
  request: Request,
  env: Env,
  services: AuthServices = {},
) {
  const { pathname } = new URL(request.url);
  try {
    const personal = await handlePersonal(request, env, services);
    if (personal) return personal;
    const internal = await handleInternal(request, env, services);
    if (internal) return internal;
    const ingest = await handleIngestPublic(request, env, services);
    if (ingest) return ingest;
    const admin = await handleAdmin(request, env, services);
    if (admin) return admin;
    const auth = await handleAuth(request, env, services);
    if (auth) return auth;
    if (!['GET', 'HEAD'].includes(request.method))
      return json(
        { error: 'Method not allowed' },
        { status: 405, headers: { Allow: 'GET, HEAD' } },
      );
    if (pathname === '/api/health') {
      const row = await env.DB.prepare('SELECT 1 AS ok').first<{
        ok: number;
      }>();
      return json(
        { status: row?.ok === 1 ? 'ready' : 'waiting' },
        { headers: { 'Cache-Control': 'no-store' } },
      );
    }
    if (pathname === '/api/market')
      return etagged(
        request,
        JSON.stringify(await readMarket(env, services.now?.())),
      );
    if (pathname === '/api/catalog') {
      const catalog = await readCatalog(env.DB);
      if (!catalog)
        return json(
          { error: '资料目录尚未初始化' },
          { status: 503, headers: { 'Cache-Control': 'no-store' } },
        );
      return etagged(request, JSON.stringify(catalog));
    }
  } catch (error) {
    if (error instanceof HttpError)
      return json(
        { error: error.message },
        {
          status: error.status,
          headers: {
            'Cache-Control': 'no-store',
            ...(error.retryAfter
              ? { 'Retry-After': String(error.retryAfter) }
              : {}),
          },
        },
      );
    return json(
      { error: '服务暂不可用' },
      { status: 503, headers: { 'Cache-Control': 'no-store' } },
    );
  }
  return json({ error: 'Not found' }, { status: 404 });
}

export default {
  async scheduled(_event: unknown, env: Env) {
    await dispatchPending(env);
  },
  async fetch(request: Request, env: Env) {
    const { pathname } = new URL(request.url);
    if (pathname === '/api' || pathname.startsWith('/api/'))
      return handleApi(request, env);
    return env.ASSETS.fetch(request);
  },
};
