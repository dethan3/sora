// Local integration entry only. wrangler.jsonc deploys src/index.ts.
// Replace only external Siteverify; real workerd crypto, routing and D1 run here.
import worker, { handleApi } from '../index.ts';
import type { Env } from '../types.ts';
const used = new Set<string>();
const verify: typeof fetch = async (_input, init) => {
  const token = new URLSearchParams(String(init?.body)).get('response') ?? '';
  const success = token.startsWith('runtime-') && !used.has(token);
  used.add(token);
  return Response.json({ success, hostname: 'localhost', action: 'test' });
};
export default {
  fetch(request: Request, env: Env) {
    const path = new URL(request.url).pathname;
    return path === '/api' || path.startsWith('/api/')
      ? handleApi(request, env, { fetch: verify })
      : worker.fetch(request, env);
  },
};
