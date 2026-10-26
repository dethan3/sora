import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readSnapshot } from './storage.ts';
export function snapshotServer(path: string) {
  return createServer(async (request, response) => {
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.setHeader('Content-Type', 'application/json; charset=utf-8');
    if (!['GET', 'HEAD'].includes(request.method ?? '')) {
      response.writeHead(405, { Allow: 'GET, HEAD' });
      response.end();
      return;
    }
    if (!['/api/market', '/health'].includes(request.url ?? '')) {
      response.writeHead(404);
      response.end(JSON.stringify({ error: 'Not found' }));
      return;
    }
    try {
      const snapshot = await readSnapshot(path);
      if (request.url === '/health') {
        response.setHeader('Cache-Control', 'no-store');
        response.end(
          request.method === 'HEAD'
            ? undefined
            : JSON.stringify({
                status: snapshot?.series.length ? 'ready' : 'waiting',
              }),
        );
        return;
      }
      if (!snapshot?.series.length) {
        response.writeHead(503, { 'Cache-Control': 'no-store' });
        response.end(JSON.stringify({ error: '行情快照尚不可用' }));
        return;
      }
      const body = JSON.stringify(snapshot);
      const etag = '"' + createHash('sha256').update(body).digest('hex') + '"';
      response.setHeader('ETag', etag);
      response.setHeader('Cache-Control', 'no-cache');
      if (request.headers['if-none-match'] === etag) {
        response.writeHead(304);
        response.end();
        return;
      }
      response.end(request.method === 'HEAD' ? undefined : body);
    } catch {
      response.writeHead(503, { 'Cache-Control': 'no-store' });
      response.end(JSON.stringify({ error: '行情快照暂不可用' }));
    }
  });
}
