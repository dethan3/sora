import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { updateSnapshot, yahooFallback } from './update.ts';
import { atomicWrite, readSnapshot, withUpdateLock } from './storage.ts';
import { snapshotServer } from './server.ts';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const path = resolve(
  process.env.SORA_SNAPSHOT_PATH ?? resolve(root, 'data/market/market.json'),
);
try {
  const action = process.argv[2];
  if (action === 'update') {
    await withUpdateLock(path + '.lock', async () => {
      const previous = await readSnapshot(path);
      const result = await updateSnapshot({
        previous,
        baseUrl: process.env.AKTOOLS_BASE_URL,
        fallback:
          process.env.SORA_YAHOO_FALLBACK === '1' ? yahooFallback : undefined,
      });
      // A malformed previous snapshot is never silently replaced; an empty first run does not publish data.
      if (result.snapshot.series.length)
        await atomicWrite(path, result.snapshot);
      await atomicWrite(path + '.report.json', {
        at: result.snapshot.generatedAt,
        successes: result.successes,
        available: result.snapshot.series.length,
        attempts: result.attempts,
      });
      for (const attempt of result.attempts)
        console.log(
          `${attempt.id} / ${attempt.provider}: ${attempt.ok ? 'ok' : attempt.error}`,
        );
      console.log(
        `Updated ${result.successes}/3 directions; ${result.snapshot.series.length} available. Snapshot: ${path}`,
      );
      if (!result.successes) process.exitCode = 1;
    });
  } else if (action === 'serve') {
    const port = Number(process.env.SORA_DATA_PORT ?? '8788');
    if (!Number.isInteger(port) || port < 1 || port > 65535)
      throw new Error('Invalid data port');
    const server = snapshotServer(path);
    server.on('error', (error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
    server.listen(port, '127.0.0.1', () =>
      console.log(
        `Snapshot API: http://127.0.0.1:${port}/api/market (read-only)`,
      ),
    );
    for (const signal of ['SIGINT', 'SIGTERM'] as const)
      process.on(signal, () => server.close());
  } else if (action === 'export') {
    const snapshot = await readSnapshot(path);
    if (!snapshot?.series.length)
      throw new Error('Collect at least one real series before exporting');
    await atomicWrite(
      resolve(root, 'apps/web/dist-market/data/market.json'),
      snapshot,
    );
    console.log('Exported verified snapshot into dist-market/data/market.json');
  } else throw new Error('Use update, serve or export');
} catch (error) {
  console.error(error instanceof Error ? error.message : 'Data task failed');
  process.exitCode = 1;
}
