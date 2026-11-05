import {
  claimedJobSchema,
  collectionSchema,
  type Collection,
  type jobStepsSchema,
  z,
} from '@sora/market-data/ingest';
import {
  aktoolsClient,
  collectDirectory,
  collectHoldings,
  collectSeries,
  directoryScopes,
  type Aktools,
} from './collect.ts';
import type { Fetcher } from './providers.ts';

export function workerClient(
  baseUrl: string,
  token: string,
  fetcher: Fetcher = fetch,
) {
  const base = new URL(baseUrl);
  if (
    (base.protocol !== 'https:' &&
      !(
        base.protocol === 'http:' &&
        ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)
      )) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash ||
    base.pathname !== '/' ||
    token.length < 32
  )
    throw new Error(
      '需要有效的 SORA_WORKER_URL（HTTPS 源）和至少 32 字符的 SORA_INGEST_TOKEN',
    );
  return async (path: string, body: unknown = {}) => {
    if (!path.startsWith('/api/internal/'))
      throw new Error('Invalid machine endpoint');
    const response = await fetcher(new URL(path, base), {
      method: 'POST',
      redirect: 'error',
      signal: AbortSignal.timeout(30_000),
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(body),
    });
    const data = (await response.json()) as Record<string, unknown>;
    if (!response.ok)
      throw new Error(
        `Worker HTTP ${response.status}: ${typeof data.error === 'string' ? data.error : '内部接口失败'}`,
      );
    return data;
  };
}
export type Machine = ReturnType<typeof workerClient>;
export async function syncDirectory(
  machine: Machine,
  ak: Aktools,
  scopes: readonly string[] = directoryScopes,
) {
  let failed = 0;
  for (const scope of scopes) {
    try {
      const entries = await collectDirectory(scope, ak);
      const { id } = await machine('/api/internal/directory/start', {
        scope,
        count: entries.length,
      });
      for (let offset = 0; offset < entries.length; offset += 500)
        await machine(`/api/internal/directory/${id}/batch`, {
          entries: entries.slice(offset, offset + 500),
        });
      await machine(`/api/internal/directory/${id}/publish`);
      console.log(`${scope}: published ${entries.length} entries`);
    } catch (error) {
      failed++;
      console.error(
        `${scope}: ${error instanceof Error ? error.message : 'directory failed'}`,
      );
    }
  }
  return { failed };
}
export async function runJobs(
  machine: Machine,
  ak: Aktools,
  options: {
    maxJobs?: number;
    now?: () => string;
    log?: (line: string) => void;
  } = {},
) {
  const maxJobs = options.maxJobs ?? 50;
  if (!Number.isSafeInteger(maxJobs) || maxJobs < 1 || maxJobs > 200)
    throw new Error('maxJobs must be 1..200');
  const log = options.log ?? console.log;
  let completed = 0,
    failed = 0;
  try {
    for (let count = 0; count < maxJobs; count++) {
      const response = await machine('/api/internal/jobs/claim');
      if (!response.job) break;
      const job = claimedJobSchema.parse(response.job);
      const endpoint = `/api/internal/jobs/${encodeURIComponent(job.id)}`;
      const steps: z.infer<typeof jobStepsSchema> = [
        { step: 'metadata', status: 'done' },
        { step: 'holdings', status: 'pending' },
        { step: 'series', status: 'pending' },
      ];
      let lostLease: unknown;
      const heartbeat = async () => {
        await machine(`${endpoint}/heartbeat`, { lease: job.lease, steps });
      };
      const timer = setInterval(() => {
        void heartbeat().catch((error) => {
          lostLease = error;
        });
      }, 60_000);
      try {
        const now = options.now?.() ?? new Date().toISOString();
        steps[1].status = 'running';
        await heartbeat();
        const holdings =
          job.mode === 'refresh'
            ? { holdings: null, warnings: [] }
            : await collectHoldings(job, ak, now);
        steps[1].status = 'done';
        steps[2].status = 'running';
        await heartbeat();
        const series = await collectSeries(job, ak, now);
        steps[2].status = 'done';
        if (lostLease) throw lostLease;
        const result: Collection = collectionSchema.parse({
          series,
          ...holdings,
        });
        // Complete is idempotent; a lost response can safely be retried with the same lease and payload.
        try {
          await machine(`${endpoint}/complete`, { lease: job.lease, result });
        } catch {
          await machine(`${endpoint}/complete`, { lease: job.lease, result });
        }
        completed++;
        log(`${job.instrument.id}: done`);
      } catch (error) {
        failed++;
        const message = error instanceof Error ? error.message : '采集失败';
        for (const step of steps)
          if (step.status === 'running') step.status = 'failed';
        await machine(`${endpoint}/heartbeat`, {
          lease: job.lease,
          steps,
        }).catch(() => {});
        await machine(`${endpoint}/fail`, {
          lease: job.lease,
          error: message,
        }).catch(() => {});
        log(`${job.instrument.id}: ${message}`);
      } finally {
        clearInterval(timer);
      }
    }
  } finally {
    await machine('/api/internal/runner/finish').catch(() => {});
  }
  return { completed, failed };
}
export function executorFromEnvironment() {
  const machine = workerClient(
    process.env.SORA_WORKER_URL ?? '',
    process.env.SORA_INGEST_TOKEN ?? '',
  );
  const ak = aktoolsClient(process.env.AKTOOLS_BASE_URL);
  return { machine, ak };
}
