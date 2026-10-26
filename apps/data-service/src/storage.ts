import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { marketSnapshotSchema, type MarketSnapshot } from '@sora/market-data';

export async function readSnapshot(
  path: string,
): Promise<MarketSnapshot | undefined> {
  try {
    const raw = await readFile(path, 'utf8');
    if (raw.length > 8 * 1024 * 1024) throw new Error('Snapshot too large');
    const data = marketSnapshotSchema.parse(JSON.parse(raw));
    if (data.source !== 'market-snapshot')
      throw new Error('Refusing a synthetic snapshot');
    return data;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw error;
  }
}
export async function atomicWrite(path: string, data: unknown) {
  await mkdir(dirname(path), { recursive: true });
  const temporary = path + '.' + randomUUID() + '.tmp';
  try {
    const handle = await open(temporary, 'wx', 0o600);
    try {
      await handle.writeFile(JSON.stringify(data, null, 2) + '\n', 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(temporary, path);
  } finally {
    await unlink(temporary).catch(() => {});
  }
}
export async function withUpdateLock<T>(
  path: string,
  run: () => Promise<T>,
): Promise<T> {
  await mkdir(dirname(path), { recursive: true });
  const lock = await open(path, 'wx', 0o600);
  try {
    await lock.writeFile(
      JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }),
    );
    return await run();
  } finally {
    await lock.close();
    await unlink(path);
  }
}
