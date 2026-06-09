import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs'
import { join, dirname } from 'node:path'

export interface CacheEntry<T> {
  fetchedAt: string
  source: string
  data: T
}

export const ONE_DAY_MS = 24 * 60 * 60 * 1000
export const SEVEN_DAYS_MS = 7 * ONE_DAY_MS

export function readCache<T>(cacheDir: string, key: string): CacheEntry<T> | null {
  const filePath = join(cacheDir, `${key}.json`)
  if (!existsSync(filePath)) return null
  try {
    const content = readFileSync(filePath, 'utf-8')
    return JSON.parse(content) as CacheEntry<T>
  } catch {
    return null
  }
}

export function writeCache<T>(cacheDir: string, key: string, source: string, data: T): void {
  const filePath = join(cacheDir, `${key}.json`)
  mkdirSync(dirname(filePath), { recursive: true })
  const entry: CacheEntry<T> = {
    fetchedAt: new Date().toISOString(),
    source,
    data,
  }
  writeFileSync(filePath, JSON.stringify(entry, null, 2), 'utf-8')
}

export function isCacheStale(entry: CacheEntry<unknown>, maxAgeMs: number): boolean {
  const fetchedAt = new Date(entry.fetchedAt).getTime()
  return Date.now() - fetchedAt > maxAgeMs
}
