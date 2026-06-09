import { join, resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const REPO_ROOT = resolve(dirname(__filename), '../../../..')

export const SEEDS_DIR = process.env.SORA_SEEDS_DIR ?? join(REPO_ROOT, 'data/seeds')
export const CACHE_DIR = process.env.SORA_CACHE_DIR ?? join(REPO_ROOT, 'data/cache')
export const DB_PATH = process.env.SORA_DB_PATH ?? join(REPO_ROOT, 'data/sora.db')
export const PI_API_KEY = process.env.SORA_PI_API_KEY
export const PI_BASE_URL = process.env.SORA_PI_BASE_URL ?? 'https://api.pi.ai/v1'
export const TAVILY_API_KEY = process.env.TAVILY_API_KEY
