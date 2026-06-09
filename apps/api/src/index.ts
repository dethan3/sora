import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { EvidenceDirectionSchema, EvidenceStrengthSchema } from '@sora/core'
import {
  addEvidence,
  getAssetExposure,
  getContradictions,
  getEvidenceGroups,
  getEvidenceTimeline,
  getThesis,
  listTheses,
  reviewTheses,
} from '@sora/research'
import { closeDb, dbInit, type SoraDb } from '@sora/storage'

const __filename = fileURLToPath(import.meta.url)
const REPO_ROOT = resolve(dirname(__filename), '../../../..')
const DEFAULT_DB_PATH = process.env.SORA_DB_PATH ?? join(REPO_ROOT, 'data/sora.db')
const DEFAULT_PORT = Number(process.env.SORA_API_PORT ?? 3000)
const DISCLAIMER = '以上内容为信息分析，不构成任何投资建议。'

interface ApiOptions {
  dbPath?: string
}

interface ApiRouteRequest {
  method: string
  path: string
  body?: unknown
}

export interface ApiRouteResponse {
  status: number
  body: unknown
}

interface EvidencePayload {
  source?: unknown
  title?: unknown
  summary?: unknown
  url?: unknown
  direction?: unknown
  strength?: unknown
  rationale?: unknown
  observedAt?: unknown
}

interface ValidationResult {
  ok: boolean
  errors: string[]
  value?: {
    source: string
    title: string
    summary: string
    url?: string
    direction: 'support' | 'against' | 'neutral'
    strength: 'weak' | 'medium' | 'strong'
    rationale: string
    observedAt: string
  }
}

export function createApiServer(options: ApiOptions = {}) {
  const { sqlite, db } = dbInit(options.dbPath ?? DEFAULT_DB_PATH)

  const server = createServer(async (req, res) => {
    try {
      const response = await handleRequest(db, req)
      sendJson(res, response.status, response.body)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      sendJson(res, 500, { error: { message }, disclaimer: DISCLAIMER })
    }
  })

  server.on('close', () => closeDb(sqlite))
  return server
}

async function handleRequest(db: SoraDb, req: IncomingMessage): Promise<ApiRouteResponse> {
  return handleApiRequest(db, {
    method: req.method ?? 'GET',
    path: req.url ?? '/',
    body: await readJson(req),
  })
}

export function handleApiRequest(db: SoraDb, request: ApiRouteRequest): ApiRouteResponse {
  const method = request.method
  const url = new URL(request.path, 'http://localhost')
  const parts = url.pathname.split('/').filter(Boolean)

  if (method === 'GET' && url.pathname === '/health') {
    return { status: 200, body: { data: { ok: true }, disclaimer: DISCLAIMER } }
  }

  if (method === 'GET' && url.pathname === '/api/theses') {
    return { status: 200, body: { data: listTheses(db), disclaimer: DISCLAIMER } }
  }

  if (method === 'GET' && url.pathname === '/api/review') {
    return { status: 200, body: { data: reviewTheses(db), disclaimer: DISCLAIMER } }
  }

  if (parts[0] === 'api' && parts[1] === 'theses' && parts[2]) {
    return handleThesisRoute(db, request.body, method, parts)
  }

  return { status: 404, body: { error: { message: 'Not found' }, disclaimer: DISCLAIMER } }
}

function handleThesisRoute(
  db: SoraDb,
  body: unknown,
  method: string,
  parts: string[]
): ApiRouteResponse {
  const thesisId = parts[2]

  if (method === 'GET' && parts.length === 3) {
    const thesis = getThesis(db, thesisId)
    if (!thesis) {
      return {
        status: 404,
        body: { error: { message: `Thesis not found: ${thesisId}` }, disclaimer: DISCLAIMER },
      }
    }

    return {
      status: 200,
      body: {
        data: {
          thesis,
          evidence: getEvidenceGroups(db, thesisId),
          contradictions: getContradictions(db, thesisId),
          exposure: getAssetExposure(db, thesisId),
        },
        disclaimer: DISCLAIMER,
      },
    }
  }

  if (method === 'GET' && parts[3] === 'evidence') {
    return { status: 200, body: { data: getEvidenceTimeline(db, thesisId), disclaimer: DISCLAIMER } }
  }

  if (method === 'GET' && parts[3] === 'exposure') {
    return { status: 200, body: { data: getAssetExposure(db, thesisId), disclaimer: DISCLAIMER } }
  }

  if (method === 'POST' && parts[3] === 'evidence') {
    const validation = validateEvidencePayload(body)
    if (!validation.ok || !validation.value) {
      return {
        status: 400,
        body: {
          error: { message: 'Validation failed', details: validation.errors },
          disclaimer: DISCLAIMER,
        },
      }
    }

    try {
      const result = addEvidence(db, { ...validation.value, thesisId })
      return { status: 201, body: { data: result, disclaimer: DISCLAIMER } }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      const status = message.startsWith('Thesis not found') ? 404 : 400
      return { status, body: { error: { message }, disclaimer: DISCLAIMER } }
    }
  }

  return { status: 404, body: { error: { message: 'Not found' }, disclaimer: DISCLAIMER } }
}

function validateEvidencePayload(payload: unknown): ValidationResult {
  if (!payload || typeof payload !== 'object') {
    return { ok: false, errors: ['Body must be a JSON object.'] }
  }

  const body = payload as EvidencePayload
  const errors: string[] = []

  if (typeof body.title !== 'string' || body.title.trim() === '') {
    errors.push('title is required.')
  }

  const direction = EvidenceDirectionSchema.safeParse(body.direction)
  if (!direction.success) {
    errors.push('direction must be support, against, or neutral.')
  }

  const strength = EvidenceStrengthSchema.safeParse(body.strength)
  if (!strength.success) {
    errors.push('strength must be weak, medium, or strong.')
  }

  for (const key of ['source', 'summary', 'url', 'rationale', 'observedAt'] as const) {
    const value = body[key]
    if (value != null && typeof value !== 'string') {
      errors.push(`${key} must be a string.`)
    }
  }

  if (errors.length > 0 || !direction.success || !strength.success || typeof body.title !== 'string') {
    return { ok: false, errors }
  }

  return {
    ok: true,
    errors: [],
    value: {
      source: optionalString(body.source)?.trim() || 'api',
      title: body.title.trim(),
      summary: optionalString(body.summary)?.trim() || body.title.trim(),
      url: optionalString(body.url)?.trim() || undefined,
      direction: direction.data,
      strength: strength.data,
      rationale: optionalString(body.rationale)?.trim() || 'Manual evidence entry.',
      observedAt: optionalString(body.observedAt)?.trim() || new Date().toISOString(),
    },
  }
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  const text = Buffer.concat(chunks).toString('utf8')
  if (text.trim() === '') return {}

  try {
    return JSON.parse(text) as unknown
  } catch {
    return null
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status
  res.setHeader('content-type', 'application/json; charset=utf-8')
  res.end(JSON.stringify(body, null, 2))
}

if (process.env.NODE_ENV !== 'test') {
  const server = createApiServer()
  server.listen(DEFAULT_PORT, () => {
    console.log(`Sora API listening on http://127.0.0.1:${DEFAULT_PORT}`)
  })
}
