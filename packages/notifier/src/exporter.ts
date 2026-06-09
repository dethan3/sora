import { writeFileSync, mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import type { NotificationEvent } from '@sora/core'

export interface NotificationExport {
  exportedAt: string
  events: NotificationEvent[]
}

export function exportEvents(events: NotificationEvent[], outputPath: string): NotificationExport {
  const payload: NotificationExport = {
    exportedAt: new Date().toISOString(),
    events,
  }
  mkdirSync(dirname(outputPath), { recursive: true })
  writeFileSync(outputPath, JSON.stringify(payload, null, 2), 'utf-8')
  return payload
}
