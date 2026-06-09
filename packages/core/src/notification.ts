import { z } from 'zod'

export const NotificationLevelSchema = z.enum(['info', 'watch', 'warning', 'critical'])
export type NotificationLevel = z.infer<typeof NotificationLevelSchema>

export const NotificationEventSchema = z.object({
  id: z.string(),
  level: NotificationLevelSchema,
  title: z.string(),
  summary: z.string(),
  source: z.literal('sora'),
  type: z.string(),
  relatedEntityType: z.string().optional(),  // 'fund' | 'index' | 'market' | 'research_card'
  relatedEntityId: z.string().optional(),
  payload: z.record(z.unknown()).optional(),
  createdAt: z.string(),  // ISO datetime string
})
export type NotificationEvent = z.infer<typeof NotificationEventSchema>
