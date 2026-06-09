import { z } from 'zod'

export const AlertLevelSchema = z.enum(['info', 'watch', 'warning', 'critical'])
export type AlertLevel = z.infer<typeof AlertLevelSchema>

export const AlertSchema = z.object({
  id: z.string(),
  level: AlertLevelSchema,
  title: z.string(),
  fundId: z.string().optional(),
  message: z.string(),
  createdAt: z.string(),  // ISO datetime string
})
export type Alert = z.infer<typeof AlertSchema>
