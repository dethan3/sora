import { z } from 'zod'

export const IndexSchema = z.object({
  id: z.string(),
  name: z.string(),
  marketId: z.string(),
  ticker: z.string(),
  description: z.string().optional(),
})
export type Index = z.infer<typeof IndexSchema>
