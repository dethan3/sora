import { z } from 'zod'

export const MarketCategorySchema = z.enum(['us', 'hk', 'cn', 'commodity', 'global'])
export type MarketCategory = z.infer<typeof MarketCategorySchema>

export const MarketSchema = z.object({
  id: z.string(),
  name: z.string(),
  category: MarketCategorySchema,
  description: z.string().optional(),
})
export type Market = z.infer<typeof MarketSchema>
