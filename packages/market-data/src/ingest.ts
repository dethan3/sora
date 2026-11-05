import { z } from 'zod';
export { z } from 'zod';

export const marketSchema = z.enum(['CN', 'US', 'HK']);
export const kindSchema = z.enum(['fund', 'etf', 'stock', 'index']);
export const impactTagsSchema = z
  .array(
    z.object({
      label: z.string().min(2).max(16),
      tone: z.enum(['positive', 'headwind', 'watch']),
    }),
  )
  .max(5);
const https = z.url().refine((value) => {
  const url = new URL(value);
  return url.protocol === 'https:' && !url.username && !url.password;
});
export function instrumentKey(market: string, kind: string, symbol: string) {
  return `${market}:${kind}:${symbol}`;
}
export const directoryEntrySchema = z
  .object({
    id: z.string().max(80),
    market: marketSchema,
    kind: kindSchema,
    symbol: z.string().regex(/^[A-Z0-9.^-]{1,24}$/),
    name: z.string().min(1).max(160),
    currency: z.enum(['CNY', 'USD', 'HKD']),
    pinyin: z.string().max(200).default(''),
    sourceName: z.string().min(1).max(100),
    sourceUrl: https,
  })
  .strict()
  .superRefine((entry, ctx) => {
    if (
      entry.id !== instrumentKey(entry.market, entry.kind, entry.symbol) ||
      entry.currency !==
        ({ CN: 'CNY', US: 'USD', HK: 'HKD' } as const)[entry.market]
    )
      ctx.addIssue({ code: 'custom', message: '标的身份或币种不匹配' });
  });
export type DirectoryEntry = z.infer<typeof directoryEntrySchema>;
export const directoryScopeSchema = z
  .string()
  .regex(/^(CN|US|HK):(fund|etf|stock|index)$/);
export const sourceSchema = z.object({
  name: z.string().min(1).max(100),
  url: https,
  fetchedAt: z.iso.datetime(),
});
const decimal = z
  .string()
  .regex(/^\d{1,12}(?:\.\d{1,8})?$/)
  .refine((value) => Number(value) > 0);
export const collectedSeriesSchema = z
  .object({
    type: z.enum(['price', 'nav', 'index']),
    currency: z.enum(['CNY', 'USD', 'HKD']),
    timeZone: z.enum(['Asia/Shanghai', 'America/New_York', 'Asia/Hong_Kong']),
    adjust: z.literal('unadjusted'),
    source: sourceSchema,
    history: z
      .array(z.object({ date: z.iso.date(), close: decimal }))
      .min(2)
      .max(1000),
  })
  .strict()
  .superRefine((series, ctx) => {
    if (
      series.history.some(
        (row, index) => index > 0 && row.date <= series.history[index - 1].date,
      )
    )
      ctx.addIssue({ code: 'custom', message: '日线日期必须唯一递增' });
  });
export type CollectedSeries = z.infer<typeof collectedSeriesSchema>;
export const holdingSchema = z
  .object({
    id: z.string(),
    market: marketSchema,
    symbol: z.string().regex(/^[A-Z0-9.-]{1,24}$/),
    name: z.string().min(1).max(160),
    weightBp: z.number().int().positive().max(10000),
  })
  .strict()
  .superRefine((row, ctx) => {
    if (row.id !== instrumentKey(row.market, 'stock', row.symbol))
      ctx.addIssue({ code: 'custom', message: '持仓标的身份不匹配' });
  });
export const holdingsSchema = z
  .object({
    asOf: z.iso.date(),
    coverage: z.enum(['top10', 'partial', 'full']),
    source: sourceSchema,
    holdings: z.array(holdingSchema).min(1).max(500),
  })
  .strict()
  .superRefine((snapshot, ctx) => {
    const rows = snapshot.holdings;
    if (
      (snapshot.coverage === 'top10' && rows.length > 10) ||
      new Set(rows.map((row) => row.id)).size !== rows.length ||
      rows.reduce((sum, row) => sum + row.weightBp, 0) > 10005 ||
      rows.some(
        (row, index) => index > 0 && row.weightBp > rows[index - 1].weightBp,
      )
    )
      ctx.addIssue({ code: 'custom', message: '持仓权重、顺序或覆盖范围无效' });
  });
export type CollectedHoldings = z.infer<typeof holdingsSchema>;
export const collectionSchema = z
  .object({
    series: collectedSeriesSchema,
    holdings: holdingsSchema.nullable(),
    warnings: z.array(z.string().min(1).max(300)).max(8).default([]),
  })
  .strict();
export type Collection = z.infer<typeof collectionSchema>;
export const jobStepsSchema = z
  .array(
    z.object({
      step: z.enum(['metadata', 'holdings', 'series']),
      status: z.enum(['pending', 'running', 'done', 'failed']),
      message: z.string().max(200).optional(),
    }),
  )
  .max(3);
export const claimedJobSchema = z.object({
  id: z.string(),
  lease: z.string(),
  leaseUntil: z.string(),
  mode: z.enum(['full', 'refresh']),
  attempt: z.number(),
  instrument: directoryEntrySchema,
  instrumentId: z.string(),
  sinceDate: z.string().nullable(),
});
export type ClaimedJob = z.infer<typeof claimedJobSchema>;
export const publicInstrumentSchema = z.object({
  id: z.string(),
  code: z.string(),
  market: marketSchema,
  kind: kindSchema,
  symbol: z.string(),
  name: z.string(),
  currency: z.string(),
  status: z.enum(['pending', 'ready', 'failed', 'archived']),
  impactTags: impactTagsSchema,
  chainNode: z.string().nullable(),
  revision: z.number(),
  holdings: holdingsSchema.nullable(),
  series: collectedSeriesSchema.nullable(),
  warnings: z.array(z.string()),
  lastError: z.string().nullable(),
  companies: z
    .array(
      z.object({
        id: z.string(),
        impactTags: impactTagsSchema,
        chainNode: z.string().nullable(),
      }),
    )
    .default([]),
});
export type PublicInstrument = z.infer<typeof publicInstrumentSchema>;
