import { z, impactTagsSchema, jobStepsSchema } from '@sora/market-data/ingest';
export { publicInstrumentSchema } from '@sora/market-data/ingest';
export const searchItemSchema = z.object({
  id: z.string(),
  name: z.string(),
  symbol: z.string(),
  market: z.string(),
  kind: z.string(),
  currency: z.string(),
  instrumentId: z.string().nullable(),
  code: z.string().nullable(),
  status: z.string().nullable(),
});
export const searchSchema = z.object({
  items: z.array(searchItemSchema),
  directoryUpdatedAt: z.string().nullable().optional(),
});
export type SearchItem = z.infer<typeof searchItemSchema>;
export const myRequestsSchema = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      directoryId: z.string(),
      name: z.string(),
      symbol: z.string(),
      market: z.string(),
      kind: z.string(),
      status: z.string(),
      reason: z.string().nullable(),
      reviewNote: z.string().nullable(),
      createdAt: z.string(),
      instrumentId: z.string().nullable(),
      instrumentStatus: z.string().nullable(),
    }),
  ),
});
export const requestsSchema = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      name: z.string(),
      symbol: z.string(),
      market: z.string(),
      kind: z.string(),
      requesterCount: z.number(),
      reason: z.string().nullable(),
      createdAt: z.string(),
    }),
  ),
});
export const jobsSchema = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      instrumentId: z.string(),
      name: z.string(),
      symbol: z.string(),
      status: z.string(),
      mode: z.string(),
      attempt: z.number(),
      steps: jobStepsSchema,
      error: z.string().nullable(),
      createdAt: z.string(),
      finishedAt: z.string().nullable(),
      leaseUntil: z.string().nullable(),
    }),
  ),
  dispatch: z
    .object({
      status: z.string(),
      error: z.string().optional(),
      at: z.string(),
    })
    .nullable(),
});
export const instrumentsSchema = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      code: z.string(),
      name: z.string(),
      symbol: z.string(),
      market: z.string(),
      kind: z.string(),
      status: z.string(),
      impactTags: impactTagsSchema,
      chainNode: z.string().nullable(),
      revision: z.number(),
    }),
  ),
});
export type EditableInstrument = z.infer<
  typeof instrumentsSchema
>['items'][number];
export const acceptedSchema = z.object({
  dispatch: z
    .object({ status: z.string(), error: z.string().optional() })
    .optional(),
});
export const okSchema = z.object({});
export const stateLabel = (status: string | null) =>
  ({
    ready: '已收录',
    pending: '采集中',
    queued: '排队中',
    running: '采集中',
    done: '已完成',
    failed: '采集失败',
    archived: '已下架',
    approved: '已批准',
    rejected: '已驳回',
  })[status ?? ''] ?? '暂未收录';
export const kindLabel = (kind: string) =>
  ({ fund: '基金', etf: 'ETF', stock: '股票', index: '指数' })[kind] ?? kind;
export const errorText = (error: unknown) =>
  error instanceof Error ? error.message : '请求失败，请重试';
