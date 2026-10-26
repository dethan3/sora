import { z } from 'zod';
import { currencySchema } from '../features/portfolio/model';

const amount = z.number().finite().nonnegative();
export const demoSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.enum(['fictional-demo', 'reference-catalog']),
    asOf: z.iso.date(),
    funds: z.array(
      z.object({
        code: z.string().min(1),
        name: z.string().min(1),
        market: z.string(),
        category: z.string(),
        changePct: z.number().finite().nullable(),
        note: z.string(),
        currency: currencySchema,
        nav: z
          .string()
          .refine(
            (value) =>
              /^\d{1,5}(\.\d{1,6})?$/.test(value) &&
              Number(value) > 0 &&
              Number(value) <= 10000,
          )
          .nullable(),
        history: z
          .array(
            z.object({
              date: z.iso.date(),
              value: z.number().positive().max(10000),
            }),
          )
          .max(1000),
        sourceUrl: z
          .url()
          .refine((value) => value.startsWith('https://'))
          .optional(),
        sourceName: z.string().optional(),
        benchmark: z.string().optional(),
      }),
    ),
    watchlist: z.array(z.string()),
    modules: z.array(
      z.object({
        id: z.string().regex(/^[a-z0-9-]+$/),
        title: z.string(),
        kind: z.enum(['region', 'theme', 'asset', 'index']),
        symbol: z.string().max(4),
        description: z.string(),
        fundCodes: z.array(z.string()),
        scope: z.array(z.string()).default([]),
        keywords: z.array(z.string()).default([]),
        benchmark: z.string().optional(),
        benchmarkNote: z.string().optional(),
        fundReasons: z.record(z.string(), z.string()).default({}),
        sourceUrl: z
          .url()
          .refine((value) => value.startsWith('https://'))
          .optional(),
        sourceName: z.string().optional(),
      }),
    ),
    positions: z.array(
      z.object({ code: z.string(), cost: amount, value: amount }),
    ),
  })
  .superRefine((data, ctx) => {
    if (
      data.source === 'reference-catalog' &&
      (data.positions.length ||
        data.watchlist.length ||
        data.funds.some(
          (fund) =>
            fund.nav !== null ||
            fund.history.length ||
            fund.changePct !== null ||
            !fund.sourceUrl ||
            !fund.sourceName,
        ) ||
        data.modules.some(
          (module) =>
            !module.sourceUrl ||
            !module.benchmark ||
            module.fundCodes.some((code) => !module.fundReasons[code]),
        ))
    ) {
      ctx.addIssue({
        code: 'custom',
        message: '资料目录必须包含来源和关联依据，不能混入行情或个人记录',
      });
    }
    const codes = new Set(data.funds.map((fund) => fund.code));
    if (codes.size !== data.funds.length)
      ctx.addIssue({ code: 'custom', message: '基金代码重复' });
    if (
      [
        ...data.watchlist,
        ...data.positions.map((p) => p.code),
        ...data.modules.flatMap((module) => module.fundCodes),
      ].some((code) => !codes.has(code))
    ) {
      ctx.addIssue({ code: 'custom', message: '引用不存在的基金' });
    }
    if (
      new Set(data.modules.map((module) => module.id)).size !==
      data.modules.length
    )
      ctx.addIssue({ code: 'custom', message: '模块代码重复' });
    for (const fund of data.funds) {
      if (
        fund.history.some(
          (point, index) =>
            point.date > data.asOf ||
            (index > 0 && point.date <= fund.history[index - 1].date),
        )
      )
        ctx.addIssue({
          code: 'custom',
          message: '历史日期必须递增且不晚于快照',
        });
      const last = fund.history.at(-1);
      if (
        fund.nav !== null &&
        last &&
        (last.date !== data.asOf ||
          Math.abs(last.value - Number(fund.nav)) > 0.0000001)
      )
        ctx.addIssue({ code: 'custom', message: '净值与历史末点不一致' });
    }
  });
export type DemoData = z.infer<typeof demoSchema>;
export type Fund = DemoData['funds'][number];

export async function loadDemo(signal?: AbortSignal): Promise<DemoData> {
  const response = await fetch(`${import.meta.env.BASE_URL}data/catalog.json`, {
    signal,
  });
  if (!response.ok) throw new Error('数据读取失败');
  return demoSchema.parse(await response.json());
}

export function portfolioSummary(positions: DemoData['positions']) {
  const cost = positions.reduce((total, p) => total + p.cost, 0);
  const value = positions.reduce((total, p) => total + p.value, 0);
  return {
    cost,
    value,
    profit: value - cost,
    changePct: cost ? ((value - cost) / cost) * 100 : 0,
  };
}
