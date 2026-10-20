import { z } from 'zod';

const amount = z.number().finite().nonnegative();
export const demoSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.literal('fictional-demo'),
    asOf: z.iso.date(),
    funds: z.array(
      z.object({
        code: z.string().min(1),
        name: z.string().min(1),
        market: z.string(),
        category: z.string(),
        changePct: z.number().finite(),
        note: z.string(),
      }),
    ),
    watchlist: z.array(z.string()),
    positions: z.array(
      z.object({ code: z.string(), cost: amount, value: amount }),
    ),
  })
  .superRefine((data, ctx) => {
    const codes = new Set(data.funds.map((fund) => fund.code));
    if (codes.size !== data.funds.length)
      ctx.addIssue({ code: 'custom', message: '基金代码重复' });
    if (
      [...data.watchlist, ...data.positions.map((p) => p.code)].some(
        (code) => !codes.has(code),
      )
    ) {
      ctx.addIssue({ code: 'custom', message: '示例引用不存在的基金' });
    }
  });
export type DemoData = z.infer<typeof demoSchema>;
export type Fund = DemoData['funds'][number];

export async function loadDemo(signal?: AbortSignal): Promise<DemoData> {
  const response = await fetch(`${import.meta.env.BASE_URL}data/demo.json`, {
    signal,
  });
  if (!response.ok) throw new Error('示例数据读取失败');
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
