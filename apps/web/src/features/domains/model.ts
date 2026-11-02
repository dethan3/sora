import { z } from 'zod';
import raw from './ai-chips.json';

const text = z.string().min(1);
const symbol = z.string().regex(/^[A-Z0-9.-]{1,16}$/);
const date = z.iso.date();
export const domainSchema = z
  .object({
    schemaVersion: z.literal(1),
    id: z.literal('ai-chips'),
    name: text,
    description: text,
    reviewedAt: date,
    companies: z
      .array(
        z.object({
          symbol,
          name: text,
          role: text,
          description: text,
          sourceId: text,
        }),
      )
      .min(1),
    events: z
      .array(
        z.object({
          id: text,
          company: symbol,
          publishedAt: date,
          period: text,
          title: text,
          fact: text,
          interpretation: text,
          watch: text,
          sourceId: text,
          researchId: z
            .string()
            .regex(/^[a-z0-9-]+$/)
            .nullable(),
        }),
      )
      .min(1),
    fundSnapshots: z
      .array(
        z.object({
          code: symbol,
          asOf: date,
          coverage: z.enum(['full-equity', 'top-ten']),
          reportedEquityCount: z.number().int().min(10).nullable(),
          sourceId: text,
          scope: text,
          description: text,
          holdings: z
            .array(
              z.object({
                symbol,
                name: text,
                weight: z.number().finite().positive().max(100),
              }),
            )
            .min(10),
        }),
      )
      .min(1),
    sources: z
      .array(
        z.object({
          id: text,
          name: text,
          url: z.url().refine((value) => value.startsWith('https://')),
          kind: z.enum(['company-release', 'issuer-holdings']),
        }),
      )
      .min(1),
  })
  .superRefine((domain, context) => {
    const fail = (message: string) =>
      context.addIssue({ code: 'custom', message });
    const unique = (ids: string[]) => new Set(ids).size === ids.length;
    if (
      !unique(domain.companies.map((item) => item.symbol)) ||
      !unique(domain.events.map((item) => item.id)) ||
      !unique(domain.fundSnapshots.map((item) => item.code)) ||
      !unique(domain.sources.map((item) => item.id))
    )
      fail('领域记录重复');
    const source = (id: string, kind: string) =>
      domain.sources.some((item) => item.id === id && item.kind === kind);
    if (
      domain.companies.some(
        (item) => !source(item.sourceId, 'company-release'),
      ) ||
      domain.events.some(
        (item) =>
          !source(item.sourceId, 'company-release') ||
          !domain.companies.some((company) => company.symbol === item.company),
      )
    )
      fail('公司或事件来源无效');
    if (
      domain.events.some((item) => item.publishedAt > domain.reviewedAt) ||
      domain.fundSnapshots.some((item) => item.asOf > domain.reviewedAt)
    )
      fail('资料日期不能晚于核对日期');
    for (const snapshot of domain.fundSnapshots) {
      const total = snapshot.holdings.reduce(
        (sum, item) => sum + item.weight,
        0,
      );
      if (
        !source(snapshot.sourceId, 'issuer-holdings') ||
        !unique(snapshot.holdings.map((item) => item.symbol))
      )
        fail('基金持仓来源或标识无效');
      if (
        snapshot.holdings.some(
          (item, index) =>
            index > 0 && item.weight > snapshot.holdings[index - 1].weight,
        )
      )
        fail('持仓须按权重降序');
      if (
        total > 101 ||
        (snapshot.coverage === 'full-equity' &&
          (snapshot.reportedEquityCount !== snapshot.holdings.length ||
            total < 99)) ||
        (snapshot.coverage === 'top-ten' && snapshot.holdings.length !== 10)
      )
        fail('持仓覆盖范围与记录不一致');
    }
  });

export const aiChips = domainSchema.parse(raw);
export type Domain = typeof aiChips;
export type HoldingSnapshot = Domain['fundSnapshots'][number];
export function fundSnapshot(code: string) {
  return aiChips.fundSnapshots.find((snapshot) => snapshot.code === code);
}
export function holdingFor(snapshot: HoldingSnapshot, company: string) {
  const holding = snapshot.holdings.find((item) => item.symbol === company);
  if (holding) return { status: 'held' as const, weight: holding.weight };
  return {
    status:
      snapshot.coverage === 'full-equity'
        ? ('not-held' as const)
        : ('unknown' as const),
    weight: null,
  };
}
export function topTenWeight(snapshot: HoldingSnapshot) {
  return (
    Math.round(
      snapshot.holdings
        .slice(0, 10)
        .reduce((sum, item) => sum + item.weight, 0) * 100,
    ) / 100
  );
}
export function equityOverlap(first: HoldingSnapshot, second: HoldingSnapshot) {
  if (first.asOf !== second.asOf)
    return { weight: null, reason: '两份持仓日期不同，暂不计算重叠率。' };
  if (first.coverage !== 'full-equity' || second.coverage !== 'full-equity')
    return { weight: null, reason: '含持仓节选，无法计算完整重叠率。' };
  const weight = first.holdings.reduce(
    (sum, item) =>
      sum +
      Math.min(
        item.weight,
        second.holdings.find((other) => other.symbol === item.symbol)?.weight ??
          0,
      ),
    0,
  );
  return {
    weight: Math.round(weight * 100) / 100,
    reason: '同日权益持仓的最小权重之和，不含现金、衍生品或穿透持仓。',
  };
}
export function companyHref(company: string) {
  return '/discover?' + new URLSearchParams({ company });
}
