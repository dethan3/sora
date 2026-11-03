import { z } from 'zod';
import raw from './research.json';

const text = z.string().min(1);
const date = z.iso.date();
const signalSchema = z
  .object({
    id: z.string().regex(/^[a-z0-9-]+$/),
    themeId: z.enum(['ai-chips', 'ai-storage']).optional(),
    title: text,
    summary: text,
    prosTags: z.array(z.string().min(2).max(20)).min(1).max(4).optional(),
    consTags: z.array(z.string().min(2).max(20)).min(1).max(4).optional(),
    directionIds: z.array(text).min(1),
    eventDate: date,
    reviewedAt: date,
    status: z.enum(['待验证', '已有支持', '存在反证', '已归档']),
    company: z.object({ symbol: text, name: text, role: text }),
    facts: z.array(z.object({ text, sourceId: text })).min(1),
    hypothesis: text,
    chain: z.array(z.object({ title: text, text })).min(1),
    counterpoints: z.array(text).min(1),
    checkpoints: z
      .array(z.object({ title: text, question: text, state: text }))
      .min(1),
    funds: z
      .array(
        z.object({
          code: text,
          relation: z.enum(['同领域', '相邻主题', '宽基参照']),
          scope: text,
          reason: text,
          difference: text,
          sourceId: text,
          holdingsAsOf: date.nullable(),
          weight: z.number().min(0).max(100).nullable(),
        }),
      )
      .min(1),
    extension: z.object({ title: text, text }),
    sources: z
      .array(
        z.object({
          id: text,
          name: text,
          url: z.url().refine((value) => value.startsWith('https://')),
          publishedAt: date.nullable(),
        }),
      )
      .min(1),
  })
  .superRefine((signal, context) => {
    const sources = new Set(signal.sources.map((source) => source.id));
    const references = [...signal.facts, ...signal.funds];
    if (
      sources.size !== signal.sources.length ||
      references.some((item) => !sources.has(item.sourceId))
    )
      context.addIssue({ code: 'custom', message: '研究证据引用无效' });
    if (
      signal.eventDate > signal.reviewedAt ||
      signal.sources.some(
        (source) =>
          source.publishedAt && source.publishedAt > signal.reviewedAt,
      )
    )
      context.addIssue({ code: 'custom', message: '研究日期顺序无效' });
    if (
      new Set(signal.funds.map((fund) => fund.code)).size !==
        signal.funds.length ||
      signal.funds.some(
        (fund) =>
          (fund.weight !== null && !fund.holdingsAsOf) ||
          (fund.holdingsAsOf && fund.holdingsAsOf > signal.reviewedAt),
      )
    )
      context.addIssue({ code: 'custom', message: '基金持仓依据无效' });
  });
export const researchSchema = z
  .object({ schemaVersion: z.literal(1), signals: z.array(signalSchema) })
  .superRefine((value, context) => {
    if (
      new Set(value.signals.map((signal) => signal.id)).size !==
      value.signals.length
    )
      context.addIssue({ code: 'custom', message: '研究线索重复' });
  });
export const research = researchSchema.parse(raw).signals;
export type ResearchSignal = (typeof research)[number];
export const researchUrl = (signal: ResearchSignal) => '/research/' + signal.id;
export function relatedSignals(directions: string[], funds: string[]) {
  return research.filter(
    (signal) =>
      signal.directionIds.some((id) => directions.includes(id)) ||
      signal.funds.some((fund) => funds.includes(fund.code)),
  );
}
