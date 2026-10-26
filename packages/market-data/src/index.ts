import { z } from 'zod';

const pointSchema = z.object({
  date: z.iso.date(),
  value: z.number().positive().finite(),
});
export const marketSeriesSchema = z
  .object({
    id: z.string().min(1),
    name: z.string().min(1),
    unit: z.enum(['points', 'usd-per-ounce']),
    timeZone: z.enum(['America/New_York', 'Asia/Hong_Kong', 'UTC']),
    session: z.enum(['open', 'closed', 'holiday', 'unknown']),
    freshness: z.enum(['current', 'stale']),
    quote: z
      .object({
        value: z.number().positive().finite(),
        previousClose: z.number().positive().finite().nullable(),
        asOf: z.union([z.iso.datetime(), z.iso.date()]),
        kind: z.enum(['daily-close', 'observation']).default('observation'),
        delayMinutes: z.number().int().nonnegative().nullable(),
      })
      .nullable(),
    history: z.array(pointSchema).max(2000),
    provenance: z
      .object({
        provider: z.enum(['aktools', 'yahoo']),
        upstream: z.string().min(1),
        symbol: z.string().min(1),
        sourceUrl: z.url().refine((url) => url.startsWith('https://')),
        adjustment: z.literal('unadjusted'),
        retrievedAt: z.iso.datetime(),
        attemptedAt: z.iso.datetime(),
        updateStatus: z.enum(['ok', 'failed']),
      })
      .optional(),
  })
  .superRefine((series, ctx) => {
    const last = series.history.at(-1);
    if (
      series.history.some(
        (point, index) =>
          index > 0 && point.date <= series.history[index - 1].date,
      )
    )
      ctx.addIssue({
        code: 'custom',
        message: 'Market history must have unique ascending dates',
      });
    if (series.quote && last) {
      const quoteDate = !series.quote.asOf.includes('T')
        ? series.quote.asOf
        : new Intl.DateTimeFormat('en-CA', {
            timeZone: series.timeZone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
          }).format(new Date(series.quote.asOf));
      if (last.date !== quoteDate || last.value !== series.quote.value)
        ctx.addIssue({
          code: 'custom',
          message:
            'Quote and final history point must describe the same observation',
        });
    }
  });
export const marketSnapshotSchema = z
  .object({
    schemaVersion: z.literal(1),
    source: z.enum(['mock', 'market-snapshot']),
    generatedAt: z.iso.datetime(),
    series: z.array(marketSeriesSchema),
  })
  .superRefine((snapshot, ctx) => {
    if (
      snapshot.source === 'market-snapshot' &&
      snapshot.series.some(
        (series) =>
          !series.provenance ||
          series.quote?.kind !== 'daily-close' ||
          series.quote.asOf.includes('T') ||
          series.history.length < 2 ||
          series.quote.previousClose !== series.history.at(-2)?.value ||
          series.provenance.retrievedAt > series.provenance.attemptedAt ||
          series.provenance.attemptedAt > snapshot.generatedAt,
      )
    )
      ctx.addIssue({
        code: 'custom',
        message: 'Collected series require daily-close provenance',
      });
    if (
      new Set(snapshot.series.map((series) => series.id)).size !==
      snapshot.series.length
    )
      ctx.addIssue({ code: 'custom', message: 'Duplicate market series' });
    if (
      snapshot.series.some(
        (series) => series.quote && series.quote.asOf > snapshot.generatedAt,
      )
    )
      ctx.addIssue({
        code: 'custom',
        message: 'Quote cannot be newer than its snapshot',
      });
  });
export type MarketSeries = z.infer<typeof marketSeriesSchema>;
export type MarketSnapshot = z.infer<typeof marketSnapshotSchema>;
export const periods = ['1W', '1M', '3M', '1Y'] as const;
export type Period = (typeof periods)[number];
export const periodLabels: Record<Period, string> = {
  '1W': '1周',
  '1M': '1月',
  '3M': '3月',
  '1Y': '1年',
};
export function changePercent(value: number, base: number | null) {
  return base === null || base <= 0 ? null : (value / base - 1) * 100;
}
export function periodHistory(
  history: MarketSeries['history'],
  period: Period,
) {
  const last = history.at(-1);
  if (!last) return [];
  const cutoff = new Date(last.date + 'T00:00:00Z');
  if (period === '1W') cutoff.setUTCDate(cutoff.getUTCDate() - 7);
  else {
    const day = cutoff.getUTCDate();
    cutoff.setUTCDate(1);
    cutoff.setUTCMonth(
      cutoff.getUTCMonth() - { '1M': 1, '3M': 3, '1Y': 12 }[period],
    );
    const month = cutoff.getUTCMonth();
    cutoff.setUTCDate(day);
    if (cutoff.getUTCMonth() !== month) cutoff.setUTCDate(0);
  }
  return history.filter(
    (point) => point.date >= cutoff.toISOString().slice(0, 10),
  );
}
export const numberText = (value: number) =>
  new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
export const unitText = (unit: MarketSeries['unit']) =>
  unit === 'points' ? '点' : 'USD / 盎司';
export function observationTime(series: MarketSeries) {
  if (!series.quote) return '暂无报价时间';
  if (!series.quote.asOf.includes('T'))
    return (
      series.quote.asOf +
      ' 收盘 · ' +
      {
        'America/New_York': '纽约交易日',
        'Asia/Hong_Kong': '香港交易日',
        UTC: 'UTC 日期',
      }[series.timeZone]
    );
  const date = new Intl.DateTimeFormat('zh-CN', {
    timeZone: series.timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(new Date(series.quote.asOf));
  return (
    date +
    ' ' +
    {
      'America/New_York': '纽约时间',
      'Asia/Hong_Kong': '香港时间',
      UTC: 'UTC',
    }[series.timeZone]
  );
}
export function isStale(series: MarketSeries, now = Date.now()) {
  return (
    series.freshness === 'stale' ||
    (!!series.provenance &&
      !!series.quote &&
      now - Date.parse(series.quote.asOf) > 4 * 86400000)
  );
}
