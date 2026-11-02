import { marketSnapshotSchema, type MarketSeries } from './model';

// Deterministic synthetic observations. No vendor data or historical prices are used.
function history(seed: number, lastValue: number, end: string, flat = false) {
  const endDate = new Date(end + 'T00:00:00Z');
  const points: MarketSeries['history'] = [];
  for (let days = 370; days >= 0; days--) {
    const date = new Date(endDate);
    date.setUTCDate(date.getUTCDate() - days);
    if ([0, 6].includes(date.getUTCDay())) continue;
    const t = 370 - days;
    const value = flat
      ? lastValue
      : lastValue *
        (0.87 +
          (t / 370) * 0.13 +
          Math.sin(t / 13 + seed) * 0.018 +
          Math.cos(t / 4.7 + seed) * 0.006);
    points.push({
      date: date.toISOString().slice(0, 10),
      value: Number(value.toFixed(2)),
    });
  }
  points[points.length - 1].value = lastValue;
  return points;
}
const configs = [
  {
    id: 'ai',
    name: 'Indxx Artificial Intelligence & Big Data Index',
    value: 1542.75,
    movement: 1.24,
  },
  {
    id: 'semiconductors',
    name: 'NYSE Semiconductor Index',
    value: 8120.4,
    movement: -1.36,
  },
  { id: 'gold', name: '黄金现货参考价', value: 2684.2, movement: 0.48 },
  { id: 'nasdaq100', name: 'Nasdaq-100 Index', value: 22365.8, movement: 0.86 },
  { id: 'sp500', name: 'S&P 500 Index', value: 6210.35, movement: 0 },
  { id: 'hangseng', name: '恒生指数', value: 24580.6, movement: -0.72 },
];
export const mockMarket = marketSnapshotSchema.parse({
  schemaVersion: 1,
  source: 'mock',
  generatedAt: '2026-10-24T00:00:00Z',
  series: [
    ...configs.map((item, index) => {
      const gold = item.id === 'gold';
      const hongKong = item.id === 'hangseng';
      const points = history(
        index,
        item.value,
        gold ? '2026-10-22' : '2026-10-23',
        item.id === 'sp500',
      );
      const previousClose = Number(
        (item.value / (1 + item.movement / 100)).toFixed(2),
      );
      points[points.length - 2].value = previousClose;
      return {
        id: item.id,
        name: item.name,
        unit: gold ? 'usd-per-ounce' : 'points',
        timeZone: hongKong
          ? 'Asia/Hong_Kong'
          : gold
            ? 'UTC'
            : 'America/New_York',
        session: hongKong ? 'holiday' : 'closed',
        freshness: gold ? 'stale' : 'current',
        quote: {
          value: item.value,
          previousClose,
          asOf: gold
            ? '2026-10-22T20:00:00Z'
            : hongKong
              ? '2026-10-23T08:00:00Z'
              : '2026-10-23T20:00:00Z',
          delayMinutes: item.id === 'semiconductors' ? 15 : null,
        },
        history: points,
      };
    }),
    ...[
      ['AIQ', 'Global X 人工智能与科技 ETF', 42.6],
      ['SOXX', 'iShares 半导体 ETF', 220.4],
      ['IAU', 'iShares 黄金信托', 52.8],
      ['QQQ', 'Invesco 纳斯达克100 ETF', 480.2],
      ['IVV', 'iShares 标普500 ETF', 562.1],
      ['2800.HK', '盈富基金', 24.2],
    ].map(([rawCode, rawName, rawValue], index) => {
      const code = String(rawCode);
      const points = history(index + 10, Number(rawValue), '2026-10-23');
      const hk = code === '2800.HK';
      return {
        id: `fund:${code}`,
        name: String(rawName),
        instrumentType: 'fund-price',
        fundCode: code,
        unit: hk ? 'hkd-per-share' : 'usd-per-share',
        timeZone: hk ? 'Asia/Hong_Kong' : 'America/New_York',
        session: 'closed',
        freshness: 'current',
        history: points,
        quote: {
          value: Number(rawValue),
          previousClose: points.at(-2)!.value,
          asOf: '2026-10-23',
          kind: 'daily-close',
          delayMinutes: null,
        },
      };
    }),
  ],
});
