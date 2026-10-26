import { z } from 'zod';
import type { Fund } from '../../data/demo';

export const currencySchema = z.enum(['CNY', 'USD', 'KRW', 'HKD']);
export type Currency = z.infer<typeof currencySchema>;
export const currencyDigits = (currency: Currency) =>
  currency === 'KRW' ? 0 : 2;

export function scaledDecimal(value: string, digits: number): bigint {
  if (!new RegExp(`^\\d{1,10}(?:\\.\\d{1,${digits}})?$`).test(value))
    throw new Error(`请输入非负数字，最多 ${digits} 位小数`);
  const [whole, fraction = ''] = value.split('.');
  return (
    BigInt(whole) * 10n ** BigInt(digits) + BigInt(fraction.padEnd(digits, '0'))
  );
}
export function parseUnits(value: string): string {
  const units = scaledDecimal(value.trim(), 4);
  if (units <= 0n || units > 10000000000n)
    throw new Error('持有份额须大于 0，且不超过 1,000,000');
  return value.trim();
}
export function parseCost(value: string, currency: Currency): number {
  const text = value.trim();
  const digits = currencyDigits(currency);
  if (!digits && !/^\d{1,10}$/.test(text))
    throw new Error('韩元成本须为非负整数');
  const minor = digits ? scaledDecimal(text, digits) : BigInt(text);
  if (minor > 1000000000000n) throw new Error('成本金额超出支持范围');
  return Number(minor);
}
export const positionSchema = z
  .object({
    code: z.string().min(1).max(50),
    units: z.string().refine((value) => {
      try {
        parseUnits(value);
        return true;
      } catch {
        return false;
      }
    }, '持仓份额无效'),
    costMinor: z.number().int().min(0).max(1000000000000),
    currency: currencySchema,
  })
  .strict();
export type Position = z.infer<typeof positionSchema>;

export function valuePosition(position: Position, fund?: Fund): number | null {
  if (!fund || fund.nav === null || fund.currency !== position.currency)
    return null;
  const product = scaledDecimal(position.units, 4) * scaledDecimal(fund.nav, 6);
  const divisor = 10n ** BigInt(10 - currencyDigits(position.currency));
  return Number((product + divisor / 2n) / divisor);
}
export function summarizePositions(positions: Position[], funds: Fund[]) {
  return (['CNY', 'USD', 'HKD', 'KRW'] as const).flatMap((currency) => {
    const rows = positions.filter((p) => p.currency === currency);
    if (!rows.length) return [];
    const values = rows.map((p) =>
      valuePosition(
        p,
        funds.find((f) => f.code === p.code),
      ),
    );
    const complete = values.every((value) => value !== null);
    const cost = rows.reduce((total, p) => total + p.costMinor, 0);
    const knownValue = values.reduce<number>(
      (total, value) => total + (value ?? 0),
      0,
    );
    const value = complete ? knownValue : null;
    const profit = value === null ? null : value - cost;
    return [
      {
        currency,
        cost,
        value,
        knownValue,
        profit,
        changePct: profit === null || cost === 0 ? null : (profit / cost) * 100,
        missing: values.filter((v) => v === null).length,
      },
    ];
  });
}
export function money(minor: number, currency: Currency): string {
  return new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency,
    minimumFractionDigits: currencyDigits(currency),
    maximumFractionDigits: currencyDigits(currency),
  }).format(minor / 10 ** currencyDigits(currency));
}
export function costInput(position: Position): string {
  return (position.costMinor / 10 ** currencyDigits(position.currency)).toFixed(
    currencyDigits(position.currency),
  );
}
