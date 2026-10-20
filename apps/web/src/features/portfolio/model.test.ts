import { describe, expect, it } from 'vitest';
import fixture from '../../../public/data/demo.json';
import { demoSchema } from '../../data/demo';
import {
  money,
  parseCost,
  parseUnits,
  positionSchema,
  summarizePositions,
  valuePosition,
  type Position,
} from './model';

const data = demoSchema.parse(fixture);
const position: Position = {
  code: 'DEMO-CN',
  units: '10000',
  costMinor: 1000000,
  currency: 'CNY',
};
describe('portfolio arithmetic and input boundaries', () => {
  it('parses money exactly and rounds valuation half-up to the currency minor unit', () => {
    expect(parseCost('0.29', 'CNY')).toBe(29);
    expect(parseCost('999', 'KRW')).toBe(999);
    expect(valuePosition(position, data.funds[0])).toBe(1042000);
    expect(
      valuePosition(
        { ...position, units: '0.5' },
        { ...data.funds[0], nav: '1.01' },
      ),
    ).toBe(51);
    expect(
      valuePosition(
        { ...position, currency: 'KRW', units: '0.5' },
        { ...data.funds[0], currency: 'KRW', nav: '1' },
      ),
    ).toBe(1);
    expect(money(29, 'CNY')).toBe('¥0.29');
  });
  it.each([
    '-1',
    '0',
    '1e3',
    'NaN',
    'Infinity',
    '0.00001',
    '1000001',
    '1,000',
    '',
  ])('rejects invalid shares %s', (value) => {
    expect(() => parseUnits(value)).toThrow();
  });
  it('accepts zero cost but rejects unsupported precision and overflow', () => {
    expect(parseCost('0', 'CNY')).toBe(0);
    expect(() => parseCost('1.001', 'CNY')).toThrow();
    expect(() => parseCost('1.5', 'KRW')).toThrow();
    expect(() => parseCost('-1', 'USD')).toThrow();
    expect(
      positionSchema.safeParse({ ...position, costMinor: 1.2 }).success,
    ).toBe(false);
    expect(
      positionSchema.safeParse({ ...position, units: '1000001' }).success,
    ).toBe(false);
    expect(
      positionSchema.safeParse({ ...position, costMinor: 1000000000001 })
        .success,
    ).toBe(false);
  });
  it('separates currencies, leaves unknown valuations incomplete and handles zero cost', () => {
    const groups = summarizePositions(
      [
        position,
        { ...position, code: 'DEMO-US', currency: 'USD', costMinor: 0 },
        { ...position, code: 'DEMO-BOND' },
      ],
      data.funds,
    );
    expect(groups).toHaveLength(2);
    expect(groups[0]).toMatchObject({
      currency: 'CNY',
      cost: 2000000,
      value: null,
      knownValue: 1042000,
      profit: null,
      missing: 1,
      changePct: null,
    });
    expect(groups[1]).toMatchObject({
      currency: 'USD',
      cost: 0,
      value: 964000,
      profit: 964000,
      changePct: null,
    });
    expect(valuePosition(position)).toBeNull();
    expect(
      valuePosition(position, { ...data.funds[0], currency: 'USD' }),
    ).toBeNull();
    expect(summarizePositions([], data.funds)).toEqual([]);
  });
});
