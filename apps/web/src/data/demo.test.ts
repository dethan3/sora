import { describe, expect, it, vi } from 'vitest';
import fixture from '../test/fixtures/demo.json';
import { demoSchema, loadDemo, portfolioSummary } from './demo';

describe('static demo contract', () => {
  it('accepts the shipped fixture and totals the portfolio', () => {
    const data = demoSchema.parse(fixture);
    expect(portfolioSummary(data.positions)).toEqual({
      cost: 15000,
      value: 15240,
      profit: 240,
      changePct: 1.6,
    });
    expect(portfolioSummary([])).toEqual({
      cost: 0,
      value: 0,
      profit: 0,
      changePct: 0,
    });
  });
  it('rejects invalid amounts, versions, duplicate codes and missing references', () => {
    expect(demoSchema.safeParse({ ...fixture, schemaVersion: 2 }).success).toBe(
      false,
    );
    expect(
      demoSchema.safeParse({
        ...fixture,
        positions: [{ code: 'missing', cost: -1, value: 0 }],
      }).success,
    ).toBe(false);
    expect(
      demoSchema.safeParse({ ...fixture, watchlist: ['missing'] }).success,
    ).toBe(false);
    expect(
      demoSchema.safeParse({
        ...fixture,
        funds: [...fixture.funds, fixture.funds[0]],
      }).success,
    ).toBe(false);
  });
  it('fails visibly for HTTP errors or malformed JSON', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
      new Response('', { status: 503 }),
    );
    await expect(loadDemo()).rejects.toThrow('数据读取失败');
    vi.mocked(fetch).mockResolvedValueOnce(new Response('not json'));
    await expect(loadDemo()).rejects.toThrow();
  });
});
