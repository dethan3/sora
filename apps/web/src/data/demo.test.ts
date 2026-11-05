import { afterEach, describe, expect, it, vi } from 'vitest';
import fixture from '../test/fixtures/demo.json';
import catalog from '../../public/data/catalog.json';
import {
  catalogUrl,
  demoSchema,
  loadCatalog,
  loadDemo,
  portfolioSummary,
} from './demo';

afterEach(() => vi.unstubAllEnvs());

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

describe('catalog source', () => {
  it('defaults to the static catalog and switches to the Worker API on opt-in', () => {
    expect(catalogUrl(undefined)).toBe('/data/catalog.json');
    expect(catalogUrl('api')).toBe('/api/catalog');
  });

  it('uses a valid API catalog without requesting the fallback', async () => {
    vi.stubEnv('VITE_CATALOG_SOURCE', 'api');
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(Response.json(catalog));
    expect(await loadCatalog()).toEqual({
      data: demoSchema.parse(catalog),
      origin: 'api',
    });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(
      '/api/catalog',
      expect.objectContaining({ cache: 'no-cache' }),
    );
  });

  it.each(['network', 'http', 'json', 'schema', 'timeout'])(
    'falls back to public reference data on an API %s failure',
    async (failure) => {
      vi.stubEnv('VITE_CATALOG_SOURCE', 'api');
      const request = vi.spyOn(globalThis, 'fetch');
      if (failure === 'network')
        request.mockRejectedValueOnce(new TypeError('offline'));
      else if (failure === 'timeout')
        request.mockRejectedValueOnce(
          new DOMException('Timed out', 'TimeoutError'),
        );
      else
        request.mockResolvedValueOnce(
          failure === 'http'
            ? new Response('', { status: 503 })
            : failure === 'json'
              ? new Response('invalid')
              : Response.json(fixture),
        );
      request.mockResolvedValueOnce(Response.json(catalog));
      const result = await loadCatalog();
      expect(result.origin).toBe('fallback');
      expect(result.data).toEqual(demoSchema.parse(catalog));
      expect(request.mock.calls.map(([url]) => url)).toEqual([
        '/api/catalog',
        '/data/catalog.json',
      ]);
    },
  );

  it('reports failure if both API and fallback fail validation', async () => {
    vi.stubEnv('VITE_CATALOG_SOURCE', 'api');
    vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response('', { status: 503 }))
      .mockResolvedValueOnce(Response.json(fixture));
    await expect(loadCatalog()).rejects.toThrow('公共目录来源无效');
  });

  it('does not fall back after the caller cancels a request', async () => {
    vi.stubEnv('VITE_CATALOG_SOURCE', 'api');
    const controller = new AbortController();
    const error = new DOMException('Aborted', 'AbortError');
    vi.spyOn(globalThis, 'fetch').mockImplementationOnce(async () => {
      controller.abort();
      throw error;
    });
    await expect(loadCatalog(controller.signal)).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
