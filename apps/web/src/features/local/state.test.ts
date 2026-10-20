import { describe, expect, it } from 'vitest';
import {
  emptyPersonal,
  MAX_IMPORT_BYTES,
  parsePersonal,
  personalSchema,
  readPersonal,
  STORAGE_KEY,
  writePersonal,
} from './state';

describe('versioned local persistence', () => {
  it('starts empty without writing, and round-trips backups', () => {
    const storage = { getItem: () => null };
    expect(readPersonal(storage)).toEqual({
      state: emptyPersonal(),
      raw: null,
      issue: null,
    });
    expect(parsePersonal(JSON.stringify(emptyPersonal()))).toEqual(
      emptyPersonal(),
    );
  });
  it('preserves corrupt and future-version data for recovery', () => {
    for (const raw of [
      'not-json',
      '{"schemaVersion":99}',
      JSON.stringify({ ...emptyPersonal(), watchlist: ['x', 'x'] }),
    ]) {
      expect(readPersonal({ getItem: () => raw })).toEqual({
        state: emptyPersonal(),
        raw,
        issue: 'corrupt',
      });
    }
  });
  it('rejects duplicate positions, unsafe numbers, excess records, unknown fields and oversized input', () => {
    const position = { code: 'x', units: '1', costMinor: 0, currency: 'CNY' };
    expect(
      personalSchema.safeParse({
        ...emptyPersonal(),
        positions: [position, position],
      }).success,
    ).toBe(false);
    expect(
      personalSchema.safeParse({
        ...emptyPersonal(),
        positions: [{ ...position, costMinor: Infinity }],
      }).success,
    ).toBe(false);
    expect(
      personalSchema.safeParse({
        ...emptyPersonal(),
        watchlist: Array.from({ length: 201 }, (_, i) => `${i}`),
      }).success,
    ).toBe(false);
    expect(() =>
      parsePersonal(JSON.stringify({ ...emptyPersonal(), extra: true })),
    ).toThrow();
    expect(() => parsePersonal(' '.repeat(MAX_IMPORT_BYTES + 1))).toThrow(
      '文件过大',
    );
  });
  it('does not overwrite another tab and propagates storage write failures', () => {
    let raw: string | null = null;
    const storage = {
      getItem: () => raw,
      setItem: (key: string, value: string) => {
        expect(key).toBe(STORAGE_KEY);
        raw = value;
      },
    };
    const saved = writePersonal(storage, null, emptyPersonal());
    expect(raw).toBe(saved);
    expect(() => writePersonal(storage, null, emptyPersonal())).toThrow(
      '另一个页面',
    );
    expect(() =>
      writePersonal(
        {
          getItem: () => saved,
          setItem: () => {
            throw new Error('quota');
          },
        },
        saved,
        emptyPersonal(),
      ),
    ).toThrow('quota');
    expect(
      readPersonal({
        getItem: () => {
          throw new Error('denied');
        },
      }).issue,
    ).toBe('unavailable');
  });
});
