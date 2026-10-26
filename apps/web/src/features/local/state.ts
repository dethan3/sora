import { z } from 'zod';
import { positionSchema } from '../portfolio/model';

export const STORAGE_KEY = 'sora.personal.v1';
export const MAX_IMPORT_BYTES = 1024 * 1024;
export const personalSchema = z
  .object({
    schemaVersion: z.literal(1),
    watchlist: z.array(z.string().min(1).max(50)).max(200),
    positions: z.array(positionSchema).max(200),
  })
  .strict()
  .superRefine((data, ctx) => {
    if (
      new Set(data.watchlist).size !== data.watchlist.length ||
      new Set(data.positions.map((p) => p.code)).size !== data.positions.length
    ) {
      ctx.addIssue({ code: 'custom', message: '关注或持仓中存在重复代码' });
    }
  });
export type PersonalState = z.infer<typeof personalSchema>;
export const emptyPersonal = (): PersonalState => ({
  schemaVersion: 1,
  watchlist: [],
  positions: [],
});
export type Snapshot = {
  state: PersonalState;
  raw: string | null;
  issue: 'corrupt' | 'unavailable' | null;
};
export function parsePersonal(text: string): PersonalState {
  if (text.length > MAX_IMPORT_BYTES)
    throw new Error('文件过大，请选择 1 MB 以内的 Sora JSON 备份');
  try {
    return personalSchema.parse(JSON.parse(text));
  } catch {
    throw new Error('备份格式或版本不支持，请选择 Sora 导出的 JSON 文件');
  }
}
export function readPersonal(storage: Pick<Storage, 'getItem'>): Snapshot {
  let raw: string | null;
  try {
    raw = storage.getItem(STORAGE_KEY);
  } catch {
    return { state: emptyPersonal(), raw: null, issue: 'unavailable' };
  }
  if (raw === null) return { state: emptyPersonal(), raw, issue: null };
  try {
    return { state: parsePersonal(raw), raw, issue: null };
  } catch {
    return { state: emptyPersonal(), raw, issue: 'corrupt' };
  }
}
export function writePersonal(
  storage: Pick<Storage, 'getItem' | 'setItem'>,
  expected: string | null,
  state: PersonalState,
): string {
  if (storage.getItem(STORAGE_KEY) !== expected)
    throw new Error('另一个页面已更新数据，请刷新后重试，避免覆盖修改');
  const raw = JSON.stringify(personalSchema.parse(state));
  storage.setItem(STORAGE_KEY, raw);
  return raw;
}
