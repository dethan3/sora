import { z } from 'zod';
export { z } from 'zod';
export const currencySchema = z.enum(['CNY', 'USD', 'KRW', 'HKD']);
export const positionSchema = z
  .object({
    code: z.string().min(1).max(50),
    units: z
      .string()
      .trim()
      .refine((value) => {
        const raw = value.trim();
        if (!/^\d{1,10}(?:\.\d{1,4})?$/.test(raw)) return false;
        const [whole, fraction = ''] = raw.split('.');
        const scaled = BigInt(whole) * 10000n + BigInt(fraction.padEnd(4, '0'));
        return scaled > 0n && scaled <= 10000000000n;
      }, '持仓份额无效'),
    costMinor: z.number().int().min(0).max(1000000000000),
    currency: currencySchema,
  })
  .strict();
export type Position = z.infer<typeof positionSchema>;
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
      new Set(data.positions.map((row) => row.code)).size !==
        data.positions.length
    )
      ctx.addIssue({ code: 'custom', message: '关注或持仓中存在重复代码' });
  });
export type PersonalState = z.infer<typeof personalSchema>;
export const emptyPersonal = (): PersonalState => ({
  schemaVersion: 1,
  watchlist: [],
  positions: [],
});
export const personalEnvelopeSchema = z.object({
  ownerId: z.string(),
  revision: z.number().int().nonnegative(),
  state: personalSchema,
  replayed: z.boolean().optional(),
});
export type PersonalEnvelope = z.infer<typeof personalEnvelopeSchema>;
export const mutationBase = {
  ownerId: z.string().min(1),
  revision: z.number().int().nonnegative(),
  operationId: z.uuid(),
};
export const replacePersonalSchema = z
  .object({ ...mutationBase, state: personalSchema })
  .strict();
export const importChoicesSchema = z
  .record(z.string().min(1).max(50), z.enum(['local', 'cloud']))
  .refine((value) => Object.keys(value).length <= 200);
export const importPersonalSchema = z
  .object({
    ...mutationBase,
    localState: personalSchema,
    choices: importChoicesSchema,
  })
  .strict();
export const importPreviewSchema = z.object({
  ownerId: z.string(),
  revision: z.number().int().nonnegative(),
  current: personalSchema,
  eligible: personalSchema,
  unavailable: z.array(
    z.object({
      code: z.string(),
      part: z.enum(['watchlist', 'position']),
      reason: z.string(),
    }),
  ),
  conflicts: z.array(
    z.object({
      code: z.string(),
      local: positionSchema,
      cloud: positionSchema,
    }),
  ),
});
export type ImportPreview = z.infer<typeof importPreviewSchema>;
export function mergePersonal(
  cloud: PersonalState,
  local: PersonalState,
  choices: z.infer<typeof importChoicesSchema>,
): PersonalState {
  const positions = new Map(cloud.positions.map((row) => [row.code, row]));
  for (const row of local.positions) {
    const old = positions.get(row.code);
    if (old && JSON.stringify(old) !== JSON.stringify(row)) {
      const choice = choices[row.code];
      if (!choice) throw new Error(`请选择 ${row.code} 的持仓版本`);
      if (choice === 'cloud') continue;
    }
    positions.set(row.code, row);
  }
  return personalSchema.parse({
    schemaVersion: 1,
    watchlist: [...new Set([...cloud.watchlist, ...local.watchlist])],
    positions: [...positions.values()],
  });
}
