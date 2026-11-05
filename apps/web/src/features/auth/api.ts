import { z } from 'zod';

export const userSchema = z.object({
  id: z.string(),
  email: z.email(),
  displayName: z.string().nullable(),
  role: z.enum(['user', 'admin']),
  baseCurrency: z.string(),
});
export type AuthUser = z.infer<typeof userSchema>;
export const identitySchema = z.object({ user: userSchema.nullable() });
export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function authRequest<T>(
  path: string,
  schema: z.ZodType<T>,
  body?: unknown,
  signal?: AbortSignal,
  method?: 'POST' | 'PATCH',
) {
  const response = await fetch(path, {
    method: method ?? (body === undefined ? 'GET' : 'POST'),
    credentials: 'same-origin',
    cache: 'no-store',
    headers:
      body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.any([
      ...(signal ? [signal] : []),
      AbortSignal.timeout(15000),
    ]),
  });
  let data: unknown;
  try {
    data = await response.json();
  } catch {
    throw new Error('账号服务暂不可用，请稍后重试');
  }
  if (!response.ok) {
    const error = z.object({ error: z.string() }).safeParse(data);
    throw new ApiError(
      response.status,
      error.success ? error.data.error : '账号服务暂不可用，请稍后重试',
    );
  }
  const parsed = schema.safeParse(data);
  if (!parsed.success) throw new Error('账号服务返回异常，请稍后重试');
  return parsed.data;
}
