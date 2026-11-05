import { normalizeEmail } from './security.ts';
import type { SeedStatement } from './seed.ts';

export function bootstrapStatements(
  email: string,
  passwordHash: string,
  at: string,
): SeedStatement[] {
  const normalized = normalizeEmail(email);
  if (!normalized) throw new Error('邮箱格式无效');
  const id = crypto.randomUUID();
  return [
    {
      sql: `INSERT INTO users (id, email, password_hash, display_name, role, created_at, updated_at)
      SELECT ?, ?, ?, '管理员', 'admin', ?, ?
      WHERE NOT EXISTS (SELECT 1 FROM users WHERE role = 'admin')
      AND NOT EXISTS (SELECT 1 FROM users WHERE email = ?)`,
      params: [id, normalized, passwordHash, at, at, normalized],
    },
    {
      sql: `INSERT INTO audit_log (id, actor_id, action, target, created_at)
      SELECT ?, id, 'admin.bootstrap', id, ? FROM users WHERE id = ?`,
      params: [crypto.randomUUID(), at, id],
    },
    { sql: 'SELECT id FROM users WHERE id = ?', params: [id] },
  ];
}
