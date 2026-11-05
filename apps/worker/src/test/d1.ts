import { DatabaseSync } from 'node:sqlite';
import { readFileSync, readdirSync } from 'node:fs';
import type { D1Database, D1PreparedStatement, D1Result } from '../types.ts';

// In-memory D1 stand-in backed by node:sqlite, for unit tests only.
export function memoryD1(): D1Database {
  const sqlite = new DatabaseSync(':memory:');
  sqlite.exec('PRAGMA foreign_keys = ON');
  const statement = (query: string, values: unknown[] = []) => {
    const self: D1PreparedStatement & { _run(): D1Result } = {
      bind: (...next) => statement(query, next),
      async first<T>() {
        return (sqlite.prepare(query).get(...(values as never[])) ??
          null) as T | null;
      },
      async all<T>() {
        return {
          results: sqlite.prepare(query).all(...(values as never[])) as T[],
          success: true,
        };
      },
      async run() {
        return self._run();
      },
      _run() {
        const prepared = sqlite.prepare(query);
        if (prepared.columns().length) {
          const results = prepared.all(...(values as never[]));
          return { results, success: true };
        }
        const result = prepared.run(...(values as never[]));
        return {
          results: [],
          success: true,
          meta: { changes: Number(result.changes) },
        };
      },
    };
    return self;
  };
  return {
    prepare: (query) => statement(query),
    async batch(statements) {
      sqlite.exec('BEGIN');
      try {
        const results = statements.map((s) =>
          (s as ReturnType<typeof statement>)._run(),
        );
        sqlite.exec('COMMIT');
        return results;
      } catch (error) {
        sqlite.exec('ROLLBACK');
        throw error;
      }
    },
    async exec(query) {
      sqlite.exec(query);
      return {};
    },
  };
}

export async function migratedD1() {
  const db = memoryD1();
  const migrations = new URL('../../migrations/', import.meta.url);
  for (const file of readdirSync(migrations)
    .filter((file) => file.endsWith('.sql'))
    .sort())
    await db.exec(readFileSync(new URL(file, migrations), 'utf8'));
  return db;
}
