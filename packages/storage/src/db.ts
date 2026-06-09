import Database from 'better-sqlite3'
import { drizzle } from 'drizzle-orm/better-sqlite3'
import { schema } from './schema.js'

export type SoraDb = ReturnType<typeof drizzle<typeof schema>>

export function openDb(dbPath: string): { sqlite: Database.Database; db: SoraDb } {
  const sqlite = new Database(dbPath)
  sqlite.pragma('journal_mode = WAL')
  sqlite.pragma('foreign_keys = ON')
  const db = drizzle(sqlite, { schema })
  return { sqlite, db }
}

export function closeDb(sqlite: Database.Database): void {
  sqlite.close()
}
