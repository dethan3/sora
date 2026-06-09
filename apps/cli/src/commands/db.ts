import { Command } from 'commander'
import { dbInit, dbSeed, closeDb } from '@sora/storage'
import { SEEDS_DIR, DB_PATH } from '../utils/env.js'

export function makeDbCommand(): Command {
  const db = new Command('db').description('数据库管理')

  db.command('init')
    .description('初始化 SQLite 数据库，创建所有表')
    .action(() => {
      const { sqlite } = dbInit(DB_PATH)
      closeDb(sqlite)
      console.log(`✅ Database initialized at ${DB_PATH}`)
    })

  db.command('seed')
    .description('将 data/seeds/ 数据导入数据库')
    .action(() => {
      const { sqlite, db: soraDb } = dbInit(DB_PATH)
      const stats = dbSeed(soraDb, SEEDS_DIR)
      closeDb(sqlite)
      console.log('✅ Seed data imported:')
      console.log(`   markets:  ${stats.markets}`)
      console.log(`   indexes:  ${stats.indexes}`)
      console.log(`   funds:    ${stats.funds}`)
      console.log(`   mappings: ${stats.mappings}`)
      console.log(`   metrics:  ${stats.metrics}`)
      console.log(`   theses:   ${stats.theses}`)
      console.log(`   evidence: ${stats.thesisEvidence}`)
      console.log(`   updates:  ${stats.thesisUpdates}`)
      console.log(`   exposure: ${stats.assetExposures}`)
    })

  return db
}
