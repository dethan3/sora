#!/usr/bin/env node
import 'dotenv/config'
import { SEEDS_DIR, CACHE_DIR } from './utils/env.js'
import { Command } from 'commander'
import { makeDbCommand } from './commands/db.js'
import { makeDataCommand } from './commands/data.js'
import { makeMarketsCommand } from './commands/markets.js'
import { makeIndexesCommand } from './commands/indexes.js'
import { makeFundsCommand } from './commands/funds.js'
import { makeResearchCommand } from './commands/research.js'
import { makeAlertsCommand } from './commands/alerts.js'
import { makeNotificationsCommand } from './commands/notifications.js'

// Ensure source factories (createMarketSource / createFundSource etc.) use
// the resolved absolute paths regardless of the current working directory.
process.env.SORA_SEEDS_DIR = SEEDS_DIR
process.env.SORA_CACHE_DIR = CACHE_DIR

const program = new Command()

program
  .name('sora')
  .description('全球宽基指数基金研究工具')
  .version('0.1.0')

program.addCommand(makeDbCommand())
program.addCommand(makeDataCommand())
program.addCommand(makeMarketsCommand())
program.addCommand(makeIndexesCommand())
program.addCommand(makeFundsCommand())
program.addCommand(makeResearchCommand())
program.addCommand(makeAlertsCommand())
program.addCommand(makeNotificationsCommand())

// pnpm forwards a bare '--' as argv[2] when calling `pnpm sora -- <cmd>`.
// Strip it so Commander can correctly parse subcommand options.
const argv = process.argv[2] === '--' ? [...process.argv.slice(0, 2), ...process.argv.slice(3)] : process.argv
program.parse(argv)
