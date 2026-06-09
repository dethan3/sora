# Sora

Sora is a Thesis-first global index fund research system for Chinese investors.

Sora 是一个面向中国投资者的 Thesis-first 全球指数基金研究系统，用于持续管理市场观点、连接全球指数与国内可买基金、追踪证据变化，并通过 CLI 和 Web 呈现结构化研究结果。

## Product Direction

Sora is the application we are shipping first. Thesis Engine is the long-term reusable product direction.

For the MVP, Thesis Engine is a documented method and workflow contract rather than a required standalone package. Sora will implement the Thesis-first workflow inside its application services first, then extract stable patterns into Thesis Engine after the workflow is validated.

```text
MVP:      Sora implements Thesis-first workflows
Post-MVP: Extract stable patterns into Thesis Engine
```

## Current Baseline

The current repo is a TypeScript pnpm monorepo with:

- `apps/cli`
- `apps/api`
- `apps/worker`
- `packages/core`
- `packages/market`
- `packages/fund`
- `packages/research`
- `packages/agent`
- `packages/sources`
- `packages/storage`
- `packages/notifier`
- `packages/shared`

Existing implementation supports market / index / fund seed data, source adapters, fund execution scoring, deterministic research cards, notification export, SQLite storage, and CLI commands.

## Setup

```bash
pnpm install
pnpm test
pnpm build
pnpm lint
```

## CLI

Run commands through:

```bash
pnpm sora <command>
```

Current baseline commands include:

```bash
pnpm sora db init
pnpm sora db seed
pnpm sora data refresh --type market
pnpm sora markets list
pnpm sora indexes list --market us-tech
pnpm sora funds map --index nasdaq-100
pnpm sora funds analyze --index nasdaq-100
pnpm sora research create --market us-tech
pnpm sora notifications export
```

For offline development and deterministic demos:

```bash
USE_SEED_DATA=true pnpm sora data refresh --type market
```

## Environment

Optional environment variables:

```text
SORA_DB_PATH=./data/sora.db
SORA_CACHE_DIR=./data/cache
SORA_SEEDS_DIR=./data/seeds
SORA_PI_API_KEY=
SORA_PI_BASE_URL=https://api.pi.ai/v1
TAVILY_API_KEY=
USE_SEED_DATA=true
```

## Compliance

Sora is not an investment adviser.

Sora may provide market information, index information, domestic fund mapping, execution quality analysis, evidence summaries, confidence history, contradiction analysis, research cards, and notification events.

Sora must not provide buy / sell instructions, personalized allocation advice, guaranteed return language, automatic trading, or live copy trading.

## Current Plan

The fast-launch plan is documented in:

- `docs/PRD.md`
- `docs/DEV_Plan.md`
- `docs/dev-plan/00-overview.md`
- `docs/thesis-engine.md`

