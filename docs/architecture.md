# Architecture

## Core Boundary

```text
Sora MVP = Thesis-first workflow implemented inside Sora services
Thesis Engine = long-term reusable product extracted later
```

The fastest launch path does not require a standalone Thesis Engine package. The architecture keeps the workflow simple now and extractable later.

## MVP Layers

```text
Presentation
  apps/cli
  apps/web
  apps/api

Sora Application Services
  packages/market
  packages/fund
  packages/research
  packages/notifier
  packages/storage
  packages/sources
  packages/agent
  packages/shared

Shared Types
  packages/core

Future Product
  Thesis Engine
```

## Package Responsibilities

`packages/core`

- Zod schemas
- shared TypeScript types
- Thesis MVP data types
- no service logic

`packages/storage`

- SQLite / Drizzle persistence
- migrations and seed
- thesis / evidence / update / exposure queries
- market / index / fund / research / notification queries

`packages/research`

- Sora Thesis workflow service if that is the fastest MVP path
- ResearchCard generation from Thesis context
- deterministic generation
- optional Agent-assisted narrative generation

`packages/market`

- Sora market and index lookup
- quote retrieval through source adapters
- market/index mapping helpers

`packages/fund`

- domestic fund mapping
- execution quality scoring
- premium, liquidity, tracking, purchase-status risk analysis

`packages/notifier`

- NotificationEvent generation and export
- thesis-derived event generation

`packages/agent`

- Pi or other Agent adapters
- optional evidence classification assistance
- optional summarization
- optional contradiction explanation
- schema-validated outputs

`packages/sources`

- Yahoo
- EastMoney
- Tavily
- seed source

`packages/shared`

- cache utilities
- JSONP parsing
- common helpers

## MVP Dependency Rules

Allowed:

```text
apps/* -> Sora application services
Sora application services -> core, storage, sources, agent, market, fund, research, notifier
storage -> core
sources -> core, shared
agent -> core
```

Forbidden:

```text
apps/web -> direct DB logic
apps/cli -> duplicated confidence update logic
apps/api -> duplicated confidence update logic
agent -> silent confidence changes
agent -> compliance bypass
```

## Runtime Flow

Thesis review:

```text
CLI/Web
-> API or Sora application service
-> storage queries
-> deterministic review summary
-> response rendered by presentation layer
```

Evidence add:

```text
CLI/Web/API
-> validate evidence input
-> deterministic confidence delta
-> storage writes evidence and update
-> notifier may generate event
-> response includes previous and new confidence
```

Research card:

```text
Thesis context
-> market / index / fund exposure summary
-> deterministic research generator
-> optional Agent narrative
-> schema validation
-> compliance filter
-> storage
```

## Future Extraction

After launch, stable Thesis workflow logic can move into:

```text
packages/thesis
```

Extraction should happen only when it reduces complexity and Sora behavior is already proven.

## Compliance Architecture

Compliance is deterministic and service-level.

All user-facing output must avoid:

- buy / sell instructions
- personalized allocation advice
- guaranteed return language
- automatic trading language

All confidence changes require evidence records.

Agent output must pass schema validation and compliance filtering before display.

