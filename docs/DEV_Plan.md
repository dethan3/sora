# Sora Development Plan

This plan optimizes for fast launch.

## 1. Strategic Decision

Thesis Engine is the long-term product. Sora is the application we need to ship first.

For MVP, Thesis Engine does not need to exist as an independent package. It can first exist as:

- product method
- workflow protocol
- data model
- compliance boundary
- future extraction target

Sora MVP implements Thesis-first behavior inside Sora application services. After the workflow is validated, stable patterns can be extracted into Thesis Engine.

```text
Now:     Sora implements Thesis-first workflow.
Later:   Thesis Engine is extracted and productized.
```

## 2. Current Baseline

The repo already has a useful TypeScript rewrite:

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

Existing useful capabilities:

- seed-driven market, index, fund, and mapping data
- source adapters for Yahoo, EastMoney, Tavily, and seed data
- fund execution quality scoring
- deterministic research card generation
- notification event generation and export
- SQLite storage for existing entities
- CLI structure for existing workflows

## 3. Fastest Launch Scope

MVP must include:

- Thesis schemas in `packages/core`
- Thesis seed data
- thesis / evidence / update / exposure storage tables
- deterministic confidence update rules
- CLI thesis and evidence commands
- API thesis endpoints
- Web overview and thesis detail
- ResearchCard generation from Thesis context
- NotificationEvent generation from Thesis updates
- non-advisory compliance framing

MVP does not require:

- standalone `packages/thesis`
- generic Thesis Engine SDK
- generalized multi-domain engine
- advanced Agent automation
- brokerage integration
- personalized allocation advice
- real-time streaming

## 4. MVP Architecture

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

During MVP, Thesis workflow code can live in pragmatic Sora service modules. The important part is to keep the code simple and deterministic so it can be extracted later.

## 5. Phase Overview

| Phase | Name | Goal |
|------|------|------|
| 0 | Stabilize Current Rewrite | Fix lint, seed idempotency, data refresh failures, README baseline |
| 1 | MVP Thesis Data + Storage | Add schemas, seeds, tables, and minimum storage queries |
| 2 | Sora Thesis Workflow | Implement evidence add, confidence update, review, contradiction, exposure |
| 3 | CLI + API | Expose the workflow through CLI and API |
| 4 | Research + Notifier | Generate Thesis-based cards and events |
| 5 | Web MVP | Build first usable Thesis-first website |
| 6 | Launch Hardening | Test, document, and remove launch blockers |
| 7 | Post-MVP Thesis Engine | Extract reusable engine after workflow stabilizes |
| 8 | Post-MVP Agent | Deepen Agent-assisted workflows |
| 9 | Post-MVP Roadmap | Longer-term product expansion |

Detailed phase plans live in `docs/dev-plan/`.

## 6. Launch Acceptance

Sora can launch when:

- `pnpm install` succeeds.
- `pnpm test` succeeds.
- `pnpm build` succeeds.
- `pnpm lint` succeeds.
- `pnpm sora db init` works.
- `pnpm sora db seed` is idempotent or explicitly reset-based.
- `pnpm sora thesis list` works.
- `pnpm sora thesis show ai-infra` works.
- `pnpm sora thesis review` works.
- `pnpm sora thesis exposure ai-infra` works.
- `pnpm sora evidence add` creates evidence and confidence update records.
- `pnpm sora research create --thesis ai-infra` works without Pi.
- `pnpm sora notifications export` includes Thesis-derived events.
- API serves Thesis overview/detail/evidence/exposure data.
- Web renders overview, thesis detail, evidence timeline, and exposure.
- all user-facing output is non-advisory.

## 7. Compliance Boundary

Sora is not an investment adviser.

Forbidden:

- buy / sell instructions
- personalized position sizing
- guaranteed return language
- automatic trading
- live copy trading

Allowed:

- market information
- index information
- domestic fund mapping
- execution quality analysis
- premium / purchase-status risk alerts
- thesis evidence summaries
- confidence history
- contradiction analysis
- research cards
- notification events
- follow-up research tasks

Every user-facing surface must use information-analysis and risk-warning language.

