# Sora Development Plan Overview

## Product Direction

Sora is now a Thesis-first global index fund research system.

It has two first-class interaction surfaces:

```text
CLI
Web
```

Both surfaces use the same underlying domain services. Agent capability is decoupled from presentation.

## Thesis Engine Foundation

Thesis Engine is the reusable Agentic Finance primitive.

Sora is the index-fund research application built on top of it:

```text
Sora = Thesis Engine applied to global index fund research
```

This means `packages/thesis` should stay as platform-neutral as possible. Sora-specific concerns such as global market mapping, domestic fund exposure, QDII execution risk, and NotificationEvent exports should live in the application layer around the Thesis Engine.

## New Core Loop

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
```

The old loop is still useful:

```text
Market -> Index -> Fund Mapping -> Fund Analysis -> Research Card -> NotificationEvent
```

But it is now a supporting layer under Thesis.

## Architecture

```text
Presentation
  apps/cli
  apps/web
  apps/api

Sora Application Layer
  packages/market
  packages/fund
  packages/research
  packages/notifier

Agentic Finance Primitive
  packages/core
  packages/thesis
  packages/agent

Infrastructure
  packages/storage
  packages/sources
  packages/shared
```

`packages/thesis` owns reusable thesis lifecycle, evidence, confidence, contradiction, exposure, and review-loop logic.

Sora-specific services compose Thesis Engine with market, fund, research, notifier, storage, and source adapters.

`packages/agent` is called by services when summarization, evidence classification, causal-chain explanation, contradiction review, or question answering is needed.

## Package Plan

Existing packages remain:

- `packages/core`
- `packages/market`
- `packages/fund`
- `packages/research`
- `packages/agent`
- `packages/sources`
- `packages/storage`
- `packages/notifier`
- `packages/shared`

New package:

- `packages/thesis`

New app:

- `apps/web`

`apps/api` becomes the Web-facing API instead of only a placeholder.

## Phase Table

| Phase | Name | Goal | Dependency |
|------|------|------|------------|
| 0 | Stabilize Existing Rewrite | Fix lint, DB seed idempotency, market refresh failure handling, README baseline | current repo |
| 1 | Thesis Primitive Model | Add platform-neutral Thesis / Evidence / Update / Exposure schemas and seed data | P0 |
| 2 | Thesis Engine Package | Implement reusable confidence, contradiction, review, and exposure primitives | P1 |
| 3 | Storage Expansion | Add thesis tables and storage queries | P1 |
| 4 | CLI Thesis MVP | Add thesis and evidence commands | P2 + P3 |
| 5 | Research + Notifier Integration | Generate ResearchCard and NotificationEvent from Thesis context | P4 |
| 6 | API Layer | Expose Thesis services to Web | P2 + P3 |
| 7 | Web MVP | Build Thesis-first website | P6 |
| 8 | Agent Decoupling | Standardize Agent services used by CLI and Web | P2 + P6 |
| 9 | Testing + Docs | E2E tests and full docs | P4-P8 |

## Acceptance Criteria

Functional:

- `pnpm sora thesis list` works.
- `pnpm sora thesis show ai-infra` works.
- `pnpm sora thesis review` works.
- `pnpm sora thesis exposure ai-infra` works.
- `pnpm sora evidence add` creates evidence and updates confidence.
- `pnpm sora research create --thesis ai-infra` works.
- `pnpm sora notifications export` includes Thesis-derived events.
- Web app renders Thesis overview.
- Web app renders Thesis detail, timeline, contradictions, and asset exposure.
- API endpoints return schema-valid Thesis data.

Architecture:

- CLI and Web share services.
- Agent is not coupled to CLI or Web.
- `packages/thesis` remains platform-neutral where possible.
- Sora-specific market / fund / notification logic stays outside the Thesis Engine primitive.
- presentation layers do not duplicate confidence or exposure logic.
- deterministic fallback works without Pi.

Compliance:

- no buy / sell recommendations.
- no personalized allocation advice.
- all outputs use information-analysis and risk-warning language.
- every CLI command ends with disclaimer.
- every Web research view includes non-advisory framing.

Quality:

- `pnpm install` succeeds.
- `pnpm test` succeeds.
- `pnpm build` succeeds.
- `pnpm lint` succeeds.
- seed data validates against schemas.
- all confidence changes have evidence records.
- data refresh commands do not report false success.
