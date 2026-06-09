# Sora Quick Launch Plan Overview

## Launch Principle

Ship Sora first. Productize Thesis Engine later.

For the fastest MVP, Thesis Engine is not required to be an independent code package. It can first exist as:

- product method
- workflow protocol
- data model
- compliance boundary
- documented future engine contract

Sora MVP still implements Thesis-first behavior, but inside Sora application services. Once the workflow is validated with real use, the reusable Thesis Engine can be extracted as a long-term product.

```text
MVP:
Sora implements Thesis-first workflows.

Post-MVP:
Extract stable patterns into Thesis Engine.
```

## Current Repo State

The repository already contains a half-built TypeScript rewrite:

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

Useful existing work:

- seed-driven market / index / fund data
- source adapters
- fund execution quality scoring
- deterministic research cards
- notification export
- SQLite storage
- CLI command structure

The quickest path is to add Thesis workflow on top of this existing structure rather than pause to build a generic engine abstraction.

## MVP Architecture

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

Documented Future Product
  Thesis Engine
```

During MVP, Thesis workflow can live in Sora services such as `packages/research`, `packages/storage`, or a Sora-specific service module. A standalone `packages/thesis` is optional and should not block launch.

## MVP Product Loop

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
  -> Web / CLI review
```

## Phase Table

| Phase | Plan | Goal | Launch Critical |
|------|------|------|-----------------|
| 0 | [Stabilize Current Rewrite](./01-phase0-stabilize.md) | Make the current TS base green | Yes |
| 1 | [MVP Thesis Data + Storage](./02-phase1-thesis-model.md) | Add Thesis schemas, seed data, and storage tables | Yes |
| 2 | [Sora Thesis Workflow](./03-phase2-sora-thesis-workflow.md) | Implement evidence add, confidence update, review, exposure queries | Yes |
| 3 | [CLI + API](./04-phase3-cli-api.md) | Expose the workflow through CLI and API | Yes |
| 4 | [Research + Notifier](./05-phase4-research-notifier.md) | Generate thesis research cards and events | Yes |
| 5 | [Web MVP](./06-phase5-web-mvp.md) | Build the first usable Thesis-first website | Yes |
| 6 | [Launch Hardening](./07-phase6-launch-hardening.md) | Test, document, and remove launch blockers | Yes |
| 7 | [Post-MVP Thesis Engine](./08-post-mvp-thesis-engine.md) | Extract reusable engine after workflow stabilizes | No |
| 8 | [Post-MVP Agent](./09-post-mvp-agent.md) | Deepen Agent-assisted workflows | No |
| 9 | [Post-MVP Roadmap](./10-post-mvp-roadmap.md) | Longer-term product expansion | No |

## MVP Acceptance

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
- `pnpm sora research create --thesis ai-infra` works with deterministic fallback.
- `pnpm sora notifications export` includes Thesis-derived events.
- API serves Thesis overview/detail/evidence/exposure data.
- Web renders overview, thesis detail, evidence timeline, and exposure.
- all user-facing output is non-advisory.

Not required for launch:

- standalone `packages/thesis`
- generic Thesis Engine SDK
- fully generalized multi-domain engine
- advanced Agent automation
- real-time streaming
- brokerage integration

