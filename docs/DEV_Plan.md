# Sora Development Plan

This plan supersedes the old CLI-only roadmap.

The product direction has changed:

- Sora has two first-class interaction surfaces: CLI and Web.
- Thesis is the primary product model.
- Agent capability is a reusable service layer, decoupled from presentation.
- Existing market / fund / research / notification packages remain useful, but they now support Thesis workflows.

## Current Baseline

The repository already contains a TypeScript monorepo with:

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

Verified baseline:

- `pnpm test` passes.
- `pnpm build` passes.
- CLI core commands run.

Known gaps before moving forward:

- `pnpm lint` currently has unused-variable errors.
- README is still incomplete.
- `data refresh --type market` can report success even when Yahoo requests fail.
- `db seed` is not idempotent on an existing database.
- Pi and Tavily real-key flows still need end-to-end verification.

## New Architecture Direction

```text
                 +----------------+
                 | packages/agent |
                 +----------------+
                         ^
                         |
apps/cli ----+           |
             |           |
apps/web ----+--> Application Services
             |           |
apps/api ----+           v
                 +------------------+
                 | packages/thesis  |
                 +------------------+
                         |
     +-------------------+-------------------+
     |                   |                   |
packages/market   packages/fund      packages/research
     |                   |                   |
packages/sources  packages/storage   packages/notifier
```

Presentation layers must not own domain logic. CLI, Web, API, and Worker call shared services.

## Phase Overview

| Phase | Name | Goal |
|------|------|------|
| 0 | Stabilize Existing Rewrite | Fix lint, seed idempotency, data refresh failures, README baseline |
| 1 | Thesis Core Model | Add Thesis / Evidence / Update / Exposure schemas and seed data |
| 2 | Thesis Package | Implement lifecycle, confidence update, contradiction and exposure logic |
| 3 | Storage Expansion | Add thesis tables and queries |
| 4 | CLI Thesis MVP | Add thesis / evidence commands |
| 5 | Research + Notifier Integration | Generate ResearchCard and NotificationEvent from Thesis context |
| 6 | API Layer | Add HTTP endpoints for Web and external consumers |
| 7 | Web MVP | Build Thesis-first website |
| 8 | Agent Decoupling | Standardize Agent service interfaces used by CLI and Web |
| 9 | Testing + Docs | Add e2e coverage and update docs |

## Phase 0 - Stabilize Existing Rewrite

Goal: make the current TypeScript codebase a reliable base.

Tasks:

- Fix all `pnpm lint` errors.
- Make `db seed` idempotent or provide explicit `--reset` / `--force`.
- Fix `data refresh --type market` so all-upstream-failure exits non-zero or reports failure.
- Add clear Yahoo rate-limit handling.
- Complete README baseline for current functionality.
- Keep existing CLI behavior working.

Done when:

- `pnpm lint` passes.
- `pnpm test` passes.
- `pnpm build` passes.
- `pnpm sora db seed` is safe to run repeatedly or explicitly documents destructive behavior.
- Market refresh does not print false success.

## Phase 1 - Thesis Core Model

Goal: add Thesis as a first-class domain model in `packages/core`.

Add schemas:

- `Thesis`
- `ThesisEvidence`
- `ThesisUpdate`
- `AssetExposure`
- `EvidenceDirection`
- `EvidenceStrength`
- `ThesisStatus`
- `TimeHorizon`

Seed data:

- `data/seeds/theses.json`
- `data/seeds/thesis-evidence.json`
- `data/seeds/asset-exposures.json`

Initial seed theses:

- `ai-infra`: AI Infrastructure Supercycle
- `china-recovery`: China Asset Valuation Recovery
- `gold-allocation`: Gold Long-term Allocation Value
- `us-tech-valuation`: US Tech Valuation Risk
- `usd-liquidity`: USD Liquidity Cycle

Done when:

- schemas validate valid and invalid cases.
- seed data passes schema validation.
- existing packages still build.

## Phase 2 - Thesis Package

Goal: implement reusable Thesis domain logic.

Package:

```text
packages/thesis
```

APIs:

```ts
listTheses(): Promise<Thesis[]>
getThesis(id: string): Promise<Thesis | null>
addEvidence(input: AddEvidenceInput): Promise<ThesisUpdate>
reviewTheses(): Promise<ThesisReview>
getEvidenceTimeline(thesisId: string): Promise<ThesisEvidence[]>
getContradictions(thesisId: string): Promise<ContradictionSummary>
getAssetExposure(thesisId: string): Promise<AssetExposure[]>
```

Deterministic rules:

- strong support: `+8`
- medium support: `+4`
- weak support: `+2`
- neutral: `0`
- weak against: `-2`
- medium against: `-4`
- strong against: `-8`

Confidence must stay within `0..100`.

Done when:

- confidence update rules are covered by tests.
- every update records previous and new confidence.
- support and against evidence are both available.
- exposure sorting works by score.

## Phase 3 - Storage Expansion

Goal: persist Thesis data.

Add tables:

- `theses`
- `thesis_evidence`
- `thesis_updates`
- `asset_exposures`

Queries:

- list / get thesis
- insert evidence
- insert update
- list timeline
- list exposures
- review changed theses

Done when:

- `db init` creates new tables.
- `db seed` imports thesis seed data.
- in-memory storage tests cover the new queries.

## Phase 4 - CLI Thesis MVP

Goal: make Thesis usable from CLI.

Commands:

```bash
pnpm sora thesis list
pnpm sora thesis show <id>
pnpm sora thesis review
pnpm sora thesis exposure <id>
pnpm sora evidence add --thesis <id> --direction support --strength medium --title "..."
```

Output requirements:

- show confidence and trend.
- show latest evidence.
- show support and against evidence separately.
- show affected markets / indexes / funds.
- end with compliance disclaimer.

Done when:

- all commands work with seed data.
- commands work after DB seed.
- CLI output is readable and non-advisory.

## Phase 5 - Research + Notifier Integration

Goal: connect Thesis to existing research and notification layers.

Research changes:

- `research create --thesis <id>`
- ResearchCard includes thesis id, evidence summary, contradiction summary, exposure summary.
- deterministic generator can produce a Thesis-based card without Agent.

Notifier changes:

- confidence crossing thresholds can create NotificationEvent.
- strong contrary evidence can create warning/watch events.
- invalidated Thesis can create critical-style risk event, still non-advisory.

Done when:

- `pnpm sora research create --thesis ai-infra` works.
- `pnpm sora notifications export` includes Thesis-derived events.
- tests cover Thesis-to-card and Thesis-to-event flows.

## Phase 6 - API Layer

Goal: expose shared services to Web.

Endpoints:

```text
GET  /api/theses
GET  /api/theses/:id
GET  /api/theses/:id/evidence
GET  /api/theses/:id/exposure
POST /api/theses/:id/evidence
GET  /api/review
POST /api/research/thesis/:id
GET  /api/notifications
```

Rules:

- API calls shared services.
- API must not duplicate domain logic.
- responses use core schemas.

Done when:

- API starts locally.
- endpoint tests pass.
- Web can consume API without direct DB access.

## Phase 7 - Web MVP

Goal: build the Thesis-first website.

App:

```text
apps/web
```

Views:

- `/` Market Cognition overview
- `/theses` Thesis list
- `/theses/:id` Thesis card
- `/theses/:id/evidence` Evidence timeline
- `/theses/:id/exposure` Asset exposure
- `/agent` Agent console

UX requirements:

- first screen shows changed theses, strongest theses, challenged theses, recent evidence.
- Thesis detail shows causal chain, assumptions, confidence, support, against, exposure.
- asset exposure is easy to scan.
- no marketing landing page.

Done when:

- local Web server runs.
- desktop and mobile layouts are usable.
- data comes from API/services.
- no investment advice language appears.

## Phase 8 - Agent Decoupling

Goal: make Agent reusable across CLI and Web.

Agent service capabilities:

- classify evidence direction and strength.
- summarize evidence timeline.
- explain confidence changes.
- identify contradictions.
- answer Thesis-context questions.
- generate follow-up research tasks.

Rules:

- Agent result must be schema-validated.
- deterministic fallback remains available.
- confidence changes require evidence records.
- Agent does not own presentation formatting.

Done when:

- CLI and Web can both call the same Agent service.
- Pi unavailable path still works.
- tests cover schema failure fallback.

## Phase 9 - Testing + Docs

Goal: finish product-quality validation.

Tests:

- unit tests for Thesis schemas and update rules.
- storage tests for thesis tables.
- CLI command tests.
- API endpoint tests.
- Web smoke tests.
- end-to-end Thesis flow:

```text
seed thesis -> add evidence -> confidence update -> research card -> notification -> Web render
```

Docs:

- README
- architecture
- research card
- signal templates
- data sources
- Thesis model
- Agent architecture
- Web roadmap

Done when:

- all acceptance criteria in PRD pass.

