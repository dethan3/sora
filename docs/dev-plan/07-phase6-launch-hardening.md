# Phase 6 - Launch Hardening

## Goal

Remove blockers and ship the first Sora Thesis-first MVP.

## Required Checks

- `pnpm install`
- `pnpm test`
- `pnpm build`
- `pnpm lint`
- `pnpm sora db init`
- `pnpm sora db seed`
- `pnpm sora thesis list`
- `pnpm sora thesis show ai-infra`
- `pnpm sora thesis review`
- `pnpm sora thesis exposure ai-infra`
- `pnpm sora research create --thesis ai-infra`
- `pnpm sora notifications export`
- API smoke test
- Web desktop smoke test
- Web mobile smoke test

## Test Coverage

Launch-critical tests:

- core schemas
- idempotent seed
- confidence update rules
- evidence add workflow
- thesis review workflow
- research card generation
- notification generation
- API response shape
- Web smoke tests
- compliance filter

## Documentation

Update:

- `README.md`
- `docs/architecture.md`
- `docs/DEV_Plan.md`
- `docs/PRD.md`
- `docs/web-roadmap.md`
- `docs/research-card.md`
- `docs/signal-templates.md`
- `docs/data-sources.md`
- `docs/thesis-engine.md`

## Launch Acceptance

- the product can be run locally from README
- seed data produces a complete demo
- user can review theses from CLI
- user can review theses from Web
- user can add evidence
- confidence updates are persisted
- research card can be generated
- notification export works
- all outputs are non-advisory

