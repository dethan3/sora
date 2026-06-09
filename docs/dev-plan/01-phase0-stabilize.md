# Phase 0 - Stabilize Current Rewrite

## Goal

Turn the half-built TypeScript rewrite into a reliable base before adding Thesis Engine.

This phase does not add new product scope. It fixes the current repo so later Thesis work can move without fighting broken infrastructure.

## Current Baseline

Already present:

- TypeScript pnpm monorepo
- `apps/cli`, `apps/api`, `apps/worker`
- `packages/core`, `market`, `fund`, `research`, `agent`, `sources`, `storage`, `notifier`, `shared`
- seed data for markets, indexes, funds, mappings, and fund metrics
- CLI commands for db, data, markets, indexes, funds, research, notifications
- deterministic research card and notification generation
- SQLite storage for existing market / fund / research entities

Known gaps:

- lint is not yet a hard green baseline
- README and architecture docs are incomplete
- `db seed` must be safe to rerun or explicitly destructive
- all-upstream data refresh failure must not print success
- Pi and Tavily real-key paths still need verification
- old docs still describe market/fund as the top-level model

## Tasks

- Run `pnpm test`, `pnpm build`, and `pnpm lint`; record current failures.
- Fix lint errors without changing product behavior.
- Make `db seed` idempotent with upsert semantics, or add explicit `--reset` for destructive reseeding.
- Fix `data refresh --type market` so all-upstream failures exit non-zero and show failed tickers.
- Add Yahoo rate-limit and transient-failure messaging.
- Keep all existing CLI commands working.
- Update README baseline to reflect the current TypeScript monorepo and the new Thesis-first direction.

## Acceptance

- `pnpm install` succeeds.
- `pnpm test` succeeds.
- `pnpm build` succeeds.
- `pnpm lint` succeeds.
- `pnpm sora db init` works.
- `pnpm sora db seed` is safe to run repeatedly or requires an explicit destructive flag.
- `pnpm sora data refresh --type market` reports real failure when every upstream request fails.
- No new Thesis logic is implemented in this phase.

