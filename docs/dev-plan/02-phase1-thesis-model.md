# Phase 1 - MVP Thesis Data + Storage

## Goal

Add the minimum Thesis data model and persistence needed to make Sora Thesis-first.

This phase is not about creating a standalone Thesis Engine. It is about giving Sora enough schema, seed data, and storage to support the MVP workflow.

## Scope

Add to `packages/core`:

- `Thesis`
- `ThesisEvidence`
- `ThesisUpdate`
- `AssetExposure`
- `EvidenceDirection`
- `EvidenceStrength`
- `ThesisStatus`
- `TimeHorizon`

Add seed files:

- `data/seeds/theses.json`
- `data/seeds/thesis-evidence.json`
- `data/seeds/thesis-updates.json`
- `data/seeds/asset-exposures.json`

Add storage tables:

- `theses`
- `thesis_evidence`
- `thesis_updates`
- `asset_exposures`

Initial seed theses:

- `ai-infra`
- `china-recovery`
- `gold-allocation`
- `us-tech-valuation`
- `usd-liquidity`

## Data Rules

- confidence is `0..100`
- evidence direction is `support | against | neutral`
- evidence strength is `weak | medium | strong`
- every update references evidence ids
- asset exposure uses generic asset refs: `market | index | fund`
- seed data is deterministic and usable offline

## Storage Queries

Add the minimum query set:

- list theses
- get thesis by id
- list evidence by thesis
- insert evidence
- insert update
- update thesis confidence
- list exposures by thesis
- list changed / challenged theses

## Acceptance

- schemas validate valid and invalid cases
- seed data validates against schemas
- `db init` creates the new tables
- `db seed` imports Thesis seed data
- `db seed` is idempotent or explicitly reset-based
- storage tests cover the minimum query set
- no standalone `packages/thesis` is required

