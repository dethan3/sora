# Phase 2 - Sora Thesis Workflow

## Goal

Implement the MVP Thesis workflow inside Sora application services.

This phase deliberately avoids a generic engine abstraction. Keep the workflow simple, deterministic, and easy to ship.

## Workflow API

Implement service functions such as:

```ts
listTheses()
getThesis(id)
addEvidence(input)
reviewTheses()
getEvidenceTimeline(thesisId)
getContradictions(thesisId)
getAssetExposure(thesisId)
```

These can live in the most pragmatic Sora location for MVP, for example:

- `packages/research/src/thesis-service.ts`
- `packages/storage/src/queries/thesis.ts`
- a new Sora-specific service module

Do not block launch on a standalone `packages/thesis`.

## Confidence Rules

| Direction | Strength | Delta |
|-----------|----------|-------|
| support | strong | +8 |
| support | medium | +4 |
| support | weak | +2 |
| neutral | any | 0 |
| against | weak | -2 |
| against | medium | -4 |
| against | strong | -8 |

Rules:

- clamp confidence to `0..100`
- create one update for every evidence-driven change
- neutral evidence is still recorded
- no confidence change without evidence

## Review Rules

`reviewTheses()` should return:

- changed theses
- strongest theses
- challenged theses
- recently updated theses
- latest support / against evidence

Simple deterministic logic is enough for MVP.

## Contradiction Rules

Minimum contradiction summary:

- count support and against evidence
- list strongest against evidence
- flag thesis as challenged when strong against evidence exists or confidence falls below threshold

## Acceptance

- evidence add updates confidence deterministically
- update records contain previous and new confidence
- review works from seed data
- exposure list is sorted by exposure score
- support and against evidence are displayed separately
- implementation is simple enough to refactor into Thesis Engine later

