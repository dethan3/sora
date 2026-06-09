# Phase 5 - Web MVP

## Goal

Build the first usable Thesis-first Web interface.

Web is launch-critical, but it should stay small. It consumes API data and does not own domain logic.

## App

```text
apps/web
```

## Required Views

- `/` Market Cognition overview
- `/theses` Thesis list
- `/theses/:id` Thesis detail
- `/theses/:id/evidence` Evidence timeline
- `/theses/:id/exposure` Asset exposure

`/agent` is optional for launch.

## Homepage

The first screen shows:

- changed theses
- strongest theses
- challenged theses
- recent evidence

Skip marketing copy. The first screen is the product.

## Thesis Detail

Show:

- status and confidence
- summary
- time horizon
- causal chain
- key assumptions
- support evidence
- against evidence
- contradiction summary
- invalidation conditions
- market / index / fund exposure

## Design Direction

- quiet research workspace
- dense but readable
- scan-friendly tables and timelines
- restrained status colors
- usable desktop and mobile layouts

## Acceptance

- local Web server runs
- homepage renders from seed/API data
- thesis detail renders from seed/API data
- evidence timeline separates support and against evidence
- exposure page renders market / index / fund exposure
- no business logic is reimplemented in Web components
- no advisory wording appears

