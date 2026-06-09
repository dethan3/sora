# Phase 3 - CLI + API

## Goal

Expose the MVP Thesis workflow through CLI and API.

CLI and API are launch-critical because Web should consume API and power users need CLI immediately.

## CLI Commands

```bash
pnpm sora thesis list
pnpm sora thesis show <id>
pnpm sora thesis review
pnpm sora thesis exposure <id>
pnpm sora evidence add --thesis <id> --direction support --strength medium --title "..."
```

Required after Phase 4:

```bash
pnpm sora research create --thesis <id>
pnpm sora notifications export
```

## CLI Output

`thesis list`:

- id
- title
- status
- confidence
- latest evidence date

`thesis show`:

- summary
- time horizon
- confidence
- status
- causal chain
- assumptions
- support evidence
- against evidence
- invalidation conditions
- top exposure

`thesis review`:

- changed theses
- challenged theses
- recent evidence
- strongest theses

`evidence add`:

- evidence id
- previous confidence
- new confidence
- confidence delta

All CLI output ends with non-advisory disclaimer.

## API Endpoints

```text
GET  /api/theses
GET  /api/theses/:id
GET  /api/theses/:id/evidence
GET  /api/theses/:id/exposure
POST /api/theses/:id/evidence
GET  /api/review
```

Phase 4 adds:

```text
POST /api/research/thesis/:id
GET  /api/notifications
```

## Acceptance

- CLI commands work after `db seed`
- API starts locally
- API returns schema-valid JSON
- invalid evidence input returns validation errors
- neither CLI nor API duplicates confidence update logic
- all user-facing text is non-advisory

