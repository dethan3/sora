# Web Roadmap

## Product Position

The Web app is launch-critical for Sora MVP.

It is not a marketing landing page and not a separate product logic layer. It presents Sora's Thesis-first workflow through a visual research interface.

## Dependency

Web starts after:

- Thesis schemas exist
- thesis storage tables exist
- API exposes Thesis overview/detail/evidence/exposure endpoints
- seed data can produce a complete demo

A standalone Thesis Engine package is not required for Web MVP.

## MVP Views

| Route | Purpose |
|-------|---------|
| `/` | Market Cognition overview |
| `/theses` | Thesis list |
| `/theses/:id` | Thesis detail |
| `/theses/:id/evidence` | Evidence timeline |
| `/theses/:id/exposure` | Asset exposure |

Optional after launch:

| Route | Purpose |
|-------|---------|
| `/agent` | Agent console over Thesis context |

## Homepage

The first screen should answer:

```text
What changed in current market cognition?
```

Required sections:

- changed theses
- strongest theses
- challenged theses
- recent evidence

Do not build a hero / marketing page as the primary screen.

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
- market / index / domestic fund exposure
- related research cards if available

## Visual Direction

Sora is a research workspace:

- dense but readable
- table and timeline friendly
- restrained status colors
- no decorative landing-page composition
- mobile layouts preserve reading and review workflows

## Technical Rules

- Web consumes API or shared services.
- Web does not calculate confidence deltas.
- Web does not sort exposure independently from service rules.
- Web does not contain compliance bypasses.
- Web views include non-advisory framing.

## Launch Acceptance

- local Web server runs
- homepage renders from seed/API data
- thesis detail renders from seed/API data
- evidence timeline renders support and against evidence
- exposure page renders market / index / fund exposure
- desktop and mobile smoke tests pass

