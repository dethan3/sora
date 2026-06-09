# Phase 4 - Research + Notifier

## Goal

Turn Thesis context into useful Sora outputs: ResearchCard and NotificationEvent.

This phase makes the product feel complete enough to use.

## Research Command

```bash
pnpm sora research create --thesis <id>
```

## ResearchCard Requirements

The card includes:

- thesis id and title
- confidence and latest confidence update
- evidence summary
- contradiction summary
- affected market / index / domestic fund exposure
- fund execution risk summary
- invalidation conditions
- non-advisory framing

Deterministic generation is required.

Agent-assisted generation is optional for launch.

## NotificationEvent Requirements

Generate events from:

- confidence crossing thresholds
- strong opposing evidence
- thesis status becoming challenged
- thesis status becoming invalidated
- material exposure-risk change

Event levels:

- `info`
- `watch`
- `warning`
- `critical`

No event can contain buy / sell instructions.

## Acceptance

- `pnpm sora research create --thesis ai-infra` works without Pi
- ResearchCard can be saved to storage
- `pnpm sora notifications export` includes Thesis-derived events
- tests cover thesis-to-card and thesis-to-event paths
- compliance tests reject obvious advisory wording

