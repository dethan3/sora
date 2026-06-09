# Post-MVP - Thesis Engine Productization

## Goal

Extract Thesis Engine only after Sora's Thesis-first workflow has real usage and stable patterns.

Before extraction, Thesis Engine is a documented method and workflow contract. After extraction, it can become a reusable product or package.

## Why Post-MVP

Building a generic engine too early creates risk:

- slower Sora launch
- premature abstractions
- unclear API boundaries
- over-generalized code before product behavior is validated

The MVP should prove:

- users understand Thesis-first research
- evidence-driven confidence updates are useful
- contradiction display changes behavior
- domestic fund exposure mapping is valuable
- CLI and Web workflows are worth keeping

## Extraction Criteria

Start Thesis Engine productization when:

- Sora has at least one complete working workflow
- confidence rules have remained stable
- evidence fields have remained stable
- contradiction summary shape has remained stable
- exposure mapping needs have become clear
- at least one non-Sora use case is concrete

## Future Package

Possible future package:

```text
packages/thesis
```

Future responsibilities:

- thesis lifecycle
- evidence ingestion rules
- confidence updates
- contradiction summaries
- exposure sorting
- review loops
- action boundaries
- repository interfaces

Out of scope for the engine:

- Sora-specific market mapping
- domestic fund mapping
- QDII execution risk
- premium / purchase-status warnings
- NotificationEvent export
- Web or CLI formatting

## Future Deliverables

- `docs/thesis-engine.md`
- package API contract
- repository interface contract
- migration plan from Sora service to engine package
- compatibility tests proving Sora behavior is unchanged after extraction

