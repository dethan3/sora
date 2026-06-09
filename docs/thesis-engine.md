# Thesis Engine

## Status

Thesis Engine is a long-term product direction.

For the fastest Sora MVP, it is a documented method, workflow protocol, and future extraction target. It is not required to exist as a standalone code package before Sora launches.

## Definition

Thesis Engine is a reusable reasoning primitive for managing:

- hypotheses
- evidence
- counter-evidence
- confidence changes
- contradictions
- exposure
- action boundaries
- review loops

## Sora Relationship

```text
Sora MVP = Thesis Engine method applied inside Sora services
Future Thesis Engine = stable workflow extracted from Sora
```

Sora validates the workflow in the global index fund research domain first.

## MVP Contract

Sora MVP should implement:

- Thesis data model
- Evidence data model
- deterministic confidence update rules
- evidence timeline
- support / against separation
- contradiction summary
- market / index / fund exposure
- research card generation
- notification event generation

## Future Engine Contract

The future engine may expose:

```ts
listTheses()
getThesis(id)
addEvidence(input)
reviewTheses()
getEvidenceTimeline(thesisId)
getContradictions(thesisId)
getAssetExposure(thesisId)
```

These APIs should be extracted only after Sora's MVP workflow has stabilized.

## Extraction Rule

Do not extract until extraction reduces complexity.

Premature extraction is a launch risk. The right time to extract is when Sora has proven which abstractions are stable.

