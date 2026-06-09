# Post-MVP - Agent Deepening

## Goal

Use Agent capabilities to improve Thesis workflows after the deterministic MVP works.

Agent is not launch-critical. MVP must work without Pi or any external Agent backend.

## Agent-Assisted Workflows

Post-MVP Agent can help with:

- classify evidence direction and strength
- summarize evidence timelines
- explain confidence changes
- identify contradictions
- propose follow-up research tasks
- answer questions over Thesis context
- draft research-card narrative sections

## Rules

Agent must not:

- recommend buying or selling
- provide personalized allocation advice
- silently change confidence
- bypass deterministic scoring
- bypass compliance filters

All Agent outputs must:

- be schema-validated
- have deterministic fallback
- be clearly bounded as analysis support

## Acceptance

- CLI and Web can call the same Agent service
- invalid Agent output falls back deterministically
- Agent-generated text passes compliance filters
- no MVP command depends on Agent availability

