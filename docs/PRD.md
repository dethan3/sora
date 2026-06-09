# Sora PRD

## 1. Product Definition

Sora is a Thesis-first global index fund research system for Chinese investors.

It helps users maintain long-lived market theses, connect those theses to global indexes and domestic investable funds, track supporting and opposing evidence, and understand how market narratives change over time.

Sora is not a recommendation engine. It does not tell users what to buy or sell. Its job is to preserve reasoning, expose contradictions, map asset exposure, and generate structured research outputs.

## 2. One-line Positioning

English:

Sora helps Chinese investors manage market theses, map them to global indexes and domestic index funds, and track evidence-driven changes through CLI and Web experiences.

Chinese:

Sora 是一个面向中国投资者的 Thesis-first 全球指数基金研究系统，用于持续管理市场观点、连接全球指数与国内可买基金、追踪证据变化，并通过 CLI 和网站两种方式呈现结构化研究结果。

## 3. Core Product Shift

The previous product definition was:

```text
Market -> Index -> Domestic Fund -> Fund Analysis -> Research Card -> Alert
```

The new product definition is:

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
```

Market, index, fund analysis, research cards, and notifications remain important. They are no longer the top-level product model. They become supporting layers around Thesis.

## 4. What Is a Thesis?

A Thesis is a verifiable investment hypothesis, not a slogan.

Bad example:

```text
AI is strong, US stocks will rise.
```

Valid Thesis:

```text
Over the next 3-5 years, AI infrastructure capital expenditure remains elevated, supporting valuation and earnings momentum for Nasdaq 100, semiconductor indexes, data center assets, and related domestic ETF / QDII products.
```

A Thesis must include:

- title
- summary
- time horizon
- causal chain
- key assumptions
- affected markets / indexes / funds
- evidence for
- evidence against
- confidence score
- update history
- invalidation conditions

The product question is:

```text
Why did I believe this, and does the reason still hold?
```

## 5. Thesis Engine as an Agentic Finance Primitive

Sora is one application built on top of Thesis Engine.

The more general abstraction is:

```text
Sora = Thesis Engine applied to global index fund research
```

Thesis Engine itself should be treated as a portable Agentic Finance primitive. It can exist outside Sora and can be applied to other financial and business reasoning workflows.

The reusable primitive includes:

- Thesis
- Evidence
- Confidence Update
- Contradiction
- Exposure
- Action Boundary
- Review Loop

This primitive can apply to:

- index fund research
- single-stock research
- macro strategy
- crypto assets
- private market / VC deal memos
- company operating decisions
- risk monitoring
- investment committee review

The core of Agentic Finance is not "AI gives investment advice." The core is:

```text
Agent continuously manages hypotheses, evidence, counter-evidence, confidence changes, exposure, and action boundaries.
```

Therefore, Sora should be described as:

```text
A global index fund research product powered by Thesis Engine.
```

Thesis Engine is the underlying cognition framework. Sora is the vertical product implementation for Chinese investors researching global index funds and domestic fund exposure.

## 6. Product Principles

### 6.1 Thesis Is the Operating System

Thesis should not become a secondary menu. It is the main organizing layer of Sora.

The first screen in Web and the default overview in CLI should answer:

```text
What changed in current market cognition?
```

Not:

```text
Which fund went up today?
```

### 6.2 Evidence Beats News

Sora should not behave like a news feed.

For every piece of information, Sora asks:

```text
Does this support, weaken, or not affect a Thesis?
```

Each evidence item must record:

- source
- title
- summary
- related thesis ids
- direction: support / against / neutral
- strength: weak / medium / strong
- confidence delta
- rationale
- createdAt

### 6.3 Contradictions Are First-class

Every Thesis should show both sides:

```text
Supporting evidence
Opposing evidence
Open questions
Invalidation conditions
```

This is a core product differentiator. Sora must avoid becoming a confirmation-bias machine.

### 6.4 Fund Mapping Is the Sora-specific Layer

General research tools can stop at a market or company thesis. Sora must continue to domestic fund exposure.

For each Thesis, Sora should answer:

```text
Which indexes and domestic funds are exposed to this Thesis?
How strong is the exposure?
What are the execution risks?
```

Examples:

```text
AI Infrastructure Thesis

Nasdaq 100                 exposure 85%
S&P 500                    exposure 53%
Domestic Nasdaq ETF         exposure 80%
Domestic QDII feeder fund   exposure 65%
```

## 7. Interaction Model

Sora has two first-class interaction surfaces:

```text
apps/cli
apps/web
```

Both must call the same domain services. Neither surface should contain business logic that cannot be reused by the other.

### 7.1 CLI

The CLI is for:

- power users
- daily review
- scripting
- automation
- local-first workflows
- exporting JSON for Tickeye or other systems

Core commands:

```bash
pnpm sora thesis list
pnpm sora thesis show ai-infra
pnpm sora evidence add --thesis ai-infra
pnpm sora thesis review
pnpm sora thesis exposure ai-infra
pnpm sora research create --thesis ai-infra
pnpm sora notifications export
```

### 7.2 Web

The Web app is for:

- visual market map
- thesis cards
- evidence timeline
- contradiction review
- asset exposure heatmap
- interactive research reading
- agent chat over thesis context

Core Web views:

- Market Map
- Thesis List
- Thesis Detail
- Evidence Timeline
- Asset Exposure
- Research Cards
- Agent Console
- Settings

The Web app should consume API/domain services, not reimplement analysis logic.

## 8. Agent Architecture

Agent capability must be decoupled from presentation.

The Agent layer is not the CLI and not the Web UI. It is a reusable service layer that both surfaces can call.

```text
CLI ----\
        -> Application Services -> Agent Services -> Pi / Tools
Web ----/
```

### 8.1 Deterministic Logic

These must remain deterministic TypeScript logic:

- schema validation
- market / index / fund models
- fund mapping
- fund scoring
- exposure scoring
- evidence persistence
- confidence update rules
- notification event generation
- storage queries
- compliance filters

### 8.2 Agent-assisted Logic

Agent can assist with:

- summarizing evidence
- classifying evidence direction
- explaining causal chains
- identifying contradictions
- generating research cards
- answering user questions over Thesis context
- proposing follow-up research tasks

### 8.3 Agent Boundaries

Agent output must be structured and schema-validated.

Agent must not:

- recommend buying or selling
- produce personalized allocation advice
- promise returns
- claim certainty
- bypass deterministic scoring rules
- silently change confidence without evidence records

If Agent output fails schema validation, Sora must fall back to deterministic output.

## 9. Compliance Boundary

Sora is not an investment adviser.

Forbidden output:

- buy / sell instructions
- personalized position sizing
- guaranteed return language
- "must rise" / "must fall" claims
- automatic trading
- live copy trading
- user-specific portfolio recommendation

Allowed output:

- market information
- index information
- domestic fund mapping
- execution quality analysis
- premium / purchase-status risk alerts
- thesis evidence summaries
- confidence history
- contradiction analysis
- research cards
- notification events
- follow-up research tasks

All outputs must use information-analysis and risk-warning language.

Allowed:

```text
This fund currently has elevated premium risk. As an execution vehicle, it requires careful evaluation.
```

Not allowed:

```text
Buy this fund today.
```

## 10. Core Domain Model

### 10.1 Existing Models

Sora keeps the existing models:

- Market
- Index
- Fund
- FundMapping
- FundMetricsSnapshot
- FundAnalysis
- ResearchSignal
- ResearchCard
- Alert
- NotificationEvent

### 10.2 New Thesis Models

Sora adds:

```ts
type Thesis = {
  id: string
  title: string
  summary: string
  timeHorizon: '3m' | '6m' | '1y' | '3y' | '5y'
  status: 'draft' | 'watch' | 'active' | 'challenged' | 'invalidated' | 'archived'
  confidence: number
  causalChain: string[]
  keyAssumptions: string[]
  affectedMarketIds: string[]
  affectedIndexIds: string[]
  affectedFundIds: string[]
  invalidationConditions: string[]
  createdAt: string
  updatedAt: string
}
```

```ts
type ThesisEvidence = {
  id: string
  thesisId: string
  source: string
  title: string
  summary: string
  url?: string
  direction: 'support' | 'against' | 'neutral'
  strength: 'weak' | 'medium' | 'strong'
  confidenceDelta: number
  rationale: string
  observedAt: string
  createdAt: string
}
```

```ts
type ThesisUpdate = {
  id: string
  thesisId: string
  previousConfidence: number
  newConfidence: number
  evidenceIds: string[]
  rationale: string
  createdAt: string
}
```

```ts
type AssetExposure = {
  id: string
  thesisId: string
  assetType: 'market' | 'index' | 'fund'
  assetId: string
  exposureScore: number
  rationale: string
  updatedAt: string
}
```

## 11. Package Architecture

MVP monorepo:

```text
apps/
  cli/
  web/
  api/
  worker/

packages/
  core/
  market/
  fund/
  research/
  agent/
  sources/
  storage/
  notifier/
  shared/
```

Responsibilities:

- `packages/core`: shared Zod schemas and TypeScript types
- Sora Thesis workflow service: MVP implementation of evidence rules, confidence updates, review, contradiction, and exposure queries. It can live in the most pragmatic Sora service module before extraction.
- `packages/market`: market and index queries
- `packages/fund`: fund mapping and execution quality analysis
- `packages/research`: research card generation from Thesis context
- `packages/agent`: reusable Agent interface and Pi/tool adapters
- `packages/sources`: Yahoo / EastMoney / Tavily / seed data adapters
- `packages/storage`: SQLite persistence and queries
- `packages/notifier`: NotificationEvent generation and export
- `apps/cli`: command interface
- `apps/api`: HTTP API for Web and external consumers
- `apps/web`: visual interface
- `apps/worker`: scheduled ingestion and review jobs

Future package:

- `packages/thesis`: extracted reusable Thesis Engine package, created after Sora MVP validates the workflow.

## 12. Main User Workflows

### 12.1 Daily Review

```text
User opens Sora
-> sees strongest / weakest / changed theses
-> reviews evidence timeline
-> checks confidence changes
-> inspects affected funds
```

CLI:

```bash
pnpm sora thesis review
```

Web:

```text
Market Map -> Changed Theses -> Evidence Timeline
```

### 12.2 Add Evidence

```text
User or worker adds new evidence
-> Sora classifies direction and strength
-> confidence changes
-> update record is created
-> notification may be generated
```

### 12.3 Thesis Detail

For one Thesis, user sees:

- current judgment
- confidence
- causal chain
- key assumptions
- evidence for
- evidence against
- asset exposure
- related research cards
- invalidation conditions

### 12.4 Fund Exposure

User asks:

```text
I hold a Nasdaq fund. Which theses affect it most?
```

Sora answers:

```text
AI Infrastructure       exposure 85%
US Tech Valuation       exposure 82%
USD Liquidity           exposure 76%
```

## 13. MVP Scope

The next MVP after the current implementation should include:

- Thesis schemas in `packages/core`
- seed thesis data
- CLI thesis commands
- storage tables for thesis / evidence / updates / exposure
- deterministic confidence update rules
- ResearchCard generation from Thesis context
- NotificationEvent generation from Thesis updates
- Web skeleton with Thesis-first views
- API endpoints for Web

MVP does not need:

- standalone `packages/thesis`
- generic Thesis Engine SDK
- portfolio management
- automatic trading
- real brokerage integration
- personalized allocation advice
- complex charting
- real-time streaming

## 14. Web MVP

The Web MVP should be functional, not a marketing landing page.

First screen:

```text
Market Cognition

Changed Theses
Strongest Theses
Challenged Theses
Recent Evidence
```

Required views:

- `/` Market Map / Thesis Overview
- `/theses` Thesis List
- `/theses/:id` Thesis Detail
- `/theses/:id/evidence` Evidence Timeline
- `/theses/:id/exposure` Asset Exposure
- `/agent` Agent Console

## 15. CLI MVP

Required commands:

```bash
pnpm sora thesis list
pnpm sora thesis show <id>
pnpm sora thesis review
pnpm sora thesis exposure <id>
pnpm sora evidence add --thesis <id> --direction support --strength medium --title "..."
pnpm sora research create --thesis <id>
pnpm sora notifications export
```

All CLI output must end with the compliance disclaimer.

## 16. Data Sources

Sora continues to use:

- Yahoo Finance for market / index quotes
- EastMoney / Tiantian Fund for domestic fund data
- Tavily for search and evidence discovery
- seed data for offline tests and deterministic demos

Data source failures must be explicit. Commands must not report success when all upstream requests fail.

## 17. Acceptance Criteria

Product acceptance:

- Thesis is the primary organizing model.
- CLI and Web both consume shared services.
- Agent capability is decoupled from both CLI and Web.
- Every confidence change has evidence.
- Every Thesis shows supporting and opposing evidence.
- Every Thesis can map to affected indexes and funds.
- ResearchCard can be generated from Thesis context.
- NotificationEvent can be generated from Thesis updates.
- All output remains compliant and non-advisory.

Technical acceptance:

- `pnpm install` succeeds.
- `pnpm test` succeeds.
- `pnpm build` succeeds.
- `pnpm lint` succeeds.
- `pnpm sora thesis list` works.
- `pnpm sora thesis show ai-infra` works.
- `pnpm sora thesis review` works.
- `pnpm sora research create --thesis ai-infra` works.
- Web app starts locally and renders Thesis overview.
- API exposes Thesis endpoints consumed by Web.
- Storage supports thesis / evidence / updates / exposure.
