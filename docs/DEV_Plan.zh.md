# Sora 开发计划（中文版）

本计划取代旧的 CLI-only 路线图。

新的产品方向：

- Sora 有两个一等交互层：CLI 和 Web。
- Thesis 是主要产品模型。
- Agent 是可复用服务层，与表现层解耦。
- 现有 market / fund / research / notification 包继续保留，但它们现在服务于 Thesis 工作流。

## 当前基线

当前仓库已经是 TypeScript monorepo，包含：

- `apps/cli`
- `apps/api`
- `apps/worker`
- `packages/core`
- `packages/market`
- `packages/fund`
- `packages/research`
- `packages/agent`
- `packages/sources`
- `packages/storage`
- `packages/notifier`
- `packages/shared`

已验证基线：

- `pnpm test` 通过。
- `pnpm build` 通过。
- CLI 核心命令可运行。

继续开发前的已知问题：

- `pnpm lint` 目前存在未使用变量错误。
- README 仍未完整。
- `data refresh --type market` 在 Yahoo 请求失败时可能仍报告成功。
- `db seed` 在已有数据库上不是幂等的。
- Pi 和 Tavily 真实 key 链路仍需端到端验证。

## 新架构方向

```text
                 +----------------+
                 | packages/agent |
                 +----------------+
                         ^
                         |
apps/cli ----+           |
             |           |
apps/web ----+--> Application Services
             |           |
apps/api ----+           v
                 +------------------+
                 | packages/thesis  |
                 +------------------+
                         |
     +-------------------+-------------------+
     |                   |                   |
packages/market   packages/fund      packages/research
     |                   |                   |
packages/sources  packages/storage   packages/notifier
```

表现层不能拥有领域逻辑。CLI、Web、API 和 Worker 都调用共享服务。

## Phase 总览

| Phase | 名称 | 目标 |
|------|------|------|
| 0 | 稳定当前 TS 重写 | 修复 lint、seed 幂等、数据刷新失败处理、README 基线 |
| 1 | Thesis 核心模型 | 新增 Thesis / Evidence / Update / Exposure schemas 和 seed 数据 |
| 2 | Thesis Package | 实现生命周期、信心更新、矛盾摘要和暴露映射 |
| 3 | Storage 扩展 | 新增 thesis 相关表和查询 |
| 4 | CLI Thesis MVP | 新增 thesis / evidence 命令 |
| 5 | Research + Notifier 集成 | 从 Thesis 上下文生成 ResearchCard 和 NotificationEvent |
| 6 | API Layer | 为 Web 和外部消费者新增 HTTP endpoints |
| 7 | Web MVP | 构建 Thesis-first 网站 |
| 8 | Agent 解耦 | 统一 CLI 和 Web 共用的 Agent service 接口 |
| 9 | 测试 + 文档 | 增加 e2e 覆盖并更新文档 |

## Phase 0 - 稳定当前 TS 重写

目标：让当前 TypeScript 代码库成为可靠基础。

任务：

- 修复所有 `pnpm lint` 错误。
- 让 `db seed` 幂等，或提供明确的 `--reset` / `--force`。
- 修复 `data refresh --type market`，确保所有上游失败时非零退出或明确报告失败。
- 增加清晰的 Yahoo 限流处理。
- 补全当前功能的 README 基线。
- 保持现有 CLI 行为可用。

完成条件：

- `pnpm lint` 通过。
- `pnpm test` 通过。
- `pnpm build` 通过。
- `pnpm sora db seed` 可重复安全运行，或明确说明破坏性行为。
- Market refresh 不再打印虚假成功。

## Phase 1 - Thesis 核心模型

目标：在 `packages/core` 中把 Thesis 加为一等领域模型。

新增 schemas：

- `Thesis`
- `ThesisEvidence`
- `ThesisUpdate`
- `AssetExposure`
- `EvidenceDirection`
- `EvidenceStrength`
- `ThesisStatus`
- `TimeHorizon`

Seed 数据：

- `data/seeds/theses.json`
- `data/seeds/thesis-evidence.json`
- `data/seeds/asset-exposures.json`

初始 seed theses：

- `ai-infra`：AI Infrastructure Supercycle
- `china-recovery`：China Asset Valuation Recovery
- `gold-allocation`：Gold Long-term Allocation Value
- `us-tech-valuation`：US Tech Valuation Risk
- `usd-liquidity`：USD Liquidity Cycle

完成条件：

- schemas 覆盖 valid / invalid 测试。
- seed 数据通过 schema 校验。
- 现有 packages 仍可 build。

## Phase 2 - Thesis Package

目标：实现可复用 Thesis 领域逻辑。

新增包：

```text
packages/thesis
```

API：

```ts
listTheses(): Promise<Thesis[]>
getThesis(id: string): Promise<Thesis | null>
addEvidence(input: AddEvidenceInput): Promise<ThesisUpdate>
reviewTheses(): Promise<ThesisReview>
getEvidenceTimeline(thesisId: string): Promise<ThesisEvidence[]>
getContradictions(thesisId: string): Promise<ContradictionSummary>
getAssetExposure(thesisId: string): Promise<AssetExposure[]>
```

确定性更新规则：

- strong support：`+8`
- medium support：`+4`
- weak support：`+2`
- neutral：`0`
- weak against：`-2`
- medium against：`-4`
- strong against：`-8`

Confidence 必须保持在 `0..100`。

完成条件：

- confidence 更新规则有测试覆盖。
- 每次 update 都记录 previous 和 new confidence。
- 支持证据和反对证据都可查询。
- exposure 能按分数排序。

## Phase 3 - Storage 扩展

目标：持久化 Thesis 数据。

新增表：

- `theses`
- `thesis_evidence`
- `thesis_updates`
- `asset_exposures`

查询：

- list / get thesis
- insert evidence
- insert update
- list timeline
- list exposures
- review changed theses

完成条件：

- `db init` 创建新表。
- `db seed` 导入 thesis seed 数据。
- in-memory storage 测试覆盖新查询。

## Phase 4 - CLI Thesis MVP

目标：让 Thesis 可从 CLI 使用。

命令：

```bash
pnpm sora thesis list
pnpm sora thesis show <id>
pnpm sora thesis review
pnpm sora thesis exposure <id>
pnpm sora evidence add --thesis <id> --direction support --strength medium --title "..."
```

输出要求：

- 展示 confidence 和趋势。
- 展示最近证据。
- 分开展示支持证据和反对证据。
- 展示受影响 markets / indexes / funds。
- 末尾包含合规免责声明。

完成条件：

- 所有命令可使用 seed 数据运行。
- 命令在 DB seed 后可运行。
- CLI 输出清晰且非投顾。

## Phase 5 - Research + Notifier 集成

目标：把 Thesis 接入现有 research 和 notification 层。

Research 改动：

- `research create --thesis <id>`
- ResearchCard 包含 thesis id、证据摘要、矛盾摘要、暴露摘要。
- 确定性生成器无需 Agent 也能生成 Thesis-based card。

Notifier 改动：

- confidence 跨越阈值时可创建 NotificationEvent。
- 强反对证据可创建 warning / watch 事件。
- Thesis invalidated 可创建严重风险事件，但仍保持非投顾语气。

完成条件：

- `pnpm sora research create --thesis ai-infra` 可运行。
- `pnpm sora notifications export` 包含 Thesis-derived events。
- 测试覆盖 Thesis-to-card 和 Thesis-to-event 流程。

## Phase 6 - API Layer

目标：向 Web 暴露共享服务。

Endpoints：

```text
GET  /api/theses
GET  /api/theses/:id
GET  /api/theses/:id/evidence
GET  /api/theses/:id/exposure
POST /api/theses/:id/evidence
GET  /api/review
POST /api/research/thesis/:id
GET  /api/notifications
```

规则：

- API 调用共享服务。
- API 不得重复实现领域逻辑。
- responses 使用 core schemas。

完成条件：

- API 可本地启动。
- endpoint 测试通过。
- Web 不直接访问 DB，也能消费 API。

## Phase 7 - Web MVP

目标：构建 Thesis-first 网站。

App：

```text
apps/web
```

视图：

- `/` Market Cognition overview
- `/theses` Thesis list
- `/theses/:id` Thesis card
- `/theses/:id/evidence` Evidence timeline
- `/theses/:id/exposure` Asset exposure
- `/agent` Agent console

UX 要求：

- 首屏展示 changed theses、strongest theses、challenged theses、recent evidence。
- Thesis 详情展示因果链、假设、confidence、support、against、exposure。
- asset exposure 易于扫描。
- 不做营销落地页。

完成条件：

- 本地 Web server 可运行。
- 桌面和移动端布局可用。
- 数据来自 API / services。
- 不出现投资建议语句。

## Phase 8 - Agent 解耦

目标：让 Agent 可同时服务 CLI 和 Web。

Agent service 能力：

- 判断 evidence direction 和 strength。
- 总结 evidence timeline。
- 解释 confidence changes。
- 识别 contradictions。
- 回答 Thesis-context questions。
- 生成 follow-up research tasks。

规则：

- Agent result 必须 schema-validated。
- 保留 deterministic fallback。
- confidence changes 必须有 evidence records。
- Agent 不负责表现层格式化。

完成条件：

- CLI 和 Web 都能调用同一个 Agent service。
- Pi 不可用时路径仍可运行。
- 测试覆盖 schema failure fallback。

## Phase 9 - 测试 + 文档

目标：完成产品质量验证。

测试：

- Thesis schemas 和 update rules 单元测试。
- thesis tables storage 测试。
- CLI 命令测试。
- API endpoint 测试。
- Web smoke tests。
- Thesis 端到端流程：

```text
seed thesis -> add evidence -> confidence update -> research card -> notification -> Web render
```

文档：

- README
- architecture
- research card
- signal templates
- data sources
- Thesis model
- Agent architecture
- Web roadmap

完成条件：

- PRD 中所有验收标准通过。

