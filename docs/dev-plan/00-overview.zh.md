# Sora 开发计划总览（中文版）

## 产品方向

Sora 现在是一个 Thesis-first 全球指数基金研究系统。

它有两个一等交互层：

```text
CLI
Web
```

两者使用同一套底层领域服务。Agent 能力与表现层解耦。

## Thesis Engine Foundation

Thesis Engine 是可复用的 Agentic Finance primitive

Sora 是构建在它之上的指数基金研究应用：

```text
Sora = Thesis Engine 在全球指数基金研究场景下的应用
```

这意味着 `packages/thesis` 应尽量保持平台无关。全球市场映射、国内基金暴露、QDII 执行风险、NotificationEvent 导出等 Sora-specific 逻辑，应放在 Thesis Engine 周围的应用层。

## 新核心链路

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
```

旧链路仍然有价值：

```text
Market -> Index -> Fund Mapping -> Fund Analysis -> Research Card -> NotificationEvent
```

但它现在是 Thesis 之下的支撑层。

## 架构

```text
Presentation
  apps/cli
  apps/web
  apps/api

Sora Application Layer
  packages/market
  packages/fund
  packages/research
  packages/notifier

Agentic Finance Primitive
  packages/core
  packages/thesis
  packages/agent

Infrastructure
  packages/storage
  packages/sources
  packages/shared
```

`packages/thesis` 负责可复用的 thesis 生命周期、证据、confidence、contradiction、exposure 和 review-loop 逻辑。

Sora-specific services 负责把 Thesis Engine 与 market、fund、research、notifier、storage、sources adapters 组合起来。

`packages/agent` 在需要总结、证据分类、因果链解释、矛盾审查或问答时由服务层调用。

## 包计划

现有 packages 保留：

- `packages/core`
- `packages/market`
- `packages/fund`
- `packages/research`
- `packages/agent`
- `packages/sources`
- `packages/storage`
- `packages/notifier`
- `packages/shared`

新增 package：

- `packages/thesis`

新增 app：

- `apps/web`

`apps/api` 从占位应用变成 Web-facing API。

## Phase 表

| Phase | 名称 | 目标 | 依赖 |
|------|------|------|------|
| 0 | 稳定当前 TS 重写 | 修复 lint、DB seed 幂等、market refresh 失败处理、README 基线 | current repo |
| 1 | Thesis Primitive Model | 新增平台无关的 Thesis / Evidence / Update / Exposure schemas 和 seed 数据 | P0 |
| 2 | Thesis Engine Package | 实现可复用的 confidence、contradiction、review、exposure primitives | P1 |
| 3 | Storage 扩展 | 新增 thesis tables 和 storage queries | P1 |
| 4 | CLI Thesis MVP | 新增 thesis 和 evidence commands | P2 + P3 |
| 5 | Research + Notifier 集成 | 从 Thesis context 生成 ResearchCard 和 NotificationEvent | P4 |
| 6 | API Layer | 向 Web 暴露 Thesis services | P2 + P3 |
| 7 | Web MVP | 构建 Thesis-first website | P6 |
| 8 | Agent 解耦 | 标准化 CLI 和 Web 共用的 Agent services | P2 + P6 |
| 9 | Testing + Docs | E2E tests 和完整文档 | P4-P8 |

## 验收标准

功能：

- `pnpm sora thesis list` 可运行。
- `pnpm sora thesis show ai-infra` 可运行。
- `pnpm sora thesis review` 可运行。
- `pnpm sora thesis exposure ai-infra` 可运行。
- `pnpm sora evidence add` 创建 evidence 并更新 confidence。
- `pnpm sora research create --thesis ai-infra` 可运行。
- `pnpm sora notifications export` 包含 Thesis-derived events。
- Web app 渲染 Thesis overview。
- Web app 渲染 Thesis detail、timeline、contradictions、asset exposure。
- API endpoints 返回 schema-valid Thesis data。

架构：

- CLI 和 Web 共享 services。
- Agent 不耦合 CLI 或 Web。
- `packages/thesis` 尽可能保持平台无关。
- Sora-specific market / fund / notification 逻辑留在 Thesis Engine primitive 之外。
- 表现层不重复实现 confidence 或 exposure 逻辑。
- 没有 Pi 时 deterministic fallback 可用。

合规：

- 不输出买入 / 卖出建议。
- 不输出个性化配置建议。
- 所有输出使用信息分析和风险提示语气。
- 每个 CLI 命令末尾包含免责声明。
- 每个 Web research view 包含非投顾提示。

质量：

- `pnpm install` 成功。
- `pnpm test` 成功。
- `pnpm build` 成功。
- `pnpm lint` 成功。
- seed 数据通过 schemas 校验。
- 所有 confidence changes 都有 evidence records。
- data refresh commands 不报告虚假成功。
