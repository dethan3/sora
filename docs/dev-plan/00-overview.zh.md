# Sora 快速上线计划总览

## 上线原则

先上线 Sora，再产品化 Thesis Engine。

最快 MVP 中，Thesis Engine 不必先成为独立代码包。它可以先作为：

- 产品方法论
- 工作流协议
- 数据模型
- 合规边界
- 未来引擎合同文档

Sora MVP 仍然实现 Thesis-first 行为，但实现可以先放在 Sora 应用服务里。等真实使用验证工作流后，再把稳定部分抽取成长期研发的 Thesis Engine 产品。

```text
MVP:
Sora 内部实现 Thesis-first workflow。

Post-MVP:
从 Sora 中抽取稳定模式，产品化 Thesis Engine。
```

## 当前仓库状态

当前仓库已有一半完成的 TypeScript rewrite：

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

已有工作中有价值的部分：

- market / index / fund seed 数据
- 数据源适配器
- 基金执行质量评分
- deterministic research cards
- notification export
- SQLite storage
- CLI 命令结构

最快路径是在这些结构上加 Thesis workflow，而不是先停下来抽象通用引擎。

## MVP 架构

```text
Presentation
  apps/cli
  apps/web
  apps/api

Sora Application Services
  packages/market
  packages/fund
  packages/research
  packages/notifier
  packages/storage
  packages/sources
  packages/agent
  packages/shared

Shared Types
  packages/core

Documented Future Product
  Thesis Engine
```

MVP 阶段，Thesis workflow 可以先放在 Sora service 模块中。独立 `packages/thesis` 是可选项，不应阻塞上线。

## MVP 产品链路

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
  -> Web / CLI review
```

## Phase 表

| Phase | 计划 | 目标 | 上线关键 |
|------|------|------|----------|
| 0 | [稳定当前重写](./01-phase0-stabilize.md) | 让当前 TS 基线可依赖 | 是 |
| 1 | [MVP Thesis Data + Storage](./02-phase1-thesis-model.md) | 添加 Thesis schemas、seed 和 storage tables | 是 |
| 2 | [Sora Thesis Workflow](./03-phase2-sora-thesis-workflow.md) | 实现 evidence add、confidence update、review、exposure queries | 是 |
| 3 | [CLI + API](./04-phase3-cli-api.md) | 通过 CLI 和 API 暴露 workflow | 是 |
| 4 | [Research + Notifier](./05-phase4-research-notifier.md) | 生成 thesis research cards 和 events | 是 |
| 5 | [Web MVP](./06-phase5-web-mvp.md) | 构建可用的 Thesis-first 网站 | 是 |
| 6 | [Launch Hardening](./07-phase6-launch-hardening.md) | 测试、文档、清除上线阻塞 | 是 |
| 7 | [Post-MVP Thesis Engine](./08-post-mvp-thesis-engine.md) | 工作流稳定后抽象可复用引擎 | 否 |
| 8 | [Post-MVP Agent](./09-post-mvp-agent.md) | 深化 Agent-assisted workflows | 否 |
| 9 | [Post-MVP Roadmap](./10-post-mvp-roadmap.md) | 长期产品扩展 | 否 |

## MVP 验收

Sora 满足以下条件即可上线：

- `pnpm install` 成功。
- `pnpm test` 成功。
- `pnpm build` 成功。
- `pnpm lint` 成功。
- `pnpm sora db init` 可运行。
- `pnpm sora db seed` 幂等，或明确要求 reset。
- `pnpm sora thesis list` 可运行。
- `pnpm sora thesis show ai-infra` 可运行。
- `pnpm sora thesis review` 可运行。
- `pnpm sora thesis exposure ai-infra` 可运行。
- `pnpm sora evidence add` 创建 evidence 和 confidence update records。
- `pnpm sora research create --thesis ai-infra` 在 deterministic fallback 下可运行。
- `pnpm sora notifications export` 包含 Thesis-derived events。
- API 提供 Thesis overview/detail/evidence/exposure data。
- Web 渲染 overview、thesis detail、evidence timeline、exposure。
- 所有用户可见输出保持非投顾。

上线不要求：

- 独立 `packages/thesis`
- 通用 Thesis Engine SDK
- 完全泛化的多领域引擎
- 高级 Agent 自动化
- 实时流数据
- 券商交易接入

