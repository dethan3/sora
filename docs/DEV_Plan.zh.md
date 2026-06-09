# Sora 开发计划

本计划以快速上线为目标。

## 1. 战略决策

Thesis Engine 是长期产品，Sora 是现在要先上线的应用。

MVP 阶段，Thesis Engine 不需要先成为独立代码包。它可以先作为：

- 产品方法论
- 工作流协议
- 数据模型
- 合规边界
- 未来抽取目标

Sora MVP 在 Sora 应用服务内部实现 Thesis-first 行为。工作流被真实使用验证后，再把稳定模式抽取成 Thesis Engine。

```text
现在：Sora 实现 Thesis-first workflow。
之后：抽取并产品化 Thesis Engine。
```

## 2. 当前基线

仓库已有可复用的 TypeScript rewrite：

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

已有能力：

- market / index / fund / mapping seed 数据
- Yahoo、EastMoney、Tavily、seed 数据源适配器
- 基金执行质量评分
- deterministic research card 生成
- notification event 生成和导出
- 现有实体的 SQLite storage
- 现有工作流的 CLI 结构

## 3. 最快上线范围

MVP 必须包含：

- `packages/core` 中的 Thesis schemas
- Thesis seed data
- thesis / evidence / update / exposure storage tables
- deterministic confidence update rules
- CLI thesis 和 evidence 命令
- API thesis endpoints
- Web overview 和 thesis detail
- 从 Thesis context 生成 ResearchCard
- 从 Thesis updates 生成 NotificationEvent
- 非投顾合规表达

MVP 不要求：

- 独立 `packages/thesis`
- 通用 Thesis Engine SDK
- 完全泛化的多领域引擎
- 高级 Agent 自动化
- 券商接入
- 个性化配置建议
- 实时流数据

## 4. MVP 架构

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

Future Product
  Thesis Engine
```

MVP 阶段，Thesis workflow 代码可以先放在务实的 Sora service 模块中。关键是保持简单、确定性、可测试，方便后续抽取。

## 5. Phase 总览

| Phase | 名称 | 目标 |
|------|------|------|
| 0 | 稳定当前重写 | 修复 lint、seed 幂等、data refresh 失败处理、README 基线 |
| 1 | MVP Thesis Data + Storage | 添加 schemas、seeds、tables、最小 storage queries |
| 2 | Sora Thesis Workflow | 实现 evidence add、confidence update、review、contradiction、exposure |
| 3 | CLI + API | 通过 CLI 和 API 暴露 workflow |
| 4 | Research + Notifier | 生成 Thesis-based cards 和 events |
| 5 | Web MVP | 构建第一版可用 Thesis-first 网站 |
| 6 | Launch Hardening | 测试、文档、清除上线阻塞 |
| 7 | Post-MVP Thesis Engine | 工作流稳定后抽取可复用引擎 |
| 8 | Post-MVP Agent | 深化 Agent-assisted workflows |
| 9 | Post-MVP Roadmap | 长期产品扩展 |

详细计划见 `docs/dev-plan/`。

## 6. 上线验收

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
- `pnpm sora research create --thesis ai-infra` 不依赖 Pi 可运行。
- `pnpm sora notifications export` 包含 Thesis-derived events。
- API 提供 Thesis overview/detail/evidence/exposure data。
- Web 渲染 overview、thesis detail、evidence timeline、exposure。
- 所有用户可见输出保持非投顾。

## 7. 合规边界

Sora 不是投顾系统。

禁止：

- 买入 / 卖出指令
- 个性化仓位建议
- 保证收益语言
- 自动交易
- 跟单交易

允许：

- 市场信息
- 指数信息
- 国内基金映射
- 执行质量分析
- 溢价 / 申购状态风险提示
- thesis evidence summary
- confidence history
- contradiction analysis
- research cards
- notification events
- follow-up research tasks

所有用户可见内容必须使用信息分析和风险提示语气。

