# Sora PRD（中文版）

## 1. 产品定义

Sora 是一个面向中国投资者的 Thesis-first 全球指数基金研究系统。

它帮助用户持续维护长期市场观点，将这些观点连接到全球市场、代表性指数和国内可投资基金，追踪支持与反对证据，并理解市场认知如何随时间变化。

Sora 不是荐基工具，也不是投顾系统。它不告诉用户买什么或卖什么。它的职责是保存推理过程、暴露矛盾、映射资产暴露，并生成结构化研究输出。

## 2. 一句话定位

英文：

Sora helps Chinese investors manage market theses, map them to global indexes and domestic index funds, and track evidence-driven changes through CLI and Web experiences.

中文：

Sora 是一个面向中国投资者的 Thesis-first 全球指数基金研究系统，用于持续管理市场观点、连接全球指数与国内可买基金、追踪证据变化，并通过 CLI 和网站两种方式呈现结构化研究结果。

## 3. 核心产品转向

旧产品链路是：

```text
Market -> Index -> Domestic Fund -> Fund Analysis -> Research Card -> Alert
```

新产品链路是：

```text
Thesis
  -> Evidence
  -> Confidence Update
  -> Market / Index Mapping
  -> Domestic Fund Exposure
  -> Research Card
  -> NotificationEvent
```

市场、指数、基金分析、研究卡片和通知仍然重要，但它们不再是顶层产品模型，而是围绕 Thesis 提供支撑的能力层。

## 4. Thesis 是什么？

Thesis 是一个可验证的投资假设，不是一句观点口号。

不合格示例：

```text
AI 很强，美股会涨。
```

合格示例：

```text
未来 3-5 年，AI 基础设施资本开支仍将保持高位，因此纳斯达克 100、半导体指数、数据中心相关资产，以及国内 ETF / QDII 产品会持续受到估值和盈利预期支撑。
```

一个 Thesis 必须包含：

- 标题
- 摘要
- 时间周期
- 因果链
- 关键假设
- 受影响的市场 / 指数 / 基金
- 支持证据
- 反对证据
- 信心分数
- 更新历史
- 失效条件

产品要回答的问题是：

```text
我当初为什么相信这个判断？现在这个理由还成立吗？
```

## 5. Thesis Engine 作为 Agentic Finance Primitive

Sora 是构建在 Thesis Engine 之上的一个具体应用。

更通用的抽象是：

```text
Sora = Thesis Engine 在全球指数基金研究场景下的应用
```

Thesis Engine 本身应该被视为一种可迁移的 Agentic Finance primitive。它可以脱离 Sora 独立存在，也可以应用在其他金融和商业推理场景中。

这个可复用 primitive 包括：

- Thesis
- Evidence
- Confidence Update
- Contradiction
- Exposure
- Action Boundary
- Review Loop

它可以应用于：

- 指数基金研究
- 个股研究
- 宏观策略
- 加密资产
- 私募 / VC deal memo
- 公司经营判断
- 风险监控
- 投资委员会复盘

Agentic Finance 的核心不是“AI 给投资建议”。真正的核心是：

```text
Agent 持续管理假设、证据、反证、信心变化、资产暴露和行动边界。
```

因此，Sora 更准确的描述应该是：

```text
一个由 Thesis Engine 驱动的全球指数基金研究产品。
```

Thesis Engine 是底层认知框架。Sora 是面向中国投资者研究全球指数基金和国内基金暴露的垂直产品实现。

## 6. 产品原则

### 6.1 Thesis 是操作系统

Thesis 不应该只是一个二级菜单，而应该是 Sora 的主要组织方式。

网站首页和 CLI 默认概览应该回答：

```text
当前市场认知发生了什么变化？
```

而不是：

```text
今天哪个基金涨了？
```

### 6.2 证据比新闻更重要

Sora 不应该只是新闻流。

每一条信息进入系统时，Sora 都要问：

```text
这条信息支持、削弱，还是不影响某个 Thesis？
```

每条证据必须记录：

- 来源
- 标题
- 摘要
- 关联 Thesis
- 方向：support / against / neutral
- 强度：weak / medium / strong
- 信心变化
- 理由
- 创建时间

### 6.3 矛盾是一等能力

每个 Thesis 都应该同时展示：

```text
支持证据
反对证据
开放问题
失效条件
```

这是 Sora 的核心差异点。Sora 必须避免变成只强化既有观点的信息茧房。

### 6.4 基金映射是 Sora 的特殊价值

一般研究工具可以停留在市场或公司观点，但 Sora 必须继续连接到国内基金暴露。

对每个 Thesis，Sora 应该回答：

```text
哪些指数和国内基金暴露在这个 Thesis 下？
暴露强度是多少？
执行风险是什么？
```

示例：

```text
AI Infrastructure Thesis

Nasdaq 100                 exposure 85%
S&P 500                    exposure 53%
Domestic Nasdaq ETF         exposure 80%
Domestic QDII feeder fund   exposure 65%
```

## 7. 交互模型

Sora 有两个一等交互界面：

```text
apps/cli
apps/web
```

两者必须调用同一套领域服务。CLI 和 Web 都不应该拥有无法复用的业务逻辑。

### 7.1 CLI

CLI 面向：

- 高阶用户
- 日常复盘
- 脚本化
- 自动化
- 本地优先工作流
- 为 Tickeye 或其他系统导出 JSON

核心命令：

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

Web 面向：

- 可视化 Market Map
- Thesis Card
- Evidence Timeline
- 矛盾审查
- 资产暴露热力图
- 交互式研究阅读
- 基于 Thesis 上下文的 Agent 对话

核心视图：

- Market Map
- Thesis List
- Thesis Detail
- Evidence Timeline
- Asset Exposure
- Research Cards
- Agent Console
- Settings

Web 应该消费 API / 领域服务，不应该重新实现分析逻辑。

## 8. Agent 架构

Agent 能力必须和表现层解耦。

Agent 不是 CLI，也不是 Web UI。它是一层可复用的服务，CLI 和 Web 都可以调用。

```text
CLI ----\
        -> Application Services -> Agent Services -> Pi / Tools
Web ----/
```

### 8.1 确定性逻辑

以下能力必须保留为确定性的 TypeScript 逻辑：

- schema 校验
- 市场 / 指数 / 基金模型
- 基金映射
- 基金评分
- 暴露度评分
- 证据持久化
- 信心更新规则
- NotificationEvent 生成
- 存储查询
- 合规过滤

### 8.2 Agent 辅助逻辑

Agent 可以辅助：

- 总结证据
- 判断证据方向
- 解释因果链
- 识别矛盾
- 生成研究卡片
- 基于 Thesis 上下文回答问题
- 生成后续研究任务

### 8.3 Agent 边界

Agent 输出必须是结构化的，并且必须通过 schema 校验。

Agent 不得：

- 建议买入或卖出
- 输出个性化仓位建议
- 承诺收益
- 声称确定性
- 绕过确定性评分规则
- 在没有证据记录的情况下静默改变信心分数

如果 Agent 输出无法通过 schema 校验，Sora 必须降级到确定性输出。

## 9. 合规边界

Sora 不是投资顾问。

禁止输出：

- 买入 / 卖出指令
- 个性化仓位建议
- 保证收益语言
- 必涨 / 必跌判断
- 自动交易
- 实盘跟投
- 针对用户资产的组合建议

允许输出：

- 市场信息
- 指数信息
- 国内基金映射
- 执行质量分析
- 溢价 / 申购状态风险提示
- Thesis 证据摘要
- 信心历史
- 矛盾分析
- 研究卡片
- 通知事件
- 后续研究任务

所有输出必须使用“信息分析”和“风险提示”语气。

允许：

```text
该基金当前存在较高溢价风险，作为执行工具需要谨慎评估。
```

不允许：

```text
今天买入这只基金。
```

## 10. 核心领域模型

### 10.1 现有模型

Sora 保留现有模型：

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

### 10.2 新增 Thesis 模型

Sora 新增：

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

## 11. 包架构

目标 monorepo：

```text
apps/
  cli/
  web/
  api/
  worker/

packages/
  core/
  thesis/
  market/
  fund/
  research/
  agent/
  sources/
  storage/
  notifier/
  shared/
```

职责：

- `packages/core`：共享 Zod schema 和 TypeScript 类型
- `packages/thesis`：Thesis 生命周期、证据规则、信心更新、暴露映射
- `packages/market`：市场和指数查询
- `packages/fund`：基金映射和执行质量分析
- `packages/research`：基于 Thesis 上下文生成研究卡片
- `packages/agent`：可复用 Agent 接口、Pi / 工具适配器
- `packages/sources`：Yahoo / EastMoney / Tavily / seed 数据适配器
- `packages/storage`：SQLite 持久化和查询
- `packages/notifier`：NotificationEvent 生成和导出
- `apps/cli`：命令行界面
- `apps/api`：供 Web 和外部系统使用的 HTTP API
- `apps/web`：可视化界面
- `apps/worker`：定时采集和复盘任务

## 12. 主要用户流程

### 12.1 日常复盘

```text
用户打开 Sora
-> 查看最强 / 最弱 / 变化最大的 Thesis
-> 查看证据时间线
-> 查看信心变化
-> 查看受影响基金
```

CLI：

```bash
pnpm sora thesis review
```

Web：

```text
Market Map -> Changed Theses -> Evidence Timeline
```

### 12.2 添加证据

```text
用户或 Worker 添加新证据
-> Sora 判断方向和强度
-> 信心分数变化
-> 创建更新记录
-> 必要时生成通知
```

### 12.3 Thesis 详情

对单个 Thesis，用户可以看到：

- 当前判断
- 信心分数
- 因果链
- 关键假设
- 支持证据
- 反对证据
- 资产暴露
- 相关研究卡片
- 失效条件

### 12.4 基金暴露

用户提问：

```text
我持有纳指基金，哪些 Thesis 对它影响最大？
```

Sora 回答：

```text
AI Infrastructure       exposure 85%
US Tech Valuation       exposure 82%
USD Liquidity           exposure 76%
```

## 13. MVP 范围

当前实现之后的下一个 MVP 应包含：

- `packages/core` 中的 Thesis schemas
- `packages/thesis`
- Thesis seed 数据
- CLI Thesis 命令
- thesis / evidence / updates / exposure 存储表
- 确定性信心更新规则
- 基于 Thesis 上下文生成 ResearchCard
- 基于 Thesis updates 生成 NotificationEvent
- Thesis-first Web 骨架
- Web API endpoints

MVP 不需要：

- 组合管理
- 自动交易
- 真实券商集成
- 个性化配置建议
- 复杂图表
- 实时流式行情

## 14. Web MVP

Web MVP 应该是可用的产品界面，不是营销落地页。

首屏：

```text
Market Cognition

Changed Theses
Strongest Theses
Challenged Theses
Recent Evidence
```

必需视图：

- `/` Market Map / Thesis Overview
- `/theses` Thesis List
- `/theses/:id` Thesis Detail
- `/theses/:id/evidence` Evidence Timeline
- `/theses/:id/exposure` Asset Exposure
- `/agent` Agent Console

## 15. CLI MVP

必需命令：

```bash
pnpm sora thesis list
pnpm sora thesis show <id>
pnpm sora thesis review
pnpm sora thesis exposure <id>
pnpm sora evidence add --thesis <id> --direction support --strength medium --title "..."
pnpm sora research create --thesis <id>
pnpm sora notifications export
```

所有 CLI 输出末尾必须包含合规免责声明。

## 16. 数据源

Sora 继续使用：

- Yahoo Finance：市场 / 指数行情
- EastMoney / 天天基金：国内基金数据
- Tavily：搜索和证据发现
- seed 数据：离线测试和确定性演示

数据源失败必须显式展示。命令不能在所有上游请求失败时仍然报告成功。

## 17. 验收标准

产品验收：

- Thesis 是主要组织模型。
- CLI 和 Web 都消费共享服务。
- Agent 能力与 CLI / Web 解耦。
- 每次信心变化都有证据。
- 每个 Thesis 都展示支持和反对证据。
- 每个 Thesis 都能映射到受影响指数和基金。
- 可以基于 Thesis 上下文生成 ResearchCard。
- 可以基于 Thesis updates 生成 NotificationEvent。
- 所有输出保持合规、非投顾。

技术验收：

- `pnpm install` 成功。
- `pnpm test` 成功。
- `pnpm build` 成功。
- `pnpm lint` 成功。
- `pnpm sora thesis list` 可运行。
- `pnpm sora thesis show ai-infra` 可运行。
- `pnpm sora thesis review` 可运行。
- `pnpm sora research create --thesis ai-infra` 可运行。
- Web app 可本地启动并渲染 Thesis overview。
- API 暴露 Web 消费的 Thesis endpoints。
- Storage 支持 thesis / evidence / updates / exposure。
