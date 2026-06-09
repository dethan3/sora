# Phase 6 — Pi Agent + 搜索 + Research + Notifier

## 目标

真实接入 Pi Agent，添加 Tavily 搜索工具，实现研究卡片生成链路和通知事件生成。

## 估算

2 个 session

## 依赖

Phase 5（market + fund 包可用）

---

## packages/agent — Pi 集成

### 接口定义

```typescript
interface ResearchAgent {
  generateResearchCard(input: ResearchCardInput): Promise<ResearchCard>
  summarizeMarketSignal(signals: ResearchSignal[]): Promise<string>
  explainTransmissionPath(marketId: string): Promise<string>
  generateFollowUpTasks(card: ResearchCard): Promise<string[]>
}

interface ResearchCardInput {
  market: Market
  indexes: Index[]
  fundAnalyses: FundAnalysis[]
  searchResults?: SearchResult[]   // 来自 Tavily 的补充信息
}
```

### Pi 接入实现

**配置**（通过 `.env`）：

```
SORA_PI_API_KEY=your-pi-api-key
SORA_PI_BASE_URL=https://api.pi.ai/v1   # 或实际地址
```

**实现要点**：

- `PiResearchAgent` 实现 `ResearchAgent` 接口
- 调用 Pi API 时传入结构化 prompt，格式参考 `docs/research-card.md` 中的卡片模板
- Pi 返回结果用 Zod 验证，确保字段符合 `ResearchCard` schema
- 如果 Pi 响应格式不满足 schema，fallback 到确定性生成器（见 research 包）
- 所有 Pi 调用需记录到 `data/cache/agent-responses/` 中（便于调试）

**Prompt 设计原则**：

- 明确告知 Pi：只做信息分析，不输出买卖建议
- 提供结构化输入（JSON format）
- 要求 Pi 输出结构化 JSON（ResearchCard schema）
- 在 system prompt 中嵌入合规边界声明

### 搜索工具集成（Tool Use）

Pi 在生成研究卡片时可调用搜索工具交叉验证：

```typescript
interface SearchTool {
  name: 'tavily_search'
  description: '搜索互联网获取最新市场信息，用于验证基金数据准确性'
  execute(query: string): Promise<SearchResult[]>
}
```

**典型使用场景**：

- 验证基金申购状态是否与东方财富接口一致
- 查询指数最新成分股调整
- 获取宏观事件背景信息

---

## packages/research — 研究卡片生成

### 双模式设计

**Mode 1: Pi Agent（默认）**

输入 → 调用 `PiResearchAgent.generateResearchCard()` → 输出

**Mode 2: 确定性生成器（fallback / 离线 / 测试）**

当 Pi 不可用（无 API key、网络错误、schema 验证失败）时自动 fallback：

```typescript
// 确定性生成流程
function generateDeterministicCard(input: ResearchCardInput): ResearchCard {
  const { market, indexes, fundAnalyses } = input

  const keyEvidence = buildKeyEvidence(fundAnalyses)
  const fundExecutionRisks = extractFundRisks(fundAnalyses)
  const status = determineStatus(fundAnalyses)

  return {
    title: `${market.name} 国内基金执行质量观察`,
    summary: buildSummary(market, indexes, fundAnalyses),
    keyEvidence,
    fundExecutionRisks,
    marketImplication: buildImplication(market),
    risks: extractAllRisks(fundAnalyses),
    invalidationConditions: DEFAULT_INVALIDATION_CONDITIONS,
    status,
    ...
  }
}
```

### ResearchCard 输出风格

严格遵循 PRD 中的示例结构。输出为信息分析语气，禁止买卖建议。

---

## packages/notifier — 通知事件生成

### 职责

- 基于 ResearchCard 和 FundAnalysis 生成 NotificationEvent
- 第一版只支持 JSON 导出（不发送 Telegram / 飞书 / 邮件）
- 后续由 Tickeye 消费

### 生成规则

```typescript
// 从 FundAnalysis 生成事件
fundAnalysis.warnings → NotificationEvent[]

// 从 ResearchCard 生成事件
researchCard.status === 'active_watch' → level: 'watch'
researchCard.status === 'confirmed' → level: 'warning'
```

### 导出格式

```json
{
  "exportedAt": "2024-06-01T10:00:00Z",
  "events": [
    {
      "id": "evt-001",
      "level": "warning",
      "title": "159941 存在高溢价风险",
      "summary": "易方达纳指 ETF 当前溢价率为 4.2%，超过 3% 警戒线。",
      "source": "sora",
      "type": "premium_risk",
      "relatedEntityType": "fund",
      "relatedEntityId": "159941",
      "payload": { "premiumRate": 0.042 },
      "createdAt": "2024-06-01T10:00:00Z"
    }
  ]
}
```

---

## Done 条件

- [x] `PiResearchAgent` 实现完整 Pi API 调用（OpenAI 兼容格式），Zod 验证返回结果；生产使用需配置 `SORA_PI_API_KEY`
- [x] Pi 调用失败时自动 fallback 到 `generateDeterministicCard`（`ResearchService` 统一封装）
- [x] `TavilySearchSource` 在 Phase 4 已实现；`ResearchCardInput.searchResults` 字段已接入，供 Pi prompt 使用
- [~] `sora research create --market us-tech` 命令待 Phase 8（CLI 开发阶段）
- [x] `NotificationEvent` JSON 导出：`exportEvents()` 写入文件，格式符合 Tickeye 消费规范
- [x] 单测：确定性生成器 8 个测试全部通过（状态逻辑、keyEvidence、fundExecutionRisks）
- [x] 集成测试：Pi 路径用 `vi.stubGlobal('fetch', ...)` 模拟，3 个测试全部通过（正常路径、HTTP 错误、schema 验证失败）
- [x] 通知事件生成：10 个测试通过，覆盖 fromFundAnalysis / fromResearchCard / collectEvents 全部路径
