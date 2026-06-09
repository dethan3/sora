# Phase 2 — `packages/core` 领域模型

## 目标

定义全项目唯一的类型层。所有包围绕这些类型实现，不得各自定义冲突类型。

## 估算

1 个 session

## 依赖

Phase 1

---

## 工作内容

### 需要定义的 Zod schema + TypeScript 类型（12 个）

| 类型 | 说明 |
|------|------|
| `MarketCategory` | 枚举：us / hk / cn / commodity / global |
| `Market` | 市场（id, name, category, description） |
| `Index` | 指数（id, name, marketId, ticker, description） |
| `FundType` | 枚举：etf / etf_feeder / qdii / lof / mutual_fund |
| `PurchaseStatus` | 枚举：open / limited / suspended / unknown |
| `Fund` | 基金完整信息（见 PRD 字段列表） |
| `FundMapping` | 基金与指数的映射关系（fundId, indexId, isPrimary） |
| `FundMetricsSnapshot` | 基金指标快照（见 PRD 字段列表） |
| `FundAnalysis` | 基金分析结果（6 项评分 + warnings + summary） |
| `ResearchSignal` | 研究信号（原始观察点） |
| `ResearchCard` | 研究卡片（见 PRD 字段列表，含 status 枚举） |
| `Alert` | 警报（id, level, title, fundId, message, createdAt） |
| `NotificationEvent` | 通知事件（见 PRD 字段列表，含 level 枚举） |

### 关键字段备注

**Fund 字段**（来自 PRD）：
```typescript
id, fundCode, fundName, fundType, marketId, trackingIndexId,
manager, fee, scale, inceptionDate,
isEtf, isEtfFeeder, isQdii,
purchaseStatus, purchaseLimit,
dataSource, updatedAt
```

**FundMetricsSnapshot 字段**（来自 PRD）：
```typescript
id, fundId, nav, price, premiumRate, volume, turnover,
sharpeRatio, maxDrawdown, volatility, trackingError,
return1m, return3m, return6m, return1y, return3y,
snapshotDate, dataSource
```

**ResearchCard.status 枚举**：
`ignore | watch | active_watch | confirmed | invalidated`

**NotificationEvent.level 枚举**：
`info | watch | warning | critical`

### 导出结构

```
packages/core/src/
  market.ts
  index.ts
  fund.ts
  fund-metrics.ts
  fund-analysis.ts
  research.ts
  alert.ts
  notification.ts
  index.ts   ← 统一 re-export
```

---

## Done 条件

- [x] `packages/core/src/index.ts` 导出所有 12 个类型（含所有枚举和子类型）
- [x] 每个 schema 都有 Zod `.parse()` 验证能力
- [x] 单测：40 个 tests 全部通过，每个 schema ≥ 1 valid + 1 invalid case
- [x] 其他包可通过 `import { Fund } from '@sora/core'` 引用

> 实现备注：`Index` schema 文件命名为 `index-schema.ts`（避免与 barrel `index.ts` 冲突）；`purchaseLimit` 使用 `.nullable().optional()` 以区分"无限购"(null) 与"未知"(undefined)。
