# Phase 7 — `packages/storage` SQLite + Drizzle

## 目标

建立持久化层，支持 `db init` 和 `db seed`，为查询命令提供可选的本地存储能力。

## 估算

1 个 session

## 依赖

Phase 3（seed 数据已准备）

> 可与 Phase 6 并行开发，两者无直接依赖。

---

## 设计原则

- 数据库是**可选**的持久化层，不是唯一数据来源
- CLI 命令可以直接走 sources（API + cache），也可以优先查 SQLite
- `db seed` 将 `data/seeds/` 和 `data/cache/` 中的数据导入数据库
- 数据库文件路径通过 `SORA_DB_PATH` 配置（默认 `./data/sora.db`）

---

## Drizzle Schema（8 张表）

```typescript
// markets
markets: { id, name, category, description, createdAt }

// indexes
indexes: { id, name, marketId, ticker, description, createdAt }

// funds
funds: {
  id, fundCode, fundName, fundType, marketId, trackingIndexId,
  manager, fee, scale, inceptionDate,
  isEtf, isEtfFeeder, isQdii,
  purchaseStatus, purchaseLimit,
  dataSource, updatedAt
}

// fund_index_mappings
fund_index_mappings: { id, fundId, indexId, isPrimary, createdAt }

// fund_metrics_snapshots
fund_metrics_snapshots: {
  id, fundId, nav, price, premiumRate, volume, turnover,
  sharpeRatio, maxDrawdown, volatility, trackingError,
  return1m, return3m, return6m, return1y, return3y,
  snapshotDate, dataSource, createdAt
}

// research_cards
research_cards: {
  id, title, marketId, relatedIndexIds, relatedFundIds,
  summary, keyEvidence, fundExecutionRisks,
  marketImplication, risks, invalidationConditions,
  status, generatedAt, createdAt
}

// alerts
alerts: { id, level, title, fundId, message, createdAt }

// notification_events
notification_events: {
  id, level, title, summary, source, type,
  relatedEntityType, relatedEntityId, payload, createdAt
}
```

---

## 实现的操作

### db:init

```
1. 检查 SORA_DB_PATH 目录是否存在，不存在则创建
2. 初始化 SQLite 文件
3. 运行 Drizzle migrations，创建所有表
4. 输出：✅ Database initialized at ./data/sora.db
```

### db:seed

```
1. 读取 data/seeds/markets.json → 插入 markets 表
2. 读取 data/seeds/indexes.json → 插入 indexes 表
3. 读取 data/seeds/funds.json → 插入 funds 表
4. 读取 data/seeds/mappings.json → 插入 fund_index_mappings 表
5. 读取 data/seeds/fund-metrics.json → 插入 fund_metrics_snapshots 表
6. 同时从 data/cache/ 导入已缓存的真实数据（可选，--with-cache flag）
7. 输出导入统计
```

### 查询封装

所有包都可以选择性使用 storage，也可以直接用 sources。storage 提供：

```typescript
// packages/storage/src/queries/
findMarketById(id: string): Promise<Market | null>
listAllMarkets(): Promise<Market[]>
listIndexesByMarket(marketId: string): Promise<Index[]>
findFundsByIndex(indexId: string): Promise<Fund[]>
getLatestFundMetrics(fundId: string): Promise<FundMetricsSnapshot | null>
saveResearchCard(card: ResearchCard): Promise<void>
saveNotificationEvents(events: NotificationEvent[]): Promise<void>
```

---

## Done 条件

- [x] `dbInit(dbPath)` 成功创建 SQLite 文件和所有 8 张表（幂等，重复调用不报错）
- [x] `dbSeed(db, seedsDir)` 成功导入 seed 数据，返回正确统计（6 markets / ≥6 indexes / ≥7 funds / mappings / metrics）
- [x] 8 张表的 Drizzle schema 与 core 类型字段一致（markets/indexes/funds/fund_index_mappings/fund_metrics_snapshots/research_cards/alerts/notification_events）
- [x] 查询函数全部实现，可供 CLI 使用：`findMarketById`、`listAllMarkets`、`listIndexesByMarket`、`findFundsByIndex`、`findFundsByMarket`、`getLatestFundMetrics`、`saveResearchCard`、`saveNotificationEvents`
- [x] 单测 15 个 tests 全部通过（in-memory SQLite，覆盖 init / seed / 所有查询路径）
- [~] `pnpm sora db init/seed` CLI 命令待 Phase 8 接入

> 实现备注：`better-sqlite3` 在 Node.js v25.2.1 需从源码编译（无预编译包），已用 `npx node-gyp rebuild` 完成。后续 CI 需确保编译环境可用。
