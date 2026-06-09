# Phase 4 — `packages/sources` 真实 API 适配器

## 目标

实现数据源抽象层，将 Yahoo Finance 和东方财富接口封装为统一接口，供 market / fund 包调用。

## 估算

1 个 session

## 依赖

Phase 3（接口已验证，缓存层工具已准备）

---

## 接口设计

所有适配器实现统一接口，后续新数据源只需实现接口即可替换：

```typescript
// 市场行情数据源
interface IMarketQuoteSource {
  getIndexQuote(ticker: string): Promise<IndexQuote>
  getIndexHistory(ticker: string, days: number): Promise<IndexHistoricalQuote[]>
}

// 基金数据源
interface IFundDataSource {
  getFundDetails(fundCode: string): Promise<RawFundDetails>
  getFundNavHistory(fundCode: string, days: number): Promise<RawNavRecord[]>
  getFundMetrics(fundCode: string): Promise<RawFundMetrics>
}

// 搜索数据源
interface ISearchSource {
  search(query: string, maxResults?: number): Promise<SearchResult[]>
}
```

---

## 实现的适配器

### YahooFinanceSource

- 依赖：`yahoo-finance2`
- 实现 `IMarketQuoteSource`
- 数据映射：Yahoo Finance 字段 → `@sora/core` 类型
- 缓存：读写 `data/cache/market-quotes/`

```typescript
// 字段映射示例
yahooFinance.quote('^NDX') → {
  regularMarketPrice → nav/price
  regularMarketChangePercent → 日涨跌幅
  regularMarketVolume → volume
  fiftyTwoWeekHigh/Low → 用于计算相对位置
}
```

### EastMoneyFundSource

- 依赖：Node.js `fetch`（内置）
- 实现 `IFundDataSource`
- JSONP 解析：`http://fundgz.1234567.com.cn/js/{code}.js` 返回需提取 JSON
- 字段映射：东方财富字段 → `@sora/core` 类型
- 缓存：读写 `data/cache/fund-details/` 和 `data/cache/fund-nav-history/`
- 限流：请求队列，间隔 ≥ 500ms

### TavilySearchSource

- 依赖：`@tavily/core`
- 实现 `ISearchSource`
- 配置：`TAVILY_API_KEY` 环境变量
- 缓存：写入 `data/cache/search-results/`，带时间戳文件名

### SeedSource（兜底 / 测试用）

- 依赖：无（读取本地 JSON）
- 实现 `IMarketQuoteSource` + `IFundDataSource`
- 从 `data/seeds/*.json` 读取数据并验证 schema
- 用于：测试环境、离线开发、CI

---

## 工厂模式

通过环境变量选择数据源：

```typescript
// packages/sources/src/factory.ts
function createMarketSource(env: Env): IMarketQuoteSource {
  if (env.USE_SEED_DATA) return new SeedSource()
  return new YahooFinanceSource({ cacheDir: env.SORA_CACHE_DIR })
}

function createFundSource(env: Env): IFundDataSource {
  if (env.USE_SEED_DATA) return new SeedSource()
  return new EastMoneyFundSource({ cacheDir: env.SORA_CACHE_DIR })
}
```

---

## 字段映射文档

Phase 3 产出的 `docs/data-sources.md` 中记录完整映射表。本包实现时严格按照该映射表执行。

---

## Done 条件

- [x] `YahooFinanceSource` 可返回 Nasdaq 100 行情，字段符合 core schema（4 个单测通过）
- [x] `EastMoneyFundSource` 可返回至少 3 个国内基金详情，字段符合 core schema（5 个单测通过）
- [x] `TavilySearchSource` 可返回搜索结果（3 个单测通过，vi.mock 隔离）
- [x] `SeedSource` 可从 `data/seeds/` 读取并验证（6 个单测通过）
- [x] 缓存读写正常工作（命中缓存后 fetch/quote 只调用 1 次）
- [x] 单测：18 个 tests 全部通过，全部使用 fixture 数据，无真实网络请求
