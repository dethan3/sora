# Phase 5 — `packages/market` + `packages/fund`

## 目标

实现核心业务逻辑：市场与指数查询，基金映射，基金评分与风险分析。

## 估算

1.5 个 session

## 依赖

Phase 4（sources 适配器可用）

---

## packages/market

### 职责

- 管理市场（Market）和指数（Index）的查询
- 从 sources 获取行情数据，返回标准化结果

### 对外 API

```typescript
// 查询所有市场
listMarkets(): Promise<Market[]>

// 查询单个市场
getMarket(id: string): Promise<Market | null>

// 查询某市场的所有指数
listIndexesByMarket(marketId: string): Promise<Index[]>

// 查询所有指数
listIndexes(): Promise<Index[]>

// 查询单个指数（含实时行情）
getIndexWithQuote(indexId: string): Promise<IndexWithQuote>
```

### 数据流

```
CLI 请求 → market 包 → sources (YahooFinanceSource) → 缓存 → 返回
                     ↘ seeds (SeedSource, 离线/测试)
```

---

## packages/fund

### 职责

- 基金映射查询（按指数/市场）
- 基金指标获取（从 sources）
- 基金分析评分（纯函数，可测试）
- 风险提示生成

### 对外 API

```typescript
// 查询某指数的所有映射基金（含指标快照）
getFundsByIndex(indexId: string): Promise<FundWithMetrics[]>

// 查询某市场的所有基金
getFundsByMarket(marketId: string): Promise<FundWithMetrics[]>

// 对一组基金做横向分析
analyzeFunds(funds: FundWithMetrics[]): Promise<FundAnalysis[]>

// 对单个基金评分
scoreFund(fund: Fund, metrics: FundMetricsSnapshot): FundAnalysis
```

### 评分函数（纯函数）

```typescript
// 各子项评分：0~100
costScore       = 费率评分（越低越好，0.5% 以下满分）
liquidityScore  = 流动性评分（规模 + 成交量）
premiumRiskScore = 溢价率评分（越高溢价越低分）
trackingScore   = 跟踪误差评分（越低越好）
riskScore       = 风险评分（sharpe + maxDrawdown + volatility）

executionQualityScore =
  costScore * 0.20
  + liquidityScore * 0.20
  + premiumRiskScore * 0.25
  + trackingScore * 0.20
  + riskScore * 0.15
```

### 风险提示规则

| 条件 | 提示 | 级别 |
|------|------|------|
| `premiumRate > 3%` | 高溢价风险，场内买入需谨慎 | warning |
| `premiumRate > 1%` | 存在一定溢价，注意场内价格 | watch |
| `purchaseStatus = suspended` | 暂停申购 | warning |
| `purchaseStatus = limited` | 限购，申购上限 {purchaseLimit} | watch |
| `scale < 2亿` | 规模偏小，存在清盘风险 | warning |
| `trackingError > 2%` | 跟踪误差偏高 | watch |
| `maxDrawdown > 50%` | 历史最大回撤较大 | info |
| `volatility > 30%` | 波动率较高 | info |

**注意**：所有提示使用信息分析语气，不输出买卖建议。

---

## Done 条件

- [x] `listMarkets()` 返回 6 个市场（seeds 已添加 `global` 市场，单测通过）
- [x] `listIndexesByMarket('us-tech')` 返回 Nasdaq 100（单测通过）
- [~] `getFundsByIndex('nasdaq-100')` 返回真实基金数据（来自 EastMoneyFundSource）
  - 已实现：`FundService.getFundsByIndex()` 调用 `IFundDataSource.getFundMetrics()` 接口
  - 单测使用 `SeedSource` 通过；**与 `EastMoneyFundSource` 的集成测试待 Phase 9 覆盖**
- [x] `scoreFund()` 是纯函数，单测覆盖：
  - [x] 正常评分计算
  - [x] 高溢价场景（`ELEVATED_PREMIUM` 1-3% + `HIGH_PREMIUM` >3%）
  - [x] 限购场景（`PURCHASE_LIMITED`）
  - [x] 小规模场景（`SMALL_SCALE`）
  - [x] 高跟踪误差场景（`HIGH_TRACKING_ERROR`）
  - [x] 暂停申购场景（`PURCHASE_SUSPENDED`）
  - [x] 高波动率场景（`HIGH_VOLATILITY`）（7 个风险场景全覆盖）
  - [x] `LARGE_DRAWDOWN` 场景（计划外，额外覆盖）
- [x] `analyzeFunds()` 可对多基金排序输出横向对比（单测验证降序排列）

> 实现备注：`scoreFund` 返回 `FundAnalysis` 包含 6 个子项分数（`costScore`、`liquidityScore`、`premiumRiskScore`、`trackingScore`、`riskScore`、`executionQualityScore`）；`FundService` 依赖 `IFundDataSource` 接口，通过工厂模式在生产环境切换为 `EastMoneyFundSource`。
