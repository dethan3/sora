# Phase 3 — 真实数据源调研 + JSON 缓存层

## 目标

确定并验证所有免费金融数据 API 的可用性，建立 JSON 缓存层，为 Phase 4 的适配器开发提供稳定基础。

## 估算

1.5 个 session

## 依赖

Phase 2（core 类型已定义）

---

## 数据源规划

### 1. 美国 / 港股指数行情 — yahoo-finance2

**npm 包**：`yahoo-finance2`（免费，无需 API key）

支持的指数 ticker：

| 指数 | Yahoo Ticker |
|------|-------------|
| Nasdaq 100 | `^NDX` |
| S&P 500 | `^GSPC` |
| 恒生科技 | `^HSTECH` 或 `3033.HK`（ETF） |
| 恒生指数 | `^HSI` |
| 沪深 300 | `000300.SS` |

获取数据：`yahooFinance.quote(ticker)` 返回最新行情；`.historical()` 返回历史数据。

**注意**：Yahoo Finance 对 A 股数据覆盖有限，CSI 300 / CSI A500 可能不完整，需测试验证。

---

### 2. 国内基金数据 — 天天基金（东方财富）非官方接口

**无需 API key**，HTTP GET 接口，返回 JSON/JSONP。

#### 接口清单

| 接口用途 | URL 模板 |
|--------|---------|
| 基金实时估值（NAV estimate） | `http://fundgz.1234567.com.cn/js/{code}.js` |
| 基金历史净值列表 | `https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={code}&page=1&per=20` |
| 基金基本信息 | `https://fundmobapi.eastmoney.com/FundMApi/FundVarietieValuationDetail.ashx?FCODE={code}&deviceid=x&version=6.3.8&plat=Iphone&product=EFund&serverversion=6.3.8` |
| 基金列表（按类型搜索） | `https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=6yzf&st=desc&sd=2017-01-01&ed=2024-01-01&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1` |
| 溢价率（ETF 场内价格） | 通过 Yahoo Finance（`{code}.SH` 或 `{code}.SZ`）获取场内价格，与 NAV 计算 |

#### 注意事项

- 以上为非官方接口，无 SLA 保证，需做 retry + timeout 处理
- 返回格式部分是 JSONP（需手动解析），部分是标准 JSON
- 限流：建议请求间隔 ≥ 500ms，批量请求加队列

---

### 3. 搜索 / 爬取 — Tavily（交叉验证用途）

**npm 包**：`@tavily/core`

**用途**：
- 当 API 数据异常时，通过搜索互联网交叉验证
- Agent 研究时补充宏观信息（如"恒生科技最新成分股调整"）
- 验证基金代码、名称、管理人信息是否准确

**配置**：`TAVILY_API_KEY`（免费 tier：1000 次/月）

---

## JSON 缓存层设计

所有爬取/搜索结果缓存到 `data/cache/`，**不立即入数据库**。

### 目录结构

```
data/cache/
  market-quotes/
    ^NDX.json
    ^GSPC.json
    ^HSI.json
  fund-details/
    159941.json    ← 纳指 ETF 易方达
    513100.json    ← 纳指 ETF 华夏
  fund-nav-history/
    159941-nav.json
    513100-nav.json
  search-results/
    2024-06-01-nasdaq100-search.json
```

### 缓存策略

| 数据类型 | 缓存有效期 | 更新触发方式 |
|--------|---------|------------|
| 指数行情 | 1 天 | CLI 命令 `sora data refresh --type market` |
| 基金详情（规模/费率/申购状态） | 7 天 | CLI 命令 `sora data refresh --type fund` |
| 基金历史净值 | 1 天 | 同上 |
| 搜索结果 | 不自动过期，手动触发 | CLI 命令 `sora data search --query "..."` |

### 缓存文件格式

每个缓存文件包含：

```json
{
  "fetchedAt": "2024-06-01T10:00:00Z",
  "source": "eastmoney | yahoo-finance | tavily",
  "data": { ... }
}
```

---

## 初始 Seed 数据（兜底）

为保证 Phase 4 之前测试可用，在 `data/seeds/` 中放置一套**手工整理的静态数据**作为兜底：

- `markets.json`
- `indexes.json`
- `funds.json`（字段尽量接近真实，至少 3 个基金/核心指数）
- `fund-metrics.json`
- `mappings.json`

Seed 数据在测试环境和离线环境使用。生产（默认）走真实 API + 缓存。

---

## 工作内容

1. **验证 yahoo-finance2 接口可用性**：写脚本测试 5 个指数 ticker 能否正常返回
2. **验证东方财富接口可用性**：测试 5 个基金代码（159941/513100/007339 等）的返回格式
3. **编写 JSONP 解析工具**：东方财富部分接口返回 `var xxx = {...}` 格式需处理
4. **设计缓存读写工具**：`packages/shared` 中提供 `readCache` / `writeCache` 函数
5. **整理 seed 数据**：人工校对字段，保证覆盖 MVP 3 个闭环
6. **文档化接口**：把可用接口、字段映射整理到 `docs/data-sources.md`

---

## Done 条件

- [x] `yahoo-finance2` 能返回 Nasdaq 100 / S&P 500 / 恒生指数实时行情（已记录到 data-sources.md）
- [x] 东方财富接口能返回至少 5 个国内基金的基本信息和净值（已记录全部接口 URL + 字段映射）
- [x] `data/cache/` 目录结构建立，缓存工具函数可用（`readCache` / `writeCache` / `isCacheStale`，14 个单测通过）
- [x] `data/seeds/` 有完整的 5 个 JSON 文件，字段符合 core schema（含 7 个基金、5 个指数、5 个市场）
- [x] `docs/data-sources.md` 记录所有接口的 URL、字段映射、限制
