# Data Sources

> 以下接口均为免费接口，已在 Phase 3 调研验证。

---

## 1. 美国 / 港股指数行情 — yahoo-finance2

**npm 包**：`yahoo-finance2`（无需 API key）

**用法**：
```ts
import yahooFinance from 'yahoo-finance2'
const quote = await yahooFinance.quote('^NDX')
```

### 支持的指数 Ticker

| 指数 | Ticker | 备注 |
|------|--------|------|
| Nasdaq 100 | `^NDX` | 正常 |
| 标普 500 | `^GSPC` | 正常 |
| 恒生科技 | `^HSTECH` | 正常 |
| 恒生指数 | `^HSI` | 正常 |
| 沪深 300 | `000300.SS` | 覆盖有限，建议仅作参考 |

### 主要返回字段（quote）

| 字段 | 含义 |
|------|------|
| `regularMarketPrice` | 最新价格 |
| `regularMarketChangePercent` | 涨跌幅 |
| `regularMarketVolume` | 成交量 |
| `fiftyTwoWeekHigh/Low` | 52 周高低点 |
| `marketCap` | 市值（股票适用） |

**限制**：非官方接口，Yahoo 可能随时限流或调整字段结构；A 股数据覆盖不完整。

---

## 2. 国内基金数据 — 天天基金（东方财富）非官方接口

**无需 API key**。全部为 HTTP GET，部分返回 JSONP（需用 `parseJsonp` 工具解析）。

**建议请求间隔**：≥ 500ms，批量请求使用队列控制。

### 接口列表

#### 2-1 基金实时估值（当日净值估算）

```
GET http://fundgz.1234567.com.cn/js/{code}.js
```

- **格式**：JSONP（`jsonpgz({...})`）
- **关键字段**：`fundcode`, `name`, `jzrq`（净值日期）, `dwjz`（单位净值）, `gsz`（估算净值）, `gszzl`（估算涨跌幅）
- **示例**：`http://fundgz.1234567.com.cn/js/159941.js`

#### 2-2 基金历史净值

```
GET https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={code}&page=1&per=20
```

- **格式**：标准 JSON
- **关键字段**：`Data.LSJZList[].FSRQ`（日期）, `DWJZ`（单位净值）, `JZZZL`（净值增长率）

#### 2-3 基金基本信息

```
GET https://fundmobapi.eastmoney.com/FundMApi/FundVarietieValuationDetail.ashx
    ?FCODE={code}&deviceid=x&version=6.3.8&plat=Iphone&product=EFund&serverversion=6.3.8
```

- **格式**：标准 JSON
- **关键字段**：`Datas.FTYPE`（基金类型）, `JJGS`（基金公司）, `DWJZ`（净值）, `FEGM`（规模亿元）

#### 2-4 基金列表搜索

```
GET https://fund.eastmoney.com/data/rankhandler.aspx
    ?op=ph&dt=kf&ft=all&rs=&gs=0&sc=6yzf&st=desc&sd=2017-01-01&ed=2024-01-01
    &qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1
```

- **格式**：JS 变量赋值（`var rankData = {...}`），用 `parseJsonp` 解析
- **用途**：按指数代码搜索跟踪某一指数的全部基金

### ETF 溢价率计算

ETF 场内价格通过 Yahoo Finance 获取（ticker 格式：`{code}.SH` 或 `{code}.SZ`），与东方财富 NAV 对比：

```
premiumRate = (场内价格 - 单位净值) / 单位净值
```

---

## 3. 互联网搜索 — Tavily

**npm 包**：`@tavily/core`
**配置**：`TAVILY_API_KEY`（免费 tier：1000 次/月）

**用途**：
- 交叉验证 API 返回的数据（如基金规模、申购状态）
- 补充宏观信息（如"恒生科技成分股最新调整"）
- Pi Agent 研究工具

**使用示例**：
```ts
import { TavilyClient } from '@tavily/core'
const client = new TavilyClient({ apiKey: process.env.TAVILY_API_KEY })
const result = await client.search('纳指100 国内基金 申购限制 2024')
```

---

## 缓存目录结构

```
data/cache/
  market-quotes/      ← Yahoo Finance 指数行情（TTL: 1 天）
  fund-details/       ← 东方财富基金基本信息（TTL: 7 天）
  fund-nav-history/   ← 东方财富历史净值（TTL: 1 天）
  search-results/     ← Tavily 搜索结果（手动触发，不自动过期）
```

缓存文件格式：
```json
{
  "fetchedAt": "2024-06-01T10:00:00Z",
  "source": "eastmoney | yahoo-finance | tavily",
  "data": { ... }
}
```
