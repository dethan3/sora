# Sora 2.0 技术架构与数据契约

## 1. 架构概览与设计原则

Sora 2.0 采用 **TypeScript 全栈 Monorepo（Node 24 + pnpm Workspace + React 19 + Vite 8 + Zod 4）** 架构，通过严格的数据契约将**产业投研计算**、**市场日线行情采集**与**个人本地状态管理**解耦：

1. **强类型契约驱动（Schema-First）**：所有领域数据、研究假设、行情快照与本地存储均通过 Zod 4 Schema 在模块边界完成完整性、日期顺序与唯一性校验。
2. **微卡片纯函数派生（Deterministic Derivation）**：基金配料占比（`fundIngredients`）、主题持仓汇总（`themeExposure`）、双基金同日重叠率（`equityOverlap`）以及组合近期核心影响体检（`summarizePortfolioImpacts`）均为无副作用的纯函数计算。
3. **事实、价格与估值的物理隔离**：
   - 产业与持仓数据注明原始核对日期（`reviewedAt`）与披露日（`asOf`）；
   - 二级市场日线收盘价（`fund-price`）与参考指数点位（`index`）独立标识，绝不回填篡改基金单位净值（`Fund.nav`）。

---

## 2. 目录结构与依赖方向

```text
sora/
├── apps/
│   ├── web/                          # 前端单页与 PWA 应用 (@sora/web)
│   │   ├── public/data/catalog.json  # 公开基础基金与观察方向目录
│   │   ├── scripts/
│   │   │   ├── check-research-data.mjs # 产业链与研究数据防呆校验 CLI
│   │   │   └── check-pwa.mjs         # PWA 产物、清单、图标与缓存头校验 CLI
│   │   ├── src/
│   │   │   ├── data/                 # 目录 Zod 契约与 HTTP 加载器
│   │   │   ├── components/           # 通用 UI、响应式表格 (FundTable)、顶栏与落地页
│   │   │   ├── features/
│   │   │   │   ├── domains/          # 产业主题模型 (ai-storage, ai-chips)、配料表与影响体检
│   │   │   │   ├── research/         # 研究假设模型、正反方拔河卡与三态复盘持久化
│   │   │   │   ├── discover/         # 观察方向筛选与独立本地关注状态
│   │   │   │   ├── funds/            # 基金目录、2~3 只基金并排 PK 台与详情页
│   │   │   │   ├── market/           # 行情 Provider、走势图组件与快照缓存
│   │   │   │   ├── watchlist/        # 关注列表、研究检查点与关注组合影响体检
│   │   │   │   ├── portfolio/        # 分币种持仓表单、BigInt 定点估值与未保存变更拦截
│   │   │   │   └── local/            # 本地存储并发检测、备份导入导出与损坏恢复
│   │   │   ├── navigation/           # 站内安全返回路径白名单、滚动位置与焦点恢复
│   │   │   └── pwa/                  # Service Worker 生命周期、安装引导与升级确认
│   │   └── wrangler.jsonc            # Cloudflare Workers Static Assets 部署配置
│   └── data-service/                 # 本地行情采集与快照 API 服务 (@sora/data-service)
│       └── src/
│           ├── instruments.ts        # 指数与基金采集映射定义
│           ├── providers.ts          # AKTools 主采集器与可选备用适配器
│           ├── update.ts             # 序列校验、日期防回退与部分失败降级编排
│           ├── storage.ts            # 独占文件锁 (.lock) 与原子写入替换
│           ├── server.ts             # 本地 HTTP 快照服务 (/api/market, /health, ETag)
│           └── cli.ts                # update / serve / export 命令行入口
├── packages/
│   └── market-data/                  # 前后端共享的行情快照 Schema 与计算 (@sora/market-data)
└── services/
    └── aktools/                      # 独立 Python 3.13 AKTools (AKShare HTTP API) 服务与容器配置
```

**模块依赖方向**：
`React 路由与页面 → Feature 业务组件与纯计算模型 → Zod 数据契约 (@sora/market-data / 本地 Schema) → 静态资料 / 本地存储 / 快照 API`。

---

## 3. 产业领域与研究引擎架构

### 3.1 多主题产业链模型（`features/domains/`）

- **数据集**：`ai-storage.json`（AI 存储产业链）与 `ai-chips.json`（AI 芯片与设备）。
- **Schema 校验（`domainSchema`）**：
  - 强制要求唯一公司代码、唯一事件 ID、唯一持仓快照与唯一 `https://` 披露来源；
  - 校验公司所属产业链节点（`nodeId`）存在于当前主题的 `nodes` 列表；
  - 校验所有事件 `publishedAt <= reviewedAt`、持仓快照 `asOf <= reviewedAt`；
  - 校验持仓权重严格降序：`full-equity` 要求持仓行数与 `reportedEquityCount` 一致且总权重接近 `100%`；`top-ten` 要求恰好 10 行。
- **核心派生函数**：
  - `holdingFor(snapshot, symbol)`：精确区分 `held`（直接持有并返回权重）、`not-held`（完整持仓中确认未持有）与 `unknown`（前十节选中未列出，绝不冒充 0%）。
  - `themeExposure(snapshot, domainId)`：按主题汇总已核对直接持仓权重。
  - `fundIngredients(snapshot)`：拆解单只基金在 `AI 存储链`、`AI 芯片与光刻` 与 `其他已披露持仓` 的配料占比。
  - `summarizePortfolioImpacts(fundCodes)`：跨多只关注或持仓基金聚合底层重仓股与宽基准入档案的 `🟢 顺风利好`、`🟠 逆风风险` 与 `🔍 下一步盯防` 胶囊。

### 3.2 研究假设与三态轻复盘（`features/research/`）

- **数据集与契约（`research.json` & `researchSchema`）**：
  - 每条研究线索绑定 `themeId`、正方事实短标签（`prosTags`）、反方冷水短标签（`consTags`）、带来源引用的事实列表（`facts`）、传导步骤（`chain`）、待验证假设（`hypothesis`）、反方观点（`counterpoints`）及验证检查点（`checkpoints`）。
- **自动化防呆校验（`scripts/check-research-data.mjs`）**：
  - 在 `pnpm check` 阶段自动运行，强制检查所有标签字数上限（`impactTags` ≤ 16 字、`roleTag`/`purityTag` ≤ 12 字）、HTTPS 协议来源、日期先后顺序与目录引用完整性。

---

## 4. 本地状态管理与并发保护

浏览器本地状态划分为四个相互隔离的命名空间，互不污染，均具备**损坏原文保留**、**并发写入检测（比较上次读取原文）**与 **`storage` 跨标签页实时同步**机制：

| 存储键名             | 负责模块             | 数据结构与职责                                                                                                                                                                                                    |
| :------------------- | :------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sora.personal.v1`   | `features/local/`    | `{ schemaVersion: 1, watchlist: string[], positions: Position[] }`。保存自选基金代码与分币种持仓成本（`costMinor` 最小货币单位整数 + BigInt 定点估值计算），支持 1 MB 内 JSON 导出备份与确认导入。                |
| `sora.directions.v1` | `features/discover/` | `{ schemaVersion: 1, ids: string[] }`。保存用户关注的观察方向，支持取消后即时撤销（Undo）。                                                                                                                       |
| `sora.research.v1`   | `features/research/` | `{ schemaVersion: 1, ids: string[], verdicts?: Record<string, 'valid' \| 'watch' \| 'invalid'> }`。保存已关注的研究线索 ID 及**个人三态复盘判断**（逻辑继续成立 / 存疑观望 / 逻辑失效），支持独立 JSON 备份下载。 |
| `sora.navigation.v1` | `navigation/flow.ts` | `sessionStorage` 站内返回点栈（最多保留 20 条列表路径、触发焦点元素 ID 与滚动坐标），严格过滤外部域名，保障“发现/研究 → 基金详情 → 持仓录入 → 逐级原路返回”的焦点与筛选参数完整恢复。                             |

---

## 5. 行情采集与快照层架构（AKTools + `@sora/data-service`）

```text
官方 AKTools 服务 (Python / FastAPI, 127.0.0.1:8080)
  ↑ 指数与 ETF 未复权日线接口调用
TypeScript 采集与校验服务 (apps/data-service)
  ↓ 时区交易日过滤、去重、防日期倒退、独占锁与原子写盘
data/market/market.json (本地快照文件)
  ↓ 本地 HTTP 服务 (127.0.0.1:8788 /api/market, ETag) 或静态导出
前端 MarketProvider (apps/web)
```

- **日线口径与时区安全**：仅采纳交易所当地时区（`America/New_York` 或 `Asia/Hong_Kong`）“今天”之前的已完成日线收盘价，排除当日盘中未完结数据；产品价格显式请求未复权（`adjust=""`），并在 UI 明确提示拆分/除息边界。
- **容错与回退保护**：按单条序列独立更新；若某只产品上游采集失败，保留其上一次有效报价与原始获取时间，标记 `freshness: 'stale'` 与 `updateStatus: 'failed'`，绝不伪造价格或静默切换为虚构 Mock 数据。
- **四种构建模式严格隔离**：
  - `pnpm build`（默认 `dist`）：生产目录构建，不含任何虚构 Mock 代码块。
  - `pnpm --filter @sora/web build:mock`（`dist-mock`）：显式演示构建，包含固定日期的确定性演示数列。
  - `pnpm --filter @sora/web build:snapshot`（`dist-market`）：导出经校验的真实行情快照（不钉入 Service Worker 预缓存，走浏览器运行时缓存回退）。
  - `pnpm build:worker`（`dist-worker`）：读取 Worker `/api/catalog`，API 不可用时回退到随应用预缓存的公共目录并显示日期与重试入口；读取 D1 `/api/market` 已发布的价格日线。

---

## 6. PWA 离线架构与 Cloudflare 部署

- **Service Worker 预缓存边界**：通过 `vite-plugin-pwa`（`generateSW`）预缓存应用外壳（HTML / JS / CSS / 图标 / `catalog.json`）；导航回退指向 `/index.html`，显式排除 `/data`、`/assets`、`/icons`、`/api` 与 `/sw.js`。
- **手动确认更新策略（Prompt for Update）**：禁用自动 `skipWaiting`，仅在用户点击确认更新后才切换 Service Worker 并刷新当前页，避免打断表单编辑；`public/_headers` 确保 `/sw.js` 响应头为 `Cache-Control: no-store, must-revalidate`。
- **Cloudflare Workers Static Assets**：通过 [apps/web/wrangler.jsonc](../apps/web/wrangler.jsonc) 配置 `single-page-application` 路由回退，支持深链直接访问与预览版本上传（`cf:check` / `cf:preview`）。

---

## 7. Phase 5 架构演进：Worker + D1 与异步采集

已采用全 Cloudflare v2 方案，替代原本的 data-service 文件 SQLite 主服务设想：

```text
浏览器 SPA / PWA → 同源 Worker（静态 Assets + /api）→ D1
管理员添加 / 批准收录 → D1 ingest_jobs → GitHub Actions 调度
Actions / 管理员本机执行器 → AKTools → 内部机器鉴权接口 → D1
```

`apps/worker` 已包含 D1 初始迁移、目录种子、`/api/catalog`、健康检查与 ETag，并托管 `apps/web/dist-worker`。5.1 已完成公共目录回退、专用构建/联调入口和 CI；`pnpm check:worker` 在临时本地 D1 上验证迁移、种子和 HTTP 路由。5.2 已接入邀请码密码鉴权、HttpOnly Cookie 会话、Origin/限流/Turnstile 校验、首个管理员引导，以及账号页的邀请码生成。5.3 已提供完整管理后台及 API，0003 迁移增加用户管理版本、邀请撤销信息和最后管理员触发器；用户管理写入、审计与会话吊销在同一 D1 batch 事务内完成。5.4 已加入 0004 标的身份/快照/租约迁移、机器协议、目录同步、请求审批和 Actions 工作流；`apps/data-service` 同时提供 `sync` / `directory` 执行器命令，保留原快照命令。5.5 已使用 0005 增加精确总成本、个人版本及幂等记录，接通 `/api/me/personal`、关注 / 持仓和导入接口；当前仅在本地 D1 验收，远程部署留到 5.6。

公共库仅管理员可写；普通用户搜索 D1 中定期同步的代码目录并申请收录。搜索不在 Worker 中直接调用 AKTools。动态详情从 `/api/instruments/:id` 读取持仓和日线，按管理员维护的公司产业链归属分组；`/api/market` 输出经过共享 Schema 校验的已发布 ETF 价格，基金净值独立展示，静态精品研究保持独立。采集失败保留上一份快照，目录通过 staging 原子发布。完整契约、恢复规则与来源覆盖见 [采集说明](./INGEST.md)。

实施拆分、首个管理员引导、标的身份、持仓总成本映射和任务恢复要求见 [Phase 5 接续实施计划](./PHASE5.md)。

个人数据由共享 `personalSchema` 校验：游客维持本地存储，登录后只读写当前会话对应的账号，按 revision 原子保存并使用 operationId 防止重复导入。Provider 按账号隔离并取消旧请求，账号状态和数据读取失败不会回退为本地写入。静态登录、页头与管理员一次性结果独立于个人页面生命周期。导入先预览可用基金和冲突，保留原件。详见 [个人数据说明](./PERSONAL.md)。
