# Sora V2 技术架构

## 仓库审计与隔离

M0 基于 `origin/main` 的 `69b6525`（Merge pull request #1 from dethan3/dev）。
main 为 Python Portfolio Monitoring Core Alpha：`src/sora` 包含 domain、analysis、
alerts、orchestrator、SQLite repository、scheduler、provider registry / cache、webhook；
`main.py` 是 CLI，`tests/sora` 为既有测试。README 中的 dev 分支描述属于历史文档。
远端另有 `legacy-python-alpha`、`rewrite/ts`；M0 未复用或覆盖它们。
开始时 `feat/sora-v2-foundation` 不存在，直接从 main 创建，不重写既有历史。

Python 原代码与 requirements、配置、测试完整保留，不迁移目录、不在浏览器打包它们。
本阶段不验证真实行情和通知服务。main 不接收提交、不被推送；全部新提交只在 feature 分支。

## 目录与依赖方向

```text
apps/web/
  public/data/catalog.json   # 带来源的产品资料与观察方向，无行情值
  src/data/                 # Zod schema、HTTP 读取、组合计算
  src/components/           # 页面基础组件与基金表格
  src/features/             # M1 领域页面、计算与本地状态
  src/test/fixtures/         # 虚构测试数据，不随站点发布
  src/pages/                # Welcome 与数据页面模块
  src/App.tsx               # Shell、导航和边界状态
  src/main.tsx              # 浏览器入口
  wrangler.jsonc            # Cloudflare 纯静态资源与 SPA fallback
docs/                       # 产品、架构、阶段路线
.github/workflows/          # 检查与手动预览
src/sora/, tests/, config/  # 冻结的 Python 内核
```

依赖流：路由 / 页面 → 基础组件 / 数据读取 → JSON。
不提前建立空的 packages、数据库、共享服务或 Agent 抽象；模块需要复用时再提取。
App 负责一次读取与缓存，AbortController 避免卸载或 StrictMode 时的过期写入。
JSON 必须通过版本、字段、金额、唯一代码和跨列表引用校验后进入组件。
读取失败显示重试；未知路由和 Welcome 不依赖数据成功。
M1 个人组合零成本时收益率显示“—”；旧 portfolioSummary 仅用于首页独立演示。

## 工具链

Node 24（`.nvmrc`）、pnpm 11.25.0（packageManager 固定），Workspace 仅包含 apps/*。
React、React Router、Vite、TypeScript、Tailwind 使用锁文件固定解析版本。
Tailwind 使用官方 Vite 插件；CSS 中保留少量语义类，减少页面样式重复。
Vitest + Testing Library + jsdom 覆盖路由、导航、搜索、计算、JSON 异常与重试。
Prettier 检查前端和本次新增文档的格式，类型检查检查类型；M0 未额外引入 ESLint。

## 本地运行

```sh
pnpm install --frozen-lockfile
pnpm dev
pnpm check
pnpm preview
pnpm --filter @sora/web cf:check
```

开发默认 `http://127.0.0.1:5173`，构建输出 `apps/web/dist`，静态预览默认 4173。
`pnpm check` 顺序执行格式检查、类型检查、测试、构建。Python 按原 README 独立运行。

## Cloudflare 静态预览

采用 Workers Static Assets，不含 Worker 服务代码、D1 binding 或账户服务。
`assets.not_found_handling=single-page-application` 支持 React Router 深链刷新。
`preview_urls=true`，项目名为独立的 `sora-v2-preview`，避免与旧项目混用。
官方新项目指引推荐 Workers；这是静态托管实现选择，不扩大 M0 后端范围。

1. Cloudflare 创建专用 preview Worker 并启用 workers.dev / Version URLs。
   首次若需要初始化，管理员在本地构建后执行
   `pnpm --filter @sora/web exec wrangler deploy`；目标仅为 `sora-v2-preview`。
2. GitHub 设置 environment `cloudflare-preview`，配置 `CLOUDFLARE_ACCOUNT_ID`
   和最小必要 Workers 编辑权限的 `CLOUDFLARE_API_TOKEN`；不把 token 放在前端环境变量。
3. 从 feature 分支手动执行 `Sora V2 Cloudflare Preview`。工作流先验收再执行
   `wrangler versions upload`，输出版本预览 URL，不提升生产版本。
4. 在预览 URL 访问并刷新五条路径，检查 JSON、未知路由、导航和控制台。
   CI 的 `cf:check` 是离线打包校验，不能代表线上部署已成功。

CI 对 feature 分支 push 和 main 的 PR 执行 frozen install、check、Wrangler dry-run，
上传 dist。部署流程仅允许 foundation、m1、m2 三条 feature 分支手动执行，不处理外部 PR secrets。
未配置凭据不影响普通检查；M0 不要求或自动修改 Cloudflare 账号设置。

参考：

- [Tailwind Vite integration](https://tailwindcss.com/docs/installation/using-vite)
- [Cloudflare Static Assets SPA](https://developers.cloudflare.com/workers/static-assets/routing/single-page-application/)
- [Cloudflare Version URLs](https://developers.cloudflare.com/workers/versions-and-deployments/version-urls/)

## 后续边界

先完善静态 JSON 内容与本地交互，再决定是否需要服务端。
D1 仅在跨设备同步、账户隔离或服务端写入出现明确需求时评估，不直接迁移 Python SQLite。
Agent 作为外部 adapter 消费明确的数据契约，核心页面不依赖 Agent 或 MCP 可用性。
PWA 在主要页面和响应式稳定后增加，单独设计缓存版本与陈旧数据提示。

## M3 PWA 架构

在现有 `feat/sora-v2-m2` 上继续提交 M3，不再按里程碑新建分支，保留 main 与 Python 原文件。

- `vite-plugin-pwa` 2.x 与 Vite 8 配合，通过 generateSW / Workbox 生成生产 service worker 和 manifest；开发模式不注册。
- `src/pwa/runtime.ts` 负责原生 service worker 生命周期、安装事件、联网状态和更新检查；`PwaUi.tsx` 提供页脚安装入口、提示条和复用确认弹窗。
- `public/icons/` 保存与既有品牌一致的 SVG 源与 PNG 图标；192 / 512px 普通图标、512px maskable 图标和 180px Apple 图标具有白色实底，图形位于安全区内。
- `scripts/check-pwa.mjs` 在每次生产构建后检查 manifest、PNG 实际尺寸、HTML 引用、核心预缓存资产、更新消息和部署响应头，CI 自动执行。

### 缓存边界

预缓存只包含本次构建的 HTML / JS / CSS / 图标 / manifest / `data/catalog.json`，按内容 revision 更新。
JSON 与页面作为同一发布版本使用，现阶段不增加通用网络请求缓存，也不缓存账户或外部接口。
导航回退指向 `/index.html`，排除 data / assets / icons / api / sw.js，避免把资源或接口错误返回为页面。
新版本安装未完成时保留当前已激活版本；新版本激活后 Workbox 清理过期预缓存条目和旧格式缓存，避免永久占用旧资源。
个人 localStorage 独立于 Cache Storage，缓存清理代码不访问个人数据。
离线提示依据浏览器 online / offline 事件；该信号不保证互联网或服务端可达，始终展示的快照日期才是数据时效口径。

### 更新与安装

采用 prompt 更新，不自动 skipWaiting，不启用 clientsClaim。只有用户确认才发送 SKIP_WAITING，并在 controllerchange 后刷新当前页面。
其他页面发生 controllerchange 时只显示更新提示，不自动刷新，避免打断表单；新页面导航到已激活版本后正常读取现有个人存储。
打开应用、回到前台和每小时检查新版；前台检查有一分钟节流，离线跳过，失败可重试。激活等待超时 20 秒后恢复操作，不无限等待。
beforeinstallprompt 触发时才显示原生安装按钮，一次事件仅调用一次 prompt；appinstalled 或独立窗口模式隐藏安装入口，其他平台展示操作说明。
本次不调用操作系统安装，浏览器是否提供安装入口由平台决定。

### 部署与验收环境

`public/_headers` 随静态包上传：默认重新验证 HTTP 内容，sw.js 禁止 HTTP 持久缓存，注册使用 updateViaCache:none。
生产环境需要 HTTPS；localhost / 127.0.0.1 用于本地安全上下文验证。
验证升级必须使用同一个站点地址。Cloudflare Version URL 每版地址不同，不能证明原站点升级或跨版本数据保留；应在专用 `sora-v2-preview` 的固定 HTTPS 地址上做发布验收。
退回旧构建也按版本更新流程处理；不通过清除浏览器全部存储恢复，避免删除个人关注和持仓。
实际 Cloudflare 发布仍须配置原有 preview environment 的凭据，不宣称已经上线。

参考：[Vite PWA 预缓存](https://vite-pwa-org.netlify.app/guide/service-worker-precache)、[提示更新](https://vite-pwa-org.netlify.app/guide/prompt-for-update)、[安装事件](https://developer.mozilla.org/en-US/docs/Web/API/Window/beforeinstallprompt_event)、[Cloudflare 静态响应头](https://developers.cloudflare.com/workers/static-assets/headers/)。

## 核心流程连续性

入口改为 createBrowserRouter / RouterProvider，为 useBlocker 提供 Data Router 上下文；数据仍由既有静态 JSON 入口读取，没有新增服务端 loader 或 API。测试用 createMemoryRouter 执行同样的导航保护。
PersonalProvider 提升到应用层，首页和功能页共享关注 / 持仓状态；更换页面不重建个人状态。操作反馈随实际页面切换清除，存储损坏状态仍保留。

`navigation/flow.ts` 只接受白名单内的站内返回路径。详情的 from 参数保存目录筛选或来源；录入页的 from 参数保存详情路径，两层均规范化并拒绝外部站点。
列表离开前将一次返回点保存到 `sora.navigation.v1` sessionStorage，并提供内存降级。返回点只包含路径、基金标识和滚动位置，不包含持仓金额。
显式返回或 POP 恢复已记录的位置和焦点；尺寸变化导致目标不在视野时滚入视野。搜索的 REPLACE 更新不会抢走输入焦点。直接访问详情时回退至基金目录，不依赖外部浏览历史。

`useUnsavedChanges` 统一处理路由离开与页内替换动作；useBlocker 负责 SPA / 浏览器返回，beforeunload 负责支持该事件的刷新和关闭场景。
确认后仅执行原待处理动作；取消后恢复原字段焦点。保存成功后的清理允许修改编辑 URL，避免重复弹出离开确认。
编辑器每次重新开启都有独立会话 key，确保确认放弃后确实清空旧输入，且新输入重新启用保护。跨页面冲突检查和数值校验沿用原模型。
组合币种写入 URL，列表和汇总使用相同筛选；单笔收益仅由既有估值与最小货币单位成本计算，无新汇率或收益口径。

参考：[useBlocker](https://reactrouter.com/api/hooks/useBlocker)、[useBeforeUnload](https://reactrouter.com/api/hooks/useBeforeUnload)。原生离开提醒由浏览器决定，不能替代手机系统强制终止时的草稿恢复；本轮不提供自动草稿持久化。

## M1 领域与数据契约

从设计确认版 `0bcc320` 创建 `feat/sora-v2-m1`，不修改 main 或 M0 分支。

```text
src/features/
  discover/     # JSON 地区 / 主题模块
  funds/        # URL 筛选、详情、样本趋势和关注按钮
  watchlist/    # 本地关注列表
  portfolio/    # 份额 / 成本表单、估值与币种汇总
  local/        # schema、存储适配、Context、备份与恢复
```

`loadDemo(signal)` 是唯一目录读取入口，返回通过 Zod 校验的 DemoData，页面不自行 fetch。
目录包含币种、净值字符串、递增历史日期、地区 / 主题模块和基金引用。
净值与历史末点须匹配快照，代码唯一性和引用关系在读取边界校验。
筛选参数 `q` / `module` 位于 URL，详情支持深链与未知代码空态。

## M1 本地状态

键名 `sora.personal.v1`，版本 1，结构 `{ schemaVersion, watchlist, positions }`。
Position 为 `{ code, units, costMinor, currency }`，不保存派生估值，不上传账户信息。
M0 未创建个人存储，首次 M1 为空，无需迁移旧示例。未来版本必须新增迁移策略；当前未知版本保留原文并提示恢复。

PersonalProvider 读取一次快照，通过显式 commit 写入，不使用会覆盖初始数据的持久化 effect。
修改先校验，再比较存储原文、写入，成功后才更新界面。权限或配额失败不伪装成功。
storage 事件同步其他页面；编辑保存时比较原记录，避免覆盖已同步的修改。
原文比较是轻量冲突检查，不提供跨标签事务保证；本阶段无协作同步或数据库。

导入在客户端校验，限制 1 MB、各 200 条、唯一代码、份额精度和金额范围。
校验后须确认替换，不静默合并；导出生成 JSON Blob 并释放临时 URL。
损坏原文可单独下载；只有明确恢复操作才覆盖。

## M1 数值与趋势

份额上限 1,000,000、4 位小数；净值上限 10,000、6 位小数。
成本最小单位整数上限 1,000,000,000,000；最多 200 笔汇总保持在安全整数范围。
份额、净值以十进制字符串和 BigInt 定点乘法计算，四舍五入至最小货币单位后转为安全整数。
CNY / USD / HKD 精度 2，KRW 精度 0；分币种汇总，不隐含汇率口径。
缺失返回 null，不等于零；不完整汇总暂停总收益和权重。
SVG 时间坐标使用 UTC 日期间距，纵轴按样本范围缩放，提供说明和可展开明细表。

参考：[localStorage](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage)、[storage 事件](https://developer.mozilla.org/en-US/docs/Web/API/Window/storage_event)。

## M2 布局与可访问性

从 M1 的 `eb3c58f` 创建 `feat/sora-v2-m2`，保留旧分支与 Python 原文件。
无需新依赖，继续使用 CSS 断点与原生 HTML 交互。

responsive-table 保持唯一的 table / rowgroup / row / rowheader / cell 结构，通过 CSS Grid 将窄屏行排为卡片。
显式角色保留表格关系，thead 仅视觉隐藏；卡片的可见字段标签设 aria-hidden，避免与列标题重复朗读。
900px 为表格 / 卡片分界，640px 为双列 / 单列卡片分界；导航、表单、指标、备份按钮分别调整，不强制缩小整个页面。
SiteHeader 在 768px 以下采用折叠导航，关闭时通过 display:none 同时退出布局与键盘顺序，桌面仍使用同一份链接。按钮提供 aria-expanded / aria-controls；菜单覆盖内容而不撑高顶栏，支持路由、外部点击、焦点离开、Escape 和跨断点关闭。
图表曲线按容器宽度绘制，日期与单位移到 HTML，避免 SVG 文字等比例缩小；线条使用 non-scaling-stroke。

PositionForm 对各字段提供 aria-invalid、说明与错误关联，校验失败聚焦首个错误字段。
目录筛选结果数量使用 polite live region；输入时不把焦点从搜索框移走。
原生 dialog 提供模态语义、描述关联、明确初始焦点与边界 Tab 循环；关闭后释放滚动锁并恢复触发元素焦点。
StrictMode 重复挂载纳入测试；弹窗最大高度使用 dvh，窄屏内部可滚动，操作区保持可达。

参考：[dialog](https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/dialog)、[aria-invalid](https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/Reference/Attributes/aria-invalid)。

## 观察方向与资料目录

`/discover` 提供搜索与市场 / 类型筛选，`/discover/:id` 解释方向、参考指标与关联依据。资料目录保持静态 JSON，复用现有加载、校验、失败重试和离线缓存；不增加行情采集或数据库。

公开目录 source 为 `reference-catalog`。校验要求来源与关联依据齐全，禁止混入非空净值、历史走势、涨跌或预置个人记录；报价字段保持 null。构建同时检查 dist/data 只有 catalog.json，不发布测试 fixture。

`sora.directions.v1` 保存 `{schemaVersion:1, ids:string[]}`。未知方向保留；损坏及未来版本保留原文，明确重置前可下载恢复。写入核对上次原文，成功后才更新界面；storage 事件同步其他页面，外部修改使旧撤销失效。此存储不修改 `sora.personal.v1`，现有个人备份范围保持基金关注与持仓。

返回来源仅接受明确的站内路径和有限查询参数，嵌套解析限制深度与长度。返回点最多保存 20 个列表地址，兼容旧单点记录，让“发现列表 → 方向 → 基金”的多级返回保留各自焦点及滚动位置。

### 产品资料来源（2026-10-26 核对）

仅提取产品名称、类型、跟踪对象和方向关联，不导入网页价格或收益。

- [AIQ / Global X](https://www.globalxetfs.com/funds/aiq)
- [SOXX / iShares](https://www.ishares.com/us/products/239705/ishares-phlx-semiconductor-etf)
- [IAU / BlackRock](https://www.blackrock.com/us/individual/products/239561/ishares-gold-trust-fund)
- [QQQ / Invesco](https://www.invesco.com/us/en/financial-products/etfs/invesco-qqq-trust-series-1.html)
- [IVV / iShares](https://www.ishares.com/us/products/239726/)
- [2800.HK / 盈富基金](https://www.trahk.com.hk/en-hk/trahk-fund/fund-information/)

## 行情视图与 Mock 隔离

`features/market` 包含独立行情 schema、可注入 loader 的 `MarketProvider`、报价、状态、变化值、走势和详情面板。方向目录只提供资料及关联，不承担行情数据或个人估值。

`MarketSnapshot` 使用独立版本和来源字段；当前只接受 `source: mock`。每条方向观察包含稳定 id、指标名称、单位、交易所时区、交易状态、数据新鲜度、可空报价及日线。报价含前收、UTC asOf、可空延迟分钟数；缺失延迟不代表实时。schema 拒绝重复 id、重复 / 逆序日期、未来报价，以及最后历史点与报价日期 / 数值不一致。具体供应商接入时必须另行扩展来源契约及时间、复权、授权元数据，本轮不虚建实时接口。

`MODE=development` 或 `.env.mock` 的显式开关启用动态 Mock loader；其他构建默认不启用。开关在模块边界静态求值，正式产物不生成 Mock chunk。专用构建输出至已忽略的 `dist-mock`，与 Cloudflare 使用的 `dist` 分开；构建脚本核对两种产物是否按预期包含 Mock chunk，两者公开 data 目录均只有资料 catalog.json。

Mock 固定于 2026-10-24 的演示快照，工作日合成序列不是真实交易日历，不按系统时间伪装刷新。黄金故意呈现旧记录，半导体含延迟状态，标普呈现持平，香港呈现休市，用于组件验收。真实模式尚未实现。

Provider 支持异步载入、解析失败、重试、卸载防止过期写入，以及刷新失败保留已载入结果。失败不阻断方向资料和导航。报价与记录按方向 id 连接，不回填 Fund.nav；港元持仓扩展现有币种枚举并维持旧记录兼容。

图表不引入新依赖，以 SVG 连接已有日线记录，各观察点等距；数值 / 日期使用 HTML，提供 range 滑块、可读数值和历史表。区间写入详情 URL，返回来源仅保留合法四档 period，继续限制嵌套深度和外部来源。区间切换替换当前历史条目，避免返回时逐次回退每个区间。

该阶段规划的实际接入已在下一节实现。调研结论与待验证项见 [DATA_SOURCES.md](./DATA_SOURCES.md)。

## AKTools 与 TypeScript 快照层 · 2026-10-26

当前实现覆盖上一节仅支持 Mock 的边界：

`AKShare / AKTools → apps/data-service → packages/market-data 校验 → 私有 JSON → 只读 API 或静态导出 → MarketProvider → 行情组件`。

- `services/aktools` 提供独立 Python 环境的固定版本依赖及 Docker / Compose 文件；不改旧 Python 项目。实际连接使用本地 Python 3.13，容器尚未运行验收。
- `apps/data-service` 使用 Node 24 原生 TypeScript，固定三个指数映射、请求超时和体积限制、日期及数值校验、历史去重、回退拒绝、独占更新锁、原子文件替换和私有尝试报告。可选 Yahoo 整条序列回退默认关闭，不混合来源拼接历史。
- `packages/market-data` 共享 Zod schema 与纯计算函数。实际快照要求日期型收盘报价、至少两条历史、前收一致、来源及有序采集时间；Mock 保持独立来源标识。
- 本机只读 API 默认 127.0.0.1:8788，提供 GET / HEAD、ETag、无可用数据时 503。浏览器刷新只读快照，不触发 Python 采集；AKTools 默认仅映射本机端口。
- `data/market` 不纳入 Git。`pnpm data:update` 单次更新；`pnpm dev:market` 经 Vite 代理读取本机 API；`build:snapshot` 只导出通过校验的实际数据，不能用测试 fixture 代替。
- 浏览器仅缓存有效真实快照，损坏记录忽略且保留原文；更新失败保留旧值和时间。四个自然日的陈旧阈值是保守提醒，不代表完整交易日历。
- 实际 JSON 不进入 Service Worker 预缓存，读取使用 no-cache，失败恢复依赖独立有效快照缓存。构建检查确保正式 / 实际构建没有 Mock chunk，且实际产物的 JSON 合法。

定时调度、VPS、对外 API 访问控制和生产运维尚未实施。依赖版本、命令、故障恢复和后续容器部署见 [AKTOOLS.md](./AKTOOLS.md)。
