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
  public/data/demo.json      # 可独立替换的虚构静态数据
  src/data/                 # Zod schema、HTTP 读取、组合计算
  src/components/           # 页面基础组件与基金表格
  src/features/             # M1 领域页面、计算与本地状态
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
CNY / USD 精度 2，KRW 精度 0；分币种汇总，不隐含汇率口径。
缺失返回 null，不等于零；不完整汇总暂停总收益和权重。
SVG 时间坐标使用 UTC 日期间距，纵轴按样本范围缩放，提供说明和可展开明细表。

参考：[localStorage](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage)、[storage 事件](https://developer.mozilla.org/en-US/docs/Web/API/Window/storage_event)。

## M2 布局与可访问性

从 M1 的 `eb3c58f` 创建 `feat/sora-v2-m2`，保留旧分支与 Python 原文件。
无需新依赖，继续使用 CSS 断点与原生 HTML 交互。

responsive-table 保持唯一的 table / rowgroup / row / rowheader / cell 结构，通过 CSS Grid 将窄屏行排为卡片。
显式角色保留表格关系，thead 仅视觉隐藏；卡片的可见字段标签设 aria-hidden，避免与列标题重复朗读。
900px 为表格 / 卡片分界，640px 为双列 / 单列卡片分界；导航、表单、指标、备份按钮分别调整，不强制缩小整个页面。
图表曲线按容器宽度绘制，日期与单位移到 HTML，避免 SVG 文字等比例缩小；线条使用 non-scaling-stroke。

PositionForm 对各字段提供 aria-invalid、说明与错误关联，校验失败聚焦首个错误字段。
目录筛选结果数量使用 polite live region；输入时不把焦点从搜索框移走。
原生 dialog 提供模态语义、描述关联、明确初始焦点与边界 Tab 循环；关闭后释放滚动锁并恢复触发元素焦点。
StrictMode 重复挂载纳入测试；弹窗最大高度使用 dvh，窄屏内部可滚动，操作区保持可达。

参考：[dialog](https://developer.mozilla.org/en-US/docs/Web/HTML/Reference/Elements/dialog)、[aria-invalid](https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/Reference/Attributes/aria-invalid)。
