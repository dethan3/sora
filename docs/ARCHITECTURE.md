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
成本为零时收益率显示零，M1 再定义更完整的业务语义。

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
上传 dist。部署流程仅允许指定 feature 分支手动执行，不处理外部 PR secrets。
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
