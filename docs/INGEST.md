# 5.4 公共库采集与收录

Worker 保存搜索目录、申请、任务和已发布快照；AKTools 在 Actions 或管理员本机运行。浏览器搜索只读 D1，不直接调用上游。静态资料与原 `data:update` / `data:serve` 工作方式继续保留。

## 本机运行

1. 执行 `pnpm db:setup:local` 应用包括 `0004_ingest.sql` 的迁移。已有管理员可继续登录。
2. 生成至少 32 字符的随机机器令牌，将其写入 `apps/worker/.dev.vars` 的 `INGEST_TOKEN`，重新启动 Worker。不要使用测试夹具中的令牌。
3. 启动 AKTools：

   ```sh
   docker compose -f services/aktools/compose.yaml up -d --build
   ```

   无 Docker 时，可按 [AKTools 说明](./AKTOOLS.md) 使用 Python 3.13 独立环境启动同一固定版本。只监听本机。

4. 在执行器终端设置环境变量（令牌与 Worker 相同，不加 `VITE_` 前缀）：

   ```sh
   export SORA_WORKER_URL=http://localhost:8787
   export AKTOOLS_BASE_URL=http://127.0.0.1:8080
   # 在终端安全输入令牌，避免写入历史：
   read -rs -p '机器令牌: ' SORA_INGEST_TOKEN
   export SORA_INGEST_TOKEN
   ```

5. 运行 `pnpm data:directory` 同步代码名称目录。初次目录只有原有六只 ETF；完整同步成功后可以搜索其他标的。单个市场/品种同步失败不影响其他范围，也不替换其旧目录。
6. 管理员在 `/admin` →「公共库与采集」→「标的管理」搜索，例如 `510300`，添加并采集；普通用户在基金页搜索、申请，管理员在「收录申请」批准后进入同一队列。
7. 本机运行 `pnpm data:sync`。任务页每 4 秒刷新进度；完成后通过「详情」查看披露日期、前十大重仓、产业链分组和日线。详情就绪时重新读取在线基金目录。没有 Actions 配置时，任务保留等待本机执行。
8. 每日增量：`pnpm data:sync -- --refresh`。只刷新已收录标的的日线，不重新抓持仓；CN ETF / A 股请求包含前次最新日期之前 7 天的重叠区间。美港新浪及开放式基金接口只能返回历史全集，执行器仍只上传最近 380 天，D1 按日期 UPSERT。

`SORA_MAX_JOBS` 默认 50，允许 1～200；单个任务失败会记录错误并继续处理其他任务，命令最终返回非零退出码。`SORA_DIRECTORY_SCOPES` 可用逗号限制同步范围，例如 `CN:fund,CN:etf,CN:stock`。

## 已实现的数据覆盖

| 范围             | 目录                               | 日线                              | 自动股票持仓                                  |
| ---------------- | ---------------------------------- | --------------------------------- | --------------------------------------------- |
| 中国公募基金     | `fund_name_em`（代码、简称、拼音） | `fund_open_fund_info_em` 单位净值 | `fund_portfolio_hold_em` 最新已披露季度的前十 |
| 中国 ETF         | `fund_etf_spot_em`（仅取目录字段） | `fund_etf_hist_em` 未复权收盘     | 同上                                          |
| 沪深京 A 股      | `stock_info_a_code_name`           | `stock_zh_a_hist` 未复权收盘      | 不适用                                        |
| 美股证券名录     | `get_us_stock_name`                | `stock_us_daily` 未复权收盘       | 暂无；明确显示未提供                          |
| 港股证券名录     | `stock_hk_spot`                    | `stock_hk_daily` 未复权收盘       | 暂无；明确显示未提供                          |
| 原有六只美港 ETF | 初始参考目录保留 ETF 身份          | 同上                              | 精品配料表保留静态核对资料                    |

先以中国股票型基金 / ETF 验收完整持仓链路。美港名录沿用 AKShare 的证券列表分类，不提供全市场 ETF 类型识别；新增美港 ETF 的自动持仓和精确分类仍需专用来源适配。上游缺少股票持仓的货币、纯债、黄金等中国基金，完整采集会明确失败，不发布伪造重仓。非人民币基金份额按名称过滤；港股人民币 / 美元柜台及 B 股持仓不在支持范围，避免猜测币种。指数不在本轮全市场同步范围。原固定指数的本地快照采集仍可使用。

持仓只取最新已披露季度、按权重排序的前十，不将半年/年报的长列表当成同一季度全仓。持仓代码形态只用于选择待核验市场，还必须通过对应 A 股、港股或美股证券目录的代码与公司名称核对，才能生成稳定身份；不能凭六位数字将韩国等市场代码当成 A 股。目录缺失、名称无法确认或代码无法识别时，该任务失败并等待核对。同一执行器批次复用成功读取的证券目录，失败后允许重试。标签与产业链归属由管理员维护，采集不会编造影响判断；未知部分显示待分类 / 待标注。全局精品基金 PK 与研究体检继续使用静态、已核对的研究数据，动态详情单独呈现，避免混合不同报告期。

数据契约来自 [AKShare 公募基金文档](https://akshare.akfamily.xyz/data/fund/fund_public.html) 和 [股票行情文档](https://akshare.akfamily.xyz/data/stock/stock_quote.html)，并核对项目固定版本 AKShare 1.19.3 / AKTools 0.0.91 的源码包。真实上游可能限流或更改结构，任务失败时可查看具体原因并重试。

## Actions 配置（远程启用留到 5.6）

当前只做本地开发，云端采集默认暂停：已移除 `schedule` 定时触发，任务还要求仓库变量 `SORA_INGEST_ENABLED=true` 才会执行。未设置该变量时，即使手动触发也不会启动采集器或访问远程 Worker。本机 `pnpm data:directory` / `pnpm data:sync` 不受此开关影响。

`.github/workflows/ingest.yml` 保留：

- `workflow_dispatch`：`jobs` 拉取待办、`refresh` 增量日线、`directory` 全量目录后处理待办。
- 同一仓库采集串行，正在执行的任务不会被新触发取消；单次最多 100 个任务、40 分钟。超时未完成的工作通过租约重领恢复。

未来完成 5.6 的远程 Worker 和 Secrets 配置后，再设置上述开关并手动验收。定时采集需要另行恢复 `schedule`：原计划为每日 UTC 22:17（北京时间次日 06:17）刷新日线、每周六 UTC 21:37（北京时间周日 05:37）同步目录。

GitHub 仓库 Secrets：`SORA_WORKER_URL`（正式 HTTPS Worker 源，不带路径）、`SORA_INGEST_TOKEN`。

Worker Secrets / 配置：`INGEST_TOKEN`、`GITHUB_TOKEN`（限定目标仓库，Actions 写权限）、`GITHUB_REPOSITORY=owner/repo`、`GITHUB_REF`（默认 `main`）。令牌只用于 Worker 服务端，前端不会获得机器或 GitHub 令牌。手动运行本机执行器不需要 GitHub 令牌。

管理员先写 D1 队列再调用 [GitHub workflow dispatch API](https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event)。15 分钟内合并触发，失败后保留队列并在 60 秒后允许重试；Worker 每 5 分钟检查待办和过期租约。运行中的执行器通过心跳延续锁，结束后释放；管理页也可手动「触发执行器」。GitHub workflow 文件需要进入默认分支后才能使用手动和定时触发。远程 Worker URL、Secrets 与 workflow 均就绪后，再验收真实 Actions 采集。

## 协议和恢复规则

公共接口：`GET /api/search?q=`、`GET /api/instruments/:id`、`GET /api/market`。详情保留价格/净值/指数类型、币种、时区、未复权口径、来源和采集时间；`/api/market` 用两次 D1 查询输出最近发布的最多 200 份 ETF 价格与指数，开放基金净值单独展示。任何采集值都不写入参考目录的 `Fund.nav`；现有个人估值行为保持原有契约。

用户接口：`GET/POST /api/me/requests`，只可读取当前用户的申请；按上海自然日，每人最多 5 个不同标的，重复提交不扣次数。申请被批准后仍展示标的采集状态；驳回附理由。同一用户同一标的不会反复新建申请。

管理员接口：`GET/POST /api/admin/instruments`、`PATCH /api/admin/instruments/:id`、`GET /api/admin/jobs`、`POST /api/admin/jobs/:id/retry`、`POST /api/admin/jobs/dispatch`、`GET /api/admin/requests`、`POST /api/admin/requests/:directoryId/approve|reject`。标的编辑必须带 `revision`，审计与写入在同一事务中完成。下架后详情和公共目录不可见，运行中的采集不会将它重新上架。

机器接口均为 POST，仅接受 `Authorization: Bearer <INGEST_TOKEN>`：

- `/api/internal/directory/start` → `/:id/batch` → `/:id/publish`：每批最多 500 条，每批保存一个 JSON 数组（按内容哈希幂等）；完整且去重后的计数匹配后按市场/品种原子切换，只改写有变化的在线记录；过期或并发发布不能覆盖更新版本。
- `/api/internal/jobs/claim`：原子领取，10 分钟租约、每 60 秒心跳；同标的只有一个 queued/running 任务。租约令牌只存哈希。
- `/api/internal/jobs/:id/heartbeat|complete|fail`：只有当前租约能更新；结果通过共享 Schema 校验，同一任务相同回写可重试，不同内容返回冲突。日线、持仓、元数据与任务完成状态在同一 D1 batch 发布；失败保留已发布数据，拒绝报告期或最新日线倒退。
- `/api/internal/refresh`：按日幂等创建刷新任务，已在采集中的标的不重复排队；自动发现的持仓公司不会全量递归刷新，除非管理员显式添加。
- `/api/internal/runner/finish`：释放批次触发锁。崩溃执行器租约最多自动尝试三次，之后转为失败供管理员重试。

内部正文上限 512 KiB。远程机器 URL 必须 HTTPS，只有本机允许 HTTP；禁止认证跳转。所有外部 AKTools 方法与上游 URL 由执行器固定选择，不接收目录提供的任意采集地址。

## 本轮验证边界

新增后端协议、执行器适配和前端交互测试；`check:worker` 在全新临时 D1、真实 workerd 与实际执行器之间验证目录 → 申请 → 批准 → 发布 → 增量刷新，上游响应使用明确的测试夹具。Playwright 使用正式 Worker 入口和官方 Turnstile 测试 keys 验证管理页添加、真实 D1 任务与详情、响应式布局。

当前机器未提供 Docker 命令或 Python 3.13，未运行固定版本 AKTools 的真实网络采集；只下载公开源码包核对适配契约。也未部署远程 Worker、设置远程 Secrets 或运行 GitHub Actions。真实网络 / Actions 端到端验收随 5.6 完成，不能以本地夹具结果代替。
