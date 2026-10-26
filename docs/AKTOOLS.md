# AKTools 与 TypeScript 行情更新

本轮在本地使用官方 AKTools 0.0.91 / AKShare 1.19.3，TypeScript 通过 HTTP 获取数据。Python 环境独立于旧项目，完整依赖约束记录在 `services/aktools/requirements.lock`。没有购买服务、开放公网入口或部署 Cloudflare / VPS。

## 运行结构

```text
官方 AKTools（Python / FastAPI，127.0.0.1:8080）
  ↑ 仅三个明确的指数接口调用
TypeScript 更新命令（apps/data-service）
  ↓ 校验、来源、错误报告、原子替换
data/market/market.json（不提交到 Git）
  ↓ 只读 API 或显式静态导出
Sora 页面（读取统一快照，不直接采集）
```

定时更新可由未来 VPS 的任务调度或独立 CI 执行一次性 `pnpm data:update`；本轮未创建任何定时任务。网页刷新按钮只重读已有快照，不调用上游采集。

## 本地启动

需要 Node 24、pnpm 11.25 和 Python 3.13。本轮实际 Python 为 3.13.14。

先安装官方 Python 工具。以下为 Linux / VPS 命令，Windows 使用对应 `.venv/Scripts/python.exe`：

```sh
cd services/aktools
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m aktools --host 127.0.0.1 --port 8080
```

仓库根目录，在另一个终端执行：

```sh
pnpm install --frozen-lockfile
pnpm data:update
pnpm data:serve
```

`data:update` 是一次性采集命令，成功后退出；`data:serve` 在 127.0.0.1:8788 提供只读 `/api/market` 与 `/health`。没有快照时行情接口返回 503，健康接口显示 waiting。不要把同名的 pnpm 内置 update 命令当成脚本；根命令已经显式使用 `run update`。

再打开终端运行：

```sh
pnpm dev:market
```

Vite 的 live 模式把 `/api/market` 转发到 127.0.0.1:8788。现有 `pnpm dev` 仍为 Mock 模式。二者的数据来源明确分离，真实模式失败时不会退回 Mock。

## 可配置项

| 环境变量            | 默认值                             | 用途                                                 |
| ------------------- | ---------------------------------- | ---------------------------------------------------- |
| AKTOOLS_BASE_URL    | http://127.0.0.1:8080              | TS 更新任务访问的 AKTools 服务根地址                 |
| SORA_SNAPSHOT_PATH  | 仓库根目录 data/market/market.json | 生成、读取、导出的快照路径                           |
| SORA_DATA_PORT      | 8788                               | 本地只读服务端口；修改后需同步 Vite 代理             |
| SORA_YAHOO_FALLBACK | 未启用                             | 设为 1 时，在 AKTools 某个指标失败后尝试 Yahoo chart |

环境变量属于本地任务或服务，不包含在 Vite 构建内。不要给 AKTOOLS_BASE_URL 填写带账号密码的 URL。远端访问入口与鉴权等 VPS 确定后再配置。

Yahoo 备用读取复用了既有项目的 chart 端点思路；只接受符号、INDEX 类型和交易所时区都匹配的结果。选择整条来源序列，不把不同供应商的历史线拼在一起。本轮真实成功读取的是 AKTools；Yahoo 备用仅完成自动化测试，没有宣称联网可用。

## 首轮数据范围与口径

| 页面方向            | AKShare 接口              | 上游代码 | 单位   |
| ------------------- | ------------------------- | -------- | ------ |
| 纳斯达克100         | index_us_stock_sina       | .NDX     | 指数点 |
| 标普500             | index_us_stock_sina       | .INX     | 指数点 |
| 香港市场 / 恒生指数 | stock_hk_index_daily_sina | HSI      | 指数点 |

只采纳交易地“今天”之前的日线，保守排除可能尚未完成的当日记录；即使收盘后，当天记录也在交易地日期变更后才进入快照。本轮不是盘中或实时行情。

保留最近约 380 个自然日的数据，按日期排序，拒绝冲突日期、非正数、无效 / 未来日期和不足两条的历史。前收为同一来源中前一条有效日线的 close。所有记录为未复权价格指数点，不含分红，不是 ETF 交易价或基金净值。

日线只提供交易日期，所以 `quote.asOf` 保存 YYYY-MM-DD，不虚构时分秒；采集完成时间另存 provenance.retrievedAt。页面显示“收盘 / 纽约或香港交易日”，不据此推断当前市场开闭状态。

AI、半导体和黄金仍没有接入准确对应的参考指标，真实模式显示暂无数据，不用 AIQ、SOXX、IAU 的价格冒充参考指数或现货。基金净值及个人持仓估值本轮也不回填。

## 失败、缓存与恢复

- 更新按单个方向处理，失败不阻断其他方向。保留该方向原始报价日期、历史和原获取时间，标记 stale / failed；不生成价格，也不默默切换模拟数据。
- 首次全部失败时不创建空的行情文件，命令以非零状态退出，并保存 report.json。后续全部失败时保留旧记录、写入失败状态，同样返回非零退出码。
- 更新使用独占锁，重复运行会失败；写文件通过临时文件、同步写盘与原子替换。损坏的原快照会阻止更新，保留文件，先备份并检查再处理。进程意外退出遗留锁时，确认没有更新进程在运行后再移除该 `.lock` 文件。
- 私有报告为 market.json.report.json，包含每个来源的成功 / 失败，不提供给网页；只读服务不接收任意供应商路径或更新请求，支持 ETag / HEAD。
- 浏览器保存通过校验的最近快照。重载或读取失败时可继续显示，原报价日期保持可见。超过四个自然日或上次采集失败会标记滞后；尚未实现真实交易日历，四天是保守提示，不等于精确的缺失交易日计算。

## 静态预览与之后的 VPS

```sh
pnpm --filter @sora/web build:snapshot
pnpm --filter @sora/web exec vite preview --outDir dist-market
```

显式构建输出到忽略的 `dist-market`，导出已经验证且非空的真实快照，继续与默认 dist / Mock 产物分开。构建校验来源及数据契约，不把 market.json 钉在 PWA 应用预缓存中；浏览器数据缓存负责旧记录回退。Cloudflare 的既有配置仍只指向默认 dist，没有发布这些行情文件。

部署小服务的 Docker 配置位于 `services/aktools`：

```sh
cd services/aktools
docker compose up --build -d
```

Compose 默认只映射本机回环端口，AKTools 本身运行在非 root 用户下。该配置留待 VPS 环境验证；本机 Docker 引擎未启动，本轮没有完成镜像构建或容器运行验收。

参考：[AKTools 官方工具](https://aktools.akfamily.xyz/)、[AKShare 指数接口](https://akshare.akfamily.xyz/data/index/index.html)。软件和数据使用边界继续见 [DATA_SOURCES.md](./DATA_SOURCES.md)。
