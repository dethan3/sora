# Sora

`dev` 分支当前承载的是新的 `Sora` 监控内核，而不是旧版 ETF 量化实验。

当前目标不是一次性做成完整产品，而是先把下面这条主链路做稳定：

`watchlist` 管理 -> 行情获取 -> 规则分析 -> 告警评估 -> 结果持久化 -> 组合查询

后续的 `OpenClaw skill`、飞书 Bot、Web UI，都应作为外层 adapter 逐步接入这套内核。

当前更明确的方向是：

- 把 `sora` 做成“持仓与观察池的监控内核”
- 先服务基金 / 指数 / ETF 的持续跟踪、告警和复盘
- 先把组合监控、规则体系和运行可靠性做深，再考虑 Web / Bot / Agent

## 当前状态

- 阶段：Portfolio Monitoring Core Alpha Frozen
- 分支：`dev`
- 当前策略：冻结 core，进入真实使用验证期
- 已有能力：
  - 初始化本地 SQLite 数据库
  - 管理监控标的
  - 为新基金记录入场基准点位
  - 为基金记录轻量持仓信息（份额 / 持仓成本）
  - 执行单次监控运行
  - 基于 interval 的本地调度与运行锁
  - 查询当前运行状态和最近运行历史
  - 查询资产最新结果、历史快照、告警和通知
  - 查询组合摘要、回撤、仓位分布和持仓列表
  - 导出资产简版报告（markdown / json）
  - 导出组合简版报告（markdown / json）
  - provider registry 与本地快照降级路径
  - 查询当前 provider 能力与标的支持情况
  - 发送 pending 通知事件到配置好的 webhook
  - 保存快照、分析结果、告警事件和待发送通知事件
- 尚未包含：
  - 多数据源聚合
  - 组合级规则的抑制 / 节流 / 去重
  - 守护进程 / 服务化运行形态
  - skill / Bot / HTTP API / Web UI

当前不再主动扩展新功能。接下来只根据真实运行反馈处理三类问题：运行 bug、阻碍使用的 CLI / 数据问题、以及阻断下一阶段的结构问题。

## 当前能力

### 1. 数据持久化

当前使用 `sqlite3` 直接落库，默认数据库路径为 `data/sora.db`。

已落表：

- `watchlist_assets`
- `monitoring_runs`
- `snapshots`
- `analysis_history`
- `alert_rules`
- `alert_events`
- `notification_events`

### 2. Watchlist 管理

支持管理监控标的：

- `fund`
- `index`

对新加入的基金，CLI 可以在添加时记录当下净值作为入场基准，后续运行时可以直接看到“持有以来涨幅”。

如果已经持有该基金，也可以记录：

- `position_units`
- `position_cost_amount`

这样在后续 `run-once` 中可以直接看到浮盈金额和浮盈比例。

当前市场枚举：

- `cn`
- `global`

当前 `global` 的首个落地范围已经接入：

- `fund`:
  - `QQQ`
  - `SPY`
  - `VOO`
  - `DIA`
  - `IWM`
  - `VTI`
  - `VT`
- `index`:
  - `SPX`
  - `NDX`
  - `IXIC`
  - `DJI`

这个范围之外的 global 标的暂时仍不支持。

### 3. 单次运行编排

`run-once` 会执行一次完整链路：

1. 读取启用中的监控标的
2. 拉取行情和历史序列
3. 生成规则化分析结果
4. 评估告警规则
5. 持久化运行结果

### 4. 规则分析

当前分析引擎是 V1 规则版，主要输出：

- 日涨跌幅
- 7 日变化
- 30 日变化
- `SMA5` / `SMA20` 趋势判断
- 基础波动率风险等级
- 简单综合分数 `score`

这还不是完整策略系统，也没有 AI 总结层。

### 5. 告警能力

支持定义阈值规则：

- scope：
  - `asset`
  - `portfolio`
- 指标：
  - `asset` scope:
  - `daily_change_pct`
  - `change_7d_pct`
  - `change_30d_pct`
  - `score`
  - `portfolio` scope:
  - `portfolio_unrealized_pnl_amount`
  - `portfolio_unrealized_pnl_pct`
  - `portfolio_daily_pnl_amount`
  - `portfolio_since_entry_pnl_pct`
- 方向：
  - `above`
  - `below`

规则命中后当前会：

- 写入 `alert_events`
- 为每个 channel 生成一条 `notification_events`

之后可通过 `notifications send-pending` 将事件发送到配置好的 webhook。当前已经具备通用 webhook adapter，但还没有 Telegram、企业微信等特定平台 adapter。

其中组合级规则当前只在“全量 `run-once`”时评估；使用 `run-once --code ...` 进行局部运行时，会跳过组合规则，避免用局部刷新结果误判整个组合。

### 6. 数据源能力

当前 provider 层已经具备 registry 和 fallback 机制。

实时 provider：

- 中国公募基金代码，例如 `510300`
- 部分中国指数代码，例如：
  - `sh000001`
  - `sz399001`
  - `sz399006`
  - 或内建别名 `000001` / `399001` / `399006`
- 首批 global ETF / 指数代理标的：
  - `QQQ`
  - `SPY`
  - `VOO`
  - `DIA`
  - `IWM`
  - `VTI`
  - `VT`
  - `SPX`
  - `NDX`
  - `IXIC`
  - `DJI`

当前降级路径：

- 当实时 provider 拉取失败时，如果本地数据库里已经有至少 2 条历史快照，`snapshot_cache` provider 会回退到本地持久化快照继续生成分析结果

当前可以通过 CLI 查看 provider 声明能力和支持情况：

- `providers list`
- `providers check`

暂未实现或仍有限制：

- 更广的 global market 覆盖
- 非首批 ticker 的自动发现与归一化

### 7. 查询与报表

当前已经支持通过 CLI 读取运行结果：

- 资产最新概览：最新快照、分析结果、最近一次运行
- 资产快照历史
- 组合摘要：总成本、总市值、浮盈亏、当日盈亏、入场以来盈亏、峰值市值、当前回撤、最大仓位占比、前三仓位集中度
- 持仓列表：逐资产查看当前值、趋势、分数、盈亏、权重占比
- 告警历史
- 通知历史
- 资产简版报告导出
- 组合简版报告导出（包含组合历史曲线）

## 项目结构

```text
src/sora/
  __init__.py
  alerts.py
  analysis.py
  config.py
  domain.py
  notifications.py
  orchestrator.py
  repository.py
  scheduler.py
  notifiers/
    __init__.py
    base.py
    webhook.py
  providers/
    __init__.py
    akshare.py
    cache.py
    base.py
    registry.py

tests/sora/
  test_alerts.py
  test_analysis.py
  test_cli.py
  test_config.py
  test_notifications.py
  test_orchestrator.py
  test_repository.py
  test_scheduler.py
```

## 快速开始

要求：

- Python 3.11+
- 已安装 `requirements.txt`

安装依赖：

```bash
python -m pip install -r requirements.txt
```

初始化数据库：

```bash
python main.py init-db
```

添加监控标的：

```bash
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn
python main.py watchlist add --code sh000001 --name "上证指数" --asset-type index --market cn
```

对于新加入的基金，命令会询问是否记录当前净值为入场基准。也可以显式指定：

```bash
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn --record-baseline
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn --skip-baseline
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn --skip-baseline --position-units 100 --position-cost-amount 123.45
```

查看监控列表：

```bash
python main.py watchlist list
```

添加告警规则：

```bash
python main.py alert-rule add --asset-code 510300 --metric daily_change_pct --direction above --threshold 1.5 --channel feishu
python main.py alert-rule add --scope portfolio --metric portfolio_unrealized_pnl_pct --direction below --threshold -3 --channel feishu
python main.py alert-rule list
```

执行一次分析：

```bash
python main.py run-once
python main.py run-once --code 510300
```

查看运行状态和最近运行历史：

```bash
python main.py runs status
python main.py runs list --limit 10
```

查看资产最新结果和历史：

```bash
python main.py assets latest --code 510300
python main.py assets history --code 510300 --limit 10
```

查看组合摘要和持仓列表：

```bash
python main.py portfolio summary
python main.py portfolio positions
```

查看 provider 能力和支持情况：

```bash
python main.py providers list
python main.py providers check --code 510300 --asset-type fund --market cn
python main.py providers check --code QQQ --asset-type fund --market global
```

查看告警和通知历史：

```bash
python main.py alerts list --code 510300 --limit 20
python main.py notifications list --code 510300 --limit 20
```

导出资产报告：

```bash
python main.py reports asset --code 510300 --format markdown
python main.py reports asset --code 510300 --format json
python main.py reports portfolio --format markdown
python main.py reports portfolio --format json
```

发送 pending 通知：

```bash
python main.py notifications send-pending --limit 100
```

## 配置

默认配置文件为 `config/sora.yaml`：

```yaml
database_path: data/sora.db
analysis:
  lookback_days: 90
  short_window: 7
  long_window: 30
notifications:
  request_timeout_seconds: 10
  webhook_urls:
    feishu: https://example.com/feishu-webhook
    telegram: https://example.com/telegram-webhook
```

当前支持的配置项：

- `database_path`
- `analysis.lookback_days`
- `analysis.short_window`
- `analysis.long_window`
- `notifications.request_timeout_seconds`
- `notifications.webhook_urls`

配置校验规则：

- `lookback_days > long_window`
- `short_window <= long_window`
- 所有窗口参数都必须大于 0

## 测试

运行测试：

```bash
pytest -q tests
```

阶段验收直接运行真实 CLI 并观察结果：

```bash
python main.py init-db
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn --skip-baseline
python main.py alert-rule add --asset-code 510300 --metric score --direction above --threshold 0 --channel feishu
python main.py run-once --code 510300
python main.py runs status
python main.py notifications send-pending --limit 100
```

说明：

- 阶段验收以真实运行反馈为准，而不是脚本内置断言
- `run-once` 依赖外网访问 `AkShare / Eastmoney`
- `notifications send-pending` 是否成功，取决于当前配置中的 webhook 是否可用

当前测试主要覆盖：

- 配置加载与校验
- 分析引擎
- 告警评估
- 仓储层
- 单次运行编排
- CLI 命令行为
- 调度器
- 通知发送成功 / 失败路径
- 查询与报表读取接口
- 简版报告导出
- provider registry 与 fallback

尚未覆盖：

- 更广的 provider fallback 与多数据源切换
- 更完整的 AkShare 联网稳定性和错误分类
- 更完整的查询 / 报表层回归测试

## 设计原则

- `src/sora/*` 只负责核心领域能力
- 外部交互层不直接侵入 core
- 先做稳定的规则化监控链路
- 再逐步补调度、通知、查询、展示和多数据源
- 当前优先保持实现简单，继续使用 `sqlite3` 直接持久化

## 当前限制

- 仅支持有限的中国市场标的
- 没有对外 HTTP / Web 查询接口
- 报表仍是简版导出
- 本地快照 fallback 依赖历史运行数据，不能替代真实行情源
- 没有 schema migration 机制

## 下一步

建议按这个顺序继续扩展：

1. 扩大全球市场支持范围
2. CI / 覆盖率 / 更完整集成测试
3. schema migration 策略
4. Web / Bot / API 等外层 adapter

## 风险提示

> 投资有风险，规则分析与告警结果仅供参考。
> 
> 当前输出基于第三方行情源和简化规则，不构成投资建议。

## 开源协议

本项目采用 MIT License。
