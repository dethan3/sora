# Sora 开发计划（dev 分支监控内核）

更新时间：2026-03-20 22:10 +08:00

## 1. 项目定位

- 当前分支：`dev`
- 当前阶段：Portfolio Monitoring Core Alpha
- 当前目标：把 `watchlist -> 行情获取 -> 规则分析 -> 告警评估 -> 持久化 -> 组合查询` 这条链路做稳定
- 当前范围：
  - 基金 / 指数 / ETF 监控
  - 单机 SQLite 持久化
  - 单次运行编排
  - 规则化分析和告警事件生成
  - 组合级读取与报告

- 北极星定位：
  - 把 `sora` 做成“持仓与观察池的监控内核”
  - 先服务基金 / 指数 / ETF 的持续跟踪、告警和复盘
  - 后续 `skill / bot / web` 都作为外层 adapter 接入，不反向污染 core

`dev` 分支已经不再沿用旧版 ETF 量化实验的模块划分。当前主代码集中在 `src/sora/*`，后续所有规划都以这套监控内核为基线。

## 2. 当前基线

### 已完成

- [x] 核心领域模型
- [x] SQLite 仓储层
- [x] `watchlist` 管理
- [x] 新基金入 watchlist 时记录入场基准点位
- [x] 基金轻量持仓信息（份额 / 成本）记录
- [x] `alert-rule` 管理
- [x] `run-once` 单次运行编排
- [x] AkShare provider 首版接入
- [x] 规则分析引擎 V1
- [x] 告警事件与通知事件入库
- [x] interval scheduler 与运行锁
- [x] 运行状态 / 历史查询 CLI
- [x] webhook 通知发送闭环
- [x] 查询与报表层基础能力
- [x] provider registry 与本地快照 fallback
- [x] global market 首个落地范围
- [x] 组合摘要 / 持仓列表 / 组合报告读取面
- [x] 核心单元测试

### 当前实现包含

- 标的模型：
  - `Asset`
  - `MarketSeries`
  - `Snapshot`
  - `AnalysisResult`
  - `AlertRule`
  - `AlertEvent`
  - `NotificationEvent`
- 仓储表：
  - `watchlist_assets`
  - `monitoring_runs`
  - `snapshots`
  - `analysis_history`
  - `alert_rules`
  - `alert_events`
  - `notification_events`
- CLI 命令：
  - `init-db`
  - `watchlist add`
  - `watchlist list`
  - `run-once`
  - `assets latest`
  - `assets history`
  - `portfolio summary`
  - `portfolio positions`
  - `providers list`
  - `providers check`
  - `runs status`
  - `runs list`
  - `alerts list`
  - `notifications send-pending`
  - `notifications list`
  - `reports asset`
  - `reports portfolio`
  - `alert-rule add`
  - `alert-rule list`

### 尚未完成

- [ ] 组合级规则与告警
- [ ] 更广的 global market 支持
- [ ] 守护进程 / 服务化运行形态
- [ ] CI / 覆盖率门槛
- [ ] schema migration 机制

## 3. 当前主要问题

### P0 文档与代码脱节

- README 中曾混入旧版 ETF 项目内容，容易误导使用者
- 旧 DEV_PLAN 仍引用 `src/data/*`、`src/analytics/*`、`TaskScheduler`、`SQLAlchemy` 等已不再代表当前实现的内容

### P1 运行形态还停留在手动触发

- 当前以 `run-once` 为核心执行单元
- 已有 interval scheduler，但还没有守护进程 / 服务化运行形态
- 运行锁只覆盖单库单实例场景

### P1 组合视角刚起步

- 当前已能查看组合汇总和持仓列表
- 但组合级规则、组合回撤、仓位分布和资产贡献度还没有落地
- 目前仍以单资产告警为主，组合层决策支持不足

### P1 告警只生成事件，没有交付闭环

- `notification_events` 已可发送到 webhook
- 但 channel 仍停留在通用 webhook
- 重试策略、退避和死信处理还未完善

### P2 接口层尚未打开

- 当前仍是 CLI-first
- 对 skill / bot / web 来说，虽然读取面已增强，但还没有稳定的 API / adapter 层

## 4. 里程碑

### M1 文档与 CLI 收口（短期）

目标：

- README 与 DEV_PLAN 全面同步到当前 `dev` 分支实现
- 为现有 CLI 增加最小可运行测试
- 明确当前能力边界，避免把未实现能力写进文档

任务：

- [x] 更新 README
- [x] 更新 DEV_PLAN
- [x] 增加 `CliRunner` 测试覆盖 `init-db`
- [x] 增加 `CliRunner` 测试覆盖 `watchlist add/list`
- [x] 增加 `CliRunner` 测试覆盖 `alert-rule add/list`
- [x] 增加 `CliRunner` 测试覆盖 `run-once` 的无标的场景

验收标准：

- 文档不再出现旧架构路径和过期命令
- CLI 基本命令可由测试驱动验证

### M2 调度器与运行锁（短期）

目标：

- 让监控内核具备周期执行能力
- 避免重复运行或并发写库污染

建议实现：

- `Scheduler` 抽象，先支持 interval 模式
- 单实例运行锁
- 运行状态记录与最近一次执行结果查询

任务：

- [x] 设计调度接口与运行锁策略
- [x] 实现本地 interval scheduler
- [x] 为调度运行补充测试
- [x] 增加调度状态查询入口

验收标准：

- 能按固定周期执行 `run-once`
- 同一时刻不会重复执行同一任务
- 调度失败会留下明确运行记录

### M3 通知发送闭环（中期）

目标：

- 把“告警命中”推进到“通知可交付”

建议实现：

- `Notifier` 抽象
- 首个 adapter 选飞书或通用 webhook
- 通知状态管理：`pending` / `sent` / `failed`
- 可选重试次数与失败原因记录

任务：

- [x] 定义 notifier 接口
- [x] 增加 sender worker 或命令
- [x] 实现首个实际发送 adapter
- [x] 增加发送成功 / 失败测试

验收标准：

- `notification_events` 不再只是占位记录
- 至少一种 channel 可完成真实发送

### M4 查询与报表层（中期）

目标：

- 为 bot / web / skill 提供稳定读取面

建议实现：

- 运行历史查询
- 最新分析结果查询
- 指定资产历史快照查询
- 告警 / 通知历史查询
- 可导出 markdown / json 摘要

任务：

- [x] 增加 repository 读取接口
- [x] 增加 CLI 查询命令
- [x] 增加简版报告导出
- [x] 为读取接口补充测试

验收标准：

- 不依赖直接查库，也能查看核心运行结果
- skill / bot / web 可以复用统一读取接口

### M5 Provider 扩展与健壮性（中期）

目标：

- 降低 AkShare 单点依赖
- 扩大可监控标的范围

建议实现：

- provider fallback 机制
- 更规范的 symbol 归一化
- 明确 provider capability 声明

任务：

- [x] 设计 provider registry
- [x] 为 CN 市场增加备用 provider 或降级路径
- [x] 评估并实现 global market 的首个落地范围
- [x] 增加 provider 集成测试

验收标准：

- provider 出现局部故障时仍有降级路径
- 代码层面可明确判断 asset 是否可支持

### M6 组合监控与持仓视角（下一阶段）

目标：

- 从“单资产监控”升级到“组合监控”
- 让持仓用户直接看到组合总览，而不是手工拼接单资产结果

建议实现：

- 组合摘要：总成本 / 总市值 / 浮盈亏 / 当日盈亏
- 持仓列表：按资产查看最新净值、趋势、分数和盈亏
- 组合报告导出
- 为后续组合级规则提供统一读取模型

任务：

- [x] 增加组合读取领域模型
- [x] 增加 repository 组合摘要 / 持仓读取接口
- [x] 增加 `portfolio summary`
- [x] 增加 `portfolio positions`
- [x] 增加 `reports portfolio`
- [ ] 增加组合级告警规则设计
- [ ] 增加组合回撤 / 仓位分布指标

验收标准：

- 不查库也能直接看到组合层面的核心指标
- 已记录持仓的资产可以按组合作为一个整体被查看

### M7 规则体系升级（下一阶段）

目标：

- 让 `sora` 更贴近持仓管理和持续跟踪，而不只是简单阈值命中

建议实现：

- 回撤阈值
- 连续上涨 / 下跌
- 趋势反转
- 波动率放大
- 偏离入场基准 / 偏离组合目标

任务：

- [ ] 扩展规则模型支持派生指标
- [ ] 增加组合级规则
- [ ] 增加规则去重 / 抑制 / 节流
- [ ] 补规则回归测试

### M8 运行可靠性与服务化（中期）

目标：

- 从“能跑”升级到“能长期稳定跑”

任务：

- [ ] 守护进程 / 常驻运行模式
- [ ] 通知失败重试与退避
- [ ] 运行幂等 / 告警去重
- [ ] 更清晰的失败分类和恢复路径

### M9 接口外放（中后期）

目标：

- 在不污染 core 的前提下，为外层入口提供统一能力

任务：

- [ ] skill adapter
- [ ] bot adapter
- [ ] HTTP API
- [ ] 只读 Web 查询页

### M10 工程化加固（持续）

目标：

- 提升可维护性和可回归能力

任务：

- [ ] 接入 CI
- [ ] 增加覆盖率报告
- [ ] 补齐更完整的集成测试
- [ ] 明确数据库 schema 演进策略
- [ ] 评估数据清理与 retention 策略

验收标准：

- 新改动能被自动化验证
- 数据库变更不再依赖人工猜测

## 5. 设计原则

- `src/sora/*` 是唯一核心实现目录
- adapter 能力应放在 core 之外
- 当前继续采用 `sqlite3` 直连实现，先保持简单
- 只有在查询复杂度、迁移需求明显上升后，再评估引入 ORM / migration framework
- 先补运行闭环，再补展示层

## 6. 测试策略

当前已覆盖：

- 配置加载
- 分析引擎
- 告警评估
- 仓储层基础行为
- 单次编排成功 / 失败 / 告警场景
- 调度器与运行锁
- 通知发送成功 / 失败路径
- CLI 命令行为
- 查询与报表读取接口
- 简版报告导出
- provider registry 与 fallback
- 组合摘要 / 持仓列表 / 组合报告

接下来优先补：

1. 组合级规则与指标
2. 守护进程 / 可靠性补强
3. CI 与覆盖率门槛
4. schema migration 验证

## 7. 风险与应对

- 外部数据源不稳定：
  - 增加 provider fallback
  - 增加集成测试和错误分类
- 文档再次过期：
  - 任何 CLI 或架构变更必须同步 README / DEV_PLAN
- SQLite 并发能力有限：
  - 在引入调度器前先设计运行锁和写入边界
- 功能面增长过快导致 core 污染：
  - 坚持 adapter 外置原则
- 过早走向全量 AI 产品：
  - 先把监控、组合、规则和可靠性做深
  - 暂不把 Web / Agent / 新闻搜索作为主线

## 8. 近期优先级

建议按以下顺序继续推进：

1. 做组合级规则与指标
2. 做守护进程 / 可靠性 / 通知重试
3. 扩大全球市场支持范围
4. 做 CI / 覆盖率 / 更完整集成测试
5. 做 schema migration 策略
