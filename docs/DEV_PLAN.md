# Sora 开发计划（Testing + Storage 模块）

更新时间：2025-08-28 16:49 +08:00

## 1. 背景与目标
- 平台定位：专注中国市场 ETF 的量化分析与自动化调度。
- 当前状态：
  - 已完成 AKShare 接入、数据获取与批量优化、Parquet 缓存（依赖 pandas 2.3.x + pyarrow）。
  - 调度器 `TaskScheduler` 与默认任务已就绪（`src/scheduler/scheduler.py`）。
  - 测试仅覆盖基础数据获取（`tests/test_fetcher.py`）。
- 本阶段目标：
  1) 建立覆盖核心流程的测试体系，保证“获取-缓存-分析-决策-调度-报告”闭环稳定。
  2) 设计与实现独立的持久化存储模块（Storage），用于结构化保存行情、分析结果、信号与运行日志，支撑回溯与报告。

---

## 2. 工作分解（WBS）与里程碑

- M1 测试框架扩展（D+3）
  - Pytest 基础、覆盖率、通用 fixtures、Mock（AKShare/IO/时间）。
  - 完成 DataCache + DataFetcher 关键用例（含异常与回退）。

- M2 分析/决策测试（D+5）
  - `AnalyticsCalculator.analyze_fund()` 指标与阈值分档测试。
  - `DecisionEngine` 买/卖/持有边界条件与异常容错。

- M3 调度与报告测试（D+7）
  - `TaskScheduler` 任务生命周期（add/remove/start/stop/_should_run_task）。
  - 加速版集成测试：短周期触发分析 + 报告落盘。

- M4 Storage 模块设计与骨架（D+9）
  - 目录与接口确定，SQLite + SQLAlchemy + Repository 模式。
  - 实体与表结构：funds、quotes、history、analysis、signals、runs、reports。

- M5 Storage 接入与回归（D+12）
  - DataFetcher/Analytics/Report 写入存储，读写一致性与回归测试。
  - CLI/调度场景端到端验证。

---

## 3. 测试计划（优先级从高到低）

- A. DataCache（`src/data/cache.py`）与 Parquet
  - 读写 round-trip（索引/列类型/中文列名），空/缺列处理。
  - 过期清理 `cleanup_expired_data()` 不同 `expire_hours` 行为。
  - 目录/权限异常容错。

- B. DataFetcher（`src/data/fetcher.py`）
  - `batch_get_current_data()`：正常/部分失败/全失败（Mock AKShare）。
  - `get_historical_data()`：天数、频率边界；节假日/空数据。
  - 与 DataCache 协作：成功后写入缓存；异常回退路径断言。

- C. AnalyticsCalculator（`src/analytics/calculator.py`）
  - 指标计算：均线、波动率、动量、缺失值处理；输出 schema 固定。
  - `analyze_fund()` 推荐分档（strong_buy/buy/hold/sell/strong_sell）。

- D. DecisionEngine（`src/decision/engine.py`）
  - 各阈值边界与不同输入组合的稳定性；异常/缺失数据容错。

- E. Scheduler（`src/scheduler/scheduler.py`）
  - `add/remove/list/get` 基本行为。
  - `_should_run_task()`：最大运行次数、取消、时间比较。
  - `_execute_*` 分支与依赖交互（Mock DataFetcher/Analytics/DataCache）。
  - `setup_default_tasks()`：创建任务与 `interval_minutes` 合法性。

- F. CLI（`main.py`）
  - `click.testing.CliRunner`：`init/analyze/report/schedule/status` 最小参数可运行且返回码为 0。

- G. E2E（端到端）
  - 获取→缓存→分析→报告：Mock 数据喂入，验证文件落地与内容摘要。
  - 调度加速：1-2 分钟周期触发一次分析，断言 `run_count/next_run` 更新。

- H. 兼容性/回归
  - pandas 2.3.x + pyarrow 回归：分类列、时间索引、中文列名 round-trip。
  - 时区/交易日：节假日/周末历史数据为空时不崩溃，能优雅跳过。
  - 性能基线（可选）：批量 100 只基金运行耗时与内存阈值。

- 工具与约定
  - `pytest`, `pytest-cov`, `unittest.mock/pytest-mock`, `freezegun`, `tmp_path`。
  - 固定随机种子；输出 schema/字段名稳定断言；覆盖率目标：总覆盖 ≥ 80%，核心模块 ≥ 90%。

---

## 4. Storage 模块设计与计划（新增）

- 目标：将结构化数据持久化，支持查询与回溯，补齐缓存不可检索的短板。

- 技术选型：
  - SQLite（本地轻量，无服务依赖）
  - SQLAlchemy 2.x（ORM + 迁移友好）
  - Alembic（后续可选，做 schema 迁移）

- 目录结构（建议）：
```
src/storage/
  __init__.py
  models.py          # ORM 实体定义（Fund, Quote, History, Analysis, Signal, Run, Report）
  database.py        # SessionLocal/engine 管理，初始化工具
  repositories.py    # Repository 抽象与实现（读写接口）
  service.py         # 组合服务，供 Fetcher/Analytics/Report/Scheduler 调用
  schemas.py         # Pydantic/TypedDict（如需要对外类型）
```

- 数据模型（初版）：
  - Fund(id, code, name, market, enabled, created_at)
  - Quote(id, fund_id, ts, price, change_pct, volume, raw_json)
  - History(id, fund_id, ts, open, high, low, close, volume)
  - Analysis(id, fund_id, ts, recommendation, score, factors_json)
  - Signal(id, fund_id, ts, action, confidence, reason)
  - Run(id, task_id, start_ts, end_ts, status, message)
  - Report(id, path, generated_at, summary_json)

- 关键 API：
  - `StorageService.upsert_fund(fund)` / `get_fund_by_code(code)`
  - `StorageService.save_quotes(fund_code, df)` / `save_history(...)`
  - `StorageService.save_analysis(fund_code, result)` / `load_latest_analysis(...)`
  - `StorageService.log_task_run(task_id, status, message)`
  - `StorageService.save_report(path, summary)` / `list_reports()`

- 集成点：
  - `DataFetcher`：获取后写入 Quote/History；仍保留 Parquet 缓存以做快速读。
  - `AnalyticsCalculator`：分析结果入库；`DecisionEngine` 产生信号入库。
  - `TaskScheduler`：记录任务运行元数据（Run）。
  - `Report`：报告索引信息入库，便于查询与审计。

- 验收标准：
  - 读写接口幂等（相同 ts + fund 唯一约束）。
  - 事务安全；异常回滚；并发读安全（写较少，SQLite 足够）。
  - 查询常用检索（按 fund/time range）稳定且有索引。

---

## 5. 风险与缓解
- 外部 API 波动：严格 Mock + 重试与降级路径测试。
- 数据格式变化：输出 schema 固化 + 版本字段/迁移脚本。
- SQLite 锁争用：批量写入使用事务；分析与调度尽量读多写少。
- 时间/时区问题：统一使用本地时间存盘并存 UTC 字段（后续可扩展）。

---

## 6. 任务清单（追踪）
- [ ] 设计 storage 目录与 API（M4）
- [ ] 实现 Storage 骨架与最小表结构（M4）
- [ ] DataCache & DataFetcher 测试（M1）
- [ ] Analytics & Decision 测试（M2）
- [ ] Scheduler & 报告 测试（M3）
- [ ] CLI 测试（M3）
- [ ] E2E 闭环 & 回归（M3-M5）
- [ ] CI & 覆盖率阈值（M1-M3）

---

## 7. 执行建议
- 先测再改：M1 完成后再并行推进 Storage 设计，以免接口漂移。
- Mock 为先：外部资源（网络/时间/磁盘权限）统一 Mock，提升稳定性。
- 渐进集成：Storage 先旁路写入，再逐步替代读取路径。

建议下一步
请确认文档内容是否需要调整：

是否采用 SQLite + SQLAlchemy 的技术选型？
storage 实体/表结构是否需要新增字段（如基准指数、账户/持仓）？
先落地哪一块：优先实现 src/storage/ 骨架，还是先补齐 DataCache/DataFetcher 测试？
我建议按文档里程碑推进：

先补齐 DataCache/DataFetcher 单测（M1），
并行敲定 src/storage/ 目录与 API（M4），
再推进调度与报告测试（M3）与存储接入（M5）。
确认后我就开始创建 src/storage/ 目录与模型骨架，并提交首批测试用例。