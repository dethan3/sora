# Phase 8 — `apps/cli`

## 目标

实现所有 CLI 命令，串联所有包，完成端到端链路验证。

## 估算

1.5 个 session

## 依赖

Phase 5（market + fund）、Phase 6（agent + research + notifier）、Phase 7（storage）

---

## 技术选型

- **Commander.js**：命令路由
- **tsx**：直接运行 TypeScript，无需预编译
- 入口：`apps/cli/src/index.ts`，通过 `pnpm sora` 调用

---

## 命令列表

### 数据库

```bash
pnpm sora db init
# 初始化 SQLite 数据库，创建所有表

pnpm sora db seed
# 导入 data/seeds/ 数据到数据库
# 可选：--with-cache 同时导入 data/cache/ 中的真实抓取数据
```

### 数据刷新

```bash
pnpm sora data refresh --type market
# 刷新市场/指数行情缓存（调用 yahoo-finance2，写入 data/cache/market-quotes/）

pnpm sora data refresh --type fund --code 159941
# 刷新指定基金数据缓存（调用东方财富接口）

pnpm sora data search --query "恒生科技最新成分股"
# 通过 Tavily 搜索并缓存结果
```

### 市场

```bash
pnpm sora markets list
# 列出所有市场

pnpm sora market show --id us-tech
# 查看单个市场（含关联指数列表）
```

### 指数

```bash
pnpm sora indexes list
# 列出所有指数

pnpm sora indexes list --market us-tech
# 列出某市场下的指数（含实时行情）
```

### 基金映射与分析

```bash
pnpm sora funds map --index nasdaq-100
# 列出 Nasdaq 100 对应的国内基金（来自真实 API）

pnpm sora funds map --market us-tech
# 列出某市场下所有相关指数与基金

pnpm sora funds analyze --index nasdaq-100
# 横向分析同一指数下的基金，输出评分、风险提示
```

`funds analyze` 输出格式：

```
基金代码  基金名称              类型   费率   规模(亿)  溢价率  申购状态  夏普比率  最大回撤  跟踪误差  执行质量评分  风险提示
159941   易方达纳指ETF         ETF   0.20%   220     +0.3%  正常     1.42    -35%    0.8%    82       -
513100   华夏纳指ETF联接A      联接   0.50%   180     N/A    正常     1.38    -35%    1.2%    74       -
007339   易方达纳指C           QDII  0.75%    85     N/A   限购(1万) 1.35    -36%    1.5%    68       ⚠️ 限购
```

### 研究卡片

```bash
pnpm sora research create --market us-tech
# 生成美国科技市场研究卡片（调用 Pi Agent + Tavily）

pnpm sora research create --index nasdaq-100
# 生成 Nasdaq 100 指数研究卡片

pnpm sora research list
# 列出已生成的研究卡片（从数据库）
```

### 提醒

```bash
pnpm sora alerts list
# 列出当前所有风险提醒

pnpm sora alerts list --level warning
# 按级别过滤
```

### 通知导出

```bash
pnpm sora notifications export
# 导出所有 NotificationEvent 为 JSON（写入 stdout 或指定文件）

pnpm sora notifications export --output ./data/notifications.json
# 导出到文件
```

---

## CLI 输出规范

- 表格输出使用对齐格式，终端友好
- 风险提示用 `⚠️` 标记
- 严重风险用 `🚨` 标记
- 所有输出末尾附加免责声明：
  ```
  ─────────────────────────────────────────────────────
  ℹ️  以上内容为信息分析，不构成任何投资建议。
  ```

---

## Done 条件

- [x] `pnpm sora db init` 可运行（创建 `data/sora.db`，路径自动解析为 repo root）
- [x] `pnpm sora db seed` 可运行（6 markets / 6 indexes / 7 funds / 7 mappings / 7 metrics）
- [x] `pnpm sora data refresh --type market` 接口已实现（调用 createMarketSource）
- [x] `pnpm sora markets list` 可运行（6 个市场，表格对齐）
- [x] `pnpm sora indexes list --market us-tech` 可运行（只显示 nasdaq-100，filter 正确）
- [x] `pnpm sora funds map --index nasdaq-100` 返回 3 只基金，含费率/规模/申购状态
- [x] `pnpm sora funds analyze --index nasdaq-100` 返回评分(67–83)和风险提示（限购/溢价）
- [x] `pnpm sora research create --market us-tech` 确定性生成研究卡片（Pi fallback）；可 --save 保存到 DB
- [x] `pnpm sora notifications export` 导出 JSON，含 NotificationEvent 数组
- [x] 所有命令末尾有免责声明

> 关键修复：pnpm 转发 `--` 给 tsx，`index.ts` 在 Commander.parse 前剥离 argv[2]=='--'；source factory 路径通过 `process.env.SORA_SEEDS_DIR/SORA_CACHE_DIR` 注入（解决 CWD 为 apps/cli/ 时 './data/seeds' 解析错误的问题）。
