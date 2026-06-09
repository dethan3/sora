# Phase 9 — 测试完善 + Docs

## 目标

覆盖 PRD 要求的所有测试类型，完善项目文档，通过全部验收标准。

## 估算

1 个 session

## 依赖

Phase 8（CLI 全部可运行）

---

## 测试要求

使用 Vitest。测试分两类：

### 单元测试（各包内部）

| 测试项 | 位置 | 说明 |
|------|------|------|
| Core schema validation | `packages/core` | 每个 Zod schema valid/invalid case |
| Market list | `packages/market` | 返回正确市场数量和字段 |
| Index query | `packages/market` | 按 marketId 过滤 |
| Fund mapping | `packages/fund` | 按 indexId 返回正确基金列表 |
| Fund analysis scoring | `packages/fund` | 评分公式计算正确 |
| Premium risk detection | `packages/fund` | premiumRate > 3% 触发 warning |
| Purchase status detection | `packages/fund` | limited/suspended 触发提示 |
| Research card generation | `packages/research` | 确定性生成器路径（不依赖 Pi） |
| Notification event generation | `packages/notifier` | 从 FundAnalysis 生成正确事件 |

### 集成测试

| 测试项 | 说明 |
|------|------|
| EastMoneyFundSource（fixture） | 使用本地 fixture JSON，不真实网络请求 |
| YahooFinanceSource（fixture） | 同上 |
| TavilySearchSource（fixture） | 使用 fixture response，不消耗 quota |
| Pi Agent（fixture） | 使用 fixture Pi response，验证 schema 解析 |
| db:init + db:seed | 使用 in-memory SQLite |
| End-to-end：Nasdaq 100 链路 | Seed 数据 → fund analysis → research card → notification |

### 测试覆盖率目标

- `packages/core`：100%
- `packages/fund`（评分函数）：100%
- `packages/research`（确定性路径）：≥ 90%
- 其他包：≥ 70%

---

## 文档要求

### README.md（根目录）

必须包含：

1. 项目定位（英文 + 中文，来自 PRD）
2. 非目标和合规边界
3. 技术栈
4. 环境变量配置说明（含 Pi API key、Tavily key）
5. 项目结构
6. CLI 用法（所有命令示例）
7. Seed 数据说明
8. 数据缓存说明（`data/cache/` 结构）
9. 后续路线图
10. Tickeye 集成方式（消费 NotificationEvent JSON）
11. Pi Agent 集成说明（当前已接入，如何配置）

### docs/ 文档

| 文件 | 内容 |
|------|------|
| `docs/architecture.md` | 模块依赖图、数据流、核心决策说明 |
| `docs/research-card.md` | ResearchCard 结构说明 + 示例 + Pi prompt 模板 |
| `docs/signal-templates.md` | NotificationEvent 类型和 payload 格式 |
| `docs/data-sources.md` | 所有接口 URL、字段映射、限制（Phase 3 产出） |
| `docs/web-roadmap.md` | Web UI 后续规划（不在第一版实现） |

---

## 最终验收检查清单

### 功能

- [ ] `pnpm install` 成功
- [ ] `pnpm test` 全部通过
- [ ] `pnpm sora db init` 可运行
- [ ] `pnpm sora db seed` 可运行
- [ ] `pnpm sora data refresh --type market` 可运行
- [ ] `pnpm sora markets list` 可运行
- [ ] `pnpm sora indexes list --market us-tech` 可运行
- [ ] `pnpm sora funds map --index nasdaq-100` 返回真实数据
- [ ] `pnpm sora funds analyze --index nasdaq-100` 可运行
- [ ] `pnpm sora research create --market us-tech` 调用 Pi 生成卡片
- [ ] `pnpm sora notifications export` 导出正确 JSON
- [ ] `data/cache/` 下有真实抓取的数据

### 合规

- [ ] 所有输出使用"信息分析"和"风险提示"语气
- [ ] 无任何买入/卖出建议输出
- [ ] CLI 末尾有免责声明

### 代码质量

- [ ] 无旧 Python 代码
- [ ] 无 Web UI 代码
- [ ] `eslint` 无 error
- [ ] TypeScript 编译无 error
- [ ] README 已更新为新项目定位
