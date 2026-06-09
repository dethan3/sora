# Phase 1 — Monorepo 基础搭建

## 目标

跑通 `pnpm install`，TypeScript 编译无报错，项目骨架可被所有后续 phase 依赖。

## 估算

1 个 session

## 依赖

无

---

## 工作内容

### 根目录配置

- `package.json`：workspace root，含 scripts：
  - `"test": "pnpm -r run test"`
  - `"sora": "pnpm --filter @sora/cli run start"`
  - `"build": "pnpm -r run build"`
  - `"lint": "eslint ."`
  - `"format": "prettier --write ."`
- `pnpm-workspace.yaml`：声明 `apps/*` 和 `packages/*`
- `tsconfig.base.json`：共享 TS 配置（strict, ESM, Node20 target）

### 工具配置

- `eslint.config.js`：TypeScript ESLint 规则
- `.prettierrc`：统一格式
- `.gitignore`：node_modules, dist, .env, data/cache, data/sora.db
- `.env.example`：列出所有需要配置的环境变量（见总览）

### 目录骨架（各包只含 package.json + tsconfig.json，无业务代码）

```
apps/
  cli/          package.json, tsconfig.json
  worker/       package.json, tsconfig.json（预留，空）
  api/          package.json, tsconfig.json（预留，空）
packages/
  core/         package.json, tsconfig.json
  market/       package.json, tsconfig.json
  fund/         package.json, tsconfig.json
  research/     package.json, tsconfig.json
  sources/      package.json, tsconfig.json
  storage/      package.json, tsconfig.json
  agent/        package.json, tsconfig.json
  notifier/     package.json, tsconfig.json
  shared/       package.json, tsconfig.json
data/
  cache/        .gitkeep
  seeds/        .gitkeep（Phase 3 填充）
docs/
  dev-plan/     （当前文件夹）
  architecture.md
  web-roadmap.md
```

### 包间引用约定

所有包使用 `@sora/` namespace，如 `@sora/core`、`@sora/fund`。

依赖方向：

```
cli → market, fund, research, notifier, agent, storage
research → core, fund, market
fund → core, sources
market → core, sources
sources → core
agent → core, research（接口）
notifier → core
storage → core
```

---

## Done 条件

- [x] `pnpm install` 成功，无报错
- [x] `pnpm run build` 在各包（即使是空包）无编译错误
- [x] `.env.example` 包含所有需要的 key
- [x] 目录结构与 PRD 一致
