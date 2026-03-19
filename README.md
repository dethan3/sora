# Sora

`dev` 分支已经把这个仓库从旧的 ETF 量化实验，切到新的 `Sora` 监控内核起步版本。

当前目标不是做完整产品，而是先建立一个可复用的监控内核：
- 统一管理基金 / 指数监控标的
- 拉取行情和历史序列
- 生成规则化分析结果
- 持久化快照与分析历史
- 给后续 `OpenClaw skill`、飞书 Bot、Web UI 提供稳定底座

## 当前能力

- `SQLite` 持久化
- `watchlist` 管理
- 单次运行编排 `run-once`
- 规则化分析：
  - 日涨跌
  - 7 日 / 30 日变化
  - 简单趋势判断
  - 基础风险等级
- `AkShare` Provider
  - 当前优先支持 A 股基金
  - 支持部分中国指数
  - 全局市场暂未实现

## 项目结构

```text
src/sora/
  analysis.py
  config.py
  domain.py
  orchestrator.py
  repository.py
  providers/
    akshare.py
    base.py
```

## 快速开始

要求：
- Python 3.11+
- 已安装 `requirements.txt`

初始化数据库：

```bash
python main.py init-db
```

添加监控标的：

```bash
python main.py watchlist add --code 510300 --name "沪深300ETF" --asset-type fund --market cn
python main.py watchlist add --code sh000001 --name "上证指数" --asset-type index --market cn
```

查看监控列表：

```bash
python main.py watchlist list
```

执行一次分析：

```bash
python main.py run-once
python main.py run-once --code 510300
```

## 设计原则

- `core` 只负责数据、分析、持久化、编排
- 平台交互不写进 core
- 飞书、OpenClaw、Web 都应该作为 adapter 接入
- 先做稳定的规则引擎，再逐步补 AI 总结、告警 DSL、多源行情和任务调度

## 下一步

接下来建议按这个顺序扩展：
1. `AlertRule` / `AlertEvent`
2. `NotificationEvent` 和飞书 Bot adapter
3. 多数据源抽象
4. 更完整的历史查询 API
5. skill / bot / web 三个入口层
- [配置指南](config/) - ETF列表和策略参数配置说明
- [API接口](src/) - 各模块接口文档和扩展指南

### 💻 **模块扩展**
- **新增ETF**：修改`config/funds.yaml`添加新的ETF代码
- **策略调优**：调整`config/settings.yaml`中的技术指标参数
- **数据源扩展**：在`src/data/fetcher.py`中集成新的数据接口
- **指标扩展**：在`src/analytics/calculator.py`中添加新的技术指标

### 🔧 **部署运维**
```bash
# 数据库备份
python main.py backup --path ./backup/

# 系统健康检查  
python main.py status --detail

# 日志查看
tail -f data/logs/sora.log

# 性能监控
python main.py stats
```

---

## ⚖️ 风险提示

> **投资有风险，量化模型仅供参考**
> 
> 本平台提供的技术分析和投资信号基于历史数据，不能保证未来表现。用户应结合自身投资目标、风险承受能力和市场判断做出投资决策。任何投资损失由用户自行承担。

---

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 开源协议，欢迎参与贡献和改进。

---

<div align="center">

**📊 专业ETF量化分析，数据驱动投资决策！**

[立即开始](https://github.com/yourusername/sora) • [查看架构](project.md) • [配置ETF列表](config/funds.yaml)

</div>
