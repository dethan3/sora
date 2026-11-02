# 国际市场与港股数据源调研

核对日期：2026-10-26。范围为美股、国际资产和港股，不调研中国内地行情。前文调研记录基于公开官方文档，未订阅或验证付费套餐；后续已在本地实测 AKTools，新增结果见文末。

## 当前结论

Yahoo 可作为后续本地原型的候选，公开产品展示暂不选定供应商。先用 Mock 完成页面，真实接入优先考虑日线 / 收盘数据，独立生成快照，保留原始报价时间、交易状态、来源和单位。供应商可用性、具体指数覆盖以及网站公开展示权需要分别核对，股票 / ETF 的覆盖不能推出对应指数也可用。

## Yahoo Finance / yfinance

[yfinance 官方文档](https://ranaroussi.github.io/yfinance/) 说明项目不隶属 Yahoo，面向研究和教育，并提示 Yahoo Finance API 为个人使用。它是非官方客户端，不等同于 Yahoo 提供有服务承诺的商业数据 API。

[Yahoo 服务条款](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html) 对内容商业利用与分发设有约束；能在网页看到、或通过端点读取到数据，不足以确认可在 Sora 对外展示。此处记录公开文档边界，不作法律结论，也不声称免费端点有固定 SLA / 限流额度。

仓库已存在 `src/sora/providers/yahoo.py`，访问 Yahoo chart 端点；白名单含 QQQ、SPY 等部分 ETF，以及 NDX / SPX 等指数别名。它没有覆盖 AIQ、SOXX、IAU、IVV、2800.HK 或恒生指数，不应直接宣称满足新目录。后续可以评估日期归一化、异常处理与缓存逻辑的复用，本轮不修改、运行或连接旧采集器。

具体符号仍须分别验证，包括香港代码的前导零、后缀，以及指数 / 股票 / 信托类型。Yahoo 不是本轮 Mock 的来源，页面也不标注其为数据提供者。

## EODHD

官方 [API 文档](https://eodhd.com/financial-apis/exchanges-api-list-of-tickers-and-trading-hours) 提供交易所目录与分市场 ticker 列表；[OpenAPI](https://eodhistoricaldata.github.io/EODHD-openapi/index.html) 包含历史日线、延迟报价、公司行动和交易所信息端点。

官方仓库的 [符号说明](https://github.com/EodHistoricalData/eodhd-claude-skills/blob/main/skills/eodhd-api/references/general/stock-types-ticker-suffixes-guide.md) 给出港股 `0700.HK` 格式。说明港股代码路径存在，不证明恒生指数、盈富基金或本项目全部指标已在所需产品中可用。

[套餐页](https://eodhd.com/pricing) 区分 Personal 与 Commercial。个人套餐价格不能直接作为 Sora 商业展示成本；公开站点展示、历史数据缓存与再分发的具体权利及报价需核对商业协议。基于日线接口和市场目录的能力，EODHD 是后续值得优先做小规模验证的候选；这是一项工程判断，尚未选型。

## Twelve Data

[官方文档](https://twelvedata.com/docs) 提供报价、时间序列和交易所等接口。[交易所目录](https://twelvedata.com/exchanges?level=overview) 列出香港 XHKG，但 [EOD 文档](https://support.twelvedata.com/en/articles/12682324-end-of-day-eod-pricing-market-data) 又把香港列在需要额外许可或未支持、需确认状态的名单中。因此港股不能按普通套餐默认可用来设计。

[商业与个人使用说明](https://support.twelvedata.com/en/articles/5332349-commercial-and-personal-usage) 明确区分两者：商业展示受交易所授权约束，美国之外的价格数据需额外批准，再分发需单独协议。需进一步核对 ETF、指数、贵金属的具体品种与展示权，不能只依据“全球覆盖”宣传做选择。

## 港股市场与产品资料

[港交所数据授权页面](https://www.hkex.com.hk/Services/Market-Data-Services/Real-Time-Data-Services/Data-Licensing?sc_lang=en) 提供市场数据授权信息；[官方 FAQ](https://www.hkex.com.hk/Global/Exchange/FAQ/Market-Data?sc_lang=en) 涵盖延迟数据接入 / 分发。延迟和日线也需要按使用方式核对权利，不能推断为可免费公开展示。

[盈富基金官方资料](https://www.trahk.com.hk/en-hk/trahk-fund/) 与 [基金资料页](https://www.trahk.com.hk/en-hk/trahk-fund/fund-information/) 用来核对港元柜台 2800、基金名称与恒生指数的关联。这里只把产品资料写入 catalog，不导入其网站价格 / 净值。产品资料来源、指数行情提供者和交易所是不同概念。

## 下一轮验证顺序

1. 明确首版采用日线 / 收盘观察，以及公开站点的使用方式、预算与缓存周期。
2. 先核对 Nasdaq-100 / S&P 500 的指数品种与日线，再核对香港恒生指数和 2800 ETF；AI、半导体的具体参考指数及黄金现货报价分别确认，不用 ETF 冒充指数，也不用黄金期货替代现货。
3. 比较 EODHD 与其他候选实际返回的日期、单位、币种、时区、延迟、缺失、复权及公司行动；香港逐一核对代码映射与产品级授权。
4. 用独立脚本 / 服务获取数据和保护密钥，通过受验证的版本化 JSON 交给前端。不要把供应商 key 放进 `VITE_*`；如采用静态托管，只有在协议允许的范围内发布快照。
5. 验收休市、交易日历、夏令时、更新失败与陈旧数据标识，再考虑调度与线上部署。本轮不购买套餐、联系供应商或实现采集。

## 后续实施：AKTools 本地接入

用户确认采用 [AKTools](https://aktools.akfamily.xyz/) 将 AKShare 暴露为 HTTP API，网站与数据适配继续使用 TypeScript。后续小服务计划放独立 VPS，暂不限定供应商；不使用 AI 生成行情。

2026-10-26 本地实测 AKTools 0.0.91 / AKShare 1.19.3，新浪上游的 `.NDX`、`.INX` 和 HSI 三个指数历史接口成功，经 TS 校验后生成快照，最近记录均为 2026-10-23。覆盖与调用方式不代表已验证长期稳定性或网站公开展示权；没有改变前文对来源使用条件的判断。

Yahoo 仅作为显式可选备用读取，完成了符号 / 类型 / 时区验证的自动测试；本轮没有用它生成真实快照。具体运行、数据口径和恢复方式见 [AKTOOLS.md](./AKTOOLS.md)。
