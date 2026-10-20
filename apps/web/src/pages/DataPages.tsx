import { useState } from 'react';
import { Link, Route, Routes } from 'react-router';
import { portfolioSummary, type DemoData } from '../data/demo';
import { FundTable } from '../components/FundTable';
import {
  ActionLink,
  Change,
  EmptyState,
  PageHeader,
  Panel,
} from '../components/ui';

export function DataPages({ data }: { data: DemoData }) {
  const [query, setQuery] = useState('');
  const summary = portfolioSummary(data.positions);
  const money = (value: number) =>
    new Intl.NumberFormat('zh-CN', {
      style: 'currency',
      currency: 'CNY',
    }).format(value);
  return (
    <Routes>
      <Route
        path="/discover"
        element={
          <>
            <PageHeader
              eyebrow="DISCOVER"
              title="发现市场"
              description="从地区与主题出发，找到值得持续了解的方向。"
            />
            <div className="market-grid">
              {data.funds.map((fund, i) => (
                <section className={`market-card market-${i}`} key={fund.code}>
                  <div className="market-card-top">
                    <span className="market-symbol" aria-hidden="true">
                      {['CN', 'US', 'KR'][i] ?? 'GL'}
                    </span>
                    <span className="market-tag">市场示例</span>
                  </div>
                  <h2>{fund.market}</h2>
                  <span className="index-label">
                    0{i + 1} / {fund.category}
                  </span>
                  <p className="market-note">{fund.note}</p>
                  <Change value={fund.changePct} />
                  <p className="muted text-sm">虚构示例 · 非真实行情</p>
                  <ActionLink to="/funds">查看基金示例</ActionLink>
                </section>
              ))}
            </div>
          </>
        }
      />
      <Route
        path="/funds"
        element={
          <>
            <PageHeader
              eyebrow="FUND LIBRARY"
              title="基金"
              description="按名称、代码或市场查找基金，了解它们的类型与变化。"
            />
            <Panel title="基金目录">
              <label className="search-label" htmlFor="fund-search">
                搜索示例基金
              </label>
              <input
                id="fund-search"
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="名称、代码或市场"
              />
              <FundTable
                funds={data.funds.filter((f) =>
                  `${f.name} ${f.code} ${f.market}`
                    .toLowerCase()
                    .includes(query.trim().toLowerCase()),
                )}
              />
            </Panel>
          </>
        }
      />
      <Route
        path="/watchlist"
        element={
          <>
            <PageHeader
              eyebrow="WATCHLIST"
              title="我的关注"
              description="集中回看你关心的基金。当前为只读示例列表。"
            />
            <Panel title={`示例关注 · ${data.watchlist.length}`}>
              <FundTable
                funds={data.funds.filter((f) =>
                  data.watchlist.includes(f.code),
                )}
              />
            </Panel>
          </>
        }
      />
      <Route
        path="/portfolio"
        element={
          <>
            <PageHeader
              eyebrow="YOUR PORTFOLIO"
              title="我的组合"
              description="虚构人民币组合，供界面演示；没有连接真实账户。"
            />
            <div className="summary-grid">
              <Panel title="示例总市值 · CNY">
                <p className="metric">{money(summary.value)}</p>
              </Panel>
              <Panel title="示例总投入">
                <p className="metric">{money(summary.cost)}</p>
              </Panel>
              <Panel title="示例持有收益">
                <p className="metric">{money(summary.profit)}</p>
                <Change value={summary.changePct} />
              </Panel>
            </div>
            <Panel title="示例持仓分布">
              {data.positions.length ? (
                data.positions.map((position) => (
                  <div className="position" key={position.code}>
                    <span>
                      {data.funds.find((f) => f.code === position.code)?.name}
                    </span>
                    <strong>{money(position.value)}</strong>
                    <span className="allocation-track" aria-hidden="true">
                      <span
                        style={{
                          width: `${summary.value ? (position.value / summary.value) * 100 : 0}%`,
                        }}
                      />
                    </span>
                    <span className="allocation-percent">
                      {summary.value
                        ? ((position.value / summary.value) * 100).toFixed(1)
                        : '0.0'}
                      %
                    </span>
                  </div>
                ))
              ) : (
                <EmptyState title="暂无持仓">组合示例尚无内容。</EmptyState>
              )}
            </Panel>
          </>
        }
      />
      <Route
        path="*"
        element={
          <EmptyState title="没有找到这个页面">
            <Link to="/">返回首页</Link>
          </EmptyState>
        }
      />
    </Routes>
  );
}
