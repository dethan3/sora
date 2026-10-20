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
              title="从市场开始，打开视野。"
              description="三个地区，三种观察角度。这里展示可持续扩展的内容模块。"
            />
            <div className="market-grid">
              {data.funds.map((fund, i) => (
                <Panel key={fund.code} title={fund.market}>
                  <span className="index-label">
                    0{i + 1} / {fund.category}
                  </span>
                  <p className="market-note">{fund.note}</p>
                  <Change value={fund.changePct} />
                  <p className="muted text-sm">虚构示例 · 非真实行情</p>
                  <ActionLink to="/funds">查看基金示例</ActionLink>
                </Panel>
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
              title="基金，清楚地放在一起。"
              description="先了解市场与类型，再慢慢建立自己的观察范围。"
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
              title="让关注，有一个安静的位置。"
              description="以下为示例关注列表。添加、移除与本地保存将在后续阶段开放。"
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
              title="从整体，看见每一份变化。"
              description="虚构人民币组合，供界面演示；没有连接真实账户。"
            />
            <div className="summary-grid">
              <Panel title="示例总市值">
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
                    <span className="muted">
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
            <Link to="/">返回 Welcome</Link>
          </EmptyState>
        }
      />
    </Routes>
  );
}
