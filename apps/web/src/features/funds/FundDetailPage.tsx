import { useState } from 'react';
import { Link, useLocation, useParams, useSearchParams } from 'react-router';
import type { DemoData, Fund } from '../../data/demo';
import { Change, EmptyState, PageHeader, Panel } from '../../components/ui';
import { WatchButton } from './WatchButton';
import { listSource, sourceLabel } from '../../navigation/flow';
import { usePersonal } from '../local/PersonalProvider';

export function historyWindow(
  history: Fund['history'],
  period: 'week' | 'all',
) {
  if (period === 'all' || !history.length) return history;
  const end = new Date(`${history.at(-1)!.date}T00:00:00Z`).getTime();
  return history.filter(
    (point) =>
      new Date(`${point.date}T00:00:00Z`).getTime() >= end - 7 * 86400000,
  );
}
export function FundDetailPage({ data }: { data: DemoData }) {
  const { code } = useParams();
  const location = useLocation();
  const [params] = useSearchParams();
  const source = listSource(params.get('from')) ?? '/funds';
  const personal = usePersonal();
  const fund = data.funds.find((item) => item.code === code);
  const [period, setPeriod] = useState<'week' | 'all'>('all');
  if (!fund)
    return (
      <EmptyState title="未找到这只基金">
        <Link to={source} state={{ restoreList: true }}>
          返回{sourceLabel(source)}
        </Link>
      </EmptyState>
    );
  const history = historyWindow(fund.history, period);
  const first = history[0];
  const last = history.at(-1);
  const change =
    history.length > 1 ? (last!.value / first.value - 1) * 100 : null;
  const minimum = Math.min(...history.map((point) => point.value));
  const maximum = Math.max(...history.map((point) => point.value));
  const points = history
    .map(
      (point) =>
        `${40 + ((Date.parse(point.date) - Date.parse(first.date)) / (Date.parse(last!.date) - Date.parse(first.date) || 1)) * 720},${170 - ((point.value - minimum) / (maximum - minimum || 1)) * 130}`,
    )
    .join(' ');
  return (
    <>
      <Link className="back-link" to={source} state={{ restoreList: true }}>
        ← {sourceLabel(source)}
      </Link>
      <div className="detail-heading">
        <PageHeader
          eyebrow={fund.code}
          title={fund.name}
          description={`${fund.market} · ${fund.category} · ${fund.currency} 计价`}
        />
        <WatchButton fund={fund} />
      </div>
      <div className="detail-grid">
        <Panel title="净值样本">
          <div className="fund-quote">
            <strong>{fund.nav ?? '待更新'}</strong>
            <span>{fund.currency} / 份</span>
            {fund.nav !== null && <Change value={fund.changePct} />}
          </div>
          <p className="muted">快照日期 {data.asOf} · 来源：Sora 虚构样本</p>
          <div className="chart-toolbar">
            <h3>样本走势</h3>
            <div className="segmented" aria-label="样本区间">
              <button
                aria-pressed={period === 'all'}
                onClick={() => setPeriod('all')}
              >
                全部样本
              </button>
              <button
                aria-pressed={period === 'week'}
                onClick={() => setPeriod('week')}
              >
                近 7 天
              </button>
            </div>
          </div>
          {history.length > 1 ? (
            <>
              <figure className="history-figure">
                <div className="chart-scale">
                  <span>区间净值范围</span>
                  <span>
                    {minimum.toFixed(4)} — {maximum.toFixed(4)} {fund.currency}{' '}
                    / 份
                  </span>
                </div>
                <svg
                  className="history-chart"
                  viewBox="0 0 800 200"
                  preserveAspectRatio="none"
                  role="img"
                  aria-label={`${fund.name}净值样本趋势，${first.date}至${last!.date}`}
                >
                  <path
                    d="M40 40H760 M40 105H760 M40 170H760"
                    className="chart-grid"
                  />
                  <polyline
                    points={points}
                    fill="none"
                    stroke="#635bff"
                    strokeWidth="3"
                    strokeLinejoin="round"
                    vectorEffect="non-scaling-stroke"
                  />
                </svg>
                <figcaption className="chart-dates">
                  <time dateTime={first.date}>{first.date}</time>
                  <time dateTime={last!.date}>{last!.date}</time>
                </figcaption>
              </figure>
              <p className="trend-explanation">
                样本区间{change! > 0 ? '上行' : change! < 0 ? '下行' : '持平'}{' '}
                <Change value={change!} />
                。按区间首末净值计算，不含分红、费用或汇率变化。
              </p>
              <details className="history-details">
                <summary>查看 {history.length} 条样本数据</summary>
                <table>
                  <caption className="sr-only">净值样本明细</caption>
                  <thead>
                    <tr>
                      <th scope="col">日期</th>
                      <th scope="col">净值（{fund.currency} / 份）</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((point) => (
                      <tr key={point.date}>
                        <th scope="row">{point.date}</th>
                        <td>{point.value.toFixed(6)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </details>
            </>
          ) : (
            <EmptyState title="暂无足够的净值样本">
              历史样本不足，暂不展示走势。
              {fund.nav === null
                ? '当前净值也暂不可用，持仓会标记为待估值。'
                : '当前净值仍可用于持仓估值。'}
            </EmptyState>
          )}
        </Panel>
        <aside className="detail-aside">
          <Panel title="观察这只基金">
            <p className="muted">{fund.note}</p>
            <p className="muted">
              历史仅为虚构样本，不代表真实收益或未来表现。
            </p>
            <Link
              className="action"
              to={`/portfolio?${new URLSearchParams({ add: fund.code, currency: fund.currency, from: location.pathname + location.search })}`}
            >
              {personal.state.positions.some(
                (position) => position.code === fund.code,
              )
                ? '编辑持仓'
                : '录入持仓'}{' '}
              <span aria-hidden="true">→</span>
            </Link>
          </Panel>
          <Panel title="如何理解这条曲线">
            <p className="muted">
              纵轴按当前样本范围缩放，不从零起。区间变化只说明这些样本的首末差异。
            </p>
            <p className="muted">
              报价缺失时不使用旧值冒充当前净值；不同币种在组合中分别统计。
            </p>
          </Panel>
        </aside>
      </div>
    </>
  );
}
