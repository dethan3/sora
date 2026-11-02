import { Link, useLocation, useNavigate, useSearchParams } from 'react-router';
import type { DemoData } from '../../data/demo';
import { detailUrl, fundLinkId, rememberList } from '../../navigation/flow';
import { WatchButton } from '../funds/WatchButton';
import { research } from '../research/model';
import { FollowResearch } from '../research/Research';
import { aiChips, equityOverlap, holdingFor, topTenWeight } from './model';
import { DomainSource } from './FundStructure';
import './domains.css';

export function DomainPage({ data }: { data: DemoData }) {
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  const navigate = useNavigate();
  const requested = params.get('company');
  const company = aiChips.companies.find((item) => item.symbol === requested);
  const events = aiChips.events
    .filter((item) => !company || item.company === company.symbol)
    .toSorted((a, b) => b.publishedAt.localeCompare(a.publishedAt));
  const companies = company ? [company] : aiChips.companies;
  const source = location.pathname + location.search;
  const overlap = equityOverlap(
    aiChips.fundSnapshots[0],
    aiChips.fundSnapshots[1],
  );
  const relatedResearch = research.find(
    (signal) => signal.id === 'semiconductor-equipment',
  )!;
  function select(symbol: string) {
    const next = new URLSearchParams(params);
    if (symbol) next.set('company', symbol);
    else next.delete('company');
    setParams(next, { replace: true });
  }
  function showFunds(symbol: string) {
    navigate(
      {
        pathname: '/discover',
        search: '?' + new URLSearchParams({ company: symbol }),
        hash: '#company-funds',
      },
      { replace: true },
    );
  }
  return (
    <div className="domain-page">
      <header className="domain-header">
        <div>
          <p className="eyebrow">领域研究 / 01</p>
          <h1>AI 芯片</h1>
          <p>{aiChips.description}</p>
        </div>
        <Link className="domain-directory-link" to="/discover?view=markets">
          浏览其他市场资料 →
        </Link>
      </header>
      <div className="domain-dataset-line">
        <span className="domain-live-dot" aria-hidden="true" />
        <span>人工核对 {aiChips.reviewedAt}</span>
        <span>
          {aiChips.companies.length} 家公司 · {aiChips.events.length} 份业绩披露
          · {aiChips.fundSnapshots.length} 只基金
        </span>
        <span>尚未自动更新</span>
      </div>
      <div className="domain-workspace">
        <aside className="domain-company-rail" aria-label="研究范围">
          <h2>产业位置</h2>
          <div
            className="domain-company-buttons"
            role="group"
            aria-label="选择关联公司"
          >
            <button aria-pressed={!company} onClick={() => select('')}>
              <span>全部公司</span>
              <small>计算与制造设备</small>
            </button>
            {aiChips.companies.map((item) => (
              <button
                key={item.symbol}
                aria-pressed={company?.symbol === item.symbol}
                onClick={() => select(item.symbol)}
              >
                <span>
                  {item.name}
                  <b>{item.symbol}</b>
                </span>
                <small>{item.role}</small>
              </button>
            ))}
          </div>
          <label className="domain-company-select">
            关联公司
            <select
              value={company?.symbol ?? ''}
              onChange={(event) => select(event.target.value)}
            >
              <option value="">全部公司</option>
              {aiChips.companies.map((item) => (
                <option key={item.symbol} value={item.symbol}>
                  {item.name} · {item.role}
                </option>
              ))}
            </select>
          </label>
          <p>先覆盖计算与设备三个观察对象，基金持仓用于核实关联。</p>
          <a className="text-link" href="#domain-evidence">
            核对原始资料 ↓
          </a>
        </aside>
        <div className="domain-content">
          {requested && !company && (
            <p className="domain-alert" role="status">
              暂未覆盖 {requested}，正在展示全部已覆盖公司。
              <button className="button-quiet" onClick={() => select('')}>
                清除公司筛选
              </button>
            </p>
          )}
          {company && (
            <section className="domain-company-context" aria-label="当前公司">
              <span className="domain-company-symbol">{company.symbol}</span>
              <div>
                <h2>
                  {company.name}
                  <span>{company.role}</span>
                </h2>
                <p>{company.description}</p>
                <DomainSource id={company.sourceId} />
              </div>
              <button className="button-quiet" onClick={() => select('')}>
                查看全部公司
              </button>
            </section>
          )}
          <div className="domain-reading-grid">
            <section
              className="domain-events"
              aria-labelledby="domain-events-title"
            >
              <div className="domain-section-title">
                <div>
                  <p className="eyebrow">WHAT CHANGED</p>
                  <h2 id="domain-events-title">
                    {company
                      ? company.name + ' 的相关变化'
                      : '这个领域，发生了什么'}
                  </h2>
                </div>
                <span>{events.length} 条</span>
              </div>
              <p className="domain-small-note">
                按披露时间排列；财报覆盖期与发布时间分别记录。
              </p>
              {events.map((event) => (
                <article className="domain-event" key={event.id}>
                  <div className="domain-event-meta">
                    <span>{event.company}</span>
                    <time dateTime={event.publishedAt}>
                      {event.publishedAt}
                    </time>
                    <span>公司业绩披露</span>
                  </div>
                  <h3>{event.title}</h3>
                  <p className="domain-fact">{event.fact}</p>
                  <dl className="domain-event-reading">
                    <div>
                      <dt>研究解读</dt>
                      <dd>{event.interpretation}</dd>
                    </div>
                    <div>
                      <dt>继续观察</dt>
                      <dd>{event.watch}</dd>
                    </div>
                  </dl>
                  <div className="domain-event-footer">
                    <DomainSource id={event.sourceId} />
                    <button
                      className="domain-inline-button"
                      onClick={() => showFunds(event.company)}
                    >
                      查看 {event.company} 的基金关联 →
                    </button>
                  </div>
                  <small>报告期：{event.period}</small>
                  {event.researchId && (
                    <Link
                      className="domain-deep-link"
                      to={
                        '/research/' +
                        event.researchId +
                        '?' +
                        new URLSearchParams({ from: source })
                      }
                      onClick={() => rememberList(source, 'domain-research')}
                      id={fundLinkId('domain-research')}
                    >
                      完整证据与传导分析 →
                    </Link>
                  )}
                </article>
              ))}
            </section>
            <aside
              id="company-funds"
              className="domain-fund-bridge"
              aria-label="公司与基金关联"
            >
              <div className="domain-section-title">
                <div>
                  <p className="eyebrow">COMPANY → FUND</p>
                  <h2>从公司，找到基金</h2>
                </div>
              </div>
              <p className="domain-small-note">
                按已披露的直接持仓关联，非收益排名或交易替代。购买资格需另行核实。
              </p>
              {aiChips.fundSnapshots.map((snapshot) => {
                const fund = data.funds.find(
                  (item) => item.code === snapshot.code,
                );
                if (!fund)
                  return (
                    <p key={snapshot.code}>
                      {snapshot.code} 的产品资料暂不可用。
                    </p>
                  );
                return (
                  <article className="domain-fund-card" key={snapshot.code}>
                    <div className="domain-fund-card-top">
                      <strong>{snapshot.code}</strong>
                      <WatchButton fund={fund} />
                    </div>
                    <h3>{fund.name}</h3>
                    <p>{snapshot.scope}</p>
                    <div className="domain-company-weights">
                      {companies.map((item) => {
                        const holding = holdingFor(snapshot, item.symbol);
                        return (
                          <div key={item.symbol}>
                            <span>{item.name}</span>
                            <strong>
                              {holding.weight === null
                                ? holding.status === 'unknown'
                                  ? '节选未列出'
                                  : '权益列表未持有'
                                : holding.weight.toFixed(2) + '%'}
                            </strong>
                          </div>
                        );
                      })}
                    </div>
                    <p className="domain-fund-date">
                      持仓 {snapshot.asOf} ·{' '}
                      {snapshot.coverage === 'full-equity'
                        ? '完整权益列表'
                        : '前十大节选'}
                    </p>
                    {company &&
                      holdingFor(snapshot, company.symbol).status ===
                        'unknown' && (
                        <p className="domain-unknown">
                          未列出不代表未持有，需补充完整持仓核实。
                        </p>
                      )}
                    <DomainSource id={snapshot.sourceId} />
                    <Link
                      className="domain-fund-link"
                      id={fundLinkId(fund.code)}
                      to={detailUrl(fund.code, source)}
                      onClick={() => rememberList(source, fund.code)}
                    >
                      查看 {snapshot.code} 持仓结构 →
                    </Link>
                  </article>
                );
              })}
            </aside>
          </div>
          <section
            className="domain-comparison domain-surface"
            aria-label="基金结构对照"
          >
            <div className="domain-section-title">
              <div>
                <p className="eyebrow">UNDERSTAND THE DIFFERENCE</p>
                <h2>都持有芯片公司，覆盖方式却不同</h2>
              </div>
            </div>
            <div className="domain-comparison-grid">
              {aiChips.fundSnapshots.map((snapshot) => (
                <div key={snapshot.code}>
                  <h3>
                    {snapshot.code}
                    <span>{snapshot.scope}</span>
                  </h3>
                  <p>{snapshot.description}</p>
                  <dl>
                    <div>
                      <dt>前十大合计</dt>
                      <dd>{topTenWeight(snapshot).toFixed(2)}%</dd>
                    </div>
                    <div>
                      <dt>资料日期</dt>
                      <dd>{snapshot.asOf}</dd>
                    </div>
                    <div>
                      <dt>持仓覆盖</dt>
                      <dd>
                        {snapshot.coverage === 'full-equity'
                          ? '30 项权益持仓'
                          : '仅前十大'}
                      </dd>
                    </div>
                  </dl>
                </div>
              ))}
            </div>
            <p className="domain-footnote">
              前十大合计衡量持仓集中程度，不代表 AI 收入占比。{overlap.reason}{' '}
              可分别查看两份资料中的已确认公司权重。
            </p>
          </section>
          <section className="domain-follow-through">
            <div>
              <p className="eyebrow">FOLLOW THE EVIDENCE</p>
              <h2>把产业变化，放回证据链中</h2>
              <p>
                进一步阅读设备交付与制造投入的传导假设、反向解释及待验证事项。
              </p>
              <Link
                className="text-link"
                to={
                  '/research/' +
                  relatedResearch.id +
                  '?' +
                  new URLSearchParams({ from: source })
                }
              >
                阅读半导体设备线索 →
              </Link>
            </div>
            <FollowResearch signal={relatedResearch} />
          </section>
          <section
            id="domain-evidence"
            className="domain-evidence"
            aria-label="领域原始资料"
          >
            <h2>原始资料与日期</h2>
            <p>
              此页为已核对资料集，内容不会随行情刷新自动更新。所有研究解读均与公司已披露事实分开。
            </p>
            {aiChips.sources.map((item) => (
              <div key={item.id}>
                <DomainSource id={item.id} />
                <span>
                  {item.kind === 'issuer-holdings' ? '基金持仓' : '公司披露'} ·
                  核对 {aiChips.reviewedAt}
                </span>
              </div>
            ))}
          </section>
        </div>
      </div>
    </div>
  );
}
