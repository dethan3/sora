import { useState } from 'react';
import { Link, useLocation, useParams, useSearchParams } from 'react-router';
import type { DemoData } from '../../data/demo';
import { EmptyState, PageHeader } from '../../components/ui';
import {
  detailUrl,
  researchSource,
  sourceLabel,
  rememberList,
  fundLinkId,
} from '../../navigation/flow';
import { useDirections } from '../discover/DirectionProvider';
import { usePersonal } from '../local/PersonalProvider';
import { downloadText } from '../local/BackupTools';
import { WatchButton } from '../funds/WatchButton';
import {
  research,
  relatedSignals,
  researchUrl,
  type ResearchSignal,
} from './model';
import { RESEARCH_KEY, useResearchSaved, type ResearchVerdict } from './saved';
import {
  aiStorage,
  fundSnapshot,
  holdingFor,
  themeExposure,
} from '../domains/model';
import { DomainSource } from '../domains/FundStructure';
import './research.css';

const VERDICT_OPTIONS: Array<{
  value: ResearchVerdict;
  label: string;
  desc: string;
}> = [
  { value: 'valid', label: '👍 逻辑继续成立', desc: '顺风驱动仍在兑现' },
  { value: 'watch', label: '🤔 存疑观望', desc: '等待下季财报确认' },
  { value: 'invalid', label: '🛑 逻辑失效', desc: '逆风兑现或前提改变' },
];

export function ResearchVerdictBar({ signal }: { signal: ResearchSignal }) {
  const saved = useResearchSaved();
  const current = saved.verdicts[signal.id] ?? null;
  return (
    <div
      className="research-verdict-bar"
      role="group"
      aria-label={`个人复盘打卡：${signal.title}`}
    >
      <span className="verdict-title">我的复盘判断：</span>
      <div className="verdict-buttons">
        {VERDICT_OPTIONS.map((option) => {
          const active = current === option.value;
          return (
            <button
              key={option.value}
              type="button"
              className={`verdict-btn is-${option.value}${active ? ' is-active' : ''}`}
              aria-pressed={active}
              onClick={() =>
                saved.setVerdict(signal.id, active ? null : option.value)
              }
            >
              {option.label}
            </button>
          );
        })}
      </div>
      {current && (
        <small className="verdict-hint">
          当前判断：
          {VERDICT_OPTIONS.find((item) => item.value === current)?.desc}
          （保存在本地浏览器）
        </small>
      )}
    </div>
  );
}

export function FollowResearch({ signal }: { signal: ResearchSignal }) {
  const saved = useResearchSaved();
  const active = saved.ids.includes(signal.id);
  return (
    <div className="research-follow">
      <button
        className="button-secondary"
        aria-pressed={active}
        aria-label={`${active ? '取消关注' : '关注'}线索：${signal.title}`}
        onClick={() => saved.toggle(signal.id)}
      >
        {active ? '已关注线索' : '关注线索'}
      </button>
      {(saved.issue || saved.error) && (
        <p role="alert">{saved.issue || saved.error}</p>
      )}
    </div>
  );
}

function SignalCard({
  signal,
  reason,
}: {
  signal: ResearchSignal;
  reason?: string;
}) {
  const location = useLocation();
  const source =
    researchSource(location.pathname + location.search) ??
    researchSource(location.pathname) ??
    '/discover';
  const href =
    researchUrl(signal) + '?' + new URLSearchParams({ from: source });
  const remember = () => rememberList(source, 'research:' + signal.id);
  return (
    <article className="research-card">
      <div className="research-meta">
        <span>
          {signal.themeId === 'ai-storage'
            ? 'AI 存储 / HBM与企业盘'
            : '半导体 / 设备'}
        </span>
        <span className="research-status">{signal.status}</span>
        <time dateTime={signal.eventDate}>事件 {signal.eventDate}</time>
      </div>
      {reason && <p className="research-match">{reason}</p>}
      <h3>
        <Link
          id={fundLinkId('research:' + signal.id)}
          to={href}
          onClick={remember}
        >
          {signal.title} <span aria-hidden="true">↗</span>
        </Link>
      </h3>
      <p>{signal.summary}</p>
      {(signal.prosTags || signal.consTags) && (
        <div className="research-tug-preview" aria-label="正反方核心标签">
          <div className="tug-col">
            <small>看多事实</small>
            <div className="tug-pills">
              {signal.prosTags?.map((tag) => (
                <span key={tag} className="impact-pill impact-positive">
                  🟢 {tag}
                </span>
              ))}
            </div>
          </div>
          <div className="tug-col">
            <small>冷水提醒</small>
            <div className="tug-pills">
              {signal.consTags?.map((tag) => (
                <span key={tag} className="impact-pill impact-headwind">
                  🟠 {tag}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}
      <div className="research-card-bottom">
        <span>
          {signal.company.symbol} ·{' '}
          {signal.funds.map((fund) => fund.code).join(' / ')}
        </span>
        <Link className="text-link" to={href} onClick={remember}>
          展开研究 →
        </Link>
      </div>
      <small>人工整理 · 核对 {signal.reviewedAt} · 尚未验证超额收益</small>
    </article>
  );
}

export function ResearchFeed() {
  const directions = useDirections();
  const personal = usePersonal();
  const [mode, setMode] = useState<'all' | 'mine'>('all');
  const mine = relatedSignals(directions.ids, personal.state.watchlist);
  const signals = mode === 'all' ? research : mine;
  return (
    <section className="research-feed" aria-labelledby="research-feed-title">
      <div className="research-section-heading">
        <div>
          <p className="eyebrow">RESEARCH / 01</p>
          <h2 id="research-feed-title">从变化，找到研究线索</h2>
          <p>先读正反方标签与事实证据，再看公司与基金的关联。无需录入持仓。</p>
        </div>
        <div className="segmented" aria-label="研究线索范围">
          <button aria-pressed={mode === 'all'} onClick={() => setMode('all')}>
            全部线索
          </button>
          <button
            aria-pressed={mode === 'mine'}
            onClick={() => setMode('mine')}
          >
            与我相关 · {mine.length}
          </button>
        </div>
      </div>
      {signals.length ? (
        signals.map((signal) => (
          <SignalCard
            key={signal.id}
            signal={signal}
            reason={
              mine.includes(signal)
                ? '关联你关注的方向或自选基金'
                : '探索线索 · 可先关注下方的半导体或 AI 方向'
            }
          />
        ))
      ) : (
        <div className="research-empty">
          <h3>还没有与你关注内容相关的线索</h3>
          <p>
            当前先覆盖 AI 存储与半导体设备。选择半导体、AI 方向，或关注
            SOXX、AIQ、QQQ 后，这里会展示关联研究；其他方向暂未覆盖。
          </p>
          <a className="text-link" href="#explore-directions">
            选择关注方向 ↓
          </a>
        </div>
      )}
    </section>
  );
}

export function ResearchHubPage() {
  const [themeFilter, setThemeFilter] = useState<
    'all' | 'ai-storage' | 'ai-chips'
  >('all');
  const filtered = research.filter(
    (item) => themeFilter === 'all' || item.themeId === themeFilter,
  );
  return (
    <div className="research-hub-page">
      <PageHeader
        eyebrow="RESEARCH LAB"
        title="研究线索与正反方拔河"
        description="把复杂财报拆成几个字的看多事实与冷水提醒，穿透到基金真实配料。"
      />
      <div
        className="research-theme-tabs"
        role="group"
        aria-label="按主题筛选研究"
      >
        <button
          type="button"
          aria-pressed={themeFilter === 'all'}
          onClick={() => setThemeFilter('all')}
        >
          全部研究 ({research.length})
        </button>
        <button
          type="button"
          aria-pressed={themeFilter === 'ai-storage'}
          onClick={() => setThemeFilter('ai-storage')}
        >
          AI 存储产业链
        </button>
        <button
          type="button"
          aria-pressed={themeFilter === 'ai-chips'}
          onClick={() => setThemeFilter('ai-chips')}
        >
          AI 芯片与设备
        </button>
      </div>
      <div className="research-hub-list">
        {filtered.map((signal) => (
          <div key={signal.id} className="research-hub-item">
            <SignalCard signal={signal} />
            <FollowResearch signal={signal} />
          </div>
        ))}
      </div>
    </div>
  );
}

export function RelatedResearch({
  fundCode,
  directionId,
}: {
  fundCode?: string;
  directionId?: string;
}) {
  const items = research.filter((signal) =>
    fundCode
      ? signal.funds.some((fund) => fund.code === fundCode)
      : !!directionId && signal.directionIds.includes(directionId),
  );
  if (!items.length) return null;
  return (
    <section className="related-research" aria-label="关联研究线索">
      <div className="research-section-heading">
        <div>
          <h2>关联研究线索</h2>
          <p>从事件与结构出发，理解这项关联。</p>
        </div>
      </div>
      {items.map((signal) => (
        <SignalCard key={signal.id} signal={signal} />
      ))}
      {fundCode && (
        <p className="research-disclosure">
          结构观察：
          {fundSnapshot(fundCode)?.description ??
            items[0].funds.find((fund) => fund.code === fundCode)
              ?.difference}{' '}
          {fundSnapshot(fundCode)
            ? '已核对发行人持仓，请在上方结构中查看公司权重与资料日期；不同日期或节选资料不用于完整重叠率计算。'
            : '当前未取得带日期的持仓明细，暂不计算公司权重、集中度及基金重叠。'}
        </p>
      )}
    </section>
  );
}

export function SavedResearch() {
  const saved = useResearchSaved();
  const [exportError, setExportError] = useState('');
  const signals = research.filter((signal) => saved.ids.includes(signal.id));
  const missing = saved.ids.filter(
    (id) => !research.some((signal) => signal.id === id),
  );
  return (
    <section className="saved-research" aria-label="关注的研究线索">
      <div className="research-section-heading">
        <div>
          <h2>研究线索 · {saved.ids.length}</h2>
          <p>
            关注后可在这里继续盯住下次验证大考。研究状态由资料核对更新，当前未提供自动提醒。
          </p>
        </div>
      </div>
      {(saved.issue || saved.error || exportError) && (
        <p role="alert">{saved.issue || saved.error || exportError}</p>
      )}
      {signals.map((signal) => (
        <div key={signal.id} className="saved-research-card-wrap">
          <SignalCard signal={signal} />
          <div className="saved-checkpoints-strip" aria-label="下次验证大考">
            <strong>下次大考盯什么：</strong>
            {signal.checkpoints.map((cp) => (
              <span key={cp.title} className="impact-pill impact-watch">
                🔍 {cp.title}（{cp.state}）
              </span>
            ))}
          </div>
          <ResearchVerdictBar signal={signal} />
          <FollowResearch signal={signal} />
        </div>
      ))}
      {missing.map((id) => (
        <div className="unavailable-row" key={id}>
          <span>{id} · 线索暂不可用，关注记录仍保留</span>
          <button className="button-quiet" onClick={() => saved.toggle(id)}>
            移除线索 {id}
          </button>
        </div>
      ))}
      {!saved.ids.length && !saved.issue && (
        <p className="muted">
          还没有关注的研究线索。
          <Link className="text-link" to="/discover">
            发现研究线索 →
          </Link>
        </p>
      )}
      <p className="research-disclosure">
        研究关注保存在此浏览器，与基金关注分别管理。
        <button
          className="button-quiet"
          onClick={() => {
            setExportError('');
            try {
              downloadText(
                localStorage.getItem(RESEARCH_KEY) ??
                  JSON.stringify({ schemaVersion: 1, ids: [] }),
                'sora-research-backup.json',
              );
            } catch {
              setExportError('无法下载研究关注记录，请检查浏览器权限后重试。');
            }
          }}
        >
          下载研究关注记录
        </button>
      </p>
    </section>
  );
}

export function ResearchPage({ data }: { data: DemoData }) {
  const { id } = useParams();
  const location = useLocation();
  const [params] = useSearchParams();
  const source = researchSource(params.get('from')) ?? '/discover';
  const signal = research.find((item) => item.id === id);
  if (!signal)
    return (
      <EmptyState title="没有找到这条研究线索">
        <Link to="/discover">返回发现市场</Link>
      </EmptyState>
    );
  const activeDomain = signal.themeId === 'ai-storage' ? aiStorage : undefined;
  return (
    <article className="research-page">
      <Link className="back-link" to={source} state={{ restoreList: true }}>
        ← {sourceLabel(source)}
      </Link>
      <header className="research-heading">
        <div>
          <p className="eyebrow">
            {signal.themeId === 'ai-storage'
              ? 'SORA RESEARCH / AI 存储产业链'
              : 'SORA RESEARCH / 半导体'}
          </p>
          <h1>{signal.title}</h1>
          <p>{signal.summary}</p>
          <div className="research-meta">
            <span className="research-status">{signal.status}</span>
            <span>事件 {signal.eventDate}</span>
            <span>人工核对 {signal.reviewedAt}</span>
          </div>
        </div>
        <FollowResearch signal={signal} />
      </header>

      {(signal.prosTags || signal.consTags) && (
        <section className="research-tug-board" aria-label="三秒看懂正反方拔河">
          <div className="tug-board-col is-pro">
            <h3>🟢 看多理由（已披露事实）</h3>
            <div className="tug-pills">
              {signal.prosTags?.map((tag) => (
                <span key={tag} className="impact-pill impact-positive">
                  🟢 {tag}
                </span>
              ))}
            </div>
          </div>
          <div className="tug-board-col is-con">
            <h3>🟠 泼盆冷水（潜在风险与干扰）</h3>
            <div className="tug-pills">
              {signal.consTags?.map((tag) => (
                <span key={tag} className="impact-pill impact-headwind">
                  🟠 {tag}
                </span>
              ))}
            </div>
          </div>
        </section>
      )}

      <nav className="research-anchor-nav" aria-label="研究章节">
        <a href="#evidence">事实与证据</a>
        <a href="#transmission">影响路径</a>
        <a href="#fund-mapping">基金映射</a>
        <a href="#verification">后续验证</a>
      </nav>
      <div className="research-layout">
        <div className="research-main">
          <section id="evidence" className="research-block">
            <p className="eyebrow">01 / EVIDENCE</p>
            <h2>先确认，发生了什么</h2>
            {signal.facts.map((fact, index) => (
              <div className="research-fact" key={fact.text}>
                <span>事实 {index + 1}</span>
                <p>{fact.text}</p>
                <SourceLink signal={signal} id={fact.sourceId} />
              </div>
            ))}
          </section>
          <section id="transmission" className="research-block">
            <p className="eyebrow">02 / HYPOTHESIS</p>
            <h2>从一家公司，延伸到一个方向</h2>
            <div className="research-company">
              <strong>{signal.company.symbol}</strong>
              <span>{signal.company.role}</span>
            </div>
            <ol className="research-chain">
              {signal.chain.map((step) => (
                <li key={step.title}>
                  <h3>{step.title}</h3>
                  <p>{step.text}</p>
                </li>
              ))}
            </ol>
            <div className="research-hypothesis">
              <strong>研究假设 · 尚待验证</strong>
              <p>{signal.hypothesis}</p>
            </div>
            <h3>哪些情况会削弱这条线索？</h3>
            <ul>
              {signal.counterpoints.map((point) => (
                <li key={point}>{point}</li>
              ))}
            </ul>
          </section>
          <section id="fund-mapping" className="research-block">
            <p className="eyebrow">03 / FUND MAPPING</p>
            <h2>无法直接买入公司时，如何继续研究基金？</h2>
            <p>
              区分已确认的直接持仓与投资范围关联，分别查看资料日期和证据。基金关联不构成交易替代或优劣排名，购买渠道与资格需单独核实。
            </p>
            <div className="research-mapping">
              {signal.funds.map((mapping) => {
                const fund = data.funds.find(
                  (item) => item.code === mapping.code,
                );
                const snapshot = fundSnapshot(mapping.code, activeDomain);
                const holding =
                  snapshot && holdingFor(snapshot, signal.company.symbol);
                const confirmed = holding?.status === 'held';
                const exposure =
                  snapshot && signal.themeId
                    ? themeExposure(snapshot, signal.themeId)
                    : undefined;
                return (
                  <article key={mapping.code}>
                    <div className="research-mapping-heading">
                      <span className="research-status">
                        {confirmed
                          ? '直接持有 ' + signal.company.symbol
                          : mapping.relation}
                      </span>
                      <strong>{mapping.code}</strong>
                      <span>{mapping.scope}</span>
                    </div>
                    <h3>{fund?.name ?? mapping.code}</h3>
                    <p>{mapping.reason}</p>
                    <p className="research-difference">
                      {confirmed ? snapshot!.description : mapping.difference}
                    </p>
                    <dl>
                      <div>
                        <dt>持仓资料日期</dt>
                        <dd>
                          {snapshot?.asOf ?? mapping.holdingsAsOf ?? '未取得'}
                        </dd>
                      </div>
                      <div>
                        <dt>目标公司权重</dt>
                        <dd>
                          {confirmed
                            ? holding.weight.toFixed(2) + '%'
                            : holding?.status === 'unknown'
                              ? '节选未列出，持有状态待核实'
                              : mapping.weight === null
                                ? '未核实'
                                : mapping.weight + '%'}
                        </dd>
                      </div>
                      {exposure && (
                        <div>
                          <dt>{exposure.domainName}合计</dt>
                          <dd>
                            {exposure.weight > 0
                              ? `${exposure.weight.toFixed(2)}%`
                              : '前十未列出'}
                          </dd>
                        </div>
                      )}
                      <div>
                        <dt>核对日期</dt>
                        <dd>{signal.reviewedAt}（投资范围）</dd>
                      </div>
                    </dl>
                    {snapshot ? (
                      <DomainSource id={snapshot.sourceId} />
                    ) : (
                      <SourceLink signal={signal} id={mapping.sourceId} />
                    )}
                    <div className="research-mapping-actions">
                      {fund ? (
                        <>
                          <Link
                            className="text-link"
                            id={fundLinkId(fund.code)}
                            to={detailUrl(
                              fund.code,
                              location.pathname + location.search,
                            )}
                            onClick={() =>
                              rememberList(
                                location.pathname + location.search,
                                fund.code,
                              )
                            }
                          >
                            研究这只基金 →
                          </Link>
                          <WatchButton fund={fund} />
                        </>
                      ) : (
                        <span>产品资料暂不可用</span>
                      )}
                    </div>
                  </article>
                );
              })}
            </div>
            <p className="research-disclosure">
              已核对产品可在基金详情查看前十大持仓；资料日期和覆盖范围不同，暂不计算完整重叠率或替代评分。未核对的产品保留缺失状态。
            </p>
          </section>
          <section id="verification" className="research-block">
            <p className="eyebrow">04 / FOLLOW THROUGH</p>
            <h2>接下来，用什么验证？</h2>
            <div className="research-checkpoints">
              {signal.checkpoints.map((point) => (
                <div key={point.title}>
                  <span className="research-status">{point.state}</span>
                  <h3>{point.title}</h3>
                  <p>{point.question}</p>
                </div>
              ))}
            </div>
            <p className="research-disclosure">
              当前仅建立研究问题，尚未自动追踪后续公告，也没有回测或验证超额收益。
            </p>
            <ResearchVerdictBar signal={signal} />
            <FollowResearch signal={signal} />
          </section>
          <section className="research-block research-extension">
            <p className="eyebrow">BEYOND THE SECTOR</p>
            <h2>{signal.extension.title}</h2>
            <p>{signal.extension.text}</p>
            {signal.id === 'semiconductor-equipment' ? (
              <Link className="text-link" to="/research/ai-storage-hbm">
                阅读 AI 存储产业链传导线索 →
              </Link>
            ) : (
              <Link
                className="text-link"
                to="/research/semiconductor-equipment"
              >
                阅读半导体设备传导线索 →
              </Link>
            )}
          </section>
        </div>
        <aside className="research-sidebar">
          <section className="research-block">
            <h2>这条研究如何阅读</h2>
            <p>
              事实来自原始披露；影响路径是待验证假设；基金的直接持仓与投资范围关联分别标明。
            </p>
            <p>线索用于辅助研究，不提供买卖结论。</p>
            <h3>关联方向</h3>
            {signal.directionIds.map((directionId) => (
              <Link
                className="research-direction-link"
                key={directionId}
                to={'/discover/' + directionId}
              >
                {data.modules.find((item) => item.id === directionId)?.title ??
                  directionId}{' '}
                →
              </Link>
            ))}
          </section>
          <section className="research-block">
            <h2>原始资料</h2>
            {signal.sources.map((source) => (
              <div className="research-source" key={source.id}>
                <SourceLink signal={signal} id={source.id} />
                <small>
                  {source.publishedAt
                    ? '发布 ' + source.publishedAt
                    : '产品页 · 发布日期未标注'}
                  <br />
                  核对 {signal.reviewedAt}
                </small>
              </div>
            ))}
            <p>研究资料日期与基金行情日期分别记录。</p>
          </section>
        </aside>
      </div>
    </article>
  );
}
function SourceLink({ signal, id }: { signal: ResearchSignal; id: string }) {
  const source = signal.sources.find((item) => item.id === id)!;
  return (
    <a
      className="research-source-link"
      href={source.url}
      target="_blank"
      rel="noreferrer"
    >
      {source.name} ↗
    </a>
  );
}
