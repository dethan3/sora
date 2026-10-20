import { Link } from 'react-router';
import { portfolioSummary, type DemoData } from '../data/demo';
import { ActionLink, Change } from '../components/ui';

export function Welcome({ data }: { data?: DemoData }) {
  const summary = data ? portfolioSummary(data.positions) : undefined;
  return (
    <>
      <section className="hero">
        <div className="hero-light" aria-hidden="true" />
        <div className="hero-copy">
          <p className="eyebrow">
            <span className="eyebrow-line" />
            YOUR INVESTMENT, IN PERSPECTIVE
          </p>
          <h1>
            让每一份投资，
            <br />
            <span>清晰可见。</span>
          </h1>
          <p className="hero-description">
            从发现市场，到理解你的组合。
            <br />
            把基金、关注与持仓连在一起，看清变化的全貌。
          </p>
          <div className="hero-actions">
            <ActionLink to="/discover">探索 Sora</ActionLink>
            <Link className="text-link" to="/portfolio">
              查看组合示例 <span aria-hidden="true">→</span>
            </Link>
          </div>
          <p className="hero-caption">无需账户，即可浏览示例</p>
        </div>
        <div className="preview-scene">
          <div className="preview-caption">
            <span className="status-dot" />
            产品预览 <span>静态示例</span>
          </div>
          <section className="portfolio-preview" aria-label="组合示例预览">
            <div className="preview-top">
              <span className="preview-wordmark">sora</span>
              <span>
                我的组合 <span aria-hidden="true">↗</span>
              </span>
            </div>
            <div className="preview-balance">
              <p>
                组合总市值 <span>CNY</span>
              </p>
              {summary ? (
                <>
                  <strong>
                    {summary.value.toLocaleString('zh-CN', {
                      minimumFractionDigits: 2,
                    })}
                  </strong>
                  <div className="preview-profit">
                    <span>持有收益</span>
                    <span>¥{summary.profit.toFixed(2)}</span>
                    <Change value={summary.changePct} />
                  </div>
                </>
              ) : (
                <p className="muted">组合示例正在准备中，可先探索市场。</p>
              )}
            </div>
            <div className="illustration-chart">
              <svg
                viewBox="0 0 440 116"
                role="img"
                aria-label="走势示意图，不代表历史净值"
              >
                <defs>
                  <linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#635bff" stopOpacity=".18" />
                    <stop offset="100%" stopColor="#635bff" stopOpacity="0" />
                  </linearGradient>
                </defs>
                <path
                  className="chart-grid"
                  d="M0 18H440 M0 58H440 M0 98H440"
                />
                <path
                  d="M0 99L20 91L40 97L60 77L80 81L100 72L120 77L140 55L160 61L180 53L200 64L220 44L240 47L260 30L280 40L300 27L320 33L340 19L360 27L380 14L400 21L420 8L440 12V116H0Z"
                  fill="url(#chart-fill)"
                />
                <path
                  d="M0 99L20 91L40 97L60 77L80 81L100 72L120 77L140 55L160 61L180 53L200 64L220 44L240 47L260 30L280 40L300 27L320 33L340 19L360 27L380 14L400 21L420 8L440 12"
                  fill="none"
                  stroke="#635bff"
                  strokeWidth="2.5"
                  strokeLinejoin="round"
                />
              </svg>
              <p>走势示意 · 非历史净值</p>
            </div>
            <div className="preview-holdings">
              <div className="preview-row-label">
                <span>持仓分布</span>
                <span>市值 / 占比</span>
              </div>
              {data?.positions.map((position, i) => (
                <div className="preview-position" key={position.code}>
                  <span className={'asset-dot asset-' + i} />
                  <span>
                    {
                      data.funds.find((fund) => fund.code === position.code)
                        ?.name
                    }
                  </span>
                  <strong>¥{position.value.toLocaleString('zh-CN')}</strong>
                  <span>
                    {summary?.value
                      ? ((position.value / summary.value) * 100).toFixed(1)
                      : '0.0'}
                    %
                  </span>
                </div>
              ))}
            </div>
            <div className="preview-foot">
              虚构数据 <span>{data?.asOf ?? '示例预览'}</span>
            </div>
          </section>
          <div className="preview-note">
            <span className="note-icon" aria-hidden="true">
              ↗
            </span>
            <div>
              <strong>从整体，理解变化</strong>
              <p>市值、收益与持仓分布，一处呈现。</p>
            </div>
          </div>
        </div>
      </section>
      <section className="journey" aria-labelledby="journey-title">
        <div className="journey-intro">
          <p className="eyebrow">ONE CONNECTED VIEW</p>
          <h2 id="journey-title">从市场，到你的组合。</h2>
          <p>沿着自己的投资路径，逐步建立清晰的认识。</p>
        </div>
        <div className="journey-grid">
          {[
            {
              number: '01',
              title: '发现值得了解的市场',
              description: '从地区与主题出发，找到你想持续观察的方向。',
              to: '/discover',
              label: '发现市场',
            },
            {
              number: '02',
              title: '把关注集中在一起',
              description: '在同一个视图中，回看关心的基金与日常变化。',
              to: '/watchlist',
              label: '查看关注',
            },
            {
              number: '03',
              title: '理解组合的全貌',
              description: '看清投入、收益和分布，建立整体视角。',
              to: '/portfolio',
              label: '了解组合',
            },
          ].map((item) => (
            <Link className="journey-item" to={item.to} key={item.number}>
              <span className="journey-number">{item.number}</span>
              <h3>{item.title}</h3>
              <p>{item.description}</p>
              <span className="journey-link">
                {item.label} <span aria-hidden="true">→</span>
              </span>
            </Link>
          ))}
        </div>
        <p className="journey-disclaimer">
          当前提供只读示例，关注与持仓尚不支持编辑。
        </p>
      </section>
    </>
  );
}
