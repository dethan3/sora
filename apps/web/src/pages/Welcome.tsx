import { Link } from 'react-router';
import type { DemoData } from '../data/demo';
import { ActionLink } from '../components/ui';
import { usePersonal } from '../features/local/PersonalProvider';
import { LandingSections } from '../components/landing/LandingSections';

export function Welcome({ data }: { data?: DemoData }) {
  const personal = usePersonal();
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
            <ActionLink to="/funds">浏览基金</ActionLink>
            <Link className="text-link" to="/portfolio">
              {personal.state.positions.length
                ? '回到我的组合'
                : '建立我的组合'}{' '}
              <span aria-hidden="true">→</span>
            </Link>
          </div>
          <p className="hero-caption">无需账户，即可开始探索</p>
        </div>
        <div className="preview-scene">
          <div className="preview-caption">
            <span className="status-dot" />
            从兴趣开始 <span>发现 · 研究 · 关注</span>
          </div>
          <section className="portfolio-preview" aria-label="Sora 观察路径">
            <div className="preview-top">
              <span className="preview-wordmark">sora</span>
              <span>
                我的观察 <span aria-hidden="true">↗</span>
              </span>
            </div>
            <div className="preview-balance">
              <p>找到想持续了解的方向</p>
              <strong style={{ fontSize: '28px' }}>AI · 半导体 · 黄金</strong>
              <div className="preview-profit">
                <span>连接市场与基金，建立自己的观察路径。</span>
              </div>
            </div>
            <div className="illustration-chart">
              <svg
                viewBox="0 0 440 116"
                role="img"
                aria-label="市场、主题与基金相连的观察路径"
              >
                <path
                  d="M30 58H410"
                  fill="none"
                  stroke="#e1def8"
                  strokeWidth="2"
                />
                <path
                  d="M30 58C100 58 140 28 220 58S340 88 410 58"
                  fill="none"
                  stroke="#635bff"
                  strokeWidth="2.5"
                />
                {[30, 220, 410].map((x) => (
                  <circle key={x} cx={x} cy="58" r="6" fill="#635bff" />
                ))}
                <text
                  x="30"
                  y="98"
                  textAnchor="middle"
                  fill="#63718a"
                  fontSize="12"
                >
                  市场
                </text>
                <text
                  x="220"
                  y="98"
                  textAnchor="middle"
                  fill="#63718a"
                  fontSize="12"
                >
                  主题
                </text>
                <text
                  x="410"
                  y="98"
                  textAnchor="middle"
                  fill="#63718a"
                  fontSize="12"
                >
                  基金
                </text>
              </svg>
            </div>
            <div className="preview-holdings">
              <div className="preview-row-label">
                <span>探索观察方向</span>
                <span>继续了解</span>
              </div>
              {(data?.modules.slice(0, 3) ?? []).map((item, i) => (
                <Link
                  className="preview-position"
                  key={item.id}
                  to={'/discover/' + item.id}
                >
                  <span className={'asset-dot asset-' + i} />
                  <span>{item.title}</span>
                  <strong>{item.symbol}</strong>
                  <span aria-hidden="true">↗</span>
                </Link>
              ))}
            </div>
            <div className="preview-foot">
              按自己的节奏探索 <span>在发现页管理观察方向</span>
            </div>
          </section>
          <div className="preview-note">
            <span className="note-icon" aria-hidden="true">
              ↗
            </span>
            <div>
              <strong>从整体，理解变化</strong>
              <p>让每一次浏览，接上上一次的思考。</p>
            </div>
          </div>
        </div>
      </section>
      <LandingSections data={data} />
    </>
  );
}
