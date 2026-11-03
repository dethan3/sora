import { Link } from 'react-router';
import type { DemoData } from '../data/demo';
import { ActionLink } from '../components/ui';
import { LandingSections } from '../components/landing/LandingSections';

export function Welcome({ data }: { data?: DemoData }) {
  return (
    <>
      <section className="hero">
        <div className="hero-light" aria-hidden="true" />
        <div className="hero-copy">
          <p className="eyebrow">
            <span className="eyebrow-line" />
            YOUR FUND RESEARCH ASSISTANT
          </p>
          <h1>
            让每一份投资，
            <br />
            <span>清晰可见。</span>
          </h1>
          <p className="hero-description">
            不用看复杂 K 线，也不用啃长篇财报。
            <br />
            用几个字的直白标签，看清股票近期影响与基金真实配料。
          </p>
          <div className="hero-actions">
            <ActionLink to="/discover">发现研究线索</ActionLink>
            <Link className="text-link" to="/discover?theme=ai-storage">
              看 AI 存储产业链 <span aria-hidden="true">→</span>
            </Link>
            <Link className="text-link" to="/research/semiconductor-equipment">
              阅读第一条研究 <span aria-hidden="true">→</span>
            </Link>
          </div>
          <p className="hero-caption">无需账户，即可开始探索</p>
        </div>
        <div className="preview-scene">
          <div className="preview-caption">
            <span className="status-dot" />
            智能投研助手 <span>极简标签 · 配料穿透 · 正反拔河</span>
          </div>
          <section className="portfolio-preview" aria-label="Sora 观察路径">
            <div className="preview-top">
              <span className="preview-wordmark">sora</span>
              <span>
                近期影响速览 <span aria-hidden="true">↗</span>
              </span>
            </div>
            <div className="preview-balance">
              <p>几个字看清：这只股票近期受什么影响？</p>
              <strong style={{ fontSize: '24px' }}>
                AI 存储 · AI 算力芯片
              </strong>
              <div className="preview-profit">
                <span>从公司财报标签，一键穿透到基金配料表。</span>
              </div>
            </div>
            <div className="hero-tag-preview" aria-label="股票影响标签示意">
              <div className="hero-tag-row">
                <strong>MU 美光</strong>
                <span className="impact-pill impact-positive">
                  🟢 HBM 产能已锁死
                </span>
                <span className="impact-pill impact-headwind">
                  🟠 手机电脑复苏慢
                </span>
              </div>
              <div className="hero-tag-row">
                <strong>NVDA 英伟达</strong>
                <span className="impact-pill impact-positive">
                  🟢 数据中心高增
                </span>
                <span className="impact-pill impact-watch">
                  🔍 盯毛利率斜率
                </span>
              </div>
              <div className="hero-tag-row">
                <strong>SOXX 配料</strong>
                <span className="mini-tag is-storage">🟩 AI存储 13.6%</span>
                <span className="mini-tag is-chips">🟦 AI芯片 18.8%</span>
              </div>
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
              按自己的节奏探索 <span>在发现页切换 AI 存储与芯片</span>
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
