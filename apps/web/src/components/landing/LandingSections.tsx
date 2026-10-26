import { useId, useState } from 'react';
import { Link } from 'react-router';
import type { DemoData } from '../../data/demo';
import { ActionLink, Change } from '../ui';
import './landing.css';

function MarketPreview({ data }: { data?: DemoData }) {
  const [direction, setDirection] = useState('ai');
  const resultId = useId();
  const selected =
    data?.modules.find((module) => module.id === direction) ?? data?.modules[0];
  const funds =
    data?.funds.filter((fund) => selected?.fundCodes.includes(fund.code)) ?? [];

  return (
    <div className="landing-market-preview">
      <div className="landing-preview-bar">
        <span className="landing-preview-brand">sora / 发现</span>
        <span className="landing-example-label">找到你的方向</span>
      </div>
      <div className="landing-market-body">
        <p className="landing-preview-label">选择一个观察方向</p>
        <div className="landing-directions" role="group" aria-label="观察方向">
          {data?.modules.map((module) => (
            <button
              key={module.id}
              type="button"
              aria-pressed={selected?.id === module.id}
              aria-controls={resultId}
              onClick={() => setDirection(module.id)}
            >
              {module.title}
            </button>
          ))}
        </div>
        <div
          id={resultId}
          className="landing-market-result"
          aria-live="polite"
          aria-atomic="true"
        >
          {selected ? (
            <>
              <div className="landing-direction-heading">
                <span className="landing-direction-symbol" aria-hidden="true">
                  {selected.symbol}
                </span>
                <div>
                  <p className="landing-preview-label">
                    {selected.kind === 'index'
                      ? '宽基指数'
                      : selected.kind === 'asset'
                        ? '资产与商品'
                        : '主题视角'}
                  </p>
                  <h3>{selected.title}</h3>
                </div>
              </div>
              <p className="landing-direction-description">
                {selected.description}
              </p>
              <div className="landing-connection" aria-hidden="true">
                <span />
                关联到基金
              </div>
              <ul
                className="landing-related-funds"
                aria-label={`${selected.title}关联的基金`}
              >
                {funds.map((fund) => (
                  <li key={fund.code}>
                    <div>
                      <strong>{fund.name}</strong>
                      <span>
                        {fund.code} · {fund.currency}
                      </span>
                    </div>
                    <div className="landing-example-change">
                      <span>日变化</span>
                      {fund.nav === null ? (
                        <strong>暂无净值</strong>
                      ) : (
                        <Change value={fund.changePct} />
                      )}
                    </div>
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <p className="landing-preview-fallback">
              从地区、主题出发，查看它们与基金的关联。你也可以直接进入发现页开始浏览。
            </p>
          )}
        </div>
        <Link
          className="landing-inline-link"
          to={selected ? `/funds?module=${selected.id}` : '/discover'}
        >
          {selected ? '查看这个方向的基金' : '前往发现页'}{' '}
          <span aria-hidden="true">→</span>
        </Link>
      </div>
      <p className="landing-preview-footnote">从观察方向，继续了解关联基金。</p>
    </div>
  );
}

export function LandingSections({ data }: { data?: DemoData }) {
  const watchExamples = data?.funds.slice(0, 2) ?? [];

  return (
    <div className="landing-content">
      <section className="landing-intro" aria-labelledby="landing-intro-title">
        <div>
          <p className="landing-eyebrow">认识 SORA</p>
          <h2 id="landing-intro-title">
            给市场观察，
            <br />
            一条清晰的路径。
          </h2>
        </div>
        <div className="landing-intro-copy">
          <p>市场的变化很多，你的注意力有限。</p>
          <p>
            Sora
            是一个面向个人投资者的市场与基金观察工具。把地区、主题、基金和个人关注连接起来，让你从一个感兴趣的方向出发，逐步形成自己的认识。
          </p>
          <Link className="landing-inline-link" to="/discover">
            探索市场与主题 <span aria-hidden="true">→</span>
          </Link>
        </div>
      </section>

      <section
        className="landing-research"
        aria-labelledby="landing-research-title"
      >
        <div className="landing-research-copy">
          <p className="landing-eyebrow">SORA 如何工作</p>
          <h2 id="landing-research-title">
            从一个方向，
            <br />
            走进一只基金。
          </h2>
          <p className="landing-description">
            把一次好奇，变成有路径的探索。每一步，都能继续往下看。
          </p>
          <ol className="landing-steps">
            {[
              ['选一个观察方向', '从地区或投资主题切入，缩小需要了解的范围。'],
              [
                '查看基金的细节',
                '进入关联基金，了解净值、历史记录、计价币种与数据日期。',
              ],
              [
                '留下值得继续看的基金',
                '加入关注列表，下次回来，继续自己的观察。',
              ],
            ].map(([title, description], index) => (
              <li key={title}>
                <span className="landing-step-number" aria-hidden="true">
                  0{index + 1}
                </span>
                <div>
                  <h3>{title}</h3>
                  <p>{description}</p>
                </div>
              </li>
            ))}
          </ol>
        </div>
        <MarketPreview data={data} />
      </section>

      <section
        className="landing-continuity"
        aria-labelledby="landing-continuity-title"
      >
        <div className="landing-continuity-top">
          <div>
            <p className="landing-eyebrow">每次回来，都有起点</p>
            <h2 id="landing-continuity-title">
              让关注积累下来。
              <br />
              让下一次浏览，
              <br />
              有迹可循。
            </h2>
            <p className="landing-description">
              值得了解的方向，可以慢慢看。把关心的基金留在一起，让每次浏览接上上一次的思考。
            </p>
            <Link className="landing-inline-link" to="/funds">
              寻找想关注的基金 <span aria-hidden="true">→</span>
            </Link>
          </div>
          <div className="landing-watch-preview" aria-label="基金观察路径">
            <div className="landing-watch-heading">
              <span>发现值得关注的基金</span>
              <span>WATCHLIST</span>
            </div>
            <p>给关心的方向，留一个位置。</p>
            <ul>
              {watchExamples.map((fund) => (
                <li key={fund.code}>
                  <span className="landing-watch-star" aria-hidden="true">
                    ✦
                  </span>
                  <div>
                    <strong>{fund.name}</strong>
                    <span>
                      {fund.market} · {fund.category}
                    </span>
                  </div>
                  <span className="landing-watch-code">{fund.currency}</span>
                </li>
              ))}
              {!watchExamples.length && (
                <li className="landing-watch-placeholder">
                  从基金详情添加关注，在关注页集中查看。
                </li>
              )}
            </ul>
            <p className="landing-watch-note">
              在基金详情选择关注，建立自己的清单。
            </p>
          </div>
        </div>
        <div className="landing-benefits">
          {[
            [
              '少一些反复查找',
              '关注的基金集中查看，把精力留给真正想了解的内容。',
            ],
            [
              '多一点前后参照',
              '查看基金的历史记录与日期，让单个数字有更多上下文。',
            ],
            [
              '连接自己的视角',
              '需要时再录入持仓，回看成本、估值与各币种下的分布。',
            ],
          ].map(([title, description]) => (
            <div key={title}>
              <h3>{title}</h3>
              <p>{description}</p>
            </div>
          ))}
        </div>
      </section>

      <section
        className="landing-principles"
        aria-labelledby="landing-principles-title"
      >
        <div className="landing-section-heading">
          <p className="landing-eyebrow">为什么选择 SORA</p>
          <h2 id="landing-principles-title">清晰，也体现在细节里。</h2>
          <p className="landing-description">
            从读懂一份数据，到保留自己的记录，使用的每一步都应该心中有数。
          </p>
        </div>
        <div className="landing-principles-grid">
          {[
            [
              '01 / 信息有上下文',
              '知道自己在看什么',
              '资料来源和计价币种一起呈现。缺少行情时明确显示暂无数据，让每个数字有清楚的依据。',
            ],
            [
              '02 / 记录由你掌握',
              '带走自己的关注与记录',
              '关注与持仓保存在当前浏览器，支持导出和导入备份。换设备前可以先备份，使用边界清楚可见。',
            ],
            [
              '03 / 按自己的节奏',
              '从浏览开始，逐步深入',
              '无需账户即可体验。先探索一个主题，再了解基金；想记录持仓时，再建立自己的组合。',
            ],
          ].map(([label, title, description]) => (
            <div key={title}>
              <p className="landing-principle-label">{label}</p>
              <h3>{title}</h3>
              <p>{description}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="landing-faq" aria-labelledby="landing-faq-title">
        <div>
          <p className="landing-eyebrow">开始之前</p>
          <h2 id="landing-faq-title">你可能还想了解。</h2>
          <p className="landing-description">关于如何开始的几个直接回答。</p>
        </div>
        <div className="landing-faq-list">
          {[
            [
              '现在可以查看真实行情吗？',
              '基金资料附有来源。行情尚未接入的方向和产品显示暂无数据，不用其他数据替代；有可用行情后会标明对应日期与口径。',
            ],
            [
              '需要先注册账户吗？',
              '无需注册即可浏览基金资料、关注观察方向和基金、记录持仓。登录与注册暂未开放，目前不提供账户或跨设备同步。',
            ],
            [
              '我的关注和持仓会保存在哪里？',
              '记录保存在当前浏览器与站点地址下。清除浏览器数据、更换设备或站点地址后，记录不会自动跟随；你可以在“备份与数据管理”中导出备份，再导入恢复。',
            ],
            [
              'Sora 可以帮我买卖基金吗？',
              'Sora 用于观察基金和整理个人记录。当前不提供基金交易、买卖指令或自动投资，录入持仓也不会发起任何交易。',
            ],
          ].map(([question, answer]) => (
            <details key={question}>
              <summary>
                {question}
                <span aria-hidden="true">+</span>
              </summary>
              <p>{answer}</p>
            </details>
          ))}
        </div>
      </section>

      <section className="landing-start" aria-labelledby="landing-start-title">
        <div>
          <p className="landing-eyebrow">从这里，继续探索</p>
          <h2 id="landing-start-title">从你关心的市场开始。</h2>
          <p className="landing-description">
            选一个地区或主题，看看它与哪些基金相连。
          </p>
        </div>
        <div className="landing-start-actions">
          <ActionLink to="/discover">探索市场</ActionLink>
          <Link className="landing-inline-link" to="/funds">
            浏览基金 <span aria-hidden="true">→</span>
          </Link>
          <p>从你关心的方向开始 · 无需注册</p>
        </div>
      </section>
    </div>
  );
}
